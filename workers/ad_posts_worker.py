import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from logs.logger import logger
from db.db import query_dict, execute
from db.config_store import get_config
from services.ad_posts_service import (
    _resolve_post,
    _fetch_creative_post_data,
    _find_post_by_story_id,
    _find_post_by_instagram_permalink,
)
from integrations.meta_graph_client import MetaGraphClient
from services.job_service import heartbeat

def _job(user_token: str, creative_id: str, ad_ids: list) -> dict:
    """
    Resolves ONE creative and applies the result to ALL ads using it.
    """
    client = MetaGraphClient(user_token)
    try:
        # 1. Fetch creative data once
        creative = _fetch_creative_post_data(client, creative_id)
        if not creative:
            return {"ok": False, "count": len(ad_ids)}

        post_row_id = None
        link_type = None

        if creative.get("effective_story_id"):
            post_row_id = _find_post_by_story_id(creative["effective_story_id"])
            link_type = "facebook_story"
        
        if not post_row_id and creative.get("instagram_permalink"):
            post_row_id = _find_post_by_instagram_permalink(creative["instagram_permalink"])
            link_type = "instagram_permalink"

        if post_row_id:
            # 2. Bulk Update all ads that share this creative
            format_strings = ','.join(['%s'] * len(ad_ids))
            execute(f"""
                INSERT INTO ad_posts (ad_id, post_row_id, link_type)
                SELECT ad_id, %s, %s FROM ads WHERE ad_id IN ({format_strings})
                ON DUPLICATE KEY UPDATE 
                    post_row_id = VALUES(post_row_id), 
                    link_type = VALUES(link_type),
                    updated_at = NOW()
            """, (post_row_id, link_type, *ad_ids))
            
            return {"ok": True, "count": len(ad_ids)}
        
        return {"ok": False, "type": "skipped", "count": len(ad_ids)}

    except Exception as e:
        logger.error(f"❌ Creative {creative_id} failed: {e}")
        return {"ok": False, "type": "error", "count": len(ad_ids)}

def _batch_job(user_token: str, creative_batch: list, groups: dict):
    client = MetaGraphClient(user_token)
    
    # 🌟 FIX: Convert each ID to a string before joining
    ids_str = ",".join(map(str, creative_batch))
    
    try:
        # One API call for up to 50 creatives!
        batch_data = client.get("", params={
            "ids": ids_str, 
            "fields": "id,effective_object_story_id,instagram_permalink_url"
        })

        if not batch_data: 
            return 0

        linked_count = 0
        for cid, data in batch_data.items():
            # Meta returns keys as strings, but your 'groups' dictionary 
            # might have integer keys depending on your DB driver.
            # We normalize to ensure we find the right group.
            lookup_id = int(cid) if isinstance(list(groups.keys())[0], int) else cid
            
            story_id = data.get("effective_object_story_id")
            ig_url = data.get("instagram_permalink_url")
            
            post_row_id = None
            link_type = None

            if story_id:
                post_row_id = _find_post_by_story_id(story_id)
                link_type = "facebook_story"
            
            if not post_row_id and ig_url:
                post_row_id = _find_post_by_instagram_permalink(ig_url)
                link_type = "instagram_permalink"

            if post_row_id:
                ad_ids = groups.get(lookup_id, [])
                if not ad_ids:
                    continue
                    
                format_strings = ','.join(['%s'] * len(ad_ids))
                execute(f"""
                    INSERT INTO ad_posts (ad_id, post_row_id, link_type)
                    SELECT ad_id, %s, %s FROM ads WHERE ad_id IN ({format_strings})
                    ON DUPLICATE KEY UPDATE 
                        post_row_id=VALUES(post_row_id), 
                        link_type=VALUES(link_type), 
                        updated_at=NOW()
                """, (post_row_id, link_type, *ad_ids))
                linked_count += len(ad_ids)
        
        return linked_count
    except Exception as e:
        logger.error(f"❌ Batch failed: {e}")
        return 0

def run(job_id=None):
    try:
        user_token = get_config("META_USER_TOKEN")
        workers = 5 
        
        ads_to_sync = query_dict("""
            SELECT a.ad_id, a.creative_id
            FROM ads a
            LEFT JOIN ad_posts ap ON a.ad_id = ap.ad_id
            WHERE ap.ad_id IS NULL AND a.creative_id IS NOT NULL
        """)

        if not ads_to_sync:
            logger.info("✅ No ads to sync.")
            return {"ok": True, "linked_ads": 0}

        groups = {}
        for row in ads_to_sync:
            groups.setdefault(row['creative_id'], []).append(row['ad_id'])

        creatives = list(groups.keys())
        total_creatives = len(creatives)
        
        batch_size = 50
        batches = [creatives[i:i + batch_size] for i in range(0, len(creatives), batch_size)]
        
        logger.info(f"🚀 Processing {len(batches)} batches ({total_creatives} creatives).")

        success_ads = 0
        
        # We use a context manager for the executor
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(_batch_job, user_token, batch, groups) for batch in batches]
            for i, f in enumerate(as_completed(futures)):
                # .result() will raise an exception here if the thread crashed
                success_ads += f.result() 
                
                if i % 5 == 0:
                    logger.info(f"⏳ Progress: {round((i/len(batches))*100, 2)}% | Ads Linked: {success_ads}")
                    if job_id: heartbeat(job_id)

        return {"ok": True, "linked_ads": success_ads}

    except Exception as e:
        # 🌟 THIS IS THE KEY CHANGE
        # If anything above fails, we return ok: False so the pipeline marks it FAILED
        logger.error(f"❌ ad_posts_worker failed: {e}")
        return {"ok": False, "error": str(e)}

if __name__ == "__main__":
    run()