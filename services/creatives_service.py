# services/creatives_service.py

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient
from db.db import execute, query_dict


# =========================
# Meta fields
# =========================
# Pull ads directly under ad account with embedded creative
ADS_WITH_CREATIVE_FIELDS = (
    "id,name,"
    "creative{"
    "id,name,body,effective_object_story_id,instagram_permalink_url,link_url,"
    "thumbnail_url,video_id,object_story_spec"
    "}"
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _cutoff(days: int) -> datetime:
    return _utc_now() - timedelta(days=days)


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None or x == "":
            return None
        return int(x)
    except Exception:
        return None


def _json_or_none(x: Any):
    if x is None:
        return None
    try:
        if isinstance(x, (dict, list)):
            return json.dumps(x, ensure_ascii=False)
        if isinstance(x, str):
            try:
                json.loads(x)
                return x
            except Exception:
                return json.dumps({"raw": x}, ensure_ascii=False)
        return json.dumps(x, ensure_ascii=False)
    except Exception:
        return None


# =========================
# DB upserts
# =========================
def upsert_creative(record: dict) -> None:
    """
    Upsert into creative_ads (your table).
    """
    sql = """
    INSERT INTO creative_ads (
        creative_id,
        name,
        body,
        effective_object_story_id,
        instagram_permalink_url,
        link_url,
        page_id,
        thumbnail_url,
        video_id,
        creative_sourcing_spec,
        first_seen_at,
        last_seen_at
    ) VALUES (
        %(creative_id)s,
        %(name)s,
        %(body)s,
        %(effective_object_story_id)s,
        %(instagram_permalink_url)s,
        %(link_url)s,
        %(page_id)s,
        %(thumbnail_url)s,
        %(video_id)s,
        %(creative_sourcing_spec)s,
        NOW(),
        NOW()
    )
    ON DUPLICATE KEY UPDATE
        name = COALESCE(VALUES(name), name),
        body = COALESCE(VALUES(body), body),
        effective_object_story_id = COALESCE(VALUES(effective_object_story_id), effective_object_story_id),
        instagram_permalink_url = COALESCE(VALUES(instagram_permalink_url), instagram_permalink_url),
        link_url = COALESCE(VALUES(link_url), link_url),
        page_id = COALESCE(VALUES(page_id), page_id),
        thumbnail_url = COALESCE(VALUES(thumbnail_url), thumbnail_url),
        video_id = COALESCE(VALUES(video_id), video_id),
        creative_sourcing_spec = COALESCE(VALUES(creative_sourcing_spec), creative_sourcing_spec),
        last_seen_at = NOW(),
        updated_at = NOW();
    """
    execute(sql, record)


def update_ad_with_creative(
    ad_id: int,
    creative_id: Optional[int],
    effective_object_story_id: Optional[str],
    thumbnail_url: Optional[str],
    link_url: Optional[str],
    instagram_permalink_url: Optional[str],
) -> None:
    """
    Update ads table:
      - creative_id
      - post_id: we store effective_object_story_id (PAGEID_POSTID)
      - thumbnail_url
      - post_link: prefer instagram_permalink_url if exists else link_url
    """
    post_link = instagram_permalink_url or link_url

    sql = """
    UPDATE ads
    SET
        creative_id = %(creative_id)s,
        post_id = COALESCE(%(post_id)s, post_id),
        thumbnail_url = COALESCE(%(thumbnail_url)s, thumbnail_url),
        post_link = COALESCE(%(post_link)s, post_link),
        updated_at = NOW()
    WHERE ad_id = %(ad_id)s
    """
    execute(sql, {
        "ad_id": ad_id,
        "creative_id": creative_id,
        "post_id": effective_object_story_id,
        "thumbnail_url": thumbnail_url,
        "post_link": post_link,
    })


# =========================
# Service
# =========================
ADS_WITH_CREATIVE_FIELDS = (
    "id,name,effective_status,"
    "creative{"
    "id,name,body,effective_object_story_id,instagram_permalink_url,link_url,"
    "thumbnail_url,video_id,object_story_spec"
    "}"
)
def sync_creatives_for_account(
    client, 
    ad_account_id: int,
    mode: str = "incremental",
    days: int = 30,
) -> Dict[str, int]:
    act = f"act_{ad_account_id}"
    
    # Meta's 'ad.updated_time' filter requires a UNIX timestamp (seconds)
    cutoff_ts = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())

    # 1. Initialize filters list
    # Note: We remove the 'ACTIVE' filter here to ensure we get "all data" as you requested
    filters = []
    
    # 2. Add time filter for incremental speed
    if mode == "incremental":
        filters.append({
            "field": "ad.updated_time", 
            "operator": "GREATER_THAN", 
            "value": cutoff_ts
        })

    # 3. Correctly structure the params
    params = {
        "fields": ADS_WITH_CREATIVE_FIELDS, 
        "limit": 200, # Creative objects are heavy; 100 is safer than 250
        "filtering": json.dumps(filters) if filters else None
    }

    saved, skipped = 0, 0
    logger.info(f"▶️ creatives sync start {act} mode={mode}")

    try:
        for ad in client.get_paged(f"{act}/ads", params=params):
            ad_id = int(ad["id"])
            creative = ad.get("creative")
            
            if not creative or not creative.get("id"):
                skipped += 1
                continue

            cr_id = int(creative["id"])
            eosid = creative.get("effective_object_story_id")
            
            # Extract Page ID
            page_id = None
            if eosid and "_" in str(eosid):
                page_id = str(eosid).split("_")[0]

            # 4. Upsert the Creative Metadata
            upsert_creative({
                "creative_id": cr_id,
                "name": creative.get("name"),
                "body": creative.get("body"),
                "effective_object_story_id": eosid,
                "instagram_permalink_url": creative.get("instagram_permalink_url"),
                "link_url": creative.get("link_url"),
                "page_id": page_id,
                "thumbnail_url": creative.get("thumbnail_url"),
                "video_id": creative.get("video_id"),
                "creative_sourcing_spec": json.dumps(creative.get("object_story_spec")) if creative.get("object_story_spec") else None,
            })

            # 5. Link the Ad to the Creative
            update_ad_with_creative(
                ad_id=ad_id,
                creative_id=cr_id,
                effective_object_story_id=eosid,
                thumbnail_url=creative.get("thumbnail_url"),
                link_url=creative.get("link_url"),
                instagram_permalink_url=creative.get("instagram_permalink_url"),
            )
            saved += 1
            if saved % 50 == 0:
                logger.info(f"⏳ {act} progress: {saved} creatives saved...")

        return {"saved": saved, "skipped": skipped}

    except Exception as e:
        # Fallback logic for filtering errors
        if "filtering" in str(e).lower() and mode == "incremental":
            logger.warning(f"⚠️ Filtering rejected for {act}, falling back to full.")
            return sync_creatives_for_account(client, ad_account_id, mode="full", days=days)
        
        logger.error(f"❌ creatives failed {act}: {e}")
        raise e


def sync_creatives(user_token: str, mode: str = "incremental", days: int = 30) -> Dict[str, int]:
    """
    Non-threaded wrapper (like others).
    """
    accounts = query_dict("""
        SELECT a.ad_account_id, p.code AS portfolio_code
        FROM ad_accounts a
        JOIN portfolios p ON p.id = a.portfolio_id
        WHERE p.code IN ('RFM','MAGIC_EXTREME')
        ORDER BY p.code, a.ad_account_id
    """)

    total_saved = 0
    total_skipped = 0
    total_failed = 0

    for r in accounts:
        res = sync_creatives_for_account(
            user_token=user_token,
            ad_account_id=int(r["ad_account_id"]),
            portfolio_code=r["portfolio_code"],
            mode=mode,
            days=days,
        )
        total_saved += res.get("saved", 0)
        total_skipped += res.get("skipped", 0)
        total_failed += res.get("failed", 0)

    logger.info(f"✅ creatives sync done (all accounts) saved={total_saved} skipped={total_skipped} failed={total_failed}")
    return {"saved": total_saved, "skipped": total_skipped, "failed": total_failed}
