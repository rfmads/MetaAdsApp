# services/creative_ads_service.py
import json
from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient
from db.db import query_dict
from db.repositories.creative_ads_repo import upsert_creative_ad

# Request 1 (ad -> creative summary)
AD_FIELDS = (
    "id,name,status,"
    "creative{id,thumbnail_url,image_url,object_story_id,video_id,object_story_spec}"
)

# Request 2 (creative details)
CREATIVE_FIELDS = (
    "body,title,effective_object_story_id,name,link_url,"
    "creative_sourcing_spec,instagram_permalink_url"
)


def _safe_get(d, path, default=None):
    cur = d
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def _page_exists(page_id: int) -> bool:
    rows = query_dict("SELECT 1 FROM pages WHERE page_id=%s LIMIT 1", (page_id,))
    return bool(rows)


def sync_creatives_from_ads(user_token: str) -> None:
    client = MetaGraphClient(user_token)

    ads = query_dict("SELECT ad_id FROM ads")
    logger.info(f"Syncing creative_ads from ads={len(ads)}")

    saved = 0
    skipped = 0
    failed = 0

    for row in ads:
        ad_id = row["ad_id"]

        try:
            # 1) get ad -> creative summary
            ad_data = client.get(str(ad_id), params={"fields": AD_FIELDS})
            creative = ad_data.get("creative")

            if not creative or not creative.get("id"):
                skipped += 1
                continue

            creative_id = str(creative["id"])

            # 2) get creative details
            creative_data = client.get(creative_id, params={"fields": CREATIVE_FIELDS})

            obj_spec = creative.get("object_story_spec") or {}
            page_id = _safe_get(obj_spec, ["page_id"])

            # ✅ FK Safe: only store page_id if it exists in pages table
            page_id_safe = None
            if page_id:
                pid = int(page_id)
                if _page_exists(pid):
                    page_id_safe = pid

            link_url = creative_data.get("link_url") or _safe_get(obj_spec, ["link_data", "link"])

            # ✅ Convert dict -> JSON string for MySQL JSON column
            spec = creative_data.get("creative_sourcing_spec")
            spec_json = json.dumps(spec, ensure_ascii=False) if isinstance(spec, dict) else spec

            record = {
                "creative_id": int(creative_id),
                "name": creative_data.get("name") or ad_data.get("name"),
                "body": creative_data.get("body") or _safe_get(obj_spec, ["link_data", "message"]),
                "effective_object_story_id": (
                    creative_data.get("effective_object_story_id")
                    or creative.get("object_story_id")
                ),
                "instagram_permalink_url": creative_data.get("instagram_permalink_url"),
                "link_url": link_url,
                "page_id": page_id_safe,
                "thumbnail_url": creative.get("thumbnail_url"),
                "video_id": int(creative.get("video_id")) if creative.get("video_id") else None,
                "creative_sourcing_spec": spec_json,
            }

            upsert_creative_ad(record)
            saved += 1

        except Exception as e:
            failed += 1
            logger.error(f"⚠️ creative sync failed for ad_id={ad_id}: {e}")

    logger.info(f"✅ creative_ads sync done. saved={saved}, skipped={skipped}, failed={failed}")
  