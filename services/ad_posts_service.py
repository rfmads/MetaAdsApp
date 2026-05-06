

# # services/ad_posts_service.py
from typing import Dict, Optional, Any
from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient
from db.db import query_one, execute

AD_FIELDS = "id,creative{effective_object_story_id,instagram_permalink_url}"

def _safe_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None

def _find_post_by_story_id(story_id: str) -> Optional[int]:
    row = query_one(
        "SELECT id FROM posts WHERE effective_object_story_id = %s LIMIT 1",
        (story_id,),
    )
    return int(row["id"]) if row and row.get("id") else None

def _find_post_by_instagram_permalink(url: str) -> Optional[int]:
    row = query_one(
        "SELECT id FROM posts WHERE instagram_permalink_url = %s LIMIT 1",
        (url,),
    )
    return int(row["id"]) if row and row.get("id") else None

def _resolve_post(post_value: Optional[str]) -> tuple[Optional[int], Optional[str]]:
    if not post_value:
        return None, None
    
    pid = _safe_str(post_value)
    if not pid:
        return None, None

    # Check Facebook Story ID format (usually contains an underscore)
    if "_" in pid:
        found_id = _find_post_by_story_id(pid)
        if found_id:
            return found_id, "facebook_story"

    # Check Instagram Permalink format
    if pid.startswith("http"):
        found_id = _find_post_by_instagram_permalink(pid)
        if found_id:
            return found_id, "instagram_permalink"

    return None, None

def _upsert_ad_post(ad_id: int, post_row_id: int, link_type: str) -> None:
    execute(
        """
        INSERT INTO ad_posts (ad_id, post_row_id, link_type)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            post_row_id = VALUES(post_row_id),
            link_type   = VALUES(link_type),
            updated_at  = NOW()
        """,
        (ad_id, post_row_id, link_type),
    )

def _fetch_creative_post_data(client: MetaGraphClient, ad_id: int):
    try:
        ad_data = client.get(str(ad_id), params={"fields": AD_FIELDS})
        if not ad_data:
            return None
            
        creative = ad_data.get("creative", {})
        return {
            "effective_story_id": _safe_str(creative.get("effective_object_story_id")),
            "instagram_permalink": _safe_str(creative.get("instagram_permalink_url")),
        }
    except Exception as e:
        logger.warning(f"Meta API creative fetch failed (Ad ID {ad_id}): {e}")
        return None