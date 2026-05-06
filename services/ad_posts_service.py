from typing import Dict, Optional, Any
from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient
from db.db import query_one, execute

# We query the creative directly, so we don't need the 'creative{}' wrapper anymore
CREATIVE_FIELDS = "id,effective_object_story_id,instagram_permalink_url"

def _safe_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None

def _find_post_by_story_id(story_id: str) -> Optional[int]:
    logger.warning(f"({story_id}): Story ID is empty or None")
    if not story_id:
        return None
     
    # 1. Try exact match (e.g., "12345_678910")
    row = query_one(
        "SELECT id FROM posts WHERE effective_object_story_id = %s and effective_object_story_id IS NOT NULL LIMIT 1",
        (story_id,),
    )
    if row:
        return int(row["id"])

    # 2. Try the second part (The actual Post ID)
    if "_" in story_id:
        parts = story_id.split("_")
        if len(parts) > 1:
            post_id_only = parts[1] # ✅ FIXED: Get the second part
            row = query_one(
                "SELECT id FROM posts WHERE effective_object_story_id = %s and effective_object_story_id IS NOT NULL LIMIT 1",
                (post_id_only,),
            )
            if row:
                return int(row["id"])

    # 3. Try a LIKE match (in case DB has PageID_PostID but Meta sent just PostID)
    row = query_one(
        "SELECT id FROM posts WHERE effective_object_story_id LIKE %s and effective_object_story_id IS NOT NULL LIMIT 1",
        (f"%_{story_id}",),
    )
    if row:
        return int(row["id"])

    return None
def _find_post_by_instagram_permalink(url: str) -> Optional[int]:
    row = query_one(
        "SELECT id FROM posts WHERE instagram_permalink_url = %s and instagram_permalink_url IS NOT NULL LIMIT 1",
        (url,),
    )
    return int(row["id"]) if row and row.get("id") else None

def _resolve_post(post_value: Optional[str]) -> tuple[Optional[int], Optional[str]]:
    if not post_value:
        return None, None
    
    pid = _safe_str(post_value)
    if not pid:
        return None, None

    if "_" in pid:
        found_id = _find_post_by_story_id(pid)
        if found_id:
            return found_id, "facebook_story"

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

def _fetch_creative_post_data(client: MetaGraphClient, creative_id: Any):
    """
    Fetched data for a CREATIVE ID (not an Ad ID).
    """
    try:
        # Querying the creative_id directly
        data = client.get(str(creative_id), params={"fields": CREATIVE_FIELDS})
        if not data:
            return None
            
        # The fields are now at the top level of 'data'
        return {
            "effective_story_id": _safe_str(data.get("effective_object_story_id")),
            "instagram_permalink": _safe_str(data.get("instagram_permalink_url")),
        }
    except Exception as e:
        logger.warning(f"Meta API creative fetch failed (Creative ID {creative_id}): {e}")
        return None