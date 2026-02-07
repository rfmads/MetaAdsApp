# services/posts_service.py

from __future__ import annotations

from datetime import datetime
from typing import Optional, Tuple, Dict

from logs.logger import logger
from db.db import query_dict, execute


# =========================================
# Helpers
# =========================================

def _split_effective_story(eosid: str) -> Tuple[Optional[int], Optional[str]]:
    """
    eosid format (most common): PAGEID_POSTID
    Example: 495785640817485_1646615599829806

    Returns:
      (page_id:int|None, post_id:str|None)
    """
    if not eosid or "_" not in eosid:
        return None, None
    page_part, post_part = eosid.split("_", 1)
    try:
        return int(page_part), post_part
    except Exception:
        return None, post_part


def _parse_meta_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """
    Parse Meta datetime string -> naive datetime (MySQL DATETIME).
    Example: "2026-02-02T12:34:56+0000" or ISO.
    """
    if not dt_str:
        return None
    try:
        # handle "Z"
        s = dt_str.replace("Z", "+00:00")
        # handle +0000 -> +00:00
        if len(s) >= 5 and (s.endswith("+0000") or s.endswith("-0000")):
            s = s[:-5] + s[-5:-2] + ":" + s[-2:]
        return datetime.fromisoformat(s).replace(tzinfo=None)
    except Exception:
        return None


# =========================================
# DB Upsert
# =========================================

def upsert_post(record: dict) -> None:
    """
    Upsert into posts table.
    Unique keys:
      - (page_id, post_id)
      - (platform, post_id)

    We update URL fields only if incoming values are non-empty.
    """
    sql = """
    INSERT INTO posts (
        page_id,
        post_id,
        media_type,
        instagram_permalink_url,
        thumbnail_url,
        created_time,
        platform,
        effective_object_story_id,
        ig_media_id,
        permalink_url,
        first_seen_at,
        last_seen_at
    ) VALUES (
        %(page_id)s,
        %(post_id)s,
        %(media_type)s,
        %(instagram_permalink_url)s,
        %(thumbnail_url)s,
        %(created_time)s,
        %(platform)s,
        %(effective_object_story_id)s,
        %(ig_media_id)s,
        %(permalink_url)s,
        NOW(),
        NOW()
    )
    ON DUPLICATE KEY UPDATE
        page_id = COALESCE(VALUES(page_id), page_id),
        media_type = COALESCE(VALUES(media_type), media_type),

        instagram_permalink_url =
          CASE
            WHEN VALUES(instagram_permalink_url) IS NOT NULL AND VALUES(instagram_permalink_url) <> ''
              THEN VALUES(instagram_permalink_url)
            ELSE instagram_permalink_url
          END,

        thumbnail_url =
          CASE
            WHEN VALUES(thumbnail_url) IS NOT NULL AND VALUES(thumbnail_url) <> ''
              THEN VALUES(thumbnail_url)
            ELSE thumbnail_url
          END,

        created_time = COALESCE(VALUES(created_time), created_time),
        effective_object_story_id = COALESCE(VALUES(effective_object_story_id), effective_object_story_id),
        ig_media_id = COALESCE(VALUES(ig_media_id), ig_media_id),

        permalink_url =
          CASE
            WHEN VALUES(permalink_url) IS NOT NULL AND VALUES(permalink_url) <> ''
              THEN VALUES(permalink_url)
            ELSE permalink_url
          END,

        last_seen_at = NOW(),
        updated_at = NOW();
    """
    execute(sql, record)


# =========================================
# Main Sync (DB-only, from creative_ads)
# =========================================

def sync_posts_from_creatives(hours: int = 24) -> Dict[str, int]:
    """
    Build posts from creative_ads (DB-only) for last X hours only.
    This DOES NOT call Meta posts endpoints (permissions issue).
    It uses creative_ads.last_seen_at as the "recent" signal.
    """
    logger.info(f"▶️ posts sync start (from creative_ads) last_hours={hours}")

    rows = query_dict(
        """
        SELECT DISTINCT
            ca.effective_object_story_id,
            ca.instagram_permalink_url,
            ca.thumbnail_url,
            ca.page_id
        FROM creative_ads ca
        WHERE ca.effective_object_story_id IS NOT NULL
          AND ca.page_id IS NOT NULL
          AND ca.last_seen_at >= NOW() - INTERVAL %(hours)s HOUR
        """,
        {"hours": hours},
    )

    saved = 0
    skipped = 0

    for r in rows:
        eosid = r.get("effective_object_story_id")
        if not eosid:
            skipped += 1
            continue

        page_id, post_id = _split_effective_story(eosid)
        if not page_id or not post_id:
            skipped += 1
            continue

        ig_url = r.get("instagram_permalink_url")
        thumb = r.get("thumbnail_url")

        record = {
            "page_id": page_id,
            "post_id": post_id,
            "media_type": None,                 # unknown (needs read engagement)
            "instagram_permalink_url": ig_url,  # may be null
            "thumbnail_url": thumb,
            "created_time": None,               # unknown (needs post read permission)
            "platform": "facebook",             # eosid is fb post id style
            "effective_object_story_id": eosid,
            "ig_media_id": None,
            "permalink_url": ig_url,            # keep same for now
        }

        try:
            upsert_post(record)
            saved += 1
        except Exception as e:
            skipped += 1
            logger.warning(f"⚠️ post skipped eosid={eosid}: {e}")

    logger.info(f"✅ posts sync done (last {hours}h). saved={saved} skipped={skipped}")
    return {"saved": saved, "skipped": skipped}


# Optional: alias if you previously used other name
def sync_posts_last_24h() -> Dict[str, int]:
    return sync_posts_from_creatives(hours=24)
