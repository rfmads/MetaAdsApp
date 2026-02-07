# services/instagram_posts_service.py

from datetime import datetime, timezone, timedelta
from typing import Optional

from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient
from db.db import query_dict
from db.repositories.posts_repo import upsert_post
from utils.datetime_utils import parse_meta_datetime, to_mysql_naive_utc


# Instagram Graph endpoint:
# /{ig_user_id}/media?fields=...&since=...&limit=...
# Notes:
# - "timestamp" is returned for IG media and is ISO string with timezone.
# - permalink exists for IG.
# - media_url exists (IMAGE/VIDEO), thumbnail_url exists (VIDEO), for CAROUSEL use children.
IG_MEDIA_FIELDS = ",".join([
    "id",
    "caption",
    "media_type",
    "media_url",
    "thumbnail_url",
    "permalink",
    "timestamp"
])


def _normalize_media_type(raw: Optional[str]) -> Optional[str]:
    """
    Map IG media_type to your posts.media_type enum:
    IMAGE, VIDEO, REEL, STORY
    """
    if not raw:
        return None

    raw = raw.upper().strip()

    # IG possible: IMAGE, VIDEO, CAROUSEL_ALBUM, REELS, STORY (depends)
    if raw == "IMAGE":
        return "IMAGE"
    if raw == "VIDEO":
        return "VIDEO"
    if raw in ("REELS", "REEL"):
        return "REEL"
    if raw == "STORY":
        return "STORY"

    # CAROUSEL_ALBUM: treat as IMAGE (or keep None)
    if raw == "CAROUSEL_ALBUM":
        return "IMAGE"

    return None


def sync_instagram_posts_last_60_days(user_token: str) -> None:
    """
    Sync Instagram posts for all pages that have ig_user_id.
    Inserts/updates into the SAME 'posts' table with platform='instagram'.

    Important:
    - Uses UTC-aware datetime for comparisons.
    - Converts to MySQL naive UTC before saving (DATETIME columns).
    """
    logger.info("Starting Instagram posts sync (last 60 days)...")

    pages = query_dict("""
        SELECT page_id, ig_user_id
        FROM pages
        WHERE ig_user_id IS NOT NULL AND ig_user_id <> ''
    """)

    logger.info(f"Syncing Instagram posts for pages={len(pages)}")

    since_dt_utc = datetime.now(timezone.utc) - timedelta(days=60)
    # IG "since" can be unix timestamp
    since_ts = int(since_dt_utc.timestamp())

    total = 0
    pages_failed = 0

    client = MetaGraphClient(user_token)

    for p in pages:
        page_id = int(p["page_id"])
        ig_user_id = str(p["ig_user_id"]).strip()

        try:
            # paging handled by client.get_paged
            for item in client.get_paged(
                f"{ig_user_id}/media",
                params={
                    "fields": IG_MEDIA_FIELDS,
                    "since": since_ts,
                    "limit": 100
                }
            ):
                ig_media_id = item.get("id")
                if not ig_media_id:
                    continue

                created_dt = parse_meta_datetime(item.get("timestamp"))
                # Compare aware vs aware (UTC)
                if created_dt and created_dt < since_dt_utc:
                    # In practice, "since" already filters, but keep safe
                    continue

                record = {
                    "page_id": page_id,

                    # IMPORTANT:
                    # post_id should be UNIQUE across table; for IG we store ig_media_id in post_id
                    "post_id": str(ig_media_id),

                    "media_type": _normalize_media_type(item.get("media_type")),

                    # IG direct permalink
                    "instagram_permalink_url": item.get("permalink"),
                    "permalink_url": item.get("permalink"),

                    # IG media urls
                    "thumbnail_url": item.get("thumbnail_url") or item.get("media_url"),

                    # store MySQL naive UTC datetime
                    "created_time": to_mysql_naive_utc(created_dt),

                    "platform": "instagram",

                    # FB specific fields remain NULL
                    "effective_object_story_id": None,

                    # keep ig_media_id too (optional but useful)
                    "ig_media_id": str(ig_media_id),
                }

                upsert_post(record)
                total += 1

            logger.info(f"✅ Instagram posts synced for page_id={page_id} ig_user_id={ig_user_id}")

        except Exception as e:
            pages_failed += 1
            logger.error(f"⚠️ Instagram posts failed for ig_user_id={ig_user_id} (page_id={page_id}): {e}")

    logger.info(f"Instagram posts sync done. inserted/updated={total}, pages_failed={pages_failed}")
