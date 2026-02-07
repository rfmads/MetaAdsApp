# services/pages_posts_service.py

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient
from db.db import execute


# =========================
# Helpers
# =========================

def _parse_iso_dt(dt_str: Optional[str]) -> Optional[datetime]:
    """
    Parse ISO datetime -> naive datetime (MySQL DATETIME).
    Supports:
      - 2026-02-04T10:12:33+0000
      - 2026-02-04T10:12:33+00:00
      - 2026-02-04T10:12:33Z
    """
    if not dt_str:
        return None
    try:
        s = dt_str.replace("Z", "+00:00")
        # sometimes Meta returns +0000 (no colon)
        if len(s) >= 5 and (s[-5] in ["+", "-"]) and s[-2:] != ":00" and s[-3] != ":":
            # convert +0000 -> +00:00
            s = s[:-2] + ":" + s[-2:]
        dt = datetime.fromisoformat(s)
        return dt.replace(tzinfo=None)
    except Exception:
        return None


def _parse_ig_timestamp(ts: Optional[str]) -> Optional[datetime]:
    # IG timestamp: 2026-02-04T12:34:56+0000 or +00:00
    return _parse_iso_dt(ts)


def _normalize_media_type(raw: Optional[str]) -> Optional[str]:
    """
    posts.media_type enum: IMAGE, VIDEO, REEL, STORY
    FB: may return photo/video/link/status etc.
    IG: IMAGE, VIDEO, CAROUSEL_ALBUM, REEL
    """
    if not raw:
        return None
    r = raw.upper()

    if r in ("REEL",):
        return "REEL"
    if r in ("STORY",):
        return "STORY"
    if r in ("VIDEO", "IGTV"):
        return "VIDEO"
    if r in ("IMAGE", "PHOTO"):
        return "IMAGE"
    if r in ("CAROUSEL_ALBUM",):
        # اخترنا نعتبرها IMAGE (أو VIDEO) — خلّينا IMAGE
        return "IMAGE"
    return None


# =========================
# DB Upsert
# =========================

def upsert_post(record: dict) -> None:
    """
    posts UNIQUE:
      - uq_posts_platform_post (platform, post_id)
      - uq_pages_page_post (page_id, post_id)
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
        instagram_permalink_url = COALESCE(VALUES(instagram_permalink_url), instagram_permalink_url),
        thumbnail_url = COALESCE(VALUES(thumbnail_url), thumbnail_url),
        created_time = COALESCE(VALUES(created_time), created_time),
        effective_object_story_id = COALESCE(VALUES(effective_object_story_id), effective_object_story_id),
        ig_media_id = COALESCE(VALUES(ig_media_id), ig_media_id),
        permalink_url = COALESCE(VALUES(permalink_url), permalink_url),
        last_seen_at = NOW();
    """
    execute(sql, record)


# =========================
# Facebook Posts (page feed)
# =========================

FB_FIELDS = "id,message,created_time,permalink_url,attachments{media_type,media,subattachments}"


def sync_facebook_posts_last_hours(
    user_token: str,
    page_id: int,
    page_access_token: Optional[str],
    hours: int = 24,
    limit: int = 50,
) -> Dict[str, int]:
    """
    Fetch FB posts for last X hours from:
      /{page_id}/posts
    """
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=hours)

    token = page_access_token or user_token
    client = MetaGraphClient(token)

    saved = 0
    skipped_old = 0

    logger.info(f"🔵 FB posts sync start page={page_id} hours={hours}")

    try:
        for post in client.get_paged(
            f"{page_id}/posts",
            params={"fields": FB_FIELDS, "limit": limit},
        ):
            created = _parse_iso_dt(post.get("created_time"))
            if not created:
                skipped_old += 1
                continue

            # created is naive but represents UTC-ish; compare using naive cutoff
            if created < cutoff.replace(tzinfo=None):
                skipped_old += 1
                continue

            # Try to extract media_type + thumbnail from attachments
            media_type = None
            thumbnail_url = None
            att = (post.get("attachments") or {}).get("data") or []
            if att:
                media_type = _normalize_media_type(att[0].get("media_type"))
                media = att[0].get("media") or {}
                # media can have image src sometimes:
                if isinstance(media, dict):
                    thumbnail_url = (media.get("image") or {}).get("src") or media.get("source")

            record = {
                "page_id": int(page_id),
                "post_id": str(post.get("id")),
                "media_type": media_type,
                "instagram_permalink_url": None,
                "thumbnail_url": thumbnail_url,
                "created_time": created,
                "platform": "facebook",
                "effective_object_story_id": str(post.get("id")),  # نفس id
                "ig_media_id": None,
                "permalink_url": post.get("permalink_url"),
            }

            upsert_post(record)
            saved += 1

        logger.info(f"✅ FB posts sync done page={page_id} saved={saved} skipped_old={skipped_old}")
        return {"saved": saved, "skipped_old": skipped_old}

    except Exception as e:
        logger.error(f"❌ FB posts sync failed page={page_id}: {e}")
        return {"saved": saved, "skipped_old": skipped_old, "error": str(e)}


# =========================
# Instagram Posts (IG media)
# =========================

IG_FIELDS = "id,caption,media_type,media_url,thumbnail_url,permalink,timestamp"


def sync_instagram_posts_last_hours(
    user_token: str,
    page_id: int,
    ig_user_id: int,
    page_access_token: Optional[str],
    hours: int = 24,
    limit: int = 50,
    buffer_minutes: int = 5,
) -> Dict[str, int]:
    """
    Fetch IG posts from:
      /{ig_user_id}/media
    Then filter by timestamp last X hours.
    """
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=hours)
    cutoff_naive = cutoff.replace(tzinfo=None)

    # using page token is best
    token = page_access_token or user_token
    client = MetaGraphClient(token)

    saved = 0
    skipped_old = 0

    logger.info(f"🟣 IG posts sync start page={page_id} ig_user_id={ig_user_id} hours={hours}")

    try:
        # bring recent items, then filter
        for m in client.get_paged(
            f"{ig_user_id}/media",
            params={"fields": IG_FIELDS, "limit": limit},
        ):
            created = _parse_ig_timestamp(m.get("timestamp"))
            if not created:
                skipped_old += 1
                continue

            if created < cutoff_naive:
                skipped_old += 1
                continue

            mt = _normalize_media_type(m.get("media_type"))

            record = {
                "page_id": int(page_id),
                "post_id": str(m.get("id")),             # نخزن IG media id هنا
                "media_type": mt,
                "instagram_permalink_url": m.get("permalink"),
                "thumbnail_url": m.get("thumbnail_url") or m.get("media_url"),
                "created_time": created,
                "platform": "instagram",
                "effective_object_story_id": None,
                "ig_media_id": str(m.get("id")),
                "permalink_url": m.get("permalink"),
            }

            upsert_post(record)
            saved += 1

        logger.info(f"✅ IG posts sync done page={page_id} saved={saved} skipped_old={skipped_old}")
        return {"saved": saved, "skipped_old": skipped_old}

    except Exception as e:
        logger.error(f"❌ IG posts sync failed page={page_id} ig_user_id={ig_user_id}: {e}")
        return {"saved": saved, "skipped_old": skipped_old, "error": str(e)}
