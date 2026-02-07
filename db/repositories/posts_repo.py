# db/repositories/posts_repo.py
from db.db import execute

def upsert_post(r: dict) -> None:
    """
    posts has UNIQUE(post_id) and UNIQUE(page_id, post_id)
    We keep first_seen_at on insert, update last_seen_at each refresh.
    """
    sql = """
    INSERT INTO posts (
        page_id, post_id, media_type,
        instagram_permalink_url, permalink_url,
        thumbnail_url, created_time, platform,
        effective_object_story_id, ig_media_id,
        first_seen_at, last_seen_at, created_at, updated_at
    ) VALUES (
        %(page_id)s, %(post_id)s, %(media_type)s,
        %(instagram_permalink_url)s, %(permalink_url)s,
        %(thumbnail_url)s, %(created_time)s, %(platform)s,
        %(effective_object_story_id)s, %(ig_media_id)s,
        NOW(), NOW(), NOW(), NOW()
    )
    ON DUPLICATE KEY UPDATE
        media_type=COALESCE(VALUES(media_type), media_type),
        instagram_permalink_url=COALESCE(VALUES(instagram_permalink_url), instagram_permalink_url),
        permalink_url=COALESCE(VALUES(permalink_url), permalink_url),
        thumbnail_url=COALESCE(VALUES(thumbnail_url), thumbnail_url),
        created_time=COALESCE(VALUES(created_time), created_time),
        platform=COALESCE(VALUES(platform), platform),
        effective_object_story_id=COALESCE(VALUES(effective_object_story_id), effective_object_story_id),
        ig_media_id=COALESCE(VALUES(ig_media_id), ig_media_id),
        last_seen_at=NOW(),
        updated_at=NOW();
    """
    execute(sql, r)
