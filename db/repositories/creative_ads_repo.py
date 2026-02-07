# db/repositories/creative_ads_repo.py
from db.db import execute


def upsert_creative_ad(r: dict) -> None:
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
    )
    VALUES (
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
        name=VALUES(name),
        body=VALUES(body),
        effective_object_story_id=VALUES(effective_object_story_id),
        instagram_permalink_url=VALUES(instagram_permalink_url),
        link_url=VALUES(link_url),
        page_id=VALUES(page_id),
        thumbnail_url=COALESCE(VALUES(thumbnail_url), thumbnail_url),
        video_id=COALESCE(VALUES(video_id), video_id),
        creative_sourcing_spec=VALUES(creative_sourcing_spec),
        last_seen_at=NOW();
    """
    execute(sql, r)
