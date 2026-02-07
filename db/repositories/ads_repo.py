# db/repositories/ads_repo.py
from db.db import execute

def upsert_ad(r: dict) -> None:
    sql = """
    INSERT INTO ads (
        ad_id, adset_id, campaign_id,
        name, status, effective_status,
        thumbnail_url, image_url,
        updated_at, first_seen_at, last_seen_at
    )
    VALUES (
        %(ad_id)s, %(adset_id)s, %(campaign_id)s,
        %(name)s, %(status)s, %(effective_status)s,
        %(thumbnail_url)s, %(image_url)s,
        NOW(), NOW(), NOW()
    )
    ON DUPLICATE KEY UPDATE
        name=COALESCE(VALUES(name), name),
        status=COALESCE(VALUES(status), status),
        effective_status=COALESCE(VALUES(effective_status), effective_status),
        thumbnail_url=COALESCE(VALUES(thumbnail_url), thumbnail_url),
        image_url=COALESCE(VALUES(image_url), image_url),
        last_seen_at=NOW(),
        updated_at=NOW();
    """
    execute(sql, r)
