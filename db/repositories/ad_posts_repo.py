# db/repositories/ad_posts_repo.py
from db.db import execute

def upsert_ad_post(r: dict) -> None:
    """
    ad_posts has UNIQUE(ad_id)
    """
    sql = """
    INSERT INTO ad_posts (
        ad_id,
        post_row_id,
        link_type,
        created_at
    ) VALUES (
        %(ad_id)s,
        %(post_row_id)s,
        %(link_type)s,
        NOW()
    )
    ON DUPLICATE KEY UPDATE
        post_row_id = VALUES(post_row_id),
        link_type = VALUES(link_type);
    """
    execute(sql, r)
