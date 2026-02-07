# db/repositories/pages_repo.py
from db.db import execute

def upsert_page(r: dict) -> None:
    sql = """
    INSERT INTO pages (
        page_id,
        page_name,
        category,
        page_access_token,
        created_time,
        created_at,
        updated_at
    ) VALUES (
        %(page_id)s,
        %(page_name)s,
        %(category)s,
        %(page_access_token)s,
        %(created_time)s,
        NOW(),
        NOW()
    )
    ON DUPLICATE KEY UPDATE
        page_name=VALUES(page_name),
        category=VALUES(category),
        page_access_token=COALESCE(VALUES(page_access_token), page_access_token),
        created_time=COALESCE(VALUES(created_time), created_time),
        updated_at=NOW();
    """
    execute(sql, r)
