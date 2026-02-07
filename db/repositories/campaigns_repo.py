from db.db import execute

def upsert_campaign(r: dict):
    sql = """
    INSERT INTO campaigns (
        campaign_id, ad_account_id, name, objective,
        status, effective_status, start_time,
        first_seen_at, last_seen_at, updated_at
    )
    VALUES (
        %(campaign_id)s, %(ad_account_id)s, %(name)s, %(objective)s,
        %(status)s, %(effective_status)s, %(start_time)s,
        NOW(), NOW(), NOW()
    )
    ON DUPLICATE KEY UPDATE
        name=VALUES(name),
        objective=VALUES(objective),
        status=VALUES(status),
        effective_status=VALUES(effective_status),
        start_time=COALESCE(VALUES(start_time), start_time),
        last_seen_at=NOW(),
        updated_at=NOW();
    """
    execute(sql, r)
