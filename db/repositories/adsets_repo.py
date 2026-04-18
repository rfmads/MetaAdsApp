# db/repositories/adsets_repo.py
from db.db import execute
from db.db import get_connection
from logs.logger import logger

def upsert_adsets_batch(records: list[dict]) -> None:
    """
    Upserts multiple adsets in a single transaction.
    Prevents 'Lost connection' errors by reducing DB roundtrips.
    """
    if not records:
        return

    sql = """
    INSERT INTO adsets (
        adset_id, campaign_id, ad_account_id,
        name, status, effective_status,
        daily_budget, start_time,
        billing_event, optimization_goal,
        first_seen_at, last_seen_at, updated_at
    )
    VALUES (
        %(adset_id)s, %(campaign_id)s, %(ad_account_id)s,
        %(name)s, %(status)s, %(effective_status)s,
        %(daily_budget)s, %(start_time)s,
        %(billing_event)s, %(optimization_goal)s,
        NOW(), NOW(), NOW()
    )
    ON DUPLICATE KEY UPDATE
        name = VALUES(name),
        status = VALUES(status),
        effective_status = VALUES(effective_status),
        daily_budget = VALUES(daily_budget),
        start_time = VALUES(start_time),
        billing_event = VALUES(billing_event),
        optimization_goal = VALUES(optimization_goal),
        last_seen_at = NOW(),
        updated_at = NOW();
    """

    conn = get_connection()
    cursor = conn.cursor()
    try:
        # executemany sends all records in one go
        cursor.executemany(sql, records)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Adsets batch upsert failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
def upsert_adset(r: dict) -> None:
    sql = """
    INSERT INTO adsets (
        adset_id, campaign_id, ad_account_id,
        name, status, effective_status,
        daily_budget, start_time,
        billing_event, optimization_goal,
        first_seen_at, last_seen_at, updated_at
    )
    VALUES (
        %(adset_id)s, %(campaign_id)s, %(ad_account_id)s,
        %(name)s, %(status)s, %(effective_status)s,
        %(daily_budget)s, %(start_time)s,
        %(billing_event)s, %(optimization_goal)s,
        NOW(), NOW(), NOW()
    )
    ON DUPLICATE KEY UPDATE
        name = COALESCE(VALUES(name), name),
        status = COALESCE(VALUES(status), status),
        effective_status = COALESCE(VALUES(effective_status), effective_status),
        daily_budget = COALESCE(VALUES(daily_budget), daily_budget),
        start_time = COALESCE(VALUES(start_time), start_time),
        billing_event = COALESCE(VALUES(billing_event), billing_event),
        optimization_goal = COALESCE(VALUES(optimization_goal), optimization_goal),
        last_seen_at = NOW(),
        updated_at = NOW();
    """
    execute(sql, r)
# def upsert_adset(r: dict) -> None:
#     sql = """
#     INSERT INTO adsets (
#         adset_id, campaign_id, ad_account_id,
#         name, status, effective_status,
#         daily_budget, start_time,
#         billing_event, optimization_goal,
#         first_seen_at, last_seen_at, updated_at
#     )
#     VALUES (
#         %(adset_id)s, %(campaign_id)s, %(ad_account_id)s,
#         %(name)s, %(status)s, %(effective_status)s,
#         %(daily_budget)s, %(start_time)s,
#         %(billing_event)s, %(optimization_goal)s,
#         NOW(), NOW(), NOW()
#     ) AS new
#     ON DUPLICATE KEY UPDATE
#         name = COALESCE(new.name, adsets.name),
#         status = COALESCE(new.status, adsets.status),
#         effective_status = COALESCE(new.effective_status, adsets.effective_status),
#         daily_budget = COALESCE(new.daily_budget, adsets.daily_budget),
#         start_time = COALESCE(new.start_time, adsets.start_time),
#         billing_event = COALESCE(new.billing_event, adsets.billing_event),
#         optimization_goal = COALESCE(new.optimization_goal, adsets.optimization_goal),
#         last_seen_at = NOW(),
#         updated_at = NOW();
#     """
#     execute(sql, r)
