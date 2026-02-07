# db/repositories/adsets_repo.py
from db.db import execute


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
    ) AS new
    ON DUPLICATE KEY UPDATE
        name = COALESCE(new.name, adsets.name),
        status = COALESCE(new.status, adsets.status),
        effective_status = COALESCE(new.effective_status, adsets.effective_status),
        daily_budget = COALESCE(new.daily_budget, adsets.daily_budget),
        start_time = COALESCE(new.start_time, adsets.start_time),
        billing_event = COALESCE(new.billing_event, adsets.billing_event),
        optimization_goal = COALESCE(new.optimization_goal, adsets.optimization_goal),
        last_seen_at = NOW(),
        updated_at = NOW();
    """
    execute(sql, r)
