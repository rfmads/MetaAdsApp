# db/repositories/ad_daily_insights_repo.py
from db.db import execute


def upsert_ad_daily_insight(r: dict) -> None:
    """
    ad_daily_insights has UNIQUE(ad_id, date)
    """
    sql = """
    INSERT INTO ad_daily_insights (
        ad_id, date,
        results, cost_per_result,
        spend, impressions, reach,
        frequency,
        checked_at
    ) VALUES (
        %(ad_id)s, %(date)s,
        %(results)s, %(cost_per_result)s,
        %(spend)s, %(impressions)s, %(reach)s,
        %(frequency)s,
        NOW()
    )
    ON DUPLICATE KEY UPDATE
        results=COALESCE(VALUES(results), results),
        cost_per_result=COALESCE(VALUES(cost_per_result), cost_per_result),
        spend=COALESCE(VALUES(spend), spend),
        impressions=COALESCE(VALUES(impressions), impressions),
        reach=COALESCE(VALUES(reach), reach),
        frequency=COALESCE(VALUES(frequency), frequency),
        checked_at=NOW();
    """
    execute(sql, r)
