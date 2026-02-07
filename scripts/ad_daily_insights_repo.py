# db/repositories/ad_daily_insights_repo.py
from db.db import execute

def upsert_ad_daily_insight(r: dict) -> None:
    sql = """
    INSERT INTO ad_daily_insights (
        ad_id, date, results, cost_per_result, spend,
        impressions, reach, frequency, checked_at
    ) VALUES (
        %(ad_id)s, %(date)s, %(results)s, %(cost_per_result)s, %(spend)s,
        %(impressions)s, %(reach)s, %(frequency)s, NOW()
    )
    ON DUPLICATE KEY UPDATE
        results=VALUES(results),
        cost_per_result=VALUES(cost_per_result),
        spend=VALUES(spend),
        impressions=VALUES(impressions),
        reach=VALUES(reach),
        frequency=VALUES(frequency),
        checked_at=NOW();
    """
    execute(sql, r)
