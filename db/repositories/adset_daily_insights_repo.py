# db/repositories/adset_daily_insights_repo.py
from db.db import execute


def upsert_adset_daily_insight(r: dict) -> None:
    """
    Table: adset_daily_insights
    UNIQUE (adset_id, date)
    """
    sql = """
    INSERT INTO adset_daily_insights (
        adset_id,
        date,
        impressions,
        reach,
        spend,
        frequency,
        checked_at
    ) VALUES (
        %(adset_id)s,
        %(date)s,
        %(impressions)s,
        %(reach)s,
        %(spend)s,
        %(frequency)s,
        %(checked_at)s
    )
    ON DUPLICATE KEY UPDATE
        impressions = VALUES(impressions),
        reach       = VALUES(reach),
        spend       = VALUES(spend),
        frequency   = VALUES(frequency),
        checked_at  = VALUES(checked_at);
    """
    execute(sql, r)
