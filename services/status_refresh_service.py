 # services/status_refresh_service.py
from logs.logger import logger
from db.db import execute

def refresh_ads_real_status() -> None:
    """
    OPTIONAL:
    If you later add ads.real_status you can update it here.
    For now, your ads table relies on status directly, so we just log and skip.
    """
    logger.info("ℹ️ refresh_ads_real_status skipped (no ads.real_status column).")


def refresh_adsets_real_status() -> None:
    """
    Set adsets.real_status:
    ACTIVE if adset has >=1 ACTIVE ad (ads.status='ACTIVE')
    else PAUSED
    """
    sql = """
    UPDATE adsets s
    LEFT JOIN (
        SELECT adset_id,
               SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_ads
        FROM ads
        GROUP BY adset_id
    ) a ON s.adset_id = a.adset_id
    SET s.real_status =
        CASE
            WHEN IFNULL(a.active_ads, 0) > 0 THEN 'ACTIVE'
            ELSE 'PAUSED'
        END;
    """
    execute(sql)
    logger.info("✅ adsets.real_status refreshed based on ACTIVE ads")


def refresh_campaigns_real_status() -> None:
    """
    Set campaigns.real_status:
    ACTIVE if campaign has >=1 ACTIVE adset (adsets.real_status='ACTIVE')
    else PAUSED
    """
    sql = """
    UPDATE campaigns c
    LEFT JOIN (
        SELECT campaign_id,
               SUM(CASE WHEN real_status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_adsets
        FROM adsets
        GROUP BY campaign_id
    ) a ON c.campaign_id = a.campaign_id
    SET c.real_status =
        CASE
            WHEN IFNULL(a.active_adsets, 0) > 0 THEN 'ACTIVE'
            ELSE 'PAUSED'
        END;
    """
    execute(sql)
    logger.info("✅ campaigns.real_status refreshed based on adsets.real_status")


def refresh_all_real_status() -> None:
    """
    Call this after sync_ads/adsets/campaigns.
    """
    refresh_ads_real_status()
    refresh_adsets_real_status()
    refresh_campaigns_real_status()
    logger.info("🚀 refresh_all_real_status done")
