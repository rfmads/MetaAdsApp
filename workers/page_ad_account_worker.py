# workers/page_ad_account_worker.py

from db.db import execute

def run(job_id=None):
    execute("""
        INSERT INTO page_ad_account (page_id, ad_account_id)
        SELECT DISTINCT
            c.page_id,
            s.ad_account_id
        FROM ads a
        JOIN adsets s ON a.adset_id = s.adset_id
        JOIN creative_ads c ON a.creative_id = c.creative_id
        LEFT JOIN page_ad_account paa
            ON paa.page_id = c.page_id
            AND paa.ad_account_id = s.ad_account_id
        WHERE paa.id IS NULL
    """)

    return "page_ad_account synced"