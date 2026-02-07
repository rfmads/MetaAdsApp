# scripts/run_sync_insights_threaded.py

import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from logs.logger import logger
from db.db import query_dict

from services.insights_service import (
    sync_campaign_daily_insights_for_account,
    sync_adset_daily_insights_for_account,
    sync_ad_daily_insights_for_account,
)

PORTFOLIOS = ("RFM", "MAGIC_EXTREME")


def _job_for_account(user_token: str, ad_account_id: int, portfolio_code: str, days: int) -> dict:
    act = f"act_{ad_account_id}"
    logger.info(f"🧵 Insights Thread start {act} portfolio={portfolio_code} days={days}")

    out = {
        "ad_account_id": ad_account_id,
        "portfolio_code": portfolio_code,
        "campaigns": None,
        "adsets": None,
        "ads": None,
        "errors": [],
    }

    out["campaigns"] = sync_campaign_daily_insights_for_account(
        user_token=user_token,
        ad_account_id=ad_account_id,
        portfolio_code=portfolio_code,
        days=days,
    )
    if isinstance(out["campaigns"], dict) and out["campaigns"].get("error"):
        out["errors"].append(out["campaigns"]["error"])

    out["adsets"] = sync_adset_daily_insights_for_account(
        user_token=user_token,
        ad_account_id=ad_account_id,
        portfolio_code=portfolio_code,
        days=days,
    )
    if isinstance(out["adsets"], dict) and out["adsets"].get("error"):
        out["errors"].append(out["adsets"]["error"])

    out["ads"] = sync_ad_daily_insights_for_account(
        user_token=user_token,
        ad_account_id=ad_account_id,
        portfolio_code=portfolio_code,
        days=days,
    )
    if isinstance(out["ads"], dict) and out["ads"].get("error"):
        out["errors"].append(out["ads"]["error"])

    logger.info(f"🧵 Insights Thread done {act} portfolio={portfolio_code} errors={len(out['errors'])}")
    return out


def main():
    user_token = os.getenv("META_USER_TOKEN")
    if not user_token:
        print("❌ META_USER_TOKEN is missing. Set it first.")
        return

    max_workers = int(os.getenv("SYNC_WORKERS", "1"))  # ابدئي بـ 1 لتجنب limits
    days = int(os.getenv("INSIGHTS_DAYS", "30"))

    logger.info(f"🚀 run_sync_insights_threaded starting. workers={max_workers} days={days}")

    accounts = query_dict(
        """
        SELECT a.ad_account_id, p.code AS portfolio_code
        FROM ad_accounts a
        JOIN portfolios p ON p.id = a.portfolio_id
        WHERE p.code IN ('RFM','MAGIC_EXTREME')
        ORDER BY p.code, a.ad_account_id
        """
    )

    if not accounts:
        logger.warning("No ad accounts found for portfolios RFM/MAGIC_EXTREME")
        return

    ok = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [
            ex.submit(_job_for_account, user_token, int(r["ad_account_id"]), r["portfolio_code"], days)
            for r in accounts
        ]

        for f in as_completed(futures):
            try:
                result = f.result()
                if result.get("errors"):
                    failed += 1
                else:
                    ok += 1
            except Exception as e:
                failed += 1
                logger.error(f"❌ Insights thread crashed: {e}")

    logger.info(f"✅ run_sync_insights_threaded finished. ok_threads={ok} failed_threads={failed}")


if __name__ == "__main__":
    main()
