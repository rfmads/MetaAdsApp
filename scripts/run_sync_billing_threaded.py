# scripts/run_sync_billing_threaded.py

import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from logs.logger import logger
from db.db import query_dict
from services.billing_service import sync_billing_for_account

PORTFOLIOS = ("RFM", "MAGIC_EXTREME")


def _job(user_token: str, ad_account_id: int, portfolio_code: str) -> dict:
    return sync_billing_for_account(
        user_token=user_token,
        ad_account_id=ad_account_id,
        portfolio_code=portfolio_code
    )


def main():
    user_token = os.getenv("META_USER_TOKEN")
    if not user_token:
        print("❌ META_USER_TOKEN is missing. Set it first.")
        return

    workers = int(os.getenv("SYNC_WORKERS", "2"))
    logger.info(f"🚀 run_sync_billing_threaded starting. workers={workers}")

    accounts = query_dict("""
        SELECT a.ad_account_id, COALESCE(p.code,'') AS portfolio_code
        FROM ad_accounts a
        LEFT JOIN portfolios p ON p.id = a.portfolio_id
        WHERE COALESCE(p.code,'') IN ('RFM','MAGIC_EXTREME') OR p.code IS NULL
        ORDER BY a.ad_account_id
    """)

    if not accounts:
        logger.warning("No ad accounts found.")
        return

    ok = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [
            ex.submit(_job, user_token, int(r["ad_account_id"]), r["portfolio_code"] or "")
            for r in accounts
        ]

        for f in as_completed(futures):
            try:
                res = f.result()
                if res.get("ok"):
                    ok += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                logger.error(f"❌ billing thread crashed: {e}")

    logger.info(f"✅ run_sync_billing_threaded finished. ok_accounts={ok} failed_accounts={failed}")


if __name__ == "__main__":
    main()
