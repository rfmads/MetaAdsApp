# scripts/run_sync_creatives_threaded.py

import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from logs.logger import logger
from db.db import query_dict
from services.creatives_service import sync_creatives_for_account


def _job(user_token: str, ad_account_id: int, portfolio_code: str, mode: str, days: int) -> dict:
    act = f"act_{ad_account_id}"
    logger.info(f"🧵 Creative Thread start {act} portfolio={portfolio_code} mode={mode} days={days}")

    out = {"ad_account_id": ad_account_id, "portfolio_code": portfolio_code, "result": None, "error": None}

    try:
        out["result"] = sync_creatives_for_account(
            user_token=user_token,
            ad_account_id=ad_account_id,
            portfolio_code=portfolio_code,
            mode=mode,
            days=days,
        )
    except Exception as e:
        out["error"] = str(e)
        logger.error(f"❌ Creative Thread failed {act}: {e}")

    logger.info(f"🧵 Creative Thread done {act} portfolio={portfolio_code} error={bool(out['error'])}")
    return out


def main():
    user_token = os.getenv("META_USER_TOKEN")
    if not user_token:
        print("❌ META_USER_TOKEN is missing. Set it first.")
        return

    workers = int(os.getenv("SYNC_WORKERS", "2"))
    days = int(os.getenv("CREATIVES_DAYS", "30"))
    mode = os.getenv("CREATIVES_MODE", "incremental")  # incremental | full

    accounts = query_dict("""
        SELECT a.ad_account_id, p.code AS portfolio_code
        FROM ad_accounts a
        JOIN portfolios p ON p.id = a.portfolio_id
        WHERE p.code IN ('RFM','MAGIC_EXTREME')
        ORDER BY p.code, a.ad_account_id
    """)

    logger.info(f"🚀 run_sync_creatives_threaded starting workers={workers} mode={mode} days={days} accounts={len(accounts)}")

    ok = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [
            ex.submit(_job, user_token, int(r["ad_account_id"]), r["portfolio_code"], mode, days)
            for r in accounts
        ]

        for f in as_completed(futures):
            res = f.result()
            if res.get("error"):
                failed += 1
            else:
                ok += 1

    logger.info(f"✅ run_sync_creatives_threaded finished ok_threads={ok} failed_threads={failed}")


if __name__ == "__main__":
    main()
