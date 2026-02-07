# scripts/run_sync_entities_threaded.py
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from logs.logger import logger
from db.db import query_dict

from integrations.meta_graph_client import MetaGraphClient

from services.campaigns_service import sync_campaigns_for_account
from services.adsets_service import sync_adsets_for_account
from services.ads_service import sync_ads_for_account

PORTFOLIOS = ("RFM", "MAGIC_EXTREME")


def _job_for_account(user_token: str, ad_account_id: int, portfolio_code: str) -> dict:
    act = f"act_{ad_account_id}"
    logger.info(f"🧵 Thread start {act} portfolio={portfolio_code}")

    # ✅ client واحد داخل الثريد
    client = MetaGraphClient(user_token)

    # ✅ Smart modes: أول مرة full إذا الحساب جديد أو ما في داتا
    # (ببساطة: إذا ما في campaigns بالحساب بالـ DB → full)
    has_campaigns = query_dict(
        "SELECT 1 FROM campaigns WHERE ad_account_id=%(id)s LIMIT 1",
        {"id": ad_account_id},
    )
    first_time = not bool(has_campaigns)

    mode_campaigns = "full" if first_time else "incremental"
    mode_adsets = "full" if first_time else "incremental"
    mode_ads = "full" if first_time else "incremental"
    days = 30

    out = {
        "ad_account_id": ad_account_id,
        "portfolio_code": portfolio_code,
        "first_time": first_time,
        "campaigns": None,
        "adsets": None,
        "ads": None,
        "errors": [],
    }

    # 1) campaigns
    try:
        out["campaigns"] = sync_campaigns_for_account(
            client=client,  # ✅ new
            ad_account_id=ad_account_id,
            portfolio_code=portfolio_code,
            mode=mode_campaigns,
            days=days,
        )
    except Exception as e:
        msg = f"campaigns failed for {act}: {e}"
        out["errors"].append(msg)
        logger.error(f"❌ {msg}")

    # 2) adsets
    try:
        out["adsets"] = sync_adsets_for_account(
            client=client,  # ✅ new
            ad_account_id=ad_account_id,
            portfolio_code=portfolio_code,
            mode=mode_adsets,
            days=days,
        )
    except Exception as e:
        msg = f"adsets failed for {act}: {e}"
        out["errors"].append(msg)
        logger.error(f"❌ {msg}")

    # 3) ads
    try:
        out["ads"] = sync_ads_for_account(
            client=client,  # ✅ new
            ad_account_id=ad_account_id,
            portfolio_code=portfolio_code,
            mode=mode_ads,
            days=days,
        )
    except Exception as e:
        msg = f"ads failed for {act}: {e}"
        out["errors"].append(msg)
        logger.error(f"❌ {msg}")

    logger.info(f"🧵 Thread done {act} portfolio={portfolio_code} errors={len(out['errors'])}")
    return out


def main():
    user_token = os.getenv("META_USER_TOKEN")
    if not user_token:
        print("❌ META_USER_TOKEN is missing. Set it first.")
        return

    max_workers = int(os.getenv("SYNC_WORKERS", "3"))  # ابدأ 3
    logger.info(f"🚀 run_sync_entities_threaded starting. workers={max_workers}")

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
            ex.submit(_job_for_account, user_token, int(r["ad_account_id"]), r["portfolio_code"])
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
                logger.error(f"❌ Thread crashed: {e}")

    logger.info(f"✅ run_sync_entities_threaded finished. ok_threads={ok} failed_threads={failed}")


if __name__ == "__main__":
    main()
