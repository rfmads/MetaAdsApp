#insights_worker
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from integrations.meta_graph_client import MetaGraphClient

from logs.logger import logger
from db.db import query_dict
from db.config_store import get_config
from services.insights_service import (
    sync_campaign_daily_insights_for_account,
    sync_adset_daily_insights_for_account,
    sync_ad_daily_insights_for_account,
)
from services.job_service import heartbeat


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
    client = MetaGraphClient(user_token)
    try:
        out["campaigns"] = sync_campaign_daily_insights_for_account(
            client=client, 
            ad_account_id=ad_account_id,
            portfolio_code=portfolio_code,
            days=days,
        )
        if isinstance(out["campaigns"], dict) and out["campaigns"].get("error"):
            out["errors"].append(out["campaigns"]["error"])

        out["adsets"] = sync_adset_daily_insights_for_account(
            client=client, 
            ad_account_id=ad_account_id,
            portfolio_code=portfolio_code,
            days=days,
        )
        if isinstance(out["adsets"], dict) and out["adsets"].get("error"):
            out["errors"].append(out["adsets"]["error"])

        out["ads"] = sync_ad_daily_insights_for_account(
            client=client, 
            ad_account_id=ad_account_id,
            portfolio_code=portfolio_code,
            days=days,
        )
        if isinstance(out["ads"], dict) and out["ads"].get("error"):
            out["errors"].append(out["ads"]["error"])
    except Exception as e:
        # The thread for THIS account stops here and doesn't try adsets or ads.
        logger.error(f"❌ insights thread crashed: {e}")
    logger.info(f"🧵 Insights Thread done {act} errors={len(out['errors'])}")
    return out


# ✅ REQUIRED BY PIPELINE
def run(job_id=None):
# 1. Pull token from DB instead of OS environment
    user_token = get_config("META_USER_TOKEN")
    
    if not user_token:
        logger.error("❌ META_USER_TOKEN missing in database 'sys_config' table")
        # You can choose to raise an exception or return gracefully
        return {"ok": False, "error": "Missing Token"}

    max_workers = int(os.getenv("SYNC_WORKERS", "5"))
    days = int(os.getenv("INSIGHTS_DAYS", "30"))

    logger.info(f"🚀 insights worker starting workers={max_workers} days={days}")

    accounts = query_dict("""
        SELECT a.ad_account_id, p.code AS portfolio_code
        FROM ad_accounts a
        JOIN portfolios p ON p.id = a.portfolio_id
        WHERE p.code IN ('RFM','MAGIC_EXTREME')
        ORDER BY p.code, a.ad_account_id
    """)
    if not accounts:
        logger.warning("No ad accounts found")
        return {"ok": True, "accounts": 0}

    ok = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [
            ex.submit(_job_for_account, user_token, int(r["ad_account_id"]), r["portfolio_code"], days)
            for r in accounts
        ]
   #     import pdb; pdb.set_trace()

        for f in as_completed(futures):
            # ❤️ HEARTBEAT: Update the job timestamp every time a thread finishes an account
            if job_id:
                heartbeat(job_id)
            try:
                res = f.result()
                logger.info(f"📦 INSIGHTS RESULT: {res}")
                if res.get("errors"):
                    failed += 1
                else:
                    ok += 1
                    
            except Exception as e:
                failed += 1
                logger.error(f"❌ insights thread crashed: {e}")

    logger.info(f"✅ insights worker finished ok={ok} failed={failed}")

    return {
        "ok": True,
        "success": ok,
        "failed": failed,
        "accounts": len(accounts)
    }


if __name__ == "__main__":
    run()