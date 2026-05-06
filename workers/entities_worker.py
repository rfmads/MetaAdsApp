# entities_worker.py
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

from logs.logger import logger
from db.db import query_dict
from integrations.meta_graph_client import MetaGraphClient

from services.campaigns_service import sync_campaigns_for_account
from services.adsets_service import sync_adsets_for_account
from services.ads_service import sync_ads_for_account

from db.config_store import get_config
from services.job_service import heartbeat, log_step, log_error


# ============================================================
# SAFE ACCOUNT PROCESSOR
# ============================================================

def _process_account(job_id, user_token: str, ad_account_id: int, portfolio_code: str):
    act = f"act_{ad_account_id}"
    logger.info(f"🧵 START {act} portfolio={portfolio_code}")

    client = MetaGraphClient(user_token)

    try:
        has_campaigns = query_dict(
            "SELECT 1 FROM campaigns WHERE ad_account_id=%(id)s LIMIT 1",
            {"id": ad_account_id},
        )
        first_time = not bool(has_campaigns)

        mode = "full" if first_time else "incremental"
        sync_days = 90 if first_time else 14

        result = {
            "ad_account_id": ad_account_id,
            "portfolio_code": portfolio_code,
            "campaigns": None,
            "adsets": None,
            "ads": None,
            "errors": [],
        }

        # =========================
        # 1. CAMPAIGNS
        # =========================
        try:
            result["campaigns"] = sync_campaigns_for_account(
                client=client,
                ad_account_id=ad_account_id,
                mode=mode,
                days=sync_days
            )
        except Exception as e:
            msg = f"Campaigns failed: {str(e)}"
            logger.error(f"❌ {msg} {act}")
            result["errors"].append(msg)

            if job_id:
                log_error(job_id, "entities_campaigns", ad_account_id, msg)

        # =========================
        # 2. ADSETS
        # =========================
        try:
            result["adsets"] = sync_adsets_for_account(
                client=client,
                ad_account_id=ad_account_id,
                mode=mode,
                days=sync_days
            )
        except Exception as e:
            msg = f"Adsets failed: {str(e)}"
            logger.error(f"❌ {msg} {act}")
            result["errors"].append(msg)

            if job_id:
                log_error(job_id, "entities_adsets", ad_account_id, msg)

        # =========================
        # 3. ADS
        # =========================
        try:
            result["ads"] = sync_ads_for_account(
                client=client,
                ad_account_id=ad_account_id,
                mode=mode,
                days=sync_days
            )
        except Exception as e:
            msg = f"Ads failed: {str(e)}"
            logger.error(f"❌ {msg} {act}")
            result["errors"].append(msg)

            if job_id:
                log_error(job_id, "entities_ads", ad_account_id, msg)

        logger.info(f"🧵 DONE {act} errors={len(result['errors'])}")
        return result

    except Exception as e:
        msg = f"CRITICAL account failure: {str(e)}"
        logger.error(f"💥 {msg} {act}")

        if job_id:
            log_error(job_id, "entities_account", ad_account_id, msg)

        return {
            "ad_account_id": ad_account_id,
            "error": msg
        }


# ============================================================
# WORKER ENTRY
# ============================================================

def run(job_id=None):

    user_token = get_config("META_USER_TOKEN")

    if not user_token:
        logger.error("❌ META_USER_TOKEN missing")
        return {"ok": False, "error": "Missing Token"}

    max_workers = int(os.getenv("SYNC_WORKERS", "5"))

    accounts = query_dict("""
        SELECT a.ad_account_id, p.code AS portfolio_code
        FROM ad_accounts a
        JOIN portfolios p ON p.id = a.portfolio_id
        WHERE p.code IN ('RFM','MAGIC_EXTREME')
    """)

    if not accounts:
        return {"ok": True, "total": 0}

    logger.info(f"🚀 START entities worker accounts={len(accounts)} workers={max_workers}")

    success = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        futures = [
            executor.submit(
                _process_account,
                job_id,
                user_token,
                int(acc["ad_account_id"]),
                acc["portfolio_code"]
            )
            for acc in accounts
        ]

        for future in as_completed(futures):

            if job_id:
                heartbeat(job_id)

            try:
                res = future.result()
            except Exception as e:
                logger.error(f"❌ thread crash: {e}")
                failed += 1
                continue

            if isinstance(res, dict) and not res.get("error"):
                success += 1
            else:
                failed += 1

    logger.info(f"🚀 ENTITIES DONE success={success} failed={failed}")

    return {
        "ok": True,
        "success": success,
        "failed": failed
    }


if __name__ == "__main__":
    run()
# from concurrent import futures
# import os
# import json
# from concurrent.futures import ThreadPoolExecutor, as_completed

# from logs.logger import logger
# from db.db import query_dict
# from integrations.meta_graph_client import MetaGraphClient

# from services.campaigns_service import sync_campaigns_for_account
# from services.adsets_service import sync_adsets_for_account
# from services.ads_service import sync_ads_for_account
# from db.config_store import get_config
# from services.job_service import heartbeat

# def _process_account(user_token: str, ad_account_id: int, portfolio_code: str):
#     act = f"act_{ad_account_id}"
#     logger.info(f"🧵 START {act} portfolio={portfolio_code}")

#     client = MetaGraphClient(user_token)

#     # Detect first run
#     has_campaigns = query_dict(
#         "SELECT 1 FROM campaigns WHERE ad_account_id=%(id)s LIMIT 1",
#         {"id": ad_account_id},
#     )
#     first_time = not bool(has_campaigns)
    
#     # Logic: Full sync for new accounts, 14-day window for existing
#     mode = "full" if first_time else "incremental"
#     sync_days = 90 if first_time else 14 

#     result = {
#         "ad_account_id": ad_account_id,
#         "portfolio_code": portfolio_code,
#         "campaigns": None,
#         "adsets": None,
#         "ads": None,
#         "errors": [],
#     }

#     # 1. Campaigns
#     try:
#         result["campaigns"] = sync_campaigns_for_account(
#             client=client,
#             ad_account_id=ad_account_id,
#             mode=mode,
#             days=sync_days
#         )
#     except Exception as e:
#         logger.error(f"❌ campaigns failed {act}: {e}")
#         result["errors"].append(f"Campaigns: {str(e)}")
#         if "CRITICAL_TIMEOUT" in str(e): return result

#     # 2. Adsets
#     try:
#         result["adsets"] = sync_adsets_for_account(
#             client=client,
#             ad_account_id=ad_account_id,
#             mode=mode,
#             days=sync_days
#         )
#     except Exception as e:
#         logger.error(f"❌ adsets failed {act}: {e}")
#         result["errors"].append(f"Adsets: {str(e)}")
#         if "CRITICAL_TIMEOUT" in str(e): return result

#     # 3. Ads
#     try:
#         result["ads"] = sync_ads_for_account(
#             client=client,
#             ad_account_id=ad_account_id,
#             mode=mode,
#             days=sync_days
#         )
#     except Exception as e:
#         logger.error(f"❌ ads failed {act}: {e}")
#         result["errors"].append(f"Ads: {str(e)}")

#     logger.info(f"🧵 DONE {act} errors={len(result['errors'])}")
#     return result

# def run(job_id=None):
# # 1. Pull token from DB instead of OS environment
#     user_token = get_config("META_USER_TOKEN")
    
#     if not user_token:
#         logger.error("❌ META_USER_TOKEN missing in database 'sys_config' table")
#         # You can choose to raise an exception or return gracefully
#         return {"ok": False, "error": "Missing Token"}
#     max_workers = int(os.getenv("SYNC_WORKERS", "5")) # Increased to 5 as requests are lighter now

#     accounts = query_dict("""
#         SELECT a.ad_account_id, p.code AS portfolio_code
#         FROM ad_accounts a
#         JOIN portfolios p ON p.id = a.portfolio_id
#         WHERE p.code IN ('RFM','MAGIC_EXTREME')
#     """)
#     with ThreadPoolExecutor(max_workers=max_workers) as executor:
#         futures = []
#         for acc in accounts:
#             user_token = get_config("META_USER_TOKEN")
#             ad_account_id = int(acc["ad_account_id"])
#             client = MetaGraphClient(user_token)
#             # This prevents a slow 'Ads' sync from blocking 'Campaigns' for other accounts
#             futures.append(executor.submit(sync_campaigns_for_account, client, ad_account_id))
#             futures.append(executor.submit(sync_adsets_for_account, client, ad_account_id))
#             futures.append(executor.submit(sync_ads_for_account, client, ad_account_id))
#         total_synced = 0
#         for future in as_completed(futures):
#             # ❤️ HEARTBEAT: Move this to the top of the loop
#             if job_id:
#                 heartbeat(job_id)
#             res = future.result()
#             if isinstance(res, dict):
#                 total_synced += res.get("saved", 0)
#                 logger.info(f"Done: {res.get('level')} for {res.get('account')} | Saved: {res.get('saved')}")
        
#         logger.info(f"🚀 ALL ACCOUNTS FINISHED. Total entities saved: {total_synced}")

# if __name__ == "__main__":
#     run()