# # entities_worker.py
from concurrent import futures
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from logs.logger import logger
from db.db import query_dict, execute  # Ensure execute is imported
from integrations.meta_graph_client import MetaGraphClient

from services.campaigns_service import sync_campaigns_for_account
from services.adsets_service import sync_adsets_for_account
from services.ads_service import sync_ads_for_account
from db.config_store import get_config
from services.job_service import heartbeat

# =========================
# 🔹 Database Logger Helper
# =========================
def log_error_to_db(job_id, step, ad_account_id, error_message):
    if not job_id:
        return
    try:
        execute("""
            INSERT INTO pipeline_job_logs 
            (job_id, step_name, status, message)
            VALUES (%s, %s, 'FAILED', %s)
        """, (
            job_id,
            f"{step}:act_{ad_account_id}",
            error_message[:1000]  # Truncate to prevent DB overflow
        ))
    except Exception as e:
        logger.error(f"⚠️ Failed to write log to DB: {e}")

# =========================
# 🔹 Thread Worker Logic
# =========================

# entities_worker.py

def _process_account(user_token: str, ad_account_id: int, portfolio_code: str, job_id=None):
    act = f"act_{ad_account_id}"
    logger.info(f"🧵 START {act} portfolio={portfolio_code}")

    client = MetaGraphClient(user_token)

    result = {
        "ad_account_id": ad_account_id,
        "portfolio_code": portfolio_code,
        "campaigns": {"saved": 0},
        "adsets": {"saved": 0},
        "ads": {"saved": 0},
        "errors": [],
    }

    try:
        # Detect sync mode
        has_campaigns = query_dict(
            "SELECT 1 FROM campaigns WHERE ad_account_id=%(id)s LIMIT 1",
            {"id": ad_account_id},
        )
        first_time = not bool(has_campaigns)
        mode = "full" if first_time else "incremental"
        sync_days = 90 if first_time else 14 

        # 1. Sync Campaigns
        try:
            result["campaigns"] = sync_campaigns_for_account(
                client=client, ad_account_id=ad_account_id, mode=mode, days=sync_days
            )
        except Exception as e:
            err_msg = str(e)
            result["errors"].append(f"Campaigns: {err_msg}")
            log_error_to_db(job_id, "Campaigns", ad_account_id, err_msg)
            # We don't 'return' here, we just move to the next step

        # 2. Sync Adsets
        try:
            result["adsets"] = sync_adsets_for_account(
                client=client, ad_account_id=ad_account_id, mode=mode, days=sync_days
            )
        except Exception as e:
            err_msg = str(e)
            result["errors"].append(f"Adsets: {err_msg}")
            log_error_to_db(job_id, "Adsets", ad_account_id, err_msg)

        # 3. Sync Ads
        try:
            result["ads"] = sync_ads_for_account(
                client=client, ad_account_id=ad_account_id, mode=mode, days=sync_days
            )
        except Exception as e:
            err_msg = str(e)
            result["errors"].append(f"Ads: {err_msg}")
            log_error_to_db(job_id, "Ads", ad_account_id, err_msg)

    except Exception as e:
        # Catch-all for account-level crashes (e.g., token revoked)
        msg = f"Account Global Failure: {str(e)}"
        logger.error(f"🔥 {act} crashed: {msg}")
        log_error_to_db(job_id, "AccountGlobal", ad_account_id, msg)

    logger.info(f"🧵 DONE {act} - errors found: {len(result['errors'])}")
    return result
# def _process_account(user_token: str, ad_account_id: int, portfolio_code: str, job_id=None):
#     act = f"act_{ad_account_id}"
#     logger.info(f"🧵 START {act} portfolio={portfolio_code}")

#     client = MetaGraphClient(user_token)

#     # Detect sync mode
#     has_campaigns = query_dict(
#         "SELECT 1 FROM campaigns WHERE ad_account_id=%(id)s LIMIT 1",
#         {"id": ad_account_id},
#     )
#     first_time = not bool(has_campaigns)
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

#     # 1. Sync Campaigns
#     try:
#         result["campaigns"] = sync_campaigns_for_account(
#             client=client, ad_account_id=ad_account_id, mode=mode, days=sync_days
#         )
#     except Exception as e:
#         err_msg = str(e)
#         logger.error(f"❌ campaigns failed {act}: {err_msg}")
#         result["errors"].append(f"Campaigns: {err_msg}")
#         log_error_to_db(job_id, "Campaigns", ad_account_id, err_msg)
#         if "CRITICAL_TIMEOUT" in err_msg: return result

#     # 2. Sync Adsets
#     try:
#         result["adsets"] = sync_adsets_for_account(
#             client=client, ad_account_id=ad_account_id, mode=mode, days=sync_days
#         )
#     except Exception as e:
#         err_msg = str(e)
#         logger.error(f"❌ adsets failed {act}: {err_msg}")
#         result["errors"].append(f"Adsets: {err_msg}")
#         log_error_to_db(job_id, "Adsets", ad_account_id, err_msg)
#         if "CRITICAL_TIMEOUT" in err_msg: return result

#     # 3. Sync Ads
#     try:
#         result["ads"] = sync_ads_for_account(
#             client=client, ad_account_id=ad_account_id, mode=mode, days=sync_days
#         )
#     except Exception as e:
#         err_msg = str(e)
#         logger.error(f"❌ ads failed {act}: {err_msg}")
#         result["errors"].append(f"Ads: {err_msg}")
#         log_error_to_db(job_id, "Ads", ad_account_id, err_msg)

#     return result

# =========================
# 🔹 Main Run Loop
# =========================
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

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Pass job_id into the worker function
        future_to_acc = {
            executor.submit(
                _process_account, 
                user_token, 
                acc["ad_account_id"], 
                acc["portfolio_code"],
                job_id
            ): acc for acc in accounts
        }

        total_synced = 0
        # for future in as_completed(future_to_acc):
        #     if job_id:
        #         heartbeat(job_id)
            
        #     acc_info = future_to_acc[future]
        #     try:
        #         res = future.result() 
                
        #         # Calculate total saved
        #         for level in ["campaigns", "adsets", "ads"]:
        #             if res.get(level):
        #                 total_synced += res[level].get("saved", 0)

        #     except Exception as e:
        #         err_msg = f"Critical thread crash: {str(e)}"
        #         logger.error(f"🔥 {err_msg} for {acc_info['ad_account_id']}")
        #         log_error_to_db(job_id, "ThreadCrash", acc_info['ad_account_id'], err_msg)
        # inside run() function in entities_worker.py

        for future in as_completed(future_to_acc):
            if job_id:
                heartbeat(job_id)
            
            acc_info = future_to_acc[future]
            try:
                res = future.result() 
                
                # Safely extract 'saved' counts
                for level in ["campaigns", "adsets", "ads"]:
                    data = res.get(level)
                    if isinstance(data, dict):
                        total_synced += data.get("saved", 0)

            except Exception as e:
                # This handles errors that occur IN the thread but OUTSIDE the internal try/excepts
                err_msg = f"Critical thread crash: {str(e)}"
                logger.error(f"🔥 {err_msg} for {acc_info['ad_account_id']}")
                log_error_to_db(job_id, "ThreadCrash", acc_info['ad_account_id'], err_msg)

        logger.info(f"🚀 FINISHED. Total entities saved: {total_synced}")
        return {"ok": True, "total_saved": total_synced}
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