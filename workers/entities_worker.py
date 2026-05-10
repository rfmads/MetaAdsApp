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

def _process_account(user_token: str, ad_account_id: int, portfolio_code: str, job_id=None):
    act = f"act_{ad_account_id}"
    logger.info(f"🧵 START {act} portfolio={portfolio_code}")

    client = MetaGraphClient(user_token)

    result = {
        "ad_account_id": ad_account_id,
        "portfolio_code": portfolio_code,
        "campaigns": {},
        "adsets": {},
        "ads": {},
        "errors": [],
    }

    try:
        # =========================
        # 1. CAMPAIGNS (ROOT)
        # =========================
        has_campaigns = query_dict(
            "SELECT 1 FROM campaigns WHERE ad_account_id=%(id)s LIMIT 1",
            {"id": ad_account_id},
        )

        first_time = not bool(has_campaigns)
        mode = "full" if first_time else "incremental"
        sync_days = 90 if first_time else 14

        try:
            campaigns_res = sync_campaigns_for_account(
                client=client,
                ad_account_id=ad_account_id,
                mode=mode,
                days=sync_days
            )

            result["campaigns"] = campaigns_res

        except Exception as e:
            err = f"Campaigns failed: {str(e)}"
            logger.error(f"🔥 {act} {err}")
            log_error_to_db(job_id, "Campaigns", ad_account_id, str(e))

            result["errors"].append(err)

            # 🚨 STOP PIPELINE HERE
            return result

        # =========================
        # 2. ADBSETS (DEPENDENT ON CAMPAIGNS)
        # =========================
        try:
            adsets_res = sync_adsets_for_account(
                client=client,
                ad_account_id=ad_account_id,
                mode=mode,
                days=sync_days
            )

            result["adsets"] = adsets_res

        except Exception as e:
            err = f"Adsets failed: {str(e)}"
            logger.error(f"🔥 {act} {err}")
            log_error_to_db(job_id, "Adsets", ad_account_id, str(e))

            result["errors"].append(err)

            # 🚨 STOP PIPELINE HERE
            return result

        # =========================
        # 3. ADS (DEPENDENT ON ADBSETS)
        # =========================
        try:
            ads_res = sync_ads_for_account(
                client=client,
                ad_account_id=ad_account_id,
                mode=mode,
                days=sync_days
            )

            result["ads"] = ads_res

        except Exception as e:
            err = f"Ads failed: {str(e)}"
            logger.error(f"🔥 {act} {err}")
            log_error_to_db(job_id, "Ads", ad_account_id, str(e))

            result["errors"].append(err)

    except Exception as e:
        # global crash only
        msg = f"Account Global Failure: {str(e)}"
        logger.error(f"🔥 {act} crashed: {msg}")
        log_error_to_db(job_id, "AccountGlobal", ad_account_id, msg)
        result["errors"].append(msg)

    logger.info(
        f"🧵 DONE {act} | "
        f"C={result['campaigns'].get('saved',0)} "
        f"A={result['adsets'].get('saved',0)} "
        f"D={result['ads'].get('saved',0)}"
    )

    return result

# def _process_account(user_token: str, ad_account_id: int, portfolio_code: str, job_id=None):
#     act = f"act_{ad_account_id}"
#     logger.info(f"🧵 START {act} portfolio={portfolio_code}")

#     client = MetaGraphClient(user_token)

#     result = {
#         "ad_account_id": ad_account_id,
#         "portfolio_code": portfolio_code,
#         "campaigns": {"saved": 0},
#         "adsets": {"saved": 0},
#         "ads": {"saved": 0},
#         "errors": [],
#     }

#     try:
#         # Detect sync mode
#         has_campaigns = query_dict(
#             "SELECT 1 FROM campaigns WHERE ad_account_id=%(id)s LIMIT 1",
#             {"id": ad_account_id},
#         )
#         first_time = not bool(has_campaigns)
#         mode = "full" if first_time else "incremental"
#         sync_days = 90 if first_time else 14 

#         # 1. Sync Campaigns
#         try:
#             result["campaigns"] = sync_campaigns_for_account(
#                 client=client, ad_account_id=ad_account_id, mode=mode, days=sync_days
#             )
#         except Exception as e:
#             err_msg = str(e)
#             result["errors"].append(f"Campaigns: {err_msg}")
#             log_error_to_db(job_id, "Campaigns", ad_account_id, err_msg)
#             # We don't 'return' here, we just move to the next step

#         # 2. Sync Adsets
#         try:
#             result["adsets"] = sync_adsets_for_account(
#                 client=client, ad_account_id=ad_account_id, mode=mode, days=sync_days
#             )
#         except Exception as e:
#             err_msg = str(e)
#             result["errors"].append(f"Adsets: {err_msg}")
#             log_error_to_db(job_id, "Adsets", ad_account_id, err_msg)

#         # 3. Sync Ads
#         try:
#             result["ads"] = sync_ads_for_account(
#                 client=client, ad_account_id=ad_account_id, mode=mode, days=sync_days
#             )
#         except Exception as e:
#             err_msg = str(e)
#             result["errors"].append(f"Ads: {err_msg}")
#             log_error_to_db(job_id, "Ads", ad_account_id, err_msg)

#     except Exception as e:
#         # Catch-all for account-level crashes (e.g., token revoked)
#         msg = f"Account Global Failure: {str(e)}"
#         logger.error(f"🔥 {act} crashed: {msg}")
#         log_error_to_db(job_id, "AccountGlobal", ad_account_id, msg)

#     logger.info(f"🧵 DONE {act} - errors found: {len(result['errors'])}")
#     return result
# =========================
# 🔹 Main Run Loop
# =========================
def run(job_id=None):
    user_token = get_config("META_USER_TOKEN")
    if not user_token:
        logger.error("❌ META_USER_TOKEN missing")
        return {"ok": False, "error": "Missing Token"}

    max_workers = int(os.getenv("SYNC_WORKERS", "4"))

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