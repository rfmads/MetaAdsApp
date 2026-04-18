# billing_worker.py
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from logs.logger import logger
from db.db import query_dict
from integrations.meta_graph_client import MetaGraphClient
from services.billing_service import sync_billing_for_account
from db.config_store import get_config
from services.job_service import heartbeat

def _job(user_token: str, ad_account_id: int, portfolio_code: str) -> dict:
    act = f"act_{ad_account_id}"
    # Inject client
    client = MetaGraphClient(user_token)
    
    try:
        return sync_billing_for_account(
            client=client, 
            ad_account_id=ad_account_id,
            portfolio_code=portfolio_code,
        )
    except Exception as e:
        logger.error(f"❌ billing thread failed {act}: {e}")
        return {"ok": False, "ad_account_id": ad_account_id, "error": str(e)}

def run():
# 1. Pull token from DB instead of OS environment
    user_token = get_config("META_USER_TOKEN")
    
    if not user_token:
        logger.error("❌ META_USER_TOKEN missing in database 'sys_config' table")
        # You can choose to raise an exception or return gracefully
        return {"ok": False, "error": "Missing Token"}

    workers = int(os.getenv("SYNC_WORKERS", "5")) # Billing is fast, can handle more workers

    accounts = query_dict("""
        SELECT a.ad_account_id, p.code AS portfolio_code
        FROM ad_accounts a
        JOIN portfolios p ON p.id = a.portfolio_id
        WHERE p.code IN ('RFM','MAGIC_EXTREME') 
        ORDER BY a.ad_account_id
    """)

    if not accounts:
        logger.warning("No ad accounts found for billing")
        return {"ok": True, "accounts": 0}

    ok, failed = 0, 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [
            ex.submit(_job, user_token, int(r["ad_account_id"]), r["portfolio_code"] or "")
            for r in accounts
        ]

        for f in as_completed(futures):
            # ❤️ HEARTBEAT: Every time a single task finishes, update the job timestamp
            res = f.result()
            if res.get("ok"): ok += 1
            else: failed += 1

    logger.info(f"✅ billing DONE ok={ok} failed={failed}")
    return {"ok": True, "success": ok, "failed": failed}

if __name__ == "__main__":
    run()

# import os
# from concurrent.futures import ThreadPoolExecutor, as_completed

# from logs.logger import logger
# from db.db import query_dict
# from services.billing_service import sync_billing_for_account


# def _job(user_token: str, ad_account_id: int, portfolio_code: str):
#     return sync_billing_for_account(
#         user_token=user_token,
#         ad_account_id=ad_account_id,
#         portfolio_code=portfolio_code,
#     )


# def run():
#     """
#     Billing worker (pipeline compatible)
#     """

#     user_token = os.getenv("META_USER_TOKEN")
#     if not user_token:
#         raise Exception("META_USER_TOKEN missing")

#     workers = int(os.getenv("SYNC_WORKERS", "3"))

#     logger.info(f"🚀 START billing worker workers={workers}")

#     accounts = query_dict("""
#         SELECT a.ad_account_id, COALESCE(p.code,'') AS portfolio_code
#         FROM ad_accounts a
#         LEFT JOIN portfolios p ON p.id = a.portfolio_id
#         WHERE COALESCE(p.code,'') IN ('RFM','MAGIC_EXTREME') 
#         ORDER BY a.ad_account_id
#     """)
# # OR p.code IS NULL
#     if not accounts:
#         logger.warning("No ad accounts found for billing")
#         return {"ok": True, "accounts": 0}

#     ok = 0
#     failed = 0
#     results = []

#     with ThreadPoolExecutor(max_workers=workers) as ex:
#         futures = [
#             ex.submit(_job, user_token, int(r["ad_account_id"]), r["portfolio_code"] or "")
#             for r in accounts
#         ]

#         for f in as_completed(futures):
#             try:
#                 res = f.result()
#                 results.append(res)

#                 if res.get("ok"):
#                     ok += 1
#                 else:
#                     failed += 1

#             except Exception as e:
#                 failed += 1
#                 logger.error(f"❌ billing thread crashed: {e}")

#     logger.info(f"✅ billing DONE ok={ok} failed={failed}")

#     return {
#         "ok": True,
#         "total_accounts": len(accounts),
#         "success": ok,
#         "failed": failed,
#     }
# if __name__ == "__main__":
#     run()