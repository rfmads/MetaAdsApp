# #creative worker

# creatives_worker.py
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from logs.logger import logger
from db.db import query_dict
from integrations.meta_graph_client import MetaGraphClient
from services.creatives_service import sync_creatives_for_account
from db.config_store import get_config
from services.job_service import heartbeat
def _job(user_token: str, ad_account_id: int, portfolio_code: str, mode: str, days: int) -> dict:
    act = f"act_{ad_account_id}"
    logger.info(f"🧵 Creative Thread start {act} portfolio={portfolio_code}")

    # Inject shared client
    client = MetaGraphClient(user_token)

    out = {
        "ad_account_id": ad_account_id,
        "portfolio_code": portfolio_code,
        "result": None,
        "error": None
    }

    try:
        out["result"] = sync_creatives_for_account(
            client=client, # Changed from user_token to client
            ad_account_id=ad_account_id,
            mode=mode,
            days=days,
        )
    except Exception as e:
        out["error"] = str(e)
        logger.error(f"❌ Creative Thread failed {act}: {e}")

    return out

def run(job_id=None):
# 1. Pull token from DB instead of OS environment
    user_token = get_config("META_USER_TOKEN")
    
    if not user_token:
        logger.error("❌ META_USER_TOKEN missing in database 'sys_config' table")
        # You can choose to raise an exception or return gracefully
        return {"ok": False, "error": "Missing Token"}
    workers = int(os.getenv("SYNC_WORKERS", "5"))
    days = int(os.getenv("CREATIVES_DAYS", "14")) # Suggesting 14 for incremental
    mode = os.getenv("CREATIVES_MODE", "incremental")

    accounts = query_dict("""
        SELECT a.ad_account_id, p.code AS portfolio_code
        FROM ad_accounts a
        JOIN portfolios p ON p.id = a.portfolio_id
        WHERE p.code IN ('RFM','MAGIC_EXTREME')
    """)

    ok, failed = 0, 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [
            ex.submit(_job, user_token, int(r["ad_account_id"]), r["portfolio_code"], mode, days)
            for r in accounts
        ]
        for f in as_completed(futures):
            # ❤️ HEARTBEAT: Update the job timestamp every time a thread finishes an account
            if job_id:
                heartbeat(job_id)
            res = f.result()
            if res.get("error"): failed += 1
            else: ok += 1

    logger.info(f"✅ creatives worker finished ok={ok} failed={failed}")

if __name__ == "__main__":
     run()    
# import os
# from concurrent.futures import ThreadPoolExecutor, as_completed

# from logs.logger import logger
# from db.db import query_dict
# from services.creatives_service import sync_creatives_for_account


# def _job(user_token: str, ad_account_id: int, portfolio_code: str, mode: str, days: int) -> dict:
#     act = f"act_{ad_account_id}"
#     logger.info(f"🧵 Creative Thread start {act} portfolio={portfolio_code} mode={mode} days={days}")

#     out = {
#         "ad_account_id": ad_account_id,
#         "portfolio_code": portfolio_code,
#         "result": None,
#         "error": None
#     }

#     try:
#         out["result"] = sync_creatives_for_account(
#             user_token=user_token,
#             ad_account_id=ad_account_id,
#             portfolio_code=portfolio_code,
#             mode=mode,
#             days=days,
#         )
#     except Exception as e:
#         out["error"] = str(e)
#         logger.error(f"❌ Creative Thread failed {act}: {e}")

#     logger.info(f"🧵 Creative Thread done {act} error={bool(out['error'])}")
#     return out


# # ✅ REQUIRED FOR PIPELINE
# def run():
#     user_token = os.getenv("META_USER_TOKEN")
#     if not user_token:
#         raise Exception("META_USER_TOKEN is missing")

#     workers = int(os.getenv("SYNC_WORKERS", "3"))
#     days = int(os.getenv("CREATIVES_DAYS", "30"))
#     mode = os.getenv("CREATIVES_MODE", "incremental")

#     accounts = query_dict("""
#         SELECT a.ad_account_id, p.code AS portfolio_code
#         FROM ad_accounts a
#         JOIN portfolios p ON p.id = a.portfolio_id
#         WHERE p.code IN ('RFM','MAGIC_EXTREME')
#         ORDER BY p.code, a.ad_account_id
#     """)

#     logger.info(
#         f"🚀 creatives worker starting workers={workers} mode={mode} days={days} accounts={len(accounts)}"
#     )

#     ok = 0
#     failed = 0

#     with ThreadPoolExecutor(max_workers=workers) as ex:
#         futures = [
#             ex.submit(_job, user_token, int(r["ad_account_id"]), r["portfolio_code"], mode, days)
#             for r in accounts
#         ]

#         for f in as_completed(futures):
#             try:
#                 res = f.result()
#                 if res.get("error"):
#                     failed += 1
#                 else:
#                     ok += 1
#             except Exception as e:
#                 failed += 1
#                 logger.error(f"❌ creative thread crashed: {e}")

#     logger.info(f"✅ creatives worker finished ok={ok} failed={failed}")

#     return {
#         "ok": True,
#         "success": ok,
#         "failed": failed,
#         "accounts": len(accounts)
#     }


# if __name__ == "__main__":
#     run()