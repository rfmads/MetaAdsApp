# services/campaigns_service.py
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

from logs.logger import logger
from db.db import query_dict

from integrations.meta_graph_client import MetaGraphClient
from services.campaigns_service import sync_campaigns_for_account
from services.adsets_service import sync_adsets_for_account
from services.ads_service import sync_ads_for_account

from db.config_store import get_config
from services.job_service import heartbeat, log_error


# ============================================================
# ACCOUNT WORKER (SAFE)
# ============================================================

def _process_account(job_id, user_token: str, ad_account_id: int, portfolio_code: str):

    act = f"act_{ad_account_id}"
    logger.info(f"🧵 START {act}")

    client = MetaGraphClient(user_token)

    try:
        has_campaigns = query_dict(
            "SELECT 1 FROM campaigns WHERE ad_account_id=%(id)s LIMIT 1",
            {"id": ad_account_id},
        )
        first_time = not bool(has_campaigns)

        mode = "full" if first_time else "incremental"
        sync_days = 90 if first_time else 14

        errors = []

        # =========================
        # CAMPAIGNS (SAFE)
        # =========================
        try:
            sync_campaigns_for_account(client, ad_account_id, mode, sync_days)
        except Exception as e:
            msg = f"Campaigns failed: {str(e)}"
            logger.error(f"❌ {msg} {act}")
            errors.append(msg)

            if job_id:
                log_error(job_id, "entities_campaigns", ad_account_id, msg)

        # =========================
        # ADSETS (SAFE)
        # =========================
        try:
            sync_adsets_for_account(client, ad_account_id, mode, sync_days)
        except Exception as e:
            msg = f"Adsets failed: {str(e)}"
            logger.error(f"❌ {msg} {act}")
            errors.append(msg)

            if job_id:
                log_error(job_id, "entities_adsets", ad_account_id, msg)

        # =========================
        # ADS (SAFE)
        # =========================
        try:
            sync_ads_for_account(client, ad_account_id, mode, sync_days)
        except Exception as e:
            msg = f"Ads failed: {str(e)}"
            logger.error(f"❌ {msg} {act}")
            errors.append(msg)

            if job_id:
                log_error(job_id, "entities_ads", ad_account_id, msg)

        logger.info(f"🧵 DONE {act} errors={len(errors)}")

        # ✅ IMPORTANT:
        # return success EVEN IF SOME ADS FAILED
        return {
            "ad_account_id": ad_account_id,
            "errors": errors
        }

    except Exception as e:
        # 🔥 THIS IS THE ONLY FATAL CASE (thread crash)
        # DO NOT swallow it
        logger.error(f"💥 CRITICAL thread crash {act}: {e}")
        raise   # ⬅️ THIS stops entire pipeline


# ============================================================
# MAIN RUNNER (STOP ON THREAD CRASH ONLY)
# ============================================================

def run(job_id=None):

    user_token = get_config("META_USER_TOKEN")
    if not user_token:
        return {"ok": False, "error": "Missing token"}

    max_workers = int(os.getenv("SYNC_WORKERS", "5"))

    accounts = query_dict("""
        SELECT a.ad_account_id, p.code AS portfolio_code
        FROM ad_accounts a
        JOIN portfolios p ON p.id = a.portfolio_id
        WHERE p.code IN ('RFM','MAGIC_EXTREME')
    """)

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
                res = future.result()   # 🔥 if thread crashed → exception here

                if isinstance(res, dict):
                    success += 1
                    if res.get("errors"):
                        failed += 1

            except Exception as e:
                # 🚨 THREAD CRASH = STOP PIPELINE (your requirement)
                logger.error(f"💥 THREAD CRASH DETECTED → STOP PIPELINE: {e}")
                raise

    logger.info(f"🚀 DONE success={success} failed={failed}")

    return {"ok": True, "success": success, "failed": failed}
# from __future__ import annotations

# from datetime import datetime, timedelta, timezone
# from http import client
# import json # Use standard json for the filtersimport json
# from typing import Dict, Any, Optional

# from logs.logger import logger
# from integrations.meta_graph_client import MetaGraphClient, MetaRateLimitError
# from db.db import execute
# from services.insights_service import _to_date # Reuse your date helper

# # =========================
# # Meta fields
# # =========================
# # ملاحظة: created_time موجود بالـ API بس احنا ما رح نخزنه لأنه مش موجود بجدولك
# CAMPAIGN_FIELDS = (
#     "id,"
#     "name,"
#     "objective,"
#     "start_time,"
#     "status,"
#     "effective_status"
# )

# def _parse_dt(dt_str: Optional[str]):
#     if not dt_str:
#         return None
#     try:
#         s = dt_str.replace("Z", "+00:00")
#         # handle +0000 -> +00:00
#         if len(s) >= 5 and (s[-5] in ["+", "-"]) and s[-3] != ":":
#             s = s[:-2] + ":" + s[-2:]
#         return datetime.fromisoformat(s).replace(tzinfo=None)
#     except Exception:
#         return None


# def upsert_campaign(record: dict) -> None:
#     """
#     Upsert into campaigns based on UNIQUE(campaign_id)
#     Matches your table exactly.
#     """
#     sql = """
#     INSERT INTO campaigns (
#         campaign_id,
#         name,
#         objective,
#         start_time,
#         ad_account_id,
#         status,
#         effective_status,
#         first_seen_at,
#         last_seen_at
#     ) VALUES (
#         %(campaign_id)s,
#         %(name)s,
#         %(objective)s,
#         %(start_time)s,
#         %(ad_account_id)s,
#         %(status)s,
#         %(effective_status)s,
#         NOW(),
#         NOW()
#     )
#     ON DUPLICATE KEY UPDATE
#         name = COALESCE(VALUES(name), name),
#         objective = COALESCE(VALUES(objective), objective),
#         start_time = COALESCE(VALUES(start_time), start_time),
#         status = COALESCE(VALUES(status), status),
#         effective_status = COALESCE(VALUES(effective_status), effective_status),
#         last_seen_at = NOW(),
#         updated_at = NOW();
#     """
#     execute(sql, record)

# def upsert_campaigns_batch(records: list[dict]) -> None:
#     if not records: return
#     sql = """
#     INSERT INTO campaigns (campaign_id, name, objective, start_time, ad_account_id, status, effective_status, last_seen_at)
#     VALUES (%(campaign_id)s, %(name)s, %(objective)s, %(start_time)s, %(ad_account_id)s, %(status)s, %(effective_status)s, NOW())
#     ON DUPLICATE KEY UPDATE 
#         name=VALUES(name), status=VALUES(status), effective_status=VALUES(effective_status), 
#         last_seen_at=NOW(), real_status='ACTIVE'
#     """
#     from db.db import get_connection
#     conn = get_connection()
#     cursor = conn.cursor()
#     try:
#         cursor.executemany(sql, records)
#         conn.commit()
#     finally:
#         cursor.close()
#         conn.close()
# def _compute_real_status(effective_status: Optional[str]) -> Optional[str]:
#     """
#     real_status enum('ACTIVE','PAUSED') حسب طلبك:
#     إذا الحملة Effective Status = ACTIVE => ACTIVE
#     غير ذلك => PAUSED
#     """
#     if not effective_status:
#         return None
#     return "ACTIVE" if effective_status.upper() == "ACTIVE" else "PAUSED"


# def update_real_status(campaign_id: int) -> None:
#     """
#     Optional: يحسب real_status بناءً على adsets داخل الحملة:
#     - إذا في أي adset فعال => ACTIVE
#     - غير ذلك => PAUSED

#     (إذا ما بدك هذا المنطق، احذفي الدالة واستدعائها)
#     """
#     sql = """
#     UPDATE campaigns c
#     SET c.real_status = (
#         SELECT
#             CASE
#                 WHEN SUM(s.effective_status = 'ACTIVE') > 0 THEN 'ACTIVE'
#                 ELSE 'PAUSED'
#             END
#         FROM adsets s
#         WHERE s.campaign_id = c.campaign_id
#     )
#     WHERE c.campaign_id = %(campaign_id)s;
#     """
#     execute(sql, {"campaign_id": campaign_id})

# def sync_campaigns_for_account(client, ad_account_id, mode="full", days=30):
#     act = f"act_{ad_account_id}"
#     cutoff_str = (datetime.now(timezone.utc) - timedelta(days=days)).strftime('%Y-%m-%d')
    
#     # 1. Initialize empty filters list (Business wants all statuses)
#     filters = []
    
#     # 2. Add time filter only for incremental runs
#     if mode == "incremental":
#         filters.append({
#             "field": "updated_time", 
#             "operator": "GREATER_THAN", 
#             "value": cutoff_str
#         })
#     # Filter: Always ACTIVE.
#     # filters = [{"field": "effective_status", "operator": "IN", "value": ["ACTIVE"]}]
#     # ,"filtering": json.dumps(filters)
#     params = {
#         "fields": CAMPAIGN_FIELDS,
#         "limit": 150,
#         "filtering": json.dumps(filters) if filters else None
#     }

#     saved = 0
#     all_records = []
#     for c in client.get_paged(f"{act}/campaigns", params=params):
#         all_records.append({
#             "campaign_id": int(c["id"]),
#             "name": c.get("name"),
#             "objective": c.get("objective"),
#             "start_time": _parse_dt(c.get("start_time")),
#             "ad_account_id": ad_account_id,
#             "status": c.get("status"),
#             "effective_status": c.get("effective_status"),
#         })
    
#     if all_records:
#         upsert_campaigns_batch(all_records)
#     return {"level": "Campaigns", "account": act, "saved": len(all_records)}

# def sync_campaigns_for_account(
#     user_token: str,
#     ad_account_id: int,
#     portfolio_code: str = "",
#     mode: str = "full",
#     days: int = 365,
# ) -> Dict[str, Any]:
#     client = MetaGraphClient(user_token)
#     act = f"act_{ad_account_id}"
#     saved = 0
#     skipped = 0

#     logger.info(f"▶️ campaigns sync start {act} (ACTIVE ONLY)")

#     try:
#         # ✅ Filter at the API level for performance
#         # This tells Meta: "Only send me campaigns that are currently ACTIVE"
#         active_filter = [
#             {
#                 "field": "effective_status",
#                 "operator": "IN",
#                 "value": ["ACTIVE"]
#             }
#         ]

#         params = {
#             "fields": CAMPAIGN_FIELDS, 
#             "limit": 250, # Increased limit because filtered results are lighter
#             "filtering": json.dumps(active_filter) 
#         }

#         # If mode is not 'full', you could also add a 'time_range' filter here 
#         # to only get campaigns modified recently.

#         for c in client.get_paged(f"{act}/campaigns", params=params):
#             cid = c.get("id")
#             if not cid:
#                 skipped += 1
#                 continue

#             campaign_id = int(cid)

#             record = {
#                 "campaign_id": campaign_id,
#                 "name": c.get("name"),
#                 "objective": c.get("objective"),
#                 "start_time": _parse_dt(c.get("start_time")),
#                 "ad_account_id": int(ad_account_id),
#                 "status": c.get("status"),
#                 "effective_status": c.get("effective_status"),
#             }

#             # 1. Save the campaign
#             upsert_campaign(record)

#             # 2. Update real_status (It will always be ACTIVE because of our filter)
#             sql_real = "UPDATE campaigns SET real_status = 'ACTIVE' WHERE campaign_id = %s"
#             execute(sql_real, (campaign_id,))

#             saved += 1

        # logger.info(f"✅ campaigns synced for {act} saved={saved}")
        # return {"ok": True, "saved": saved, "skipped": skipped}

    # except Exception as e:
    #     logger.error(f"❌ campaigns failed for {act}: {e}")
    #     return {"ok": False, "saved": saved, "skipped": skipped, "error": str(e)}