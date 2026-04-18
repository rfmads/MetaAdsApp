# services/insights_service.py

from __future__ import annotations
import json
from datetime import datetime, time, timedelta, timezone, date
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional, Tuple

from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient, MetaObjectAccessError
from db.db import execute


# =========================
# Helpers
# =========================

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

def _to_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def _to_int(x: Any, default: int = 0) -> int:
    try:
        if x is None or x == "":
            return default
        return int(float(x))
    except Exception:
        return default

def _to_decimal(x: Any) -> Optional[Decimal]:
    try:
        if x is None or x == "":
            return None
        return Decimal(str(x))
    except (InvalidOperation, Exception):
        return None

def _pick_results_and_cpr(row: dict) -> Tuple[int, Optional[Decimal]]:
    results = _to_int(row.get("results"), default=0)
    cpr = _to_decimal(row.get("cost_per_result"))

    spend = _to_decimal(row.get("spend")) or Decimal("0")

    if results <= 0:
        actions = row.get("actions") or []
        preferred = [
            "onsite_conversion.messaging_conversation_started_7d",
            "messaging_conversation_started_7d",
            "lead",
            "purchase",
            "omni_purchase",
            "link_click",
            "landing_page_view",
        ]

        action_map: Dict[str, int] = {}
        for a in actions:
            try:
                at = a.get("action_type")
                val = a.get("value")
                if at:
                    action_map[at] = _to_int(val, default=0)
            except Exception:
                continue

        for at in preferred:
            if at in action_map and action_map[at] > 0:
                results = action_map[at]
                break

        if results <= 0 and action_map:
            results = sum(v for v in action_map.values() if isinstance(v, int))

    if cpr is None and results > 0:
        try:
            cpr = (spend / Decimal(results)).quantize(Decimal("0.01"))
        except Exception:
            cpr = None

    return results, cpr


# =========================
# DB Upserts
# =========================

def upsert_campaign_daily_insight(r: dict) -> None:
    sql = """
    INSERT INTO campaigns_daily_insights (
        campaign_id, date, results, cost_per_result, spend, impressions, reach, frequency, checked_at
    ) 
    SELECT %(campaign_id)s, %(date)s, %(results)s, %(cost_per_result)s, %(spend)s,
           %(impressions)s, %(reach)s, %(frequency)s, NOW()
    FROM DUAL
    WHERE EXISTS (SELECT 1 FROM campaigns WHERE campaign_id = %(campaign_id)s)
    ON DUPLICATE KEY UPDATE
        results=VALUES(results),
        cost_per_result=VALUES(cost_per_result),
        spend=VALUES(spend),
        impressions=VALUES(impressions),
        reach=VALUES(reach),
        frequency=VALUES(frequency),
        checked_at=NOW();
    """
    execute(sql, r)

def upsert_adset_daily_insight(r: dict) -> None:
    sql = """
    INSERT INTO adset_daily_insights (
        adset_id, date, results, cost_per_result, spend, impressions, reach, frequency, checked_at
    ) 
    SELECT %(adset_id)s, %(date)s, %(results)s, %(cost_per_result)s, %(spend)s,
           %(impressions)s, %(reach)s, %(frequency)s, NOW()
    FROM DUAL
    WHERE EXISTS (SELECT 1 FROM adsets WHERE adset_id = %(adset_id)s) 
    ON DUPLICATE KEY UPDATE
        results=VALUES(results),
        cost_per_result=VALUES(cost_per_result),
        spend=VALUES(spend),
        impressions=VALUES(impressions),
        reach=VALUES(reach),
        frequency=VALUES(frequency),
        checked_at=NOW();
    """
    execute(sql, r)

# def upsert_ad_daily_insight(r: dict) -> None:
#     sql = """
#     INSERT INTO ad_daily_insights (
#         ad_id, date, results, cost_per_result, spend, impressions, reach, frequency, checked_at
#     ) VALUES (
#         %(ad_id)s, %(date)s, %(results)s, %(cost_per_result)s, %(spend)s,
#         %(impressions)s, %(reach)s, %(frequency)s, NOW()
#     )
#     ON DUPLICATE KEY UPDATE
#         results=VALUES(results),
#         cost_per_result=VALUES(cost_per_result),
#         spend=VALUES(spend),
#         impressions=VALUES(impressions),
#         reach=VALUES(reach),
#         frequency=VALUES(frequency),
#         checked_at=NOW();
#     """
#     execute(sql, r)
def upsert_ad_daily_insight(r: dict) -> None:
    # Adding a check or using a different approach to prevent FK crashes
    sql = """
    INSERT INTO ad_daily_insights (
        ad_id, date, results, cost_per_result, spend, impressions, reach, frequency, checked_at
    ) 
    SELECT %(ad_id)s, %(date)s, %(results)s, %(cost_per_result)s, %(spend)s,
           %(impressions)s, %(reach)s, %(frequency)s, NOW()
    FROM DUAL
    WHERE EXISTS (SELECT 1 FROM ads WHERE ad_id = %(ad_id)s) -- Only insert if parent exists
    ON DUPLICATE KEY UPDATE
        results=VALUES(results),
        cost_per_result=VALUES(cost_per_result),
        spend=VALUES(spend),
        impressions=VALUES(impressions),
        reach=VALUES(reach),
        frequency=VALUES(frequency),
        checked_at=NOW();
    """
    execute(sql, r)

# =========================
# Core fetcher
# =========================

INSIGHTS_FIELDS = ",".join([
    # ✅ IDs لازمات عشان ما يصير skip
    "campaign_id",
    "adset_id",
    "ad_id",

    "date_start",
    "impressions",
    "reach",
    "spend",
    "frequency",

    # optional
    "results",
    "cost_per_result",
    "actions",
    "cost_per_action_type",
])


def _date_preset_for_days(days: int) -> str:
    # Graph API presets are limited; use safe fallback.
    if days <= 1:
        return "today"
    if days <= 7:
        return "last_7d"
    if days <= 14:
        return "last_14d"
    if days <= 28:
        return "last_28d"
    if days <= 30:
        return "last_30d"
    if days <= 90:
        return "last_90d"
    return "last_30d"

def _sync_level_for_account(
    client: MetaGraphClient,
    ad_account_id: int,
    level: str,                     # campaign | adset | ad
    days: int,
    portfolio_code: str = "",
    progress_every: int = 500,
) -> Dict[str, int]:
    import time
    MAX_RUNTIME_SECONDS = 600 # Increased slightly for empty DB runs
    start_time = time.time()

    act = f"act_{ad_account_id}"
    endpoint = f"{act}/insights"
    
    # 1. CRITICAL: Filter by delivery_info to ignore archived/deleted junk
    # This is the biggest speed booster for empty databases.
    filtering = [
        {
            "field": f"{level}.delivery_info", 
            "operator": "IN", 
            "value": ["active", "scheduled", "pending_review", "completed"] 
        }
    ]

    params = {
        "level": level,
        "fields": INSIGHTS_FIELDS,
        "time_increment": 1,
        "limit": 200, # Increased from 50 for better throughput
        "date_preset": _date_preset_for_days(days),
        "filtering": json.dumps(filtering) 
    }

    saved = 0
    skipped = 0

    logger.info(f"▶️ insights start {act} level={level} days={days} filtering=ACTIVE_ONLY")
    
    try:
        # 2. Meta Insights can be slow; we use a generator to process as they arrive
        for row in client.get_paged(endpoint, params=params):
            if time.time() - start_time > MAX_RUNTIME_SECONDS:
                logger.error(f"⛔ timeout {act} level={level} after {saved} records")
                break   
            
            try:
                row = row or {}
                d = _to_date(row.get("date_start"))
                if not d:
                    skipped += 1
                    continue
                
                # Extract metrics
                impressions = _to_int(row.get("impressions"), default=0)
                reach = _to_int(row.get("reach"), default=0)
                spend = _to_decimal(row.get("spend"))
                freq = _to_decimal(row.get("frequency"))
                results, cpr = _pick_results_and_cpr(row)

                # 3. Targeted Upserts
                if level == "campaign":
                    obj_id = row.get("campaign_id")
                    if obj_id:
                        upsert_campaign_daily_insight({
                            "campaign_id": int(obj_id), "date": d, "results": results,
                            "cost_per_result": cpr, "spend": spend, "impressions": impressions,
                            "reach": reach, "frequency": freq
                        })
                
                elif level == "adset":
                    obj_id = row.get("adset_id")
                    if obj_id:
                        upsert_adset_daily_insight({
                            "adset_id": int(obj_id), "date": d, "results": results,
                            "cost_per_result": cpr, "spend": spend, "impressions": impressions,
                            "reach": reach, "frequency": freq
                        })

                elif level == "ad":
                    obj_id = row.get("ad_id")
                    if obj_id:
                        upsert_ad_daily_insight({
                            "ad_id": int(obj_id), "date": d, "results": results,
                            "cost_per_result": cpr, "spend": spend, "impressions": impressions,
                            "reach": reach, "frequency": freq
                        })

                saved += 1
                if saved % progress_every == 0:
                    logger.info(f"⏳ insights {act} {level}: {saved} rows...")

            except Exception as row_err:
                skipped += 1
                continue

    except Exception as e:
        logger.error(f"❌ insights fetch failed {act} {level}: {e}")
        # We don't raise here so that 'adset' can still run if 'campaign' fails
        return {"saved": saved, "skipped": skipped, "error": str(e)}

    return {"saved": saved, "skipped": skipped}
# =========================
# Public services (per account)
# =========================
def sync_campaign_daily_insights_for_account(
    client: MetaGraphClient, # Changed from user_token to client
    ad_account_id: int,
    portfolio_code: str = "",
    days: int = 30,
) -> Dict[str, int]:
    try:
        return _sync_level_for_account(client, ad_account_id, "campaign", days, portfolio_code)
    except Exception as e:
        logger.error(f"❌ campaign insights failed act_{ad_account_id}: {e}")
        return {"saved": 0, "skipped": 0, "error": str(e)}

# 2. Update this one (The one causing the current crash)
def sync_adset_daily_insights_for_account(
    client: MetaGraphClient, # Changed from user_token to client
    ad_account_id: int,
    portfolio_code: str = "",
    days: int = 30,
) -> Dict[str, int]:
    try:
        return _sync_level_for_account(client, ad_account_id, "adset", days, portfolio_code)
    except Exception as e:
        logger.error(f"❌ adset insights failed act_{ad_account_id}: {e}")
        return {"saved": 0, "skipped": 0, "error": str(e)}

# 3. Update this one
def sync_ad_daily_insights_for_account(
    client: MetaGraphClient, # Changed from user_token to client
    ad_account_id: int,
    portfolio_code: str = "",
    days: int = 30,
) -> Dict[str, int]:
    try:
        return _sync_level_for_account(client, ad_account_id, "ad", days, portfolio_code)
    except Exception as e:
        logger.error(f"❌ ad insights failed act_{ad_account_id}: {e}")
        return {"saved": 0, "skipped": 0, "error": str(e)}
    

#     def _sync_level_for_account(
#     client: MetaGraphClient,
#     ad_account_id: int,
#     level: str,                     # campaign | adset | ad
#     days: int,
#     portfolio_code: str = "",
#     progress_every: int = 500,
# ) -> Dict[str, int]:
#     """
#     Fetch insights via:
#       act_{ad_account_id}/insights?level=...&time_increment=1&date_preset=last_30d

#     ✅ FIX: Use date_preset (string) to avoid MetaGraphClient dropping dict params.
#     """
#     import time

#     MAX_RUNTIME_SECONDS = 500
#     start_time = time.time()


#     act = f"act_{ad_account_id}"
#     endpoint = f"{act}/insights"
#     filtering_list = [
#             {
#                 "field": f"{level}.delivery_info", 
#                 "operator": "IN", 
#                 "value": ["active"]
#             }
#         ]
#     #   "filtering": json.dumps(filtering_list),
#     params = {
#         "level": level,
#         "fields": INSIGHTS_FIELDS,
#         "time_increment": 1,
#         "limit":50,#200,
#         "date_preset": _date_preset_for_days(days),
      
#     }

#     saved = 0
#     skipped = 0

#     logger.info(f"▶️ insights start {act} level={level} days={days} portfolio={portfolio_code}")
#     try:
#         for row in client.get_paged(endpoint, params=params):
#             if time.time() - start_time > MAX_RUNTIME_SECONDS:
#                 logger.error(f"⛔ timeout {act} level={level} — partial data saved")
#                 break   
            
#             try:
#                 row = row or {}
#                 d = _to_date(row.get("date_start"))
#                 if not d:
#                     skipped += 1
#                     continue
#                 impressions = _to_int(row.get("impressions"), default=0)
#                 reach = _to_int(row.get("reach"), default=0)
#                 spend = _to_decimal(row.get("spend"))
#                 freq = _to_decimal(row.get("frequency"))

#                 results, cpr = _pick_results_and_cpr(row)

#                 if level == "campaign":
#                     cid = row.get("campaign_id")
#                     if not cid:
#                         skipped += 1
#                         continue
#                     upsert_campaign_daily_insight({
#                         "campaign_id": int(cid),
#                         "date": d,
#                         "results": results,
#                         "cost_per_result": cpr,
#                         "spend": spend,
#                         "impressions": impressions,
#                         "reach": reach,
#                         "frequency": freq,
#                     })

#                 elif level == "adset":
#                     adset_id = row.get("adset_id")
#                     if not adset_id:
#                         skipped += 1
#                         continue
#                     upsert_adset_daily_insight({
#                         "adset_id": int(adset_id),
#                         "date": d,
#                         "results": results,
#                         "cost_per_result": cpr,
#                         "spend": spend,
#                         "impressions": impressions,
#                         "reach": reach,
#                         "frequency": freq,
#                     })

#                 elif level == "ad":
#                     ad_id = row.get("ad_id")
#                     if not ad_id:
#                         skipped += 1
#                         continue
#                     upsert_ad_daily_insight({
#                         "ad_id": int(ad_id),
#                         "date": d,
#                         "results": results,
#                         "cost_per_result": cpr,
#                         "spend": spend,
#                         "impressions": impressions,
#                         "reach": reach,
#                         "frequency": freq,
#                     })
#                 else:
#                     skipped += 1
#                     continue

#                 saved += 1
#                 if progress_every and saved % progress_every == 0:
#                     logger.info(f"⏳ insights progress {act} level={level} saved={saved} skipped={skipped}")

#             except Exception as e:
#                 skipped += 1
#                 logger.warning(f"⚠️ insights row skipped {act} level={level}: {e}")
#     except Exception as e:
#             # If it's our new critical timeout, raise it so the whole account job stops
#             if "CRITICAL_TIMEOUT" in str(e):
#                 raise e
#     logger.info(f"✅ insights done {act} level={level} saved={saved} skipped={skipped}")
#     return {"saved": saved, "skipped": skipped}
