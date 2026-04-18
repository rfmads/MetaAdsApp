# services/adsets_service.py

# services/adsets_service.py

from datetime import datetime, timedelta, timezone
# REMOVED: from http import client (This was causing a conflict)
import json # Use standard json for the filters

from logs.logger import logger
from db.db import query_dict
from db.repositories.adsets_repo import upsert_adset
from services.ads_service import _normalize_keys
from services.campaigns_service import _parse_dt

# ✅ Configuration
ADSET_FIELDS = "id,name,status,effective_status,daily_budget,start_time,updated_time,campaign_id"

def sync_adsets_for_account(client, ad_account_id, mode="full", days=30, **kwargs):
    """
    kwargs allows passing 'portfolio_code' without crashing if it's sent 
    from the main loop but not used here.
    """
    act = f"act_{ad_account_id}"
    cutoff_str = (datetime.now(timezone.utc) - timedelta(days=days)).strftime('%Y-%m-%d')

    # filters = [{"field": "effective_status", "operator": "IN", "value": ["ACTIVE"]}]
    filters = []
    if mode == "incremental":
        # filters.append({"field": "updated_time", "operator": "GREATER_THAN", "value": cutoff_str})
        filters.append({"field": "updated_time", "operator": "GREATER_THAN", "value": cutoff_str})
# 
    params = {
        "fields": ADSET_FIELDS, 
        "limit": 150,
        "filtering": json.dumps(filters)
    }

    saved = 0
    all_records = []
    try:
        for raw_adset in client.get_paged(f"{act}/adsets", params=params):
            adset = _normalize_keys(raw_adset)
            all_records.append({
                "adset_id": int(adset["id"]),
                "campaign_id": int(adset["campaign_id"]),
                "ad_account_id": ad_account_id,
                "name": adset.get("name"),
                "status": adset.get("status"),
                "effective_status": adset.get("effective_status"),
                "daily_budget": adset.get("daily_budget"),
                "start_time": _parse_dt(adset.get("start_time")),
                "billing_event": adset.get("billing_event"),
                "optimization_goal": adset.get("optimization_goal")
            })
        
        if all_records:
            # You'll need to create this batch function in your repo
            from db.repositories.adsets_repo import upsert_adsets_batch 
            upsert_adsets_batch(all_records)
            
        return {"level": "Adsets", "account": act, "saved": len(all_records), "ok": True}
    except Exception as e:
        logger.error(f"❌ Adset sync failed for {act}: {e}")
        return {"ok": False, "saved": 0, "error": str(e)}

def sync_adsets(user_token: str) -> None:
    from integrations.meta_graph_client import MetaGraphClient
    client_instance = MetaGraphClient(user_token)

    accounts = query_dict("""
        SELECT a.ad_account_id, p.code AS portfolio_code
        FROM ad_accounts a
        JOIN portfolios p ON p.id = a.portfolio_id
        WHERE p.code IN ('RFM','MAGIC_EXTREME')
        ORDER BY p.code, a.ad_account_id
    """)

    total_saved = 0
    failed_accounts = 0

    for row in accounts:
        # Pass the client_instance clearly
        res = sync_adsets_for_account(
            client=client_instance,
            ad_account_id=int(row["ad_account_id"]),
            portfolio_code=row["portfolio_code"],
            mode="incremental",
            days=30,
        )

        total_saved += res.get("saved", 0)
        if not res.get("ok"):
            failed_accounts += 1

    logger.info(f"✅ Adsets sync done. Total saved: {total_saved}. Failed accounts: {failed_accounts}")

# def sync_adsets_for_account(
#     client, 
#     ad_account_id: int,
#     portfolio_code: str = "",
#     mode: str = "full",
#     days: int = 30,
# ) -> dict:
#     act_id = f"act_{ad_account_id}"
    
#     # Calculate cutoff for incremental mode
#     cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
#     # Format for Meta (e.g., 2026-04-15)
#     cutoff_str = cutoff_date.strftime('%Y-%m-%d')

#     saved = 0
#     skipped = 0
#     missing_campaigns = 0

#     # 1. Load existing campaigns to avoid FK failures
#     existing_campaigns = set()
#     try:
#         rows = query_dict(
#             "SELECT campaign_id FROM campaigns WHERE ad_account_id = %(ad_account_id)s",
#             {"ad_account_id": ad_account_id},
#         )
#         existing_campaigns = {int(r["campaign_id"]) for r in rows}
#     except Exception:
#         existing_campaigns = set()

#     try:
#         # ✅ Prepare Filters
#         # We only want ACTIVE adsets. 
#         # In incremental mode, we ALSO only want adsets updated recently.
#         filters = [
#             {"field": "effective_status", "operator": "IN", "value": ["ACTIVE"]}
#         ]
        
#         if mode == "incremental":
#             filters.append({"field": "updated_time", "operator": "GREATER_THAN", "value": cutoff_str})

#         params = {
#             "fields": FIELDS,
#             "limit": 250, # Higher limit is faster
#             "filtering": json.dumps(filters)
#         }

#         logger.info(f"▶️ adsets sync start {act_id} (ACTIVE + {mode})")

#         # ✅ get_paged now receives much less data
#         for a in client.get_paged(f"{act_id}/adsets", params=params):
            
#             campaign_id = a.get("campaign_id")
#             if not campaign_id:
#                 skipped += 1
#                 continue

#             campaign_id_int = int(campaign_id)

#             # Avoid FK error
#             if existing_campaigns and campaign_id_int not in existing_campaigns:
#                 missing_campaigns += 1
#                 skipped += 1
#                 continue

#             start = _as_utc(parse_meta_datetime(a.get("start_time")))

#             upsert_adset({
#                 "adset_id": int(a["id"]),
#                 "campaign_id": campaign_id_int,
#                 "ad_account_id": int(ad_account_id),
#                 "name": a.get("name"),
#                 "status": a.get("status"),
#                 "effective_status": a.get("effective_status"),
#                 "daily_budget": a.get("daily_budget"),
#                 "start_time": start.replace(tzinfo=None) if start else None,
#                 "billing_event": a.get("billing_event"),
#                 "optimization_goal": a.get("optimization_goal"),
#             })
#             saved += 1

#         logger.info(f"✅ adsets synced {act_id} saved={saved} skipped={skipped}")
#         return {"ok": True, "saved": saved, "skipped": skipped, "missing_campaigns": missing_campaigns}

#     except Exception as e:
#         logger.error(f"❌ adsets failed for {act_id}: {e}")
#         return {"ok": False, "saved": saved, "skipped": skipped, "missing_campaigns": missing_campaigns, "error": str(e)}
