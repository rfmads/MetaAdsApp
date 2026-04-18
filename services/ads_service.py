# services/ads_service.py

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from typing import Any, Dict, Optional
from xmlrpc import client

from logs.logger import logger
from integrations.meta_graph_client import MetaObjectAccessError
from db.db import query_dict, execute
from utils.datetime_utils import parse_meta_datetime


# =========================
# Config
# =========================

# Fetch ads directly from ad account (LESS REQUESTS)
# Include adset_id/campaign_id + creative fields
ADS_FIELDS = (
    "id,name,status,effective_status,adset_id,campaign_id,updated_time,"
    "creative{id,thumbnail_url,image_url,object_story_id}"
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _cutoff(days: int) -> datetime:
    return _utc_now() - timedelta(days=days)


def _normalize_keys(d: Any) -> Any:
    """
    Sometimes Meta API dict keys may appear as bytes (rare).
    Convert bytes keys -> str recursively.
    """
    if isinstance(d, dict):
        out = {}
        for k, v in d.items():
            if isinstance(k, (bytes, bytearray)):
                try:
                    k2 = k.decode("utf-8", errors="ignore")
                except Exception:
                    k2 = str(k)
            else:
                k2 = str(k)
            out[k2] = _normalize_keys(v)
        return out
    if isinstance(d, list):
        return [_normalize_keys(x) for x in d]
    return d


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None or x == "":
            return None
        return int(x)
    except Exception:
        return None


def _as_utc(dt):
    if not dt:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# =========================
# DB Upsert (MATCHES YOUR ads TABLE)
# =========================
def upsert_ad(r: dict) -> None:
    """
    Matches your MySQL table `ads`:

    ads columns (based on your SHOW CREATE):
      - ad_id (UNIQUE)
      - adset_id (FK)
      - campaign_id
      - name
      - status
      - effective_status
      - thumbnail_url
      - image_url
      - post_link
      - post_id
      - updated_at (timestamp)  [no ON UPDATE in your schema]
    """
    sql = """
    INSERT INTO ads (
        ad_id,
        adset_id,
        campaign_id,
        name,
        status,
        effective_status,
        thumbnail_url,
        image_url,
        post_id,
        post_link,
        updated_at
    ) VALUES (
        %(ad_id)s,
        %(adset_id)s,
        %(campaign_id)s,
        %(name)s,
        %(status)s,
        %(effective_status)s,
        %(thumbnail_url)s,
        %(image_url)s,
        %(post_id)s,
        %(post_link)s,
        NOW()
    )
    ON DUPLICATE KEY UPDATE
        adset_id=COALESCE(VALUES(adset_id), adset_id),
        campaign_id=COALESCE(VALUES(campaign_id), campaign_id),
        name=COALESCE(VALUES(name), name),
        status=COALESCE(VALUES(status), status),
        effective_status=COALESCE(VALUES(effective_status), effective_status),
        thumbnail_url=COALESCE(VALUES(thumbnail_url), thumbnail_url),
        image_url=COALESCE(VALUES(image_url), image_url),
        post_id=COALESCE(VALUES(post_id), post_id),
        post_link=COALESCE(VALUES(post_link), post_link),
        updated_at=NOW();
    """
    execute(sql, r)


# =========================
# Service (thread-safe per account)
# =========================

ADS_FIELDS = "id,name,status,effective_status,adset_id,campaign_id,updated_time,creative{id,thumbnail_url,image_url,object_story_id}"

# def sync_ads_for_account(client, ad_account_id, mode="full", days=30):
#     act = f"act_{ad_account_id}"
#     cutoff_str = (datetime.now(timezone.utc) - timedelta(days=days)).strftime('%Y-%m-%d')

#     filters = []
#     if mode == "incremental":
#         filters.append({"field": "updated_time", "operator": "GREATER_THAN", "value": cutoff_str})
 
#     # REDUCED LIMIT to 100 to prevent "Please reduce the amount of data" error
#     params = {"fields": ADS_FIELDS, "limit": 100, "filtering": json.dumps(filters)}

#     saved = 0
#     try:
#         for raw_ad in client.get_paged(f"{act}/ads", params=params):
#             ad = _normalize_keys(raw_ad)
            
#             creative = ad.get("creative") or {}
#             record = {
#                 "ad_id": int(ad.get("id")),
#                 "adset_id": int(ad.get("adset_id")),
#                 "campaign_id": int(ad.get("campaign_id")),
#                 "name": ad.get("name"),
#                 "status": ad.get("status"),
#                 "effective_status": ad.get("effective_status"),
#                 "thumbnail_url": creative.get("thumbnail_url"),
#                 "image_url": creative.get("image_url"),
#                 "post_id": creative.get("object_story_id"),
#                 "post_link": ad.get("post_link")
#             }
#             upsert_ad(record)
#             saved += 1
            
#         return {"level": "Ads", "account": act, "saved": saved, "ok": True}
#     except Exception as e:
#         logger.error(f"❌ Ads sync failed for {act}: {e}")
#         return {"level": "Ads", "account": act, "saved": saved, "ok": False, "error": str(e)}
    

# Replace your existing upsert_ad and update the sync function
def upsert_ads_batch(records: list[dict]) -> None:
    if not records:
        return

    sql = """
    INSERT INTO ads (
        ad_id, adset_id, campaign_id, name, status,
        effective_status, thumbnail_url, image_url,
        post_id, post_link, updated_at
    ) VALUES (
        %(ad_id)s, %(adset_id)s, %(campaign_id)s, %(name)s, %(status)s,
        %(effective_status)s, %(thumbnail_url)s, %(image_url)s,
        %(post_id)s, %(post_link)s, NOW()
    )
    ON DUPLICATE KEY UPDATE
        adset_id=VALUES(adset_id),
        campaign_id=VALUES(campaign_id),
        name=VALUES(name),
        status=VALUES(status),
        effective_status=VALUES(effective_status),
        thumbnail_url=VALUES(thumbnail_url),
        image_url=VALUES(image_url),
        post_id=VALUES(post_id),
        post_link=VALUES(post_link),
        updated_at=NOW();
    """
    # Assuming your db module has a way to get a raw connection or an executemany wrapper
    from db.db import get_connection 
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.executemany(sql, records)
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def sync_ads_for_account(client, ad_account_id, mode="full", days=30):
    act = f"act_{ad_account_id}"
    cutoff_str = (datetime.now(timezone.utc) - timedelta(days=days)).strftime('%Y-%m-%d')

    filters = []
    if mode == "incremental":
        filters.append({"field": "updated_time", "operator": "GREATER_THAN", "value": cutoff_str})
 
    params = {"fields": ADS_FIELDS, "limit": 100, "filtering": json.dumps(filters)}

    all_records = []
    try:
        for raw_ad in client.get_paged(f"{act}/ads", params=params):
            ad = _normalize_keys(raw_ad)
            creative = ad.get("creative") or {}
            all_records.append({
                "ad_id": int(ad.get("id")),
                "adset_id": int(ad.get("adset_id")),
                "campaign_id": int(ad.get("campaign_id")),
                "name": ad.get("name"),
                "status": ad.get("status"),
                "effective_status": ad.get("effective_status"),
                "thumbnail_url": creative.get("thumbnail_url"),
                "image_url": creative.get("image_url"),
                "post_id": creative.get("object_story_id"),
                "post_link": ad.get("post_link")
            })
        
        if all_records:
            upsert_ads_batch(all_records)
            
        return {"level": "Ads", "account": act, "saved": len(all_records), "ok": True}
    except Exception as e:
        logger.error(f"❌ Ads sync failed for {act}: {e}")
        return {"level": "Ads", "account": act, "saved": len(all_records), "ok": False, "error": str(e)}    
def sync_ads(user_token: str, mode: str = "full", days: int = 30) -> Dict[str, int]:
    """
    Legacy wrapper: NOT threaded. Prefer sync_ads_for_account(client, ...)
    """
    from integrations.meta_graph_client import MetaGraphClient

    client = MetaGraphClient(user_token)

    accounts = query_dict(
        """
        SELECT a.ad_account_id, p.code AS portfolio_code
        FROM ad_accounts a
        JOIN portfolios p ON p.id = a.portfolio_id
        WHERE p.code IN ('RFM','MAGIC_EXTREME')
        ORDER BY p.code, a.ad_account_id
        """
    )

    total_saved = 0
    total_skipped = 0
    total_failed = 0

    for r in accounts:
        res = sync_ads_for_account(
            client=client,
            ad_account_id=int(r["ad_account_id"]),
            portfolio_code=r.get("portfolio_code") or "",
            mode=mode,
            days=days
        )
        total_saved += res.get("saved", 0)
        total_skipped += res.get("skipped", 0)
        total_failed += res.get("failed_ads", 0)

    logger.info(
        f"✅ ads sync done (all accounts). saved={total_saved} skipped={total_skipped} failed={total_failed}"
    )
    return {"saved": total_saved, "skipped": total_skipped, "failed_ads": total_failed}
