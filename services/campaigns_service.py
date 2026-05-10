# services/campaigns_service.py

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from http import client
import json # Use standard json for the filtersimport json
from typing import Dict, Any, Optional

from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient, MetaRateLimitError
from db.db import execute
from services.insights_service import _to_date # Reuse your date helper

# =========================
# Meta fields
# =========================
# ملاحظة: created_time موجود بالـ API بس احنا ما رح نخزنه لأنه مش موجود بجدولك
CAMPAIGN_FIELDS = (
    "id,"
    "name,"
    "objective,"
    "start_time,"
    "status,"
    "effective_status"
)

def _parse_dt(dt_str: Optional[str]):
    if not dt_str:
        return None
    try:
        s = dt_str.replace("Z", "+00:00")
        # handle +0000 -> +00:00
        if len(s) >= 5 and (s[-5] in ["+", "-"]) and s[-3] != ":":
            s = s[:-2] + ":" + s[-2:]
        return datetime.fromisoformat(s).replace(tzinfo=None)
    except Exception:
        return None


def upsert_campaign(record: dict) -> None:
    """
    Upsert into campaigns based on UNIQUE(campaign_id)
    Matches your table exactly.
    """
    sql = """
    INSERT INTO campaigns (
        campaign_id,
        name,
        objective,
        start_time,
        ad_account_id,
        status,
        effective_status,
        first_seen_at,
        last_seen_at
    ) VALUES (
        %(campaign_id)s,
        %(name)s,
        %(objective)s,
        %(start_time)s,
        %(ad_account_id)s,
        %(status)s,
        %(effective_status)s,
        NOW(),
        NOW()
    )
    ON DUPLICATE KEY UPDATE
        name = COALESCE(VALUES(name), name),
        objective = COALESCE(VALUES(objective), objective),
        start_time = COALESCE(VALUES(start_time), start_time),
        status = COALESCE(VALUES(status), status),
        effective_status = COALESCE(VALUES(effective_status), effective_status),
        last_seen_at = NOW(),
        updated_at = NOW();
    """
    execute(sql, record)

def upsert_campaigns_batch(records: list[dict]) -> None:
    if not records: return
    sql = """
    INSERT INTO campaigns (campaign_id, name, objective, start_time, ad_account_id, status, effective_status, last_seen_at)
    VALUES (%(campaign_id)s, %(name)s, %(objective)s, %(start_time)s, %(ad_account_id)s, %(status)s, %(effective_status)s, NOW())
    ON DUPLICATE KEY UPDATE 
        name=VALUES(name), status=VALUES(status), effective_status=VALUES(effective_status), 
        last_seen_at=NOW(), real_status='ACTIVE'
    """
    from db.db import get_connection
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # cursor.executemany(sql, records)
        # conn.commit()
        CHUNK_SIZE = 50

        for i in range(0, len(records), CHUNK_SIZE):
            chunk = records[i:i + CHUNK_SIZE]
            cursor.executemany(sql, chunk)
            conn.commit()
    finally:
        cursor.close()
        conn.close()
def _compute_real_status(effective_status: Optional[str]) -> Optional[str]:
    """
    real_status enum('ACTIVE','PAUSED') حسب طلبك:
    إذا الحملة Effective Status = ACTIVE => ACTIVE
    غير ذلك => PAUSED
    """
    if not effective_status:
        return None
    return "ACTIVE" if effective_status.upper() == "ACTIVE" else "PAUSED"


def update_real_status(campaign_id: int) -> None:
    """
    Optional: يحسب real_status بناءً على adsets داخل الحملة:
    - إذا في أي adset فعال => ACTIVE
    - غير ذلك => PAUSED

    (إذا ما بدك هذا المنطق، احذفي الدالة واستدعائها)
    """
    sql = """
    UPDATE campaigns c
    SET c.real_status = (
        SELECT
            CASE
                WHEN SUM(s.effective_status = 'ACTIVE') > 0 THEN 'ACTIVE'
                ELSE 'PAUSED'
            END
        FROM adsets s
        WHERE s.campaign_id = c.campaign_id
    )
    WHERE c.campaign_id = %(campaign_id)s;
    """
    execute(sql, {"campaign_id": campaign_id})

def sync_campaigns_for_account(client, ad_account_id, mode="full", days=30):
    act = f"act_{ad_account_id}"
    cutoff_str = (datetime.now(timezone.utc) - timedelta(days=days)).strftime('%Y-%m-%d')
    
    # 1. Initialize empty filters list (Business wants all statuses)
    filters = []
    
    # 2. Add time filter only for incremental runs
    if mode == "incremental":
        filters.append({
            "field": "updated_time", 
            "operator": "GREATER_THAN", 
            "value": cutoff_str
        })
    # Filter: Always ACTIVE.
    # filters = [{"field": "effective_status", "operator": "IN", "value": ["ACTIVE"]}]
    # ,"filtering": json.dumps(filters)
    params = {
        "fields": CAMPAIGN_FIELDS,
        "limit": 150,
        "filtering": json.dumps(filters) if filters else None
    }

    saved = 0
    all_records = []
    for c in client.get_paged(f"{act}/campaigns", params=params):
        all_records.append({
            "campaign_id": int(c["id"]),
            "name": c.get("name"),
            "objective": c.get("objective"),
            "start_time": _parse_dt(c.get("start_time")),
            "ad_account_id": ad_account_id,
            "status": c.get("status"),
            "effective_status": c.get("effective_status"),
        })
    
    if all_records:
        all_records.sort(key=lambda x: x["campaign_id"])
        upsert_campaigns_batch(all_records)
    return {"level": "Campaigns", "account": act, "saved": len(all_records)}
