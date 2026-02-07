# services/campaigns_service.py

from __future__ import annotations

from datetime import datetime
from typing import Dict, Any, Optional

from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient, MetaRateLimitError
from db.db import execute


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


def sync_campaigns_for_account(
    user_token: str,
    ad_account_id: int,
    portfolio_code: str = "",
    mode: str = "full",
    days: int = 365,
) -> Dict[str, Any]:
    """
    Sync campaigns for ad account -> DB
    """
    client = MetaGraphClient(user_token)
    act = f"act_{ad_account_id}"

    saved = 0
    skipped = 0

    logger.info(f"▶️ campaigns sync start {act} portfolio={portfolio_code} mode={mode} days={days}")

    try:
        params = {"fields": CAMPAIGN_FIELDS, "limit": 200}

        for c in client.get_paged(f"{act}/campaigns", params=params):
            cid = c.get("id")
            if not cid:
                skipped += 1
                continue

            campaign_id = int(str(cid).replace("camp_", "")) if str(cid).isdigit() else int(cid)

            record = {
                "campaign_id": campaign_id,
                "name": c.get("name"),
                "objective": c.get("objective"),
                "start_time": _parse_dt(c.get("start_time")),
                "ad_account_id": int(ad_account_id),
                "status": c.get("status"),
                "effective_status": c.get("effective_status"),
            }

            upsert_campaign(record)

            # ✅ خيار 1: real_status سريع بناءً على effective_status
            sql_real = """
            UPDATE campaigns
            SET real_status = %(real_status)s
            WHERE campaign_id = %(campaign_id)s
            """
            execute(sql_real, {
                "campaign_id": campaign_id,
                "real_status": _compute_real_status(c.get("effective_status")),
            })

            # ✅ خيار 2 (أدق): real_status حسب adsets داخل الحملة
            # update_real_status(campaign_id)

            saved += 1

        logger.info(f"✅ campaigns synced for {act} portfolio={portfolio_code} saved={saved} skipped={skipped}")
        return {"ok": True, "saved": saved, "skipped": skipped}

    except MetaRateLimitError as e:
        logger.error(f"⚠️ campaigns rate limit for {act}: {e}")
        return {"ok": False, "saved": saved, "skipped": skipped, "error": str(e)}

    except Exception as e:
        logger.error(f"❌ campaigns failed for {act} portfolio={portfolio_code}: {e}")
        return {"ok": False, "saved": saved, "skipped": skipped, "error": str(e)}
