# services/insights_service.py

from __future__ import annotations

from datetime import datetime, timedelta, timezone, date
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
    ) VALUES (
        %(campaign_id)s, %(date)s, %(results)s, %(cost_per_result)s, %(spend)s,
        %(impressions)s, %(reach)s, %(frequency)s, NOW()
    )
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
    ) VALUES (
        %(adset_id)s, %(date)s, %(results)s, %(cost_per_result)s, %(spend)s,
        %(impressions)s, %(reach)s, %(frequency)s, NOW()
    )
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

def upsert_ad_daily_insight(r: dict) -> None:
    sql = """
    INSERT INTO ad_daily_insights (
        ad_id, date, results, cost_per_result, spend, impressions, reach, frequency, checked_at
    ) VALUES (
        %(ad_id)s, %(date)s, %(results)s, %(cost_per_result)s, %(spend)s,
        %(impressions)s, %(reach)s, %(frequency)s, NOW()
    )
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
    """
    Fetch insights via:
      act_{ad_account_id}/insights?level=...&time_increment=1&date_preset=last_30d

    ✅ FIX: Use date_preset (string) to avoid MetaGraphClient dropping dict params.
    """
    act = f"act_{ad_account_id}"
    endpoint = f"{act}/insights"

    params = {
        "level": level,
        "fields": INSIGHTS_FIELDS,
        "time_increment": 1,
        "limit": 200,
        "date_preset": _date_preset_for_days(days),
    }

    saved = 0
    skipped = 0

    logger.info(f"▶️ insights start {act} level={level} days={days} portfolio={portfolio_code}")

    for row in client.get_paged(endpoint, params=params):
        try:
            row = row or {}
            d = _to_date(row.get("date_start"))
            if not d:
                skipped += 1
                continue

            impressions = _to_int(row.get("impressions"), default=0)
            reach = _to_int(row.get("reach"), default=0)
            spend = _to_decimal(row.get("spend"))
            freq = _to_decimal(row.get("frequency"))

            results, cpr = _pick_results_and_cpr(row)

            if level == "campaign":
                cid = row.get("campaign_id")
                if not cid:
                    skipped += 1
                    continue
                upsert_campaign_daily_insight({
                    "campaign_id": int(cid),
                    "date": d,
                    "results": results,
                    "cost_per_result": cpr,
                    "spend": spend,
                    "impressions": impressions,
                    "reach": reach,
                    "frequency": freq,
                })

            elif level == "adset":
                adset_id = row.get("adset_id")
                if not adset_id:
                    skipped += 1
                    continue
                upsert_adset_daily_insight({
                    "adset_id": int(adset_id),
                    "date": d,
                    "results": results,
                    "cost_per_result": cpr,
                    "spend": spend,
                    "impressions": impressions,
                    "reach": reach,
                    "frequency": freq,
                })

            elif level == "ad":
                ad_id = row.get("ad_id")
                if not ad_id:
                    skipped += 1
                    continue
                upsert_ad_daily_insight({
                    "ad_id": int(ad_id),
                    "date": d,
                    "results": results,
                    "cost_per_result": cpr,
                    "spend": spend,
                    "impressions": impressions,
                    "reach": reach,
                    "frequency": freq,
                })
            else:
                skipped += 1
                continue

            saved += 1
            if progress_every and saved % progress_every == 0:
                logger.info(f"⏳ insights progress {act} level={level} saved={saved} skipped={skipped}")

        except Exception as e:
            skipped += 1
            logger.warning(f"⚠️ insights row skipped {act} level={level}: {e}")

    logger.info(f"✅ insights done {act} level={level} saved={saved} skipped={skipped}")
    return {"saved": saved, "skipped": skipped}


# =========================
# Public services (per account)
# =========================

def sync_campaign_daily_insights_for_account(
    user_token: str,
    ad_account_id: int,
    portfolio_code: str = "",
    days: int = 30,
) -> Dict[str, int]:
    client = MetaGraphClient(user_token)
    try:
        return _sync_level_for_account(client, ad_account_id, "campaign", days, portfolio_code)
    except MetaObjectAccessError as e:
        logger.warning(f"⚠️ campaign insights skipped act_{ad_account_id}: {e}")
        return {"saved": 0, "skipped": 0, "error": str(e)}
    except Exception as e:
        logger.error(f"❌ campaign insights failed act_{ad_account_id}: {e}")
        return {"saved": 0, "skipped": 0, "error": str(e)}

def sync_adset_daily_insights_for_account(
    user_token: str,
    ad_account_id: int,
    portfolio_code: str = "",
    days: int = 30,
) -> Dict[str, int]:
    client = MetaGraphClient(user_token)
    try:
        return _sync_level_for_account(client, ad_account_id, "adset", days, portfolio_code)
    except MetaObjectAccessError as e:
        logger.warning(f"⚠️ adset insights skipped act_{ad_account_id}: {e}")
        return {"saved": 0, "skipped": 0, "error": str(e)}
    except Exception as e:
        logger.error(f"❌ adset insights failed act_{ad_account_id}: {e}")
        return {"saved": 0, "skipped": 0, "error": str(e)}

def sync_ad_daily_insights_for_account(
    user_token: str,
    ad_account_id: int,
    portfolio_code: str = "",
    days: int = 30,
) -> Dict[str, int]:
    client = MetaGraphClient(user_token)
    try:
        return _sync_level_for_account(client, ad_account_id, "ad", days, portfolio_code)
    except MetaObjectAccessError as e:
        logger.warning(f"⚠️ ad insights skipped act_{ad_account_id}: {e}")
        return {"saved": 0, "skipped": 0, "error": str(e)}
    except Exception as e:
        logger.error(f"❌ ad insights failed act_{ad_account_id}: {e}")
        return {"saved": 0, "skipped": 0, "error": str(e)}
