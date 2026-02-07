# services/billing_service.py

from __future__ import annotations

from typing import Dict, Optional, Any
from decimal import Decimal, InvalidOperation

from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient
from db.db import execute, query_dict


# نخلي daily_spend_limit optional (نفشل؟ لا.. نعمل fallback)
FIELDS_BASE = "name,currency,amount_spent,spend_cap,balance,account_status,disable_reason"
FIELDS_WITH_DAILY = FIELDS_BASE + ",daily_spend_limit"


CURRENCY_EXPONENT = {
    "JPY": 0, "KRW": 0,
    "KWD": 3, "BHD": 3, "JOD": 3,
}


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None or x == "":
            return None
        return int(x)
    except Exception:
        return None


def _safe_decimal(x: Any) -> Optional[Decimal]:
    try:
        if x is None or x == "":
            return None
        return Decimal(str(x))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _normalize_money(raw_value: Any, currency: Optional[str]) -> Optional[Decimal]:
    """
    Meta غالباً ترجع المبالغ بالـ minor units (سنتات للدولار).
    نحولها لـ major units حسب العملة.
    """
    v = _safe_decimal(raw_value)
    if v is None:
        return None

    exp = CURRENCY_EXPONENT.get((currency or "").upper(), 2)
    divisor = Decimal(10) ** Decimal(exp)

    # نخلي USD وغيرها 2 decimals
    if exp == 2:
        return (v / divisor).quantize(Decimal("0.01"))
    return (v / divisor)


def _get_last_activity_date_from_db(ad_account_id: int) -> Optional[str]:
    row = query_dict(
        """
        SELECT
          COALESCE(
            (
              SELECT DATE_FORMAT(MAX(i.date), '%%Y-%%m-%%d')
              FROM campaigns_daily_insights i
              JOIN campaigns c ON c.campaign_id = i.campaign_id
              WHERE c.ad_account_id = %(ad_account_id)s
            ),
            (
              SELECT DATE_FORMAT(MAX(i.date), '%%Y-%%m-%%d')
              FROM adset_daily_insights i
              JOIN adsets s ON s.adset_id = i.adset_id
              JOIN campaigns c ON c.campaign_id = s.campaign_id
              WHERE c.ad_account_id = %(ad_account_id)s
            ),
            (
              SELECT DATE_FORMAT(MAX(i.date), '%%Y-%%m-%%d')
              FROM ad_daily_insights i
              JOIN ads a ON a.ad_id = i.ad_id
              JOIN adsets s ON s.adset_id = a.adset_id
              JOIN campaigns c ON c.campaign_id = s.campaign_id
              WHERE c.ad_account_id = %(ad_account_id)s
            )
          ) AS last_activity_date
        """,
        {"ad_account_id": ad_account_id},
    )
    if not row:
        return None
    return row[0].get("last_activity_date")


def upsert_billing(record: dict) -> None:
    """
    ما رح نخزن daily_spend_limit إذا جدولك ما فيه العمود.
    (إذا بدك نخزنه، خبريني وأعطيك ALTER + تعديل SQL)
    """
    sql = """
    INSERT INTO billing (
      ad_account_id,
      last_activity_date,
      amount_spent,
      balance,
      spend_cap,
      account_status,
      disable_reason,
      checked_at
    ) VALUES (
      %(ad_account_id)s,
      %(last_activity_date)s,
      %(amount_spent)s,
      %(balance)s,
      %(spend_cap)s,
      %(account_status)s,
      %(disable_reason)s,
      NOW()
    )
    ON DUPLICATE KEY UPDATE
      last_activity_date = COALESCE(VALUES(last_activity_date), last_activity_date),
      amount_spent       = COALESCE(VALUES(amount_spent), amount_spent),
      balance            = COALESCE(VALUES(balance), balance),
      spend_cap          = COALESCE(VALUES(spend_cap), spend_cap),
      account_status     = COALESCE(VALUES(account_status), account_status),
      disable_reason     = COALESCE(VALUES(disable_reason), disable_reason),
      checked_at         = NOW(),
      updated_at         = NOW();
    """
    execute(sql, record)


def _get_account_with_optional_daily(client: MetaGraphClient, act: str) -> Dict[str, Any]:
    """
    نحاول نجيب daily_spend_limit.
    إذا Meta رفضته (#100 nonexisting field)، نعيد الطلب بدون الحقل.
    """
    try:
        return client.get(act, params={"fields": FIELDS_WITH_DAILY})
    except Exception as e:
        msg = str(e)
        # fallback: daily_spend_limit غير موجود
        if "daily_spend_limit" in msg and "nonexisting field" in msg:
            logger.warning(f"⚠️ {act}: daily_spend_limit not supported, fallback to base fields")
            return client.get(act, params={"fields": FIELDS_BASE})
        raise


def sync_billing_for_account(
    user_token: str,
    ad_account_id: int,
    portfolio_code: str = "",
) -> Dict[str, Any]:
    client = MetaGraphClient(user_token)
    act = f"act_{ad_account_id}"

    logger.info(f"▶️ billing sync start {act} portfolio={portfolio_code}")

    try:
        acc = _get_account_with_optional_daily(client, act)

        name = acc.get("name")
        currency = acc.get("currency")

        raw_amount_spent = acc.get("amount_spent")
        raw_balance = acc.get("balance")
        raw_spend_cap = acc.get("spend_cap")
        raw_daily_spend_limit = acc.get("daily_spend_limit")  # ممكن ما تكون موجودة

        amount_spent = _normalize_money(raw_amount_spent, currency)
        balance = _normalize_money(raw_balance, currency)
        spend_cap = _normalize_money(raw_spend_cap, currency)
        daily_spend_limit = _normalize_money(raw_daily_spend_limit, currency) if raw_daily_spend_limit is not None else None

        account_status = _safe_int(acc.get("account_status"))
        disable_reason = _safe_int(acc.get("disable_reason"))

        last_activity_date = _get_last_activity_date_from_db(ad_account_id)

        upsert_billing({
            "ad_account_id": int(ad_account_id),
            "last_activity_date": last_activity_date,
            "amount_spent": float(amount_spent) if amount_spent is not None else None,
            "balance": float(balance) if balance is not None else None,
            "spend_cap": float(spend_cap) if spend_cap is not None else None,
            "account_status": account_status,
            "disable_reason": disable_reason,
        })

        logger.info(f"✅ billing synced {act} portfolio={portfolio_code} balance={balance} amount_spent={amount_spent}")

        return {
            "ok": True,
            "ad_account_id": ad_account_id,
            "ad_account_name": name,
            "currency": currency,
            "last_activity_date": last_activity_date,
            "normalized": {
                "amount_spent": str(amount_spent) if amount_spent is not None else None,
                "balance": str(balance) if balance is not None else None,
                "spend_cap": str(spend_cap) if spend_cap is not None else None,
                "daily_spend_limit": str(daily_spend_limit) if daily_spend_limit is not None else None,
            },
            "raw": {
                "amount_spent": raw_amount_spent,
                "balance": raw_balance,
                "spend_cap": raw_spend_cap,
                "daily_spend_limit": raw_daily_spend_limit,
            }
        }

    except Exception as e:
        logger.error(f"❌ billing failed {act} portfolio={portfolio_code}: {e}")
        return {"ok": False, "ad_account_id": ad_account_id, "error": str(e)}
