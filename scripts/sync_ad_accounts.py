# scripts/sync_ad_accounts.py

from datetime import datetime
import os

from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient
from db.db import execute, query_dict


# =========================
# Config
# =========================
FIELDS = (
    "id,"
    "name,"
    "currency,"
    "timezone_name,"
    "created_time,"
    "account_status"
)


# =========================
# Helpers
# =========================
def _parse_datetime(dt_str):
    """Parse Meta datetime string -> naive datetime (MySQL DATETIME)."""
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


# =========================
# DB Upsert
# =========================
def upsert_ad_account(record: dict) -> None:
    """Upsert into ad_accounts (matches your table schema)."""
    sql = """
    INSERT INTO ad_accounts (
        ad_account_id,
        name,
        currency,
        account_creation_date,
        timezone,
        portfolio_id,
        first_seen_at,
        last_seen_at
    ) VALUES (
        %(ad_account_id)s,
        %(name)s,
        %(currency)s,
        %(account_creation_date)s,
        %(timezone)s,
        %(portfolio_id)s,
        NOW(),
        NOW()
    )
    ON DUPLICATE KEY UPDATE
        name=COALESCE(VALUES(name), name),
        currency=COALESCE(VALUES(currency), currency),
        account_creation_date=COALESCE(VALUES(account_creation_date), account_creation_date),
        timezone=COALESCE(VALUES(timezone), timezone),
        portfolio_id=COALESCE(VALUES(portfolio_id), portfolio_id),
        last_seen_at=NOW();
    """
    execute(sql, record)


# =========================
# Main Sync Logic
# =========================
def sync_ad_accounts(user_token: str) -> dict:
    """
    Sync all ad accounts accessible by the user token.
    Returns: {"saved": int, "skipped": int}
    """
    client = MetaGraphClient(user_token)

    saved = 0
    skipped = 0

    logger.info("▶️ ad_accounts sync start")

    # Load portfolios mapping (optional)
    portfolios = {}
    try:
        rows = query_dict("SELECT id, code FROM portfolios")
        portfolios = {r["code"]: r["id"] for r in rows}
    except Exception:
        portfolios = {}

    # Fetch ad accounts
    for acc in client.get_paged(
        "me/adaccounts",
        params={"fields": FIELDS, "limit": 200}
    ):
        raw_id = acc.get("id")
        if not raw_id:
            skipped += 1
            continue

        # Meta returns either "act_123" or "123"
        acc_id = str(raw_id).replace("act_", "")

        # portfolio mapping (simple heuristic)
        portfolio_id = None
        name = acc.get("name") or ""
        for code, pid in portfolios.items():
            if code and code.lower() in name.lower():
                portfolio_id = pid
                break

        record = {
            "ad_account_id": int(acc_id),
            "name": name,
            "currency": acc.get("currency"),
            "account_creation_date": _parse_datetime(acc.get("created_time")),
            "timezone": acc.get("timezone_name"),
            "portfolio_id": portfolio_id,
        }

        upsert_ad_account(record)
        saved += 1

    logger.info(f"✅ ad_accounts sync done. saved={saved} skipped={skipped}")
    return {"saved": saved, "skipped": skipped}


# =========================
# CLI Entry Point
# =========================
if __name__ == "__main__":
    user_token = os.getenv("META_USER_TOKEN")
    if not user_token:
        print("❌ META_USER_TOKEN is missing. Set it first.")
        raise SystemExit(1)

    sync_ad_accounts(user_token)
