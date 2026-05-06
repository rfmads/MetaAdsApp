import os
from datetime import datetime

from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient
from db.db import execute, query_dict
from db.config_store import get_config
from services.job_service import heartbeat

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
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _load_account_portfolios():
    """
    Loads mapping:
    ad_account_id -> portfolio_id
    """
    try:
        rows = query_dict("""
            SELECT ad_account_id, portfolio_id
            FROM portfolio_accounts
        """)
        return {r["ad_account_id"]: r["portfolio_id"] for r in rows}
    except Exception as e:
        logger.error(f"Failed loading portfolio_accounts: {e}")
        return {}


# =========================
# DB Upsert
# =========================

def _upsert(record):
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
        name = COALESCE(VALUES(name), name),
        currency = COALESCE(VALUES(currency), currency),
        account_creation_date = COALESCE(VALUES(account_creation_date), account_creation_date),
        timezone = COALESCE(VALUES(timezone), timezone),
        portfolio_id = COALESCE(VALUES(portfolio_id), portfolio_id),
        last_seen_at = NOW();
    """
    execute(sql, record)


# =========================
# MAIN ENTRY
# =========================

def run():
# 1. Pull token from DB instead of OS environment
    user_token = get_config("META_USER_TOKEN")
    
    if not user_token:
        logger.error("❌ META_USER_TOKEN missing in database 'sys_config' table")
        # You can choose to raise an exception or return gracefully
        return {"ok": False, "error": "Missing Token"}

    client = MetaGraphClient(user_token)

    logger.info("🚀 START ad_accounts sync")

    # 🔥 Load portfolio mapping from new table
    account_portfolios = _load_account_portfolios()

    saved = 0
    skipped = 0

    for acc in client.get_paged(
        "me/adaccounts",
        params={"fields": FIELDS, "limit": 200}
    ):

        raw_id = acc.get("id")
        if not raw_id:
            skipped += 1
            continue

        acc_id = int(str(raw_id).replace("act_", ""))

        record = {
            "ad_account_id": acc_id,
            "name": acc.get("name"),
            "currency": acc.get("currency"),
            "account_creation_date": _parse_datetime(acc.get("created_time")),
            "timezone": acc.get("timezone_name"),

            # ✅ NEW SOURCE OF TRUTH
            "portfolio_id": account_portfolios.get(acc_id),
        }
        # ❤️ HEARTBEAT: Every time a single task finishes, update the job timestamp
        _upsert(record)
        saved += 1

    logger.info(f"✅ DONE ad_accounts saved={saved} skipped={skipped}")

    return {"saved": saved, "skipped": skipped}


# =========================
# CLI
# =========================

if __name__ == "__main__":
    run()


# import os
# from datetime import datetime

# from logs.logger import logger
# from integrations.meta_graph_client import MetaGraphClient
# from db.db import execute, query_dict


# FIELDS = (
#     "id,"
#     "name,"
#     "currency,"
#     "timezone_name,"
#     "created_time,"
#     "account_status"
# )


# def _parse_datetime(dt_str):
#     if not dt_str:
#         return None
#     try:
#         return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).replace(tzinfo=None)
#     except Exception:
#         return None


# def _load_portfolios():
#     try:
#         rows = query_dict("SELECT id, code FROM portfolios")
#         return {r["code"]: r["id"] for r in rows}
#     except Exception:
#         return {}


# def _resolve_portfolio(name, portfolios):
#     for code, pid in portfolios.items():
#         if code and code.lower() in (name or "").lower():
#             return pid
#     return None


# def _upsert(record):
#     sql = """
#     INSERT INTO ad_accounts (
#         ad_account_id,
#         name,
#         currency,
#         account_creation_date,
#         timezone,
#         portfolio_id,
#         first_seen_at,
#         last_seen_at
#     ) VALUES (
#         %(ad_account_id)s,
#         %(name)s,
#         %(currency)s,
#         %(account_creation_date)s,
#         %(timezone)s,
#         %(portfolio_id)s,
#         NOW(),
#         NOW()
#     )
#     ON DUPLICATE KEY UPDATE
#         name=COALESCE(VALUES(name), name),
#         currency=COALESCE(VALUES(currency), currency),
#         account_creation_date=COALESCE(VALUES(account_creation_date), account_creation_date),
#         timezone=COALESCE(VALUES(timezone), timezone),
#         portfolio_id=COALESCE(VALUES(portfolio_id), portfolio_id),
#         last_seen_at=NOW();
#     """
#     execute(sql, record)


# # ✅ THIS is the main entry (IMPORTANT)
# def run():
#     user_token = os.getenv("META_USER_TOKEN")
#     if not user_token:
#         raise Exception("META_USER_TOKEN missing")

#     client = MetaGraphClient(user_token)

#     logger.info("🚀 START ad_accounts")

#     portfolios = _load_portfolios()

#     saved = 0
#     skipped = 0

#     for acc in client.get_paged("me/adaccounts", params={"fields": FIELDS, "limit": 200}):

#         raw_id = acc.get("id")
#         if not raw_id:
#             skipped += 1
#             continue

#         acc_id = str(raw_id).replace("act_", "")

#         record = {
#             "ad_account_id": int(acc_id),
#             "name": acc.get("name"),
#             "currency": acc.get("currency"),
#             "account_creation_date": _parse_datetime(acc.get("created_time")),
#             "timezone": acc.get("timezone_name"),
#             "portfolio_id": _resolve_portfolio(acc.get("name"), portfolios),
#         }

#         _upsert(record)
#         saved += 1

#     logger.info(f"✅ DONE ad_accounts saved={saved} skipped={skipped}")

#     return {"saved": saved, "skipped": skipped}