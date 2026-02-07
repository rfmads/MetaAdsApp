# services/magic_ad_accounts_service.py
from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient
from db.db import query_dict
from db.repositories.ad_accounts_repo import upsert_ad_account
from utils.datetime_utils import parse_meta_datetime

# ✅ عدّلي هذا
PORTFOLIO_CODE = "MAGIC_EXTREME"   # مثال: MAGIC أو "Magic Extream" حسب اللي عملتيه بجدول portfolios

# ✅ قائمة الحسابات (name, account_id)
MAGIC_ACCOUNTS = [
    ("Flamenco", 175142654),
    ("PARIS Fashion", 1692072614538636),
    ("Amori AD", 8566890756680291),
    ("Ahmad silver", 1263155321365192),
    ("Motagem_mahmoud", 447490355018427),
    ("ازهار وشموع المجد", 253109122472094),
    ("Hamza Moda", 239425029565119),
    ("Ahmad Ramzi", 1016760536714675),
    ("بوتيك تبارك", 3207220579576155),
    ("SHOWMAN", 347492718),
    ("OFF GOLD", 172347747910712),
    ("Rozenna", 1752624195204230),
    ("محمص القرعاوي", 2626495067427827),
    ("Zain moda", 746913743407503),
    ("Elina Fashion", 1175134053110541),
    ("HIGH CLASS", 217153224232854),
]

# حقول نطلبها من الحساب الإعلاني
FIELDS = "id,name,currency,timezone_name,created_time,account_status,disable_reason,spend_cap,amount_spent"

def _get_portfolio_id() -> int:
    row = query_dict("SELECT id FROM portfolios WHERE code=%(code)s LIMIT 1", {"code": PORTFOLIO_CODE})
    if not row:
        raise Exception(f"Portfolio code '{PORTFOLIO_CODE}' not found in DB.")
    return int(row[0]["id"])

def sync_magic_ad_accounts(user_token: str) -> None:
    logger.info("🚀 Starting MAGIC ad accounts import...")

    portfolio_id = _get_portfolio_id()
    client = MetaGraphClient(user_token)

    saved = 0
    no_permission = 0
    not_found = 0
    failed = 0

    for name_hint, account_id in MAGIC_ACCOUNTS:
        act_id = f"act_{account_id}"
        try:
            data = client.get(act_id, params={"fields": FIELDS})

            # Meta بيرجع id مثل: "act_123" أحياناً
            raw_id = data.get("id") or act_id
            numeric_id = int(str(raw_id).replace("act_", ""))

            record = {
                "ad_account_id": numeric_id,
                "name": data.get("name") or name_hint,
                "currency": data.get("currency"),
                "account_creation_date": parse_meta_datetime(data.get("created_time")),
                "timezone": data.get("timezone_name"),
                "portfolio_id": portfolio_id,
            }

            upsert_ad_account(record)
            saved += 1
            logger.info(f"✅ saved {numeric_id} | {record['name']}")

        except Exception as e:
            msg = str(e)

            # ✅ Permissions (#200)
            if "code\":200" in msg or "(#200)" in msg or "ads_read" in msg or "ads_management" in msg:
                no_permission += 1
                logger.warning(f"🚫 no permission for {act_id} ({name_hint})")
                continue

            # ✅ Not found / no access (code=100 subcode=33)
            if "error_subcode\":33" in msg or "Unsupported get request" in msg:
                not_found += 1
                logger.warning(f"❓ not found/no access for {act_id} ({name_hint})")
                continue

            failed += 1
            logger.error(f"⚠️ failed {act_id} ({name_hint}): {e}")

    logger.info(
        f"✅ MAGIC import done. saved={saved} | no_permission={no_permission} | not_found={not_found} | failed={failed}"
    )
