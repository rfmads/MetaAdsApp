# services/ad_accounts_service.py
from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient
from utils.datetime_utils import parse_meta_datetime
from db.repositories.portfolios_repo import get_or_create_portfolio
from db.repositories.ad_accounts_repo import upsert_ad_account

FIELDS = "account_id,id,name,currency,timezone_name,created_time,account_status"

def _normalize_act_id(act_id: str | None, account_id: str | None) -> int | None:
    """
    input:
      id: "act_240939565" OR account_id: "240939565"
    output:
      240939565 (int)
    """
    if account_id and str(account_id).isdigit():
        return int(account_id)
    if act_id and act_id.startswith("act_") and act_id[4:].isdigit():
        return int(act_id[4:])
    return None

def sync_rfm_ad_accounts(user_token: str, business_id: str) -> None:
    """
    Pull ONLY ad accounts connected to Business RFM:
    - client_ad_accounts (shared from clients)
    - owned_ad_accounts (owned by agency)
    Save them into ad_accounts with portfolio_id pointing to RFM.
    """
    client = MetaGraphClient(user_token)

    portfolio_id = get_or_create_portfolio(
        code="RFM",
        name="RFM Portfolio",
        description="Only RFM ad accounts"
    )

    endpoints = [
        f"{business_id}/client_ad_accounts",
        f"{business_id}/owned_ad_accounts",
    ]

    saved = 0
    seen = set()

    for ep in endpoints:
        logger.info(f"Syncing ad accounts from {ep} ...")

        for a in client.get_paged(ep, params={"fields": FIELDS, "limit": 200}):
            ad_account_id = _normalize_act_id(a.get("id"), a.get("account_id"))
            if not ad_account_id:
                continue

            if ad_account_id in seen:
                continue
            seen.add(ad_account_id)

            rec = {
                "ad_account_id": ad_account_id,
                "name": a.get("name"),
                "currency": a.get("currency"),
                "account_creation_date": parse_meta_datetime(a.get("created_time")),
                "timezone": a.get("timezone_name"),
                "portfolio_id": portfolio_id,
            }
            upsert_ad_account(rec)
            saved += 1

    logger.info(f"✅ RFM ad_accounts sync done. saved={saved}")
