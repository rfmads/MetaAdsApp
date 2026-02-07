from integrations.meta_graph_client import MetaGraphClient
from db.repositories.ad_accounts_repo import upsert_ad_account
from logs.logger import logger

BUSINESS_ID = "370817974370019"
PORTFOLIO_ID = 2   # عدّليها حسب DB
FIELDS = "id,name,currency,timezone_name,created_time"

MAGIC_ACCOUNTS = [
    "175142654",
    "1692072614538636",
    "8566890756680291",
    "1263155321365192",
    "447490355018427",
    "253109122472094",
    "239425029565119",
    "1016760536714675",
    "3207220579576155",
    "347492718",
    "172347747910712",
    "1752624195204230",
    "2626495067427827",
    "746913743407503",
    "1175134053110541",
    "217153224232854",
]

def run(user_token: str):
    client = MetaGraphClient(user_token)
    saved = 0
    failed = 0

    for acc_id in MAGIC_ACCOUNTS:
        act_id = f"act_{acc_id}"
        try:
            data = client.get(act_id, params={"fields": FIELDS})

            upsert_ad_account({
                "ad_account_id": int(acc_id),
                "name": data.get("name"),
                "currency": data.get("currency"),
                "timezone": data.get("timezone_name"),
                "account_creation_date": data.get("created_time"),
                "portfolio_id": PORTFOLIO_ID,
            })

            saved += 1
            logger.info(f"✅ Imported {act_id} - {data.get('name')}")

        except Exception as e:
            failed += 1
            logger.error(f"⚠️ Failed {act_id}: {e}")

    logger.info(f"🎯 Magic accounts import done. saved={saved}, failed={failed}")
