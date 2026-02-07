import os
from datetime import datetime, timedelta, timezone

from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient
from db.db import query_dict
from db.repositories.adset_daily_insights_repo import upsert_adset_daily_insight

# ضع هنا adset_id اللي رجع معك داتا بالمتصفح
TEST_ADSET_ID = "120243174025920699"

FIELDS = "date_start,impressions,reach,spend,frequency"

def _to_int(v):
    try:
        return int(v) if v not in (None, "") else None
    except Exception:
        return None

def _to_float(v):
    try:
        return float(v) if v not in (None, "") else None
    except Exception:
        return None

def main():
    token = os.getenv("META_USER_TOKEN")
    if not token:
        print("❌ META_USER_TOKEN is missing. Set it first.")
        return

    client = MetaGraphClient(token)

    # DB confirm
    print("DB:", query_dict("SELECT DATABASE() AS db")[0]["db"])
    print("adsets:", query_dict("SELECT COUNT(*) AS c FROM adsets")[0]["c"])
    print("adset_daily_insights(before):", query_dict("SELECT COUNT(*) AS c FROM adset_daily_insights")[0]["c"])

    since = (datetime.now(timezone.utc) - timedelta(days=60)).date().isoformat()
    until = datetime.now(timezone.utc).date().isoformat()

    params = {
        "fields": FIELDS,
        "time_increment": 1,
        "time_range[since]": since,
        "time_range[until]": until,
        "limit": 200,
    }

    logger.info(f"Fetching insights for adset_id={TEST_ADSET_ID} since={since} until={until}")

    rows = list(client.get_paged(f"{TEST_ADSET_ID}/insights", params=params))
    print("API rows:", len(rows))

    if rows:
        print("FIRST ROW:", rows[0])

        it = rows[0]
        rec = {
            "adset_id": int(TEST_ADSET_ID),
            "date": it.get("date_start"),
            "impressions": _to_int(it.get("impressions")),
            "reach": _to_int(it.get("reach")),
            "spend": _to_float(it.get("spend")),
            "frequency": _to_float(it.get("frequency")),
            "checked_at": datetime.now(),
        }

        upsert_adset_daily_insight(rec)
        print("Inserted one row ✅")

    print("adset_daily_insights(after):", query_dict("SELECT COUNT(*) AS c FROM adset_daily_insights")[0]["c"])

if __name__ == "__main__":
    main()
