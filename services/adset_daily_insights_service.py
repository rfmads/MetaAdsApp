# services/adset_daily_insights_service.py
import json
import time
from datetime import datetime, timedelta, timezone

from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient
from db.db import query_dict
from db.repositories.adset_daily_insights_repo import upsert_adset_daily_insight


MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

INSIGHT_FIELDS = "date_start,impressions,reach,spend,frequency"


def _to_int(v):
    try:
        return int(v) if v is not None and v != "" else None
    except Exception:
        return None


def _to_float(v):
    try:
        return float(v) if v is not None and v != "" else None
    except Exception:
        return None


def sync_adset_daily_insights_last_60_days(user_token: str) -> None:
    """
    Pull adset daily insights for last 60 days for ALL adsets in DB.
    Uses:
      /{adset_id}/insights?time_increment=1&time_range={"since": "...", "until": "..."}
    """
    client = MetaGraphClient(user_token)

    adsets = query_dict("SELECT adset_id FROM adsets")
    logger.info(f"Syncing adset_daily_insights for adsets={len(adsets)} (last 60 days)")

    since = (datetime.now(timezone.utc) - timedelta(days=60)).date().isoformat()
    until = datetime.now(timezone.utc).date().isoformat()

    saved = 0
    empty = 0
    failed = 0

    for row in adsets:
        adset_id = str(row["adset_id"])

        params = {
            "fields": INSIGHT_FIELDS,
            "time_increment": 1,
            # ✅ IMPORTANT: send time_range as JSON string (Meta expects this)
            "time_range": json.dumps({"since": since, "until": until}),
            "limit": 200,
        }

        attempt = 0
        while attempt < MAX_RETRIES:
            try:
                count_rows = 0
                first_row_logged = False

                for it in client.get_paged(f"{adset_id}/insights", params=params):
                    count_rows += 1

                    if not first_row_logged:
                        logger.info(f"Sample insight row for adset_id={adset_id}: {it}")
                        first_row_logged = True

                    rec = {
                        "adset_id": int(adset_id),
                        "date": it.get("date_start"),              # 'YYYY-MM-DD'
                        "impressions": _to_int(it.get("impressions")),
                        "reach": _to_int(it.get("reach")),
                        "spend": _to_float(it.get("spend")),
                        "frequency": _to_float(it.get("frequency")),
                        "checked_at": datetime.now(),              # stored as DATETIME
                    }
                    upsert_adset_daily_insight(rec)
                    saved += 1

                if count_rows == 0:
                    empty += 1

                break  # ✅ success for this adset

            except Exception as e:
                attempt += 1
                logger.error(f"⚠️ adset insights failed attempt={attempt} adset_id={adset_id}: {e}")

                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                else:
                    failed += 1

    logger.info(f"✅ adset_daily_insights sync done. saved={saved}, empty={empty}, failed={failed}")
