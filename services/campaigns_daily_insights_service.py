# services/campaigns_daily_insights_service.py
import json
import time
from datetime import datetime, timedelta, timezone

from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient
from db.db import query_dict
from db.repositories.campaigns_daily_insights_repo import upsert_campaign_daily_insight
from services._insights_utils import extract_results_and_cpr

MAX_RETRIES = 3
RETRY_DELAY = 5

INSIGHTS_FIELDS = "date_start,spend,impressions,reach,frequency,results,cost_per_result,actions,cost_per_action_type"


def _utc_today_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _since_date(days: int) -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=days)).isoformat()


def sync_campaigns_daily_insights_last_n_days(user_token: str, days: int = 60) -> None:
    client = MetaGraphClient(user_token)

    rows = query_dict("SELECT campaign_id FROM campaigns")
    logger.info(f"Syncing campaigns_daily_insights for campaigns={len(rows)} days={days}")

    since = _since_date(days)
    until = _utc_today_date()

    saved = 0
    skipped = 0
    failed = 0

    for r in rows:
        campaign_id = r["campaign_id"]

        attempt = 0
        while attempt < MAX_RETRIES:
            try:
                params = {
                    "fields": INSIGHTS_FIELDS,
                    "time_increment": 1,
                    "time_range": json.dumps({"since": since, "until": until}),
                    "limit": 500,
                }

                # paging
                any_row = False
                for row in client.get_paged(f"{campaign_id}/insights", params=params):
                    any_row = True
                    date_str = row.get("date_start")
                    if not date_str:
                        continue

                    results, cpr = extract_results_and_cpr(row)

                    rec = {
                        "campaign_id": int(campaign_id),
                        "date": date_str,  # 'YYYY-MM-DD'
                        "results": results,
                        "cost_per_result": cpr,
                        "spend": float(row["spend"]) if row.get("spend") is not None else None,
                        "impressions": int(row["impressions"]) if row.get("impressions") is not None else None,
                        "reach": int(row["reach"]) if row.get("reach") is not None else None,
                        "frequency": float(row["frequency"]) if row.get("frequency") is not None else None,
                    }
                    upsert_campaign_daily_insight(rec)
                    saved += 1

                if not any_row:
                    skipped += 1

                # success
                break

            except Exception as e:
                attempt += 1
                logger.error(f"Meta API campaigns insights failed attempt={attempt} campaign_id={campaign_id}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                else:
                    failed += 1

    logger.info(f"✅ campaigns_daily_insights sync done. saved={saved}, skipped={skipped}, failed={failed}")
