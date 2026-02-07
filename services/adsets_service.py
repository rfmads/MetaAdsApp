# services/adsets_service.py

from datetime import datetime, timedelta, timezone

from logs.logger import logger
from db.db import query_dict
from db.repositories.adsets_repo import upsert_adset
from utils.datetime_utils import parse_meta_datetime


# ✅ Fetch adsets directly from ad account (LESS REQUESTS)
# We MUST include campaign_id so we can save FK to campaigns table.
FIELDS = (
    "id,name,status,effective_status,daily_budget,start_time,updated_time,"
    "billing_event,optimization_goal,campaign_id"
)


def _as_utc(dt):
    """Ensure datetime is timezone-aware UTC."""
    if not dt:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def sync_adsets_for_account(
    client,                     # ✅ injected MetaGraphClient (from thread)
    ad_account_id: int,
    portfolio_code: str = "",
    mode: str = "full",
    days: int = 30,
) -> dict:
    """
    Sync adsets for ONE ad account using:
      ✅ act_{ad_account_id}/adsets   (instead of campaign_id/adsets)

    mode:
      - full: insert/update all adsets
      - incremental: only adsets updated/start within last `days`

    Returns:
      {"ok": bool, "saved": int, "skipped": int, "missing_campaigns": int}
    """
    act_id = f"act_{ad_account_id}"
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    saved = 0
    skipped = 0
    missing_campaigns = 0

    # Optional: load existing campaigns for this account to avoid FK failures
    # (Because adsets.campaign_id has FK to campaigns.campaign_id)
    existing_campaigns = set()
    try:
        rows = query_dict(
            """
            SELECT campaign_id
            FROM campaigns
            WHERE ad_account_id = %(ad_account_id)s
            """,
            {"ad_account_id": ad_account_id},
        )
        existing_campaigns = {int(r["campaign_id"]) for r in rows}
    except Exception:
        # If query fails, we still continue; FK errors will show in logs.
        existing_campaigns = set()

    try:
        for a in client.get_paged(f"{act_id}/adsets", {"fields": FIELDS, "limit": 200}):
            # Parse dates
            updated = _as_utc(parse_meta_datetime(a.get("updated_time")))
            start = _as_utc(parse_meta_datetime(a.get("start_time")))

            if mode == "incremental":
                if not ((updated and updated >= cutoff) or (start and start >= cutoff)):
                    skipped += 1
                    continue

            # campaign_id is required for FK
            campaign_id = a.get("campaign_id")
            if not campaign_id:
                skipped += 1
                continue

            campaign_id_int = int(campaign_id)

            # Avoid FK error: if campaign not found in DB, skip & count it
            # (This can happen if campaigns sync failed or partial because of rate limit)
            if existing_campaigns and campaign_id_int not in existing_campaigns:
                missing_campaigns += 1
                skipped += 1
                continue

            upsert_adset({
                "adset_id": int(a["id"]),
                "campaign_id": campaign_id_int,
                "ad_account_id": int(ad_account_id),
                "name": a.get("name"),
                "status": a.get("status"),
                "effective_status": a.get("effective_status"),
                "daily_budget": a.get("daily_budget"),
                "start_time": start.replace(tzinfo=None) if start else None,  # MySQL datetime naive
                "billing_event": a.get("billing_event"),
                "optimization_goal": a.get("optimization_goal"),
            })
            saved += 1

        logger.info(
            f"✅ adsets synced for {act_id} portfolio={portfolio_code} "
            f"saved={saved} skipped={skipped} missing_campaigns={missing_campaigns}"
        )
        return {
            "ok": True,
            "saved": saved,
            "skipped": skipped,
            "missing_campaigns": missing_campaigns,
        }

    except Exception as e:
        logger.error(f"❌ adsets failed for {act_id} portfolio={portfolio_code}: {e}")
        return {
            "ok": False,
            "saved": saved,
            "skipped": skipped,
            "missing_campaigns": missing_campaigns,
            "error": str(e),
        }


def sync_adsets(user_token: str) -> None:
    """
    Backward-compatible: sync adsets for ALL accounts (not threaded).
    Threaded runner should call sync_adsets_for_account(client, ...) instead.
    """
    from integrations.meta_graph_client import MetaGraphClient

    client = MetaGraphClient(user_token)

    accounts = query_dict("""
        SELECT a.ad_account_id, p.code AS portfolio_code
        FROM ad_accounts a
        JOIN portfolios p ON p.id = a.portfolio_id
        WHERE p.code IN ('RFM','MAGIC_EXTREME')
        ORDER BY p.code, a.ad_account_id
    """)

    total_saved = 0
    total_skipped = 0
    total_missing_campaigns = 0
    failed_accounts = 0

    for row in accounts:
        res = sync_adsets_for_account(
            client=client,
            ad_account_id=int(row["ad_account_id"]),
            portfolio_code=row["portfolio_code"],
            mode="incremental",
            days=30,
        )

        total_saved += res.get("saved", 0)
        total_skipped += res.get("skipped", 0)
        total_missing_campaigns += res.get("missing_campaigns", 0)

        if not res.get("ok"):
            failed_accounts += 1

    logger.info(
        f"✅ adsets sync done. saved={total_saved} skipped={total_skipped} "
        f"missing_campaigns={total_missing_campaigns} failed_accounts={failed_accounts}"
    )
