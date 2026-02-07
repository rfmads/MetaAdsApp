# services/ads_service.py

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from logs.logger import logger
from integrations.meta_graph_client import MetaObjectAccessError
from db.db import query_dict, execute
from utils.datetime_utils import parse_meta_datetime


# =========================
# Config
# =========================

# Fetch ads directly from ad account (LESS REQUESTS)
# Include adset_id/campaign_id + creative fields
ADS_FIELDS = (
    "id,name,status,effective_status,adset_id,campaign_id,updated_time,"
    "creative{id,thumbnail_url,image_url,object_story_id}"
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _cutoff(days: int) -> datetime:
    return _utc_now() - timedelta(days=days)


def _normalize_keys(d: Any) -> Any:
    """
    Sometimes Meta API dict keys may appear as bytes (rare).
    Convert bytes keys -> str recursively.
    """
    if isinstance(d, dict):
        out = {}
        for k, v in d.items():
            if isinstance(k, (bytes, bytearray)):
                try:
                    k2 = k.decode("utf-8", errors="ignore")
                except Exception:
                    k2 = str(k)
            else:
                k2 = str(k)
            out[k2] = _normalize_keys(v)
        return out
    if isinstance(d, list):
        return [_normalize_keys(x) for x in d]
    return d


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None or x == "":
            return None
        return int(x)
    except Exception:
        return None


def _as_utc(dt):
    if not dt:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# =========================
# DB Upsert (MATCHES YOUR ads TABLE)
# =========================
def upsert_ad(r: dict) -> None:
    """
    Matches your MySQL table `ads`:

    ads columns (based on your SHOW CREATE):
      - ad_id (UNIQUE)
      - adset_id (FK)
      - campaign_id
      - name
      - status
      - effective_status
      - thumbnail_url
      - image_url
      - post_link
      - post_id
      - updated_at (timestamp)  [no ON UPDATE in your schema]
    """
    sql = """
    INSERT INTO ads (
        ad_id,
        adset_id,
        campaign_id,
        name,
        status,
        effective_status,
        thumbnail_url,
        image_url,
        post_id,
        post_link,
        updated_at
    ) VALUES (
        %(ad_id)s,
        %(adset_id)s,
        %(campaign_id)s,
        %(name)s,
        %(status)s,
        %(effective_status)s,
        %(thumbnail_url)s,
        %(image_url)s,
        %(post_id)s,
        %(post_link)s,
        NOW()
    )
    ON DUPLICATE KEY UPDATE
        adset_id=COALESCE(VALUES(adset_id), adset_id),
        campaign_id=COALESCE(VALUES(campaign_id), campaign_id),
        name=COALESCE(VALUES(name), name),
        status=COALESCE(VALUES(status), status),
        effective_status=COALESCE(VALUES(effective_status), effective_status),
        thumbnail_url=COALESCE(VALUES(thumbnail_url), thumbnail_url),
        image_url=COALESCE(VALUES(image_url), image_url),
        post_id=COALESCE(VALUES(post_id), post_id),
        post_link=COALESCE(VALUES(post_link), post_link),
        updated_at=NOW();
    """
    execute(sql, r)


# =========================
# Service (thread-safe per account)
# =========================
def sync_ads_for_account(
    client,                     # ✅ injected MetaGraphClient (from thread)
    ad_account_id: int,
    portfolio_code: str = "",
    mode: str = "full",          # full | incremental
    days: int = 30
) -> Dict[str, int]:
    """
    Sync ads for ONE ad account using:
      ✅ act_{ad_account_id}/ads   (instead of adset_id/ads)

    mode:
      - full: upsert all ads returned for the account
      - incremental: only ads updated within last `days` (based on Meta updated_time)

    Returns:
      {"saved": x, "skipped": y, "failed_ads": z}
    """
    act = f"act_{ad_account_id}"
    act_id = f"act_{ad_account_id}"
    cutoff_dt = _cutoff(days)

    saved = 0
    skipped = 0
    failed_ads = 0

    logger.info(f"▶️ ads sync start {act} portfolio={portfolio_code} mode={mode} days={days}")

    try:
        for ad in client.get_paged(
            f"{act_id}/ads",
            params={"fields": ADS_FIELDS, "limit": 200}
        ):
            ad = _normalize_keys(ad)

            ad_id = ad.get("id")
            if not ad_id:
                skipped += 1
                continue

            # incremental filter using Meta updated_time
            if mode == "incremental":
                updated = _as_utc(parse_meta_datetime(ad.get("updated_time")))
                if not (updated and updated >= cutoff_dt):
                    skipped += 1
                    continue

            creative = _normalize_keys(ad.get("creative") or {})

            # adset_id is FK in DB (NOT NULL). If missing -> skip (avoid FK crash)
            adset_id = _safe_int(ad.get("adset_id"))
            if not adset_id:
                skipped += 1
                continue

            record = {
                "ad_id": int(ad_id),
                "adset_id": adset_id,
                "campaign_id": _safe_int(ad.get("campaign_id")),
                "name": ad.get("name"),
                "status": ad.get("status"),
                "effective_status": ad.get("effective_status"),
                "thumbnail_url": creative.get("thumbnail_url"),
                "image_url": creative.get("image_url"),

                # ✅ store object_story_id into post_id (your schema)
                "post_id": creative.get("object_story_id"),

                # optional (Meta doesn't always provide a direct link)
                "post_link": None,
            }

            upsert_ad(record)
            saved += 1

        logger.info(
            f"✅ ads synced for {act} portfolio={portfolio_code} "
            f"saved={saved} skipped={skipped} failed_ads={failed_ads}"
        )
        return {"saved": saved, "skipped": skipped, "failed_ads": failed_ads}

    except MetaObjectAccessError as e:
        # rare: account/object access issues
        logger.warning(f"⚠️ ads skipped {act} portfolio={portfolio_code}: {e}")
        return {"saved": saved, "skipped": skipped, "failed_ads": failed_ads + 1}

    except Exception as e:
        logger.error(f"❌ ads failed for {act} portfolio={portfolio_code}: {e}")
        return {"saved": saved, "skipped": skipped, "failed_ads": failed_ads + 1}


# Optional legacy wrapper
def sync_ads(user_token: str, mode: str = "full", days: int = 30) -> Dict[str, int]:
    """
    Legacy wrapper: NOT threaded. Prefer sync_ads_for_account(client, ...)
    """
    from integrations.meta_graph_client import MetaGraphClient

    client = MetaGraphClient(user_token)

    accounts = query_dict(
        """
        SELECT a.ad_account_id, p.code AS portfolio_code
        FROM ad_accounts a
        JOIN portfolios p ON p.id = a.portfolio_id
        WHERE p.code IN ('RFM','MAGIC_EXTREME')
        ORDER BY p.code, a.ad_account_id
        """
    )

    total_saved = 0
    total_skipped = 0
    total_failed = 0

    for r in accounts:
        res = sync_ads_for_account(
            client=client,
            ad_account_id=int(r["ad_account_id"]),
            portfolio_code=r.get("portfolio_code") or "",
            mode=mode,
            days=days
        )
        total_saved += res.get("saved", 0)
        total_skipped += res.get("skipped", 0)
        total_failed += res.get("failed_ads", 0)

    logger.info(
        f"✅ ads sync done (all accounts). saved={total_saved} skipped={total_skipped} failed={total_failed}"
    )
    return {"saved": total_saved, "skipped": total_skipped, "failed_ads": total_failed}
