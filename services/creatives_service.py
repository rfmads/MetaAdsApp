# services/creatives_service.py

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient
from db.db import execute, query_dict


# =========================
# Meta fields
# =========================
# Pull ads directly under ad account with embedded creative
ADS_WITH_CREATIVE_FIELDS = (
    "id,name,"
    "creative{"
    "id,name,body,effective_object_story_id,instagram_permalink_url,link_url,"
    "thumbnail_url,video_id,object_story_spec"
    "}"
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _cutoff(days: int) -> datetime:
    return _utc_now() - timedelta(days=days)


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None or x == "":
            return None
        return int(x)
    except Exception:
        return None


def _json_or_none(x: Any):
    if x is None:
        return None
    try:
        if isinstance(x, (dict, list)):
            return json.dumps(x, ensure_ascii=False)
        if isinstance(x, str):
            try:
                json.loads(x)
                return x
            except Exception:
                return json.dumps({"raw": x}, ensure_ascii=False)
        return json.dumps(x, ensure_ascii=False)
    except Exception:
        return None


# =========================
# DB upserts
# =========================
def upsert_creative(record: dict) -> None:
    """
    Upsert into creative_ads (your table).
    """
    sql = """
    INSERT INTO creative_ads (
        creative_id,
        name,
        body,
        effective_object_story_id,
        instagram_permalink_url,
        link_url,
        page_id,
        thumbnail_url,
        video_id,
        creative_sourcing_spec,
        first_seen_at,
        last_seen_at
    ) VALUES (
        %(creative_id)s,
        %(name)s,
        %(body)s,
        %(effective_object_story_id)s,
        %(instagram_permalink_url)s,
        %(link_url)s,
        %(page_id)s,
        %(thumbnail_url)s,
        %(video_id)s,
        %(creative_sourcing_spec)s,
        NOW(),
        NOW()
    )
    ON DUPLICATE KEY UPDATE
        name = COALESCE(VALUES(name), name),
        body = COALESCE(VALUES(body), body),
        effective_object_story_id = COALESCE(VALUES(effective_object_story_id), effective_object_story_id),
        instagram_permalink_url = COALESCE(VALUES(instagram_permalink_url), instagram_permalink_url),
        link_url = COALESCE(VALUES(link_url), link_url),
        page_id = COALESCE(VALUES(page_id), page_id),
        thumbnail_url = COALESCE(VALUES(thumbnail_url), thumbnail_url),
        video_id = COALESCE(VALUES(video_id), video_id),
        creative_sourcing_spec = COALESCE(VALUES(creative_sourcing_spec), creative_sourcing_spec),
        last_seen_at = NOW(),
        updated_at = NOW();
    """
    execute(sql, record)


def update_ad_with_creative(
    ad_id: int,
    creative_id: Optional[int],
    effective_object_story_id: Optional[str],
    thumbnail_url: Optional[str],
    link_url: Optional[str],
    instagram_permalink_url: Optional[str],
) -> None:
    """
    Update ads table:
      - creative_id
      - post_id: we store effective_object_story_id (PAGEID_POSTID)
      - thumbnail_url
      - post_link: prefer instagram_permalink_url if exists else link_url
    """
    post_link = instagram_permalink_url or link_url

    sql = """
    UPDATE ads
    SET
        creative_id = %(creative_id)s,
        post_id = COALESCE(%(post_id)s, post_id),
        thumbnail_url = COALESCE(%(thumbnail_url)s, thumbnail_url),
        post_link = COALESCE(%(post_link)s, post_link),
        updated_at = NOW()
    WHERE ad_id = %(ad_id)s
    """
    execute(sql, {
        "ad_id": ad_id,
        "creative_id": creative_id,
        "post_id": effective_object_story_id,
        "thumbnail_url": thumbnail_url,
        "post_link": post_link,
    })


# =========================
# Service
# =========================
def sync_creatives_for_account(
    user_token: str,
    ad_account_id: int,
    portfolio_code: str = "",
    mode: str = "incremental",   # full | incremental
    days: int = 30,
) -> Dict[str, int]:
    """
    Fetch ads under act_ with embedded creative.
    Avoids per-ad requests (no get_object needed).
    """
    client = MetaGraphClient(user_token)
    act = f"act_{ad_account_id}"

    saved = 0
    skipped = 0
    failed = 0

    cutoff = _cutoff(days)

    params = {"fields": ADS_WITH_CREATIVE_FIELDS, "limit": 200}

    if mode == "incremental":
        # ✅ Use UNIX timestamp (seconds) for filtering value
        params["filtering"] = json.dumps([{
            "field": "ad.updated_time",
            "operator": "GREATER_THAN",
            "value": int(cutoff.timestamp())
        }])

    logger.info(f"▶️ creatives sync start {act} portfolio={portfolio_code} mode={mode} days={days}")

    try:
        for ad in client.get_paged(f"{act}/ads", params=params):
            ad_id = _safe_int(ad.get("id"))
            if not ad_id:
                skipped += 1
                continue

            creative = ad.get("creative") or {}
            cr_id = _safe_int(creative.get("id"))
            if not cr_id:
                skipped += 1
                continue

            eosid = creative.get("effective_object_story_id")

            page_id = None
            if isinstance(eosid, str) and "_" in eosid:
                page_id = _safe_int(eosid.split("_", 1)[0])

            upsert_creative({
                "creative_id": cr_id,
                "name": creative.get("name"),
                "body": creative.get("body"),
                "effective_object_story_id": eosid,
                "instagram_permalink_url": creative.get("instagram_permalink_url"),
                "link_url": creative.get("link_url"),
                "page_id": page_id,
                "thumbnail_url": creative.get("thumbnail_url"),
                "video_id": _safe_int(creative.get("video_id")),
                "creative_sourcing_spec": _json_or_none(creative.get("object_story_spec")),
            })

            # ✅ FK safe: insert creative first, then update ad with creative_id
            update_ad_with_creative(
                ad_id=ad_id,
                creative_id=cr_id,
                effective_object_story_id=eosid,
                thumbnail_url=creative.get("thumbnail_url"),
                link_url=creative.get("link_url"),
                instagram_permalink_url=creative.get("instagram_permalink_url"),
            )

            saved += 1

    except Exception as e:
        msg = str(e)
        # fallback if filtering rejected
        if mode == "incremental" and ("filtering" in msg or "param filtering" in msg):
            logger.warning(f"⚠️ filtering rejected for {act}/ads, fallback to full. err={e}")
            return sync_creatives_for_account(
                user_token=user_token,
                ad_account_id=ad_account_id,
                portfolio_code=portfolio_code,
                mode="full",
                days=days,
            )
        logger.error(f"❌ creatives failed {act} portfolio={portfolio_code}: {e}")
        failed += 1

    logger.info(f"✅ creatives sync done {act} portfolio={portfolio_code} saved={saved} skipped={skipped} failed={failed}")
    return {"saved": saved, "skipped": skipped, "failed": failed}


def sync_creatives(user_token: str, mode: str = "incremental", days: int = 30) -> Dict[str, int]:
    """
    Non-threaded wrapper (like others).
    """
    accounts = query_dict("""
        SELECT a.ad_account_id, p.code AS portfolio_code
        FROM ad_accounts a
        JOIN portfolios p ON p.id = a.portfolio_id
        WHERE p.code IN ('RFM','MAGIC_EXTREME')
        ORDER BY p.code, a.ad_account_id
    """)

    total_saved = 0
    total_skipped = 0
    total_failed = 0

    for r in accounts:
        res = sync_creatives_for_account(
            user_token=user_token,
            ad_account_id=int(r["ad_account_id"]),
            portfolio_code=r["portfolio_code"],
            mode=mode,
            days=days,
        )
        total_saved += res.get("saved", 0)
        total_skipped += res.get("skipped", 0)
        total_failed += res.get("failed", 0)

    logger.info(f"✅ creatives sync done (all accounts) saved={total_saved} skipped={total_skipped} failed={total_failed}")
    return {"saved": total_saved, "skipped": total_skipped, "failed": total_failed}
