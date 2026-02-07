# services/pages_service.py

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from logs.logger import logger
from integrations.meta_graph_client import MetaGraphClient
from db.db import execute, query_dict


FIELDS = "id,name,category,access_token,created_time,instagram_business_account{id,username}"


def _parse_dt(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        s = dt_str.replace("Z", "+00:00")
        # handle +0000 -> +00:00
        if len(s) >= 5 and (s[-5] in ["+", "-"]) and s[-3] != ":":
            s = s[:-2] + ":" + s[-2:]
        return datetime.fromisoformat(s).replace(tzinfo=None)
    except Exception:
        return None


def upsert_page(rec: dict) -> None:
    sql = """
    INSERT INTO pages (
        page_id,
        page_name,
        category,
        page_access_token,
        created_time,
        ig_user_id,
        ig_username,
        first_seen_at,
        last_seen_at
    ) VALUES (
        %(page_id)s,
        %(page_name)s,
        %(category)s,
        %(page_access_token)s,
        %(created_time)s,
        %(ig_user_id)s,
        %(ig_username)s,
        NOW(),
        NOW()
    )
    ON DUPLICATE KEY UPDATE
        page_name = COALESCE(VALUES(page_name), page_name),
        category = COALESCE(VALUES(category), category),
        page_access_token = COALESCE(VALUES(page_access_token), page_access_token),
        created_time = COALESCE(VALUES(created_time), created_time),
        ig_user_id = COALESCE(VALUES(ig_user_id), ig_user_id),
        ig_username = COALESCE(VALUES(ig_username), ig_username),
        last_seen_at = NOW();
    """
    execute(sql, rec)


def sync_pages(user_token: str) -> dict:
    """
    Sync pages accessible by the user token:
      - Uses: /me/accounts
      - Stores: page_id, page_name, category, page_access_token, created_time
      - Also stores IG business account if connected: ig_user_id, ig_username
    """
    client = MetaGraphClient(user_token)

    saved = 0
    skipped = 0

    logger.info("▶️ pages sync start")

    try:
        for p in client.get_paged("me/accounts", params={"fields": FIELDS, "limit": 200}):
            page_id = p.get("id")
            if not page_id:
                skipped += 1
                continue

            ig = p.get("instagram_business_account") or {}
            ig_user_id = ig.get("id")
            ig_username = ig.get("username")

            rec = {
                "page_id": int(page_id),
                "page_name": p.get("name"),
                "category": p.get("category"),
                "page_access_token": p.get("access_token"),
                "created_time": _parse_dt(p.get("created_time")),
                "ig_user_id": int(ig_user_id) if ig_user_id else None,
                "ig_username": ig_username,
            }

            upsert_page(rec)
            saved += 1

        logger.info(f"✅ pages sync done. saved={saved} skipped={skipped}")
        return {"ok": True, "saved": saved, "skipped": skipped}

    except Exception as e:
        logger.error(f"❌ pages sync failed: {e}")
        return {"ok": False, "saved": saved, "skipped": skipped, "error": str(e)}
