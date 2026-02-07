# scripts/run_sync_posts_threaded.py

import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from logs.logger import logger
from db.db import query_dict
from services.pages_posts_service import (
    sync_facebook_posts_last_hours,
    sync_instagram_posts_last_hours,
)


def _job(user_token: str, page: dict, hours: int) -> dict:
    page_id = int(page["page_id"])
    page_token = page.get("page_access_token")
    ig_user_id = page.get("ig_user_id")

    out = {"page_id": page_id, "fb": None, "ig": None, "errors": []}

    # Facebook
    r1 = sync_facebook_posts_last_hours(
        user_token=user_token,
        page_id=page_id,
        page_access_token=page_token,
        hours=hours,
        limit=100,
    )
    out["fb"] = r1
    if r1.get("error"):
        out["errors"].append(r1["error"])

    # Instagram (only if ig_user_id exists)
    if ig_user_id:
        r2 = sync_instagram_posts_last_hours(
            user_token=user_token,
            page_id=page_id,
            ig_user_id=int(ig_user_id),
            page_access_token=page_token,
            hours=hours,
            limit=100,
        )
        out["ig"] = r2
        if r2.get("error"):
            out["errors"].append(r2["error"])

    return out


def main():
    user_token = os.getenv("META_USER_TOKEN")
    if not user_token:
        print("❌ META_USER_TOKEN is missing. Set it first.")
        return

    hours = int(os.getenv("POSTS_HOURS", "24"))
    workers = int(os.getenv("SYNC_WORKERS", "2"))

    pages = query_dict("""
        SELECT page_id, page_name, page_access_token, ig_user_id
        FROM pages
        ORDER BY page_id
    """)

    logger.info(f"🚀 run_sync_posts_threaded starting workers={workers} hours={hours} pages={len(pages)}")

    ok = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_job, user_token, p, hours) for p in pages]

        for f in as_completed(futures):
            res = f.result()
            if res.get("errors"):
                failed += 1
            else:
                ok += 1

    logger.info(f"✅ run_sync_posts_threaded done ok_pages={ok} failed_pages={failed}")


if __name__ == "__main__":
    main()
