# workers/ad_posts_worker.py

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from logs.logger import logger
from db.db import query_dict
from db.config_store import get_config
from services.ad_posts_service import (
    _resolve_post,
    _fetch_creative_post_data,
    _find_post_by_story_id,
    _find_post_by_instagram_permalink,
    _upsert_ad_post,
)
from integrations.meta_graph_client import MetaGraphClient


BATCH_SIZE = 2000
LOG_EVERY = 500


def _job(client: MetaGraphClient, row: dict) -> dict:
    ad_id = int(row["ad_id"])
    post_id_value = row.get("post_id")

    try:
        post_row_id, link_type = _resolve_post(post_id_value)

        if not post_row_id:
            creative = _fetch_creative_post_data(client, ad_id)

            if creative:
                if creative.get("effective_story_id"):
                    post_row_id = _find_post_by_story_id(
                        creative["effective_story_id"]
                    )
                    link_type = "facebook_story"

                if not post_row_id and creative.get("instagram_permalink"):
                    post_row_id = _find_post_by_instagram_permalink(
                        creative["instagram_permalink"]
                    )
                    link_type = "instagram_permalink"

        if not post_row_id:
            return {"ok": False, "type": "skipped", "ad_id": ad_id}

        _upsert_ad_post(ad_id, post_row_id, link_type or "facebook_story")

        return {"ok": True, "ad_id": ad_id}

    except Exception as e:
        logger.error(f"❌ ad_posts failed ad_id={ad_id}: {e}")
        return {"ok": False, "type": "failed", "ad_id": ad_id}


def run(job_id=None):
    user_token = get_config("META_USER_TOKEN")

    if not user_token:
        logger.error("❌ META_USER_TOKEN missing")
        return {"ok": False, "error": "Missing Token"}

    workers = int(os.getenv("AD_POSTS_WORKERS", "5"))

    ads = query_dict("""
        SELECT ad_id, post_id
        FROM ads
        ORDER BY ad_id DESC
    """)

    total = len(ads)

    if not total:
        logger.warning("No ads found for ad_posts")
        return {"ok": True, "total": 0}

    logger.info(f"🚀 START ad_posts worker ads={total} workers={workers}")

    # ⏱ START TIMER (NEW)
    start_time = time.time()
    logger.info(f"⏱ start time = {start_time}")

    client = MetaGraphClient(user_token)

    success = 0
    skipped = 0
    failed = 0
    processed = 0

    with ThreadPoolExecutor(max_workers=workers) as ex:

        futures = []

        # submit in chunks
        for i in range(0, total, BATCH_SIZE):
            batch = ads[i:i + BATCH_SIZE]

            futures.extend([
                ex.submit(_job, client, row) for row in batch
            ])

            logger.info(f"📦 Submitted batch {i}-{i+len(batch)}")

        for f in as_completed(futures):
            res = f.result()
            processed += 1

            if res.get("ok"):
                success += 1
            else:
                if res.get("type") == "skipped":
                    skipped += 1
                else:
                    failed += 1

            # =========================
            # 🔥 ALIVE CHECK (NEW)
            # =========================
            if processed % 100 == 0:
                logger.info(
                    f"🔥 alive check: processed={processed}/{total} "
                    f"success={success} skipped={skipped} failed={failed}"
                )

            # =========================
            # 📊 PROGRESS LOG
            # =========================
            if processed % LOG_EVERY == 0:
                logger.info(
                    f"⏳ progress: {processed}/{total} "
                    f"({(processed/total)*100:.2f}%) "
                    f"success={success} skipped={skipped} failed={failed}"
                )

            # =========================
            # ❤️ PIPELINE HEARTBEAT
            # =========================
            if job_id and processed % 1000 == 0:
                try:
                    from services.job_service import heartbeat
                    heartbeat(job_id)
                except Exception:
                    pass

            # =========================
            # ⏱ ELAPSED TIME (NEW)
            # =========================
            if processed % 500 == 0:
                elapsed = time.time() - start_time
                logger.info(f"⏱ elapsed = {elapsed:.2f}s")

    elapsed = time.time() - start_time

    result = {
        "ok": True,
        "total": total,
        "success": success,
        "skipped": skipped,
        "failed": failed,
        "elapsed_seconds": round(elapsed, 2),
    }

    logger.info(f"✅ ad_posts DONE {result}")

    return result


if __name__ == "__main__":
    run()