# posts_worker.py
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from logs.logger import logger
from db.db import query_dict
from integrations.meta_graph_client import MetaGraphClient
from services.job_service import heartbeat
from services.pages_posts_service import (
    sync_facebook_posts_last_hours,
    sync_instagram_posts_last_hours,
)
from db.config_store import get_config
def _job(user_token: str, page: dict, hours: int) -> dict:
    page_id = int(page["page_id"])
    page_token = page.get("page_access_token")
    ig_user_id = page.get("ig_user_id")

    # Use Page Token if available, otherwise User Token
    client = MetaGraphClient(page_token or user_token)

    out = {"page_id": page_id, "fb": None, "ig": None, "errors": []}

    # Facebook posts
    try:
        r1 = sync_facebook_posts_last_hours(
            client=client,
            page_id=page_id,
            hours=hours,
        )
        out["fb"] = r1
    except Exception as e:
        out["errors"].append(f"FB Error: {str(e)}")

    # Instagram posts
    if ig_user_id:
        try:
            r2 = sync_instagram_posts_last_hours(
                client=client,
                ig_user_id=int(ig_user_id),
                page_id=page_id,
                hours=hours,
            )
            out["ig"] = r2
        except Exception as e:
            out["errors"].append(f"IG Error: {str(e)}")

    return out

def run(job_id=None):
# 1. Pull token from DB instead of OS environment
    user_token = get_config("META_USER_TOKEN")
    
    if not user_token:
        logger.error("❌ META_USER_TOKEN missing in database 'sys_config' table")
        # You can choose to raise an exception or return gracefully
        return {"ok": False, "error": "Missing Token"}
    hours = int(os.getenv("POSTS_HOURS", "48"))
    workers = int(os.getenv("SYNC_WORKERS", "5")) # Lighter requests allow more workers

    pages = query_dict("SELECT page_id, page_access_token, ig_user_id FROM pages")

    logger.info(f"🚀 posts worker starting workers={workers} hours={hours}")

    ok, failed = 0, 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_job, user_token, p, hours) for p in pages]
        for f in as_completed(futures):
            # ❤️ HEARTBEAT: Update the job timestamp every time a thread finishes an account
            if job_id:
                heartbeat(job_id)
            res = f.result()
            if res.get("errors"): failed += 1
            else: ok += 1

    logger.info(f"✅ posts worker finished ok={ok} failed={failed}")

if __name__ == "__main__":
     run()





# import os
# from concurrent.futures import ThreadPoolExecutor, as_completed

# from logs.logger import logger
# from db.db import query_dict
# from services.pages_posts_service import (
#     sync_facebook_posts_last_hours,
#     sync_instagram_posts_last_hours,
# )


# def _job(user_token: str, page: dict, hours: int) -> dict:
#     page_id = int(page["page_id"])
#     page_token = page.get("page_access_token")
#     ig_user_id = page.get("ig_user_id")

#     out = {"page_id": page_id, "fb": None, "ig": None, "errors": []}

#     # Facebook posts
#     r1 = sync_facebook_posts_last_hours(
#         user_token=user_token,
#         page_id=page_id,
#         page_access_token=page_token,
#         hours=hours,
#         limit=100,
#     )
#     out["fb"] = r1
#     if isinstance(r1, dict) and r1.get("error"):
#         out["errors"].append(r1["error"])

#     # Instagram posts (only if exists)
#     if ig_user_id:
#         r2 = sync_instagram_posts_last_hours(
#             user_token=user_token,
#             page_id=page_id,
#             ig_user_id=int(ig_user_id),
#             page_access_token=page_token,
#             hours=hours,
#             limit=100,
#         )
#         out["ig"] = r2
#         if isinstance(r2, dict) and r2.get("error"):
#             out["errors"].append(r2["error"])

#     return out


# # ✅ IMPORTANT: pipeline entry point MUST be run()
# def run():
#     user_token = os.getenv("META_USER_TOKEN")
#     if not user_token:
#         raise Exception("META_USER_TOKEN is missing")

#     hours = int(os.getenv("POSTS_HOURS", "48"))
#     workers = int(os.getenv("SYNC_WORKERS", "3"))

#     pages = query_dict("""
#         SELECT page_id, page_name, page_access_token, ig_user_id
#         FROM pages
#         ORDER BY page_id
#     """)

#     logger.info(
#         f"🚀 posts worker starting workers={workers} hours={hours} pages={len(pages)}"
#     )

#     ok = 0
#     failed = 0

#     with ThreadPoolExecutor(max_workers=workers) as ex:
#         futures = [
#             ex.submit(_job, user_token, p, hours)
#             for p in pages
#         ]

#         for f in as_completed(futures):
#             try:
#                 res = f.result()

#                 if res.get("errors"):
#                     failed += 1
#                 else:
#                     ok += 1

#             except Exception as e:
#                 failed += 1
#                 logger.error(f"❌ posts thread crashed: {e}")

#     logger.info(
#         f"✅ posts worker finished ok_pages={ok} failed_pages={failed}"
#     )

#     return {
#         "ok": True,
#         "pages": len(pages),
#         "success": ok,
#         "failed": failed,
#     }


# # optional local run
# if __name__ == "__main__":
#     run()