# scripts/run_sync_pages_posts.py
import os
from logs.logger import logger
from services.pages_service import sync_all_pages_posts_last_hours

def main():
    token = os.getenv("META_USER_TOKEN")
    if not token:
        print("❌ META_USER_TOKEN is missing.")
        return

    hours = int(os.getenv("POSTS_HOURS", "24"))
    logger.info(f"🚀 run_sync_pages_posts starting hours={hours}")
    sync_all_pages_posts_last_hours(token, hours=hours)

if __name__ == "__main__":
    main()
