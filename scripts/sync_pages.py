# scripts/sync_pages.py

import os
from logs.logger import logger
from services.pages_service import sync_pages


def main():
    user_token = os.getenv("META_USER_TOKEN")
    if not user_token:
        print("❌ META_USER_TOKEN is missing. Set it first.")
        return

    res = sync_pages(user_token)
    logger.info(f"pages result: {res}")


if __name__ == "__main__":
    main()
