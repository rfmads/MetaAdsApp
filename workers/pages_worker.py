# # pages_worker.py

# pages_worker.py
import os
from logs.logger import logger
from services.pages_service import sync_pages
from db.config_store import get_config
def run():
# 1. Pull token from DB instead of OS environment
    user_token = get_config("META_USER_TOKEN")
    
    if not user_token:
        logger.error("❌ META_USER_TOKEN missing in database 'sys_config' table")
        # You can choose to raise an exception or return gracefully
        return {"ok": False, "error": "Missing Token"}

    logger.info("🚀 START pages sync")

    try:
        result = sync_pages(user_token)
        logger.info(f"✅ DONE pages result={result}")
        return result
    except Exception as e:
        logger.error(f"❌ pages worker failed: {e}")
        return {"ok": False, "error": str(e)}

if __name__ == "__main__":
    run()
# import os
# from logs.logger import logger
# from services.pages_service import sync_pages


# def run():
#     user_token = os.getenv("META_USER_TOKEN")
#     if not user_token:
#         raise Exception("META_USER_TOKEN missing")

#     logger.info("🚀 START pages")

#     result = sync_pages(user_token)

#     if not result.get("ok"):
#         raise Exception(result.get("error"))

#     logger.info(f"✅ DONE pages result={result}")

#     return result