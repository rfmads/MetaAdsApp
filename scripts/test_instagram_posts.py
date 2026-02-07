# scripts/test_instagram_posts.py
import os
from integrations.meta_graph_client import MetaGraphClient
from services.instagram_posts_service import sync_instagram_posts_last_60_days

def main():
    token = os.getenv("META_USER_TOKEN")
    if not token:
        print("❌ META_USER_TOKEN is missing. Set it first.")
        return

    client = MetaGraphClient(token)
    print("✅ Token OK:", client.get("me", params={"fields": "id,name"}))

    sync_instagram_posts_last_60_days(token)
    print("[DONE]")

if __name__ == "__main__":
    main()
