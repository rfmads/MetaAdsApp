# scripts/test_ad_posts.py
import os
from integrations.meta_graph_client import MetaGraphClient
from services.ad_posts_service import sync_ad_posts

def main():
    token = os.getenv("META_USER_TOKEN")
    if not token:
        print("❌ META_USER_TOKEN is missing. Set it first.")
        return

    client = MetaGraphClient(token)
    print("✅ Token OK:", client.get("me", params={"fields": "id,name"}))

    # جرّب أول 200 إعلان (غير الرقم براحتك)
    sync_ad_posts(token, limit=200)

if __name__ == "__main__":
    main()
