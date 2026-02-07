# scripts/test_ads.py
import os
from integrations.meta_graph_client import MetaGraphClient
from services.ads_service import sync_ads

def main():
    token = os.getenv("META_USER_TOKEN", "").strip()
    if not token:
        print("❌ META_USER_TOKEN is missing. Set it first.")
        return

    client = MetaGraphClient(token)
    print("✅ Token OK:", client.get("me", params={"fields": "id,name"}))

    # أول مرة:
    sync_ads(token, mode="full")

    # لاحقاً:
    # sync_ads(token, mode="incremental")

if __name__ == "__main__":
    main()
