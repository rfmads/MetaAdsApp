# scripts/test_adsets.py
from config.config import META_USER_TOKEN
from integrations.meta_graph_client import MetaGraphClient
from services.adsets_service import sync_adsets

def main():
    if not META_USER_TOKEN:
        print("❌ META_USER_TOKEN is missing.")
        return

    client = MetaGraphClient(META_USER_TOKEN)
    print("✅ Token OK:", client.get("me", params={"fields": "id,name"}))

    sync_adsets(META_USER_TOKEN)

if __name__ == "__main__":
    main()
