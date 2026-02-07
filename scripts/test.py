import os
from integrations.meta_graph_client import MetaGraphClient
from services.pages_service import sync_pages

USER_TOKEN = os.getenv("META_USER_TOKEN", "").strip()

if __name__ == "__main__":
    if not USER_TOKEN:
        raise SystemExit("❌ META_USER_TOKEN is missing.")

    client = MetaGraphClient(USER_TOKEN)
    print("✅ Token OK:", client.get("me", params={"fields": "id,name"}))

    sync_pages(USER_TOKEN)
