import os
from integrations.meta_graph_client import MetaGraphClient
from services.ig_link_service import sync_pages_ig_link

def main():
    token = os.getenv("META_USER_TOKEN")
    if not token:
        print("❌ META_USER_TOKEN is missing. Set it first.")
        return

    client = MetaGraphClient(token)
    print("✅ Token OK:", client.get("me", params={"fields": "id,name"}))

    sync_pages_ig_link(token)

if __name__ == "__main__":
    main()
