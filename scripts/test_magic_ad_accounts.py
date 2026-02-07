# scripts/test_magic_ad_accounts.py
import os
from integrations.meta_graph_client import MetaGraphClient
from services.magic_ad_accounts_service import sync_magic_ad_accounts
from logs.logger import logger

def main():
    token = os.environ.get("META_USER_TOKEN")
    if not token:
        print("❌ META_USER_TOKEN is missing. Set it first.")
        return

    client = MetaGraphClient(token)
    print("✅ Token OK:", client.get("me", params={"fields": "id,name"}))

    sync_magic_ad_accounts(token)

if __name__ == "__main__":
    main()
