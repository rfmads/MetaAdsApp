# scripts/test_rfm_ad_accounts.py
import os
from integrations.meta_graph_client import MetaGraphClient
from services.ad_accounts_service import sync_rfm_ad_accounts

RFM_BUSINESS_ID = "448333861693952"

def main():
    token = os.getenv("META_USER_TOKEN", "").strip()
    if not token:
        print("❌ META_USER_TOKEN is missing. Set it first.")
        return

    # token check
    client = MetaGraphClient(token)
    print("✅ Token OK:", client.get("me", params={"fields": "id,name"}))

    sync_rfm_ad_accounts(token, RFM_BUSINESS_ID)

if __name__ == "__main__":
    main()
