import os
from integrations.meta_graph_client import MetaGraphClient
from services.billing_service import sync_billing_for_all_accounts

USER_TOKEN = os.getenv("META_USER_TOKEN", "").strip()

if __name__ == "__main__":
    if not USER_TOKEN:
        raise SystemExit("❌ META_USER_TOKEN is missing. Set it first.")

    client = MetaGraphClient(USER_TOKEN)
    me = client.get("me", params={"fields": "id,name"})
    print("✅ Token OK:", me)

    sync_billing_for_all_accounts(USER_TOKEN)
