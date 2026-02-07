# scripts/test_campaigns.py
from config.config import META_USER_TOKEN
from integrations.meta_graph_client import MetaGraphClient
from services.campaigns_service import sync_campaigns


def main():
    if not META_USER_TOKEN:
        print("❌ META_USER_TOKEN is missing.")
        return

    client = MetaGraphClient(META_USER_TOKEN)
    me = client.get("me", params={"fields": "id,name"})
    print("✅ Token OK:", me)

    # سحب الحملات (full أو incremental حسب منطق السيرفس)
    sync_campaigns(META_USER_TOKEN)


if __name__ == "__main__":
    main()
