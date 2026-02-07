# scripts/test_adset_daily_insights.py
import os
from integrations.meta_graph_client import MetaGraphClient
from services.adset_daily_insights_service import sync_adset_daily_insights_last_60_days

USER_TOKEN = os.getenv("META_USER_TOKEN", "").strip()

if not USER_TOKEN:
    print("❌ META_USER_TOKEN is missing. Set it first.")
    raise SystemExit(1)

client = MetaGraphClient(USER_TOKEN)
print("✅ Token OK:", client.get("me", params={"fields": "id,name"}))

sync_adset_daily_insights_last_60_days(USER_TOKEN)
print("✅ Done.")
