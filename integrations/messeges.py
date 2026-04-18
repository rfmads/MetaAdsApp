import requests
from db.config_store import get_config

def get_json(url, params):
    # Determine if url is a full path (from paging) or just an endpoint
    api_ver = get_config("META_GRAPH_VERSION") 
    base = f"https://graph.facebook.com/{api_ver}"
    
    full_url = url if url.startswith("http") else f"{base}/{url.lstrip('/')}"
    
    r = requests.get(full_url, params=params, timeout=30)
    if r.status_code == 401:
        print("❌ Auth Error: Check your META_USER_TOKEN in the DB.")
        return {}
    r.raise_for_status()
    return r.json()

def fetch_all(endpoint, params):
    out = []
    next_url = endpoint
    curr_params = params
    while next_url:
        js = get_json(next_url, curr_params)
        out.extend(js.get("data", []))
        next_url = js.get("paging", {}).get("next")
        curr_params = {} # params are baked into the 'next' URL
    return out

def run_sync():
    # --- ALL CONFIG PULLED FROM DB ---
    token = get_config("META_USER_TOKEN")
    page_id = get_config("PAGE_ID")
    
    if not token or not page_id:
        print("❌ Configuration Missing: Please set META_USER_TOKEN and PAGE_ID in sys_config.")
        return

    print(f"🔄 Syncing Page: {page_id}")

    # 1. Get IG ID
    ig_data = get_json(page_id, {
        "fields": "instagram_business_account",
        "access_token": token
    })
    
    ig_user = (ig_data.get("instagram_business_account") or {}).get("id")
    if not ig_user:
        print("⚠️ No Instagram Business Account linked to this Page.")
        return

    # 2. Get Conversations
    convs = fetch_all(f"{ig_user}/conversations", {
        "platform": "instagram",
        "fields": "id,updated_time",
        "access_token": token
    })

    print(f"✅ Successfully fetched {len(convs)} conversations for IG User {ig_user}")

if __name__ == "__main__":
    run_sync()
# import os
# import requests

# PAGE_ID = "945284345334556"
# PAGE_TOKEN =  os.getenv("META_USER_TOKEN")
# API_VER = "v24.0"

# def get_json(url, params):
#     r = requests.get(url, params=params, timeout=30)
#     r.raise_for_status()
#     return r.json()

# def fetch_all(url, params):
#     out = []
#     while url:
#         js = get_json(url, params)
#         out.extend(js.get("data", []))
#         url = js.get("paging", {}).get("next")
#         params = {}  # next فيه كل شيء
#     return out

# # 1) Get IG User ID
# ig_url = f"https://graph.facebook.com/{API_VER}/{PAGE_ID}"
# ig = get_json(ig_url, {
#     "fields": "instagram_business_account",
#     "access_token": PAGE_TOKEN
# })

# ig_user = (ig.get("instagram_business_account") or {}).get("id")
# if not ig_user:
#     raise SystemExit("instagram_business_account is null: اربطي IG Professional بالصفحة أولاً.")

# print("IG_USER_ID:", ig_user)

# # 2) Instagram conversations
# conv_url = f"https://graph.facebook.com/{API_VER}/{ig_user}/conversations"
# convs = fetch_all(conv_url, {
#     "platform": "instagram",
#     "fields": "id,updated_time,participants",
#     "limit": 25,
#     "access_token": PAGE_TOKEN
# })

# print("Conversations:", len(convs))

# # 3) Messages per conversation
# for c in convs[:5]:  # جرّبي أول 5
#     cid = c["id"]
#     msg_url = f"https://graph.facebook.com/{API_VER}/{cid}/messages"
#     msgs = fetch_all(msg_url, {
#         "fields": "id,message,from,created_time",
#         "limit": 50,
#         "access_token": PAGE_TOKEN
#     })
#     print(f"\nConversation {cid} -> {len(msgs)} messages")
#     for m in msgs[:3]:
#         print(m.get("created_time"), "-", m.get("from", {}).get("name"), ":", m.get("message"))
