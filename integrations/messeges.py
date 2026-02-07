import requests

PAGE_ID = "945284345334556"
PAGE_TOKEN = "EAASC6lLmPnUBQRlL6ohZAVmMeRs53mAISEZAFn5WaOAu2Dgx9KaaEK2cvMT89hGXH3e5ZBjaNODkSHQiNGnIMVBc8SFPTE1FR7fZA9tqJKvUioxYEtYgijAdCZBKnGtZB9jjssL5KxjrZAzF3XEXAjU4HwFEZA74XXrvE188PDy8GU8F9rr55egX9WYtsbQsSgUFdRtfHJQJOqqeXiINpRvvcYkrBiZBJm32LSDyRH4sI"
API_VER = "v24.0"

def get_json(url, params):
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def fetch_all(url, params):
    out = []
    while url:
        js = get_json(url, params)
        out.extend(js.get("data", []))
        url = js.get("paging", {}).get("next")
        params = {}  # next فيه كل شيء
    return out

# 1) Get IG User ID
ig_url = f"https://graph.facebook.com/{API_VER}/{PAGE_ID}"
ig = get_json(ig_url, {
    "fields": "instagram_business_account",
    "access_token": PAGE_TOKEN
})

ig_user = (ig.get("instagram_business_account") or {}).get("id")
if not ig_user:
    raise SystemExit("instagram_business_account is null: اربطي IG Professional بالصفحة أولاً.")

print("IG_USER_ID:", ig_user)

# 2) Instagram conversations
conv_url = f"https://graph.facebook.com/{API_VER}/{ig_user}/conversations"
convs = fetch_all(conv_url, {
    "platform": "instagram",
    "fields": "id,updated_time,participants",
    "limit": 25,
    "access_token": PAGE_TOKEN
})

print("Conversations:", len(convs))

# 3) Messages per conversation
for c in convs[:5]:  # جرّبي أول 5
    cid = c["id"]
    msg_url = f"https://graph.facebook.com/{API_VER}/{cid}/messages"
    msgs = fetch_all(msg_url, {
        "fields": "id,message,from,created_time",
        "limit": 50,
        "access_token": PAGE_TOKEN
    })
    print(f"\nConversation {cid} -> {len(msgs)} messages")
    for m in msgs[:3]:
        print(m.get("created_time"), "-", m.get("from", {}).get("name"), ":", m.get("message"))
