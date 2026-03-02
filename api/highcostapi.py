import os
import requests
from datetime import datetime, timedelta
from flask import Flask, request, jsonify

app = Flask(__name__)

GRAPH_VERSION = os.getenv("GRAPH_VERSION", "v20.0")
GRAPH_BASE = f"https://graph.facebook.com/{GRAPH_VERSION}"

DEFAULT_DAYS = int(os.getenv("DEFAULT_DAYS", "5"))

# الافتراضي: محادثات الرسائل (قد تختلف حسب نوع حملاتك)
DEFAULT_RESULT_ACTION_TYPE = os.getenv(
    "DEFAULT_RESULT_ACTION_TYPE",
    "onsite_conversion.messaging_conversation_started_7d"
)

# ----------------------------
# Helpers
# ----------------------------

def _get_token(req_json: dict) -> str:
    """
    1) من الطلب (لو بتبعتيه من الـ frontend) token
    2) أو من ENV على السيرفر
    """
    token = (req_json or {}).get("access_token") or os.getenv("META_ACCESS_TOKEN")
    if not token:
        raise ValueError("Missing access_token. Provide in request or set META_ACCESS_TOKEN in server env.")
    return token

def graph_get(path: str, params: dict, token: str) -> dict:
    params = dict(params or {})
    params["access_token"] = token
    url = f"{GRAPH_BASE}/{path.lstrip('/')}"
    r = requests.get(url, params=params, timeout=60)
    try:
        data = r.json()
    except Exception:
        raise RuntimeError(f"Meta response not JSON. status={r.status_code} text={r.text[:200]}")

    if r.status_code >= 400 or ("error" in data):
        raise RuntimeError(f"Meta API error: {data.get('error', data)}")
    return data

def last_n_days_range(days: int):
    # Meta expects YYYY-MM-DD
    end = datetime.utcnow().date()
    start = end - timedelta(days=days)
    return start.isoformat(), end.isoformat()

def pick_delta(currency: str) -> float:
    c = (currency or "").upper()
    if c == "USD":
        return 1.3
    if c in ("ILS", "NIS"):
        return 5.0
    # fallback: خليها 0 أو قيمة صغيرة
    return 0.0

def safe_float(x, default=None):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def extract_action_value(actions_list, action_type: str) -> float:
    """
    actions: [{"action_type":"...", "value":"12"}, ...]
    """
    if not actions_list or not action_type:
        return 0.0
    for a in actions_list:
        if a.get("action_type") == action_type:
            return safe_float(a.get("value"), 0.0)
    return 0.0

def paginate_insights(initial: dict, token: str) -> list:
    """
    Meta insights returns: {"data":[...], "paging":{"next":"..."}}
    We'll follow paging.next if exists.
    """
    out = []
    cur = initial
    while True:
        out.extend(cur.get("data", []))
        next_url = (cur.get("paging") or {}).get("next")
        if not next_url:
            break
        r = requests.get(next_url, timeout=60)
        cur = r.json()
        if "error" in cur:
            raise RuntimeError(f"Meta paging error: {cur['error']}")
    return out

# ----------------------------
# Core logic
# ----------------------------

def get_ad_account_currency(ad_account_id: str, token: str) -> str:
    acc = graph_get(f"act_{ad_account_id}", params={"fields": "currency"}, token=token)
    return acc.get("currency")

def fetch_ads_creatives_map(ad_account_id: str, token: str, limit: int = 500) -> dict:
    """
    يرجّع map: ad_id -> thumbnail_url (+ page_id if we can infer)
    """
    fields = "id,name,effective_status,creative{thumbnail_url,object_story_id,effective_object_story_id}"
    data = graph_get(
        f"act_{ad_account_id}/ads",
        params={
            "fields": fields,
            "limit": str(min(limit, 500))
        },
        token=token
    )

    ads = data.get("data", [])
    out = {}

    # pagination for ads list (optional)
    while True:
        for ad in ads:
            ad_id = ad.get("id")
            creative = ad.get("creative") or {}
            thumb = creative.get("thumbnail_url")
            eosid = creative.get("effective_object_story_id") or creative.get("object_story_id")

            # محاولة استنتاج page_id من effective_object_story_id (عادةً يكون: {pageid}_{postid})
            inferred_page_id = None
            if isinstance(eosid, str) and "_" in eosid:
                inferred_page_id = eosid.split("_", 1)[0]

            out[ad_id] = {
                "thumbnail_url": thumb,
                "effective_object_story_id": eosid,
                "inferred_page_id": inferred_page_id,
                "effective_status": ad.get("effective_status"),
                "ad_name": ad.get("name"),
            }

        next_url = (data.get("paging") or {}).get("next")
        if not next_url:
            break
        r = requests.get(next_url, timeout=60)
        data = r.json()
        if "error" in data:
            raise RuntimeError(f"Meta ads paging error: {data['error']}")
        ads = data.get("data", [])

    return out

def fetch_ad_insights(ad_account_id: str, token: str, days: int, result_action_type: str) -> list:
    """
    Insights على مستوى الإعلان لآخر N أيام (مجمّعة).
    """
    since, until = last_n_days_range(days)

    initial = graph_get(
        f"act_{ad_account_id}/insights",
        params={
            "level": "ad",
            "time_range": f'{{"since":"{since}","until":"{until}"}}',
            "time_increment": "all_days",  # تجميع الفترة
            "fields": ",".join([
                "ad_id",
                "ad_name",
                "adset_id",
                "adset_name",
                "campaign_id",
                "campaign_name",
                "spend",
                "actions",
                "cost_per_action_type"
            ]),
            "limit": "500"
        },
        token=token
    )

    rows = paginate_insights(initial, token=token)
    # نحسب results و cost_per_result محليًا لضمان نفس المنطق
    for r in rows:
        spend = safe_float(r.get("spend"), 0.0)
        results = extract_action_value(r.get("actions"), result_action_type)
        cpr = (spend / results) if results and results > 0 else None
        r["_results"] = results
        r["_cpr"] = cpr
    return rows

def compute_avg_cpr(rows: list) -> float:
    """
    متوسط CPR عبر كل الإعلانات (weighted by results) لتكون النتيجة عادلة.
    avg = total_spend / total_results
    """
    total_spend = 0.0
    total_results = 0.0
    for r in rows:
        spend = safe_float(r.get("spend"), 0.0)
        results = safe_float(r.get("_results"), 0.0)
        if results and results > 0:
            total_spend += spend
            total_results += results
    if total_results == 0:
        return 0.0
    return total_spend / total_results

# ----------------------------
# Routes
# ----------------------------

@app.get("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})

@app.post("/api/high-cost-ads")
def high_cost_ads():
    req = request.get_json(silent=True) or {}

    ad_account_id = str(req.get("ad_account_id") or "").strip()
    if not ad_account_id:
        return jsonify({"error": "Missing ad_account_id"}), 400

    days = int(req.get("days") or DEFAULT_DAYS)
    page_id_filter = str(req.get("page_id") or "").strip() or None
    result_action_type = str(req.get("result_action_type") or DEFAULT_RESULT_ACTION_TYPE).strip()

    try:
        token = _get_token(req)
        currency = get_ad_account_currency(ad_account_id, token)
        delta = pick_delta(currency)

        insights_rows = fetch_ad_insights(ad_account_id, token, days, result_action_type)
        avg_cpr = compute_avg_cpr(insights_rows)
        threshold = avg_cpr + delta if avg_cpr else delta  # لو ما في avg (0) خلي threshold=delta

        creatives_map = fetch_ads_creatives_map(ad_account_id, token)

        results = []
        for r in insights_rows:
            ad_id = r.get("ad_id")
            cpr = r.get("_cpr")
            if cpr is None:
                continue

            # فلترة Active فقط (من /ads)
            ad_meta = creatives_map.get(ad_id) or {}
            eff_status = (ad_meta.get("effective_status") or "").upper()
            if eff_status not in ("ACTIVE", "PAUSED"):  # إذا بدك ACTIVE فقط: خليها ("ACTIVE",)
                # غالبًا الأفضل ACTIVE فقط، بس خليت PAUSED اختيارياً
                pass

            # فلترة page_id إذا انوجد
            if page_id_filter:
                inferred_page_id = ad_meta.get("inferred_page_id")
                if inferred_page_id and inferred_page_id != page_id_filter:
                    continue
                # إذا ما قدرنا نستنتج page_id، ما بنقدر نفلتر 100% بدون استعلامات إضافية
                # فبنخليه يمر (أو تمنعه). هون خليته يمر.

            if cpr > threshold:
                results.append({
                    "ad": {
                        "id": ad_id,
                        "name": r.get("ad_name") or ad_meta.get("ad_name"),
                        "effective_status": eff_status or None,
                        "thumbnail_url": ad_meta.get("thumbnail_url"),
                    },
                    "adset": {
                        "id": r.get("adset_id"),
                        "name": r.get("adset_name"),
                    },
                    "campaign": {
                        "id": r.get("campaign_id"),
                        "name": r.get("campaign_name"),
                    },
                    "metrics": {
                        "days": days,
                        "currency": currency,
                        "result_action_type": result_action_type,
                        "spend": safe_float(r.get("spend"), 0.0),
                        "results": safe_float(r.get("_results"), 0.0),
                        "cost_per_result": cpr,
                        "avg_cost_per_result": avg_cpr,
                        "delta": delta,
                        "threshold": threshold,
                    }
                })

        return jsonify({
            "ad_account_id": ad_account_id,
            "currency": currency,
            "days": days,
            "result_action_type": result_action_type,
            "avg_cost_per_result": avg_cpr,
            "delta": delta,
            "threshold": threshold,
            "high_cost_ads_count": len(results),
            "data": results
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    # للتجربة محليًا فقط
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)