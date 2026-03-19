from __future__ import annotations

from datetime import date, timedelta, datetime
from typing import Optional, Dict, Any, List

from flask import Flask, request, jsonify
from db.db import query_dict

app = Flask(__name__)

DEFAULT_DAYS = 5


def pick_delta(currency: str) -> float:
    c = (currency or "").upper()
    if c == "USD":
        return 1.3
    if c in ("ILS", "NIS"):
        return 5.0
    return 0.0


def date_since(days: int) -> str:
    return (date.today() - timedelta(days=days)).isoformat()


def get_currency(ad_account_id: str) -> str:
    row = query_dict(
        """
        SELECT currency
        FROM ad_accounts
        WHERE ad_account_id = %s
        LIMIT 1
        """,
        (ad_account_id,)
    )
    return (row[0]["currency"] if row else "") or ""


def fetch_ads_agg(ad_account_id: str, days: int, page_id: Optional[str] = None) -> List[Dict[str, Any]]:
    since = date_since(days)

    page_filter_sql = ""
    if page_id:
        page_filter_sql = " AND a.page_id = %s "
        params = (ad_account_id, since, page_id)
    else:
        params = (ad_account_id, since)

    rows = query_dict(
        f"""
        SELECT
            i.ad_id,
            a.name AS ad_name,
            a.adset_id,
            s.name AS adset_name,
            a.campaign_id,
            c.name AS campaign_name,
            SUM(i.spend) AS spend_sum,
            SUM(i.results) AS results_sum
        FROM ad_daily_insights i
        JOIN ads a
            ON a.ad_id = i.ad_id
        LEFT JOIN adsets s
            ON s.adset_id = a.adset_id
        LEFT JOIN campaigns c
            ON c.campaign_id = a.campaign_id
        WHERE c.ad_account_id = %s
          AND i.date >= %s
          {page_filter_sql}
          
        GROUP BY
            i.ad_id, a.name, a.adset_id, s.name, a.campaign_id, c.name
        """,
        params
    )

    for r in rows:
        spend = float(r.get("spend_sum") or 0.0)
        results = float(r.get("results_sum") or 0.0)
        r["cpr"] = (spend / results) if results > 0 else None

    return rows


def compute_avg_cpr(rows: List[Dict[str, Any]]) -> float:
    total_spend = 0.0
    total_results = 0.0

    for r in rows:
        spend = float(r.get("spend_sum") or 0.0)
        results = float(r.get("results_sum") or 0.0)

        if results > 0:
            total_spend += spend
            total_results += results

    return (total_spend / total_results) if total_results > 0 else 0.0


@app.get("/health")
def health():
    return jsonify({
        "status": "ok",
        "time": datetime.utcnow().isoformat()
    })


@app.get("/api/high-cost-ads")
def high_cost_ads():
    ad_account_id = request.args.get("ad_account_id", "").strip()
    page_id = request.args.get("page_id", "").strip() or None

    try:
        days = int(request.args.get("days", DEFAULT_DAYS))
    except ValueError:
        return jsonify({"error": "days must be an integer"}), 400

    if not ad_account_id:
        return jsonify({"error": "Missing ad_account_id"}), 400

    if days <= 0:
        return jsonify({"error": "days must be greater than 0"}), 400

    try:
        rows = fetch_ads_agg(
            ad_account_id=ad_account_id,
            days=days,
            page_id=page_id
        )

        currency = get_currency(ad_account_id)
        delta = pick_delta(currency)
        avg_cpr = compute_avg_cpr(rows)
        threshold = (avg_cpr + delta) if avg_cpr > 0 else delta

        data = []

        for r in rows:
            cpr = r.get("cpr")
            if cpr is None:
                continue

            if cpr > threshold:
                data.append({
                    "ad": {
                        "id": r.get("ad_id"),
                        "name": r.get("ad_name")
                    },
                    "adset": {
                        "id": r.get("adset_id"),
                        "name": r.get("adset_name")
                    },
                    "campaign": {
                        "id": r.get("campaign_id"),
                        "name": r.get("campaign_name")
                    },
                    "metrics": {
                        "days": days,
                        "currency": currency,
                        "spend": float(r.get("spend_sum") or 0.0),
                        "results": float(r.get("results_sum") or 0.0),
                        "cost_per_result": float(cpr),
                        "avg_cost_per_result": float(avg_cpr),
                        "delta": float(delta),
                        "threshold": float(threshold)
                    }
                })

        return jsonify({
            "mode": "high",
            "ad_account_id": ad_account_id,
            "days": days,
            "page_id": page_id,
            "currency": currency,
            "avg_cost_per_result": float(avg_cpr),
            "delta": float(delta),
            "threshold": float(threshold),
            "count": len(data),
            "data": data
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)