from flask import jsonify, request
import pandas as pd
import traceback
from datetime import datetime

from db.db import query_dict

# =========================
# 🔹 جلب البيانات من DB
# =========================
def fetch_ads():
    rows = query_dict("""
         SELECT 
    a.ad_id,
    a.name AS ad_name,
    a.adset_id,
    a.campaign_id,
    c.ad_account_id AS account_id,
    acc.currency AS account_currency,
    SUM(i.spend) AS spend,
    SUM(i.results) AS results
FROM
    ad_daily_insights i
        JOIN
    ads a ON a.ad_id = i.ad_id
        JOIN
    campaigns c ON c.campaign_id = a.campaign_id
        LEFT JOIN
    ad_accounts acc ON acc.ad_account_id = c.ad_account_id
WHERE
    i.date >= CURDATE() - INTERVAL 5 DAY
        AND c.status = 'ACTIVE'
        AND c.effective_status = 'ACTIVE'
        AND c.real_status = 'ACTIVE'
        AND a.status = 'ACTIVE'
        AND a.effective_status = 'ACTIVE'
GROUP BY a.ad_id
    """)
    return pd.DataFrame(rows)


def fetch_avg():
    rows = query_dict("""
        SELECT
            c.campaign_id,
            c.ad_account_id AS account_id,
            acc.currency AS account_currency,
            SUM(i.spend) / NULLIF(SUM(i.results), 0) AS avg_cost
        FROM ad_daily_insights i
        JOIN ads a ON a.ad_id = i.ad_id
        JOIN campaigns c ON c.campaign_id = a.campaign_id
        LEFT JOIN ad_accounts acc ON acc.ad_account_id = c.ad_account_id
        WHERE i.date >= CURDATE() - INTERVAL 5 DAY
        GROUP BY c.campaign_id
    """)
    return pd.DataFrame(rows)


# =========================
# 🔹 تنظيف البيانات
# =========================
def clean_types(df):
    for col in ["campaign_id", "adset_id", "ad_id", "account_id"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# =========================
# 🔹 معالجة البيانات
# =========================
def process_data():
    df_ads = fetch_ads()
    df_avg = fetch_avg()

    if df_ads.empty or df_avg.empty:
        return pd.DataFrame()

    df_ads = clean_types(df_ads)
    df_avg = clean_types(df_avg)

    # cost per result
    df_ads["cost_per_result"] = df_ads.apply(
        lambda r: round(r["spend"] / r["results"], 2) if r["results"] and r["results"] > 0 else None,
        axis=1
    )
    # merge
    merged = pd.merge(
        df_ads,
        df_avg[["campaign_id", "avg_cost"]],
        on="campaign_id",
        how="left"
    )

    merged["avg_cost"] = merged["avg_cost"].fillna(0).astype(float).round(2)

    # threshold
    def calc_threshold(row):
        currency = str(row.get("account_currency", "")).upper()
        avg_cost = float(row.get("avg_cost", 0))

        if currency in ["ILS", "NIS"]:
            return avg_cost + 5.0
        return avg_cost + 1.3

    merged["threshold"] = merged.apply(calc_threshold, axis=1).round(2)

    return merged


# =========================
# 🔹 API: High Cost Ads
# =========================

def high_cost_ads(ad_account_id):
    try:
        if not ad_account_id:
            return jsonify({"error": "Missing ad_account_id"}), 400

        df = process_data()

        if df.empty:
            return jsonify({"data": [], "message": "No data found"})

        df["account_id"] = df["account_id"].astype(str)
        df = df[df["account_id"] == str(ad_account_id)]

        if df.empty:
            return jsonify({
                "data": [],
                "message": "No data for this ad account"
            })

        high_cost = df[
            (df["cost_per_result"].notnull()) &
            (df["cost_per_result"] > df["threshold"])
        ]

        result = high_cost[[
            "ad_id",
            "ad_name",
            "cost_per_result",
            "threshold"
        ]]

        return jsonify({
            "ad_account_id": ad_account_id,
            "count": len(result),
            "data": result.to_dict(orient="records")
        })

    except Exception as e:
        return jsonify({
            "error": str(e),
            "trace": traceback.format_exc()
        }), 500
# def high_cost_ads():
#     try:
#         ad_account_id = request.args.get("ad_account_id")

#         if not ad_account_id:
#             return jsonify({"error": "Missing ad_account_id"}), 400

#         df = process_data()

#         if df.empty:
#             return jsonify({"data": [], "message": "No data found"})

#         # توحيد النوع للمقارنة
#         df["account_id"] = df["account_id"].astype(str)

#         df = df[df["account_id"] == ad_account_id]

#         if df.empty:
#             return jsonify({
#                 "data": [],
#                 "message": "No data for this ad account"
#             })

#         high_cost = df[
#             (df["cost_per_result"].notnull()) &
#             (df["cost_per_result"] > df["threshold"])
#         ]

#         result = high_cost[[
#             "ad_id",
#             "ad_name",
#             "cost_per_result",
#             "threshold"
#         ]]

#         return jsonify({
#             "ad_account_id": ad_account_id,
#             "count": len(result),
#             "data": result.to_dict(orient="records")
#         })

#     except Exception as e:
#         return jsonify({
#             "error": str(e),
#             "trace": traceback.format_exc()
#         }), 500


# =========================
# 🔹 Health Check
# =========================
def health():
    return jsonify({
        "status": "ok",
        "time": datetime.utcnow().isoformat()
    })