from flask import Flask, jsonify
import pandas as pd
import traceback

from db.db import query_dict

app = Flask(__name__)

# ------------------- جلب البيانات من DB -------------------

def fetch_ads_from_db():
    rows = query_dict("""
        SELECT
            a.ad_id,
            a.name AS ad_name,
            a.adset_id,
            s.name AS adset_name,
            a.campaign_id,
            c.name AS campaign_name,
            c.ad_account_id AS account_id,
            acc.currency AS account_currency,
            SUM(i.spend) AS spend,
            SUM(i.results) AS results
        FROM ad_daily_insights i
        JOIN ads a ON a.ad_id = i.ad_id
        LEFT JOIN adsets s ON s.adset_id = a.adset_id
        LEFT JOIN campaigns c ON c.campaign_id = a.campaign_id
        LEFT JOIN ad_accounts acc ON acc.ad_account_id = c.ad_account_id
        WHERE i.date >= CURDATE() - INTERVAL 5 DAY
        GROUP BY a.ad_id
    """)

    return pd.DataFrame(rows)


def fetch_avg_from_db():
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

# ------------------- المعالجة -------------------

def process_data():
    df_ads = fetch_ads_from_db()
    df_avg = fetch_avg_from_db()

    if df_ads.empty or df_avg.empty:
        return pd.DataFrame()

    # حساب CPR
    df_ads["cost_per_result"] = df_ads.apply(
        lambda r: (r["spend"] / r["results"]) if r["results"] and r["results"] > 0 else None,
        axis=1
    )

    # دمج
    merged = pd.merge(
        df_ads,
        df_avg[["campaign_id", "avg_cost", "account_currency"]],
        on="campaign_id",
        how="left"
    )

    merged["avg_cost"] = merged["avg_cost"].fillna(0)

    # threshold
    def calc_threshold(row):
        currency = str(row.get("account_currency", "")).upper()
        avg_cost = row.get("avg_cost", 0)

        if currency in ["ILS", "NIS"]:
            return avg_cost + 5.0
        return avg_cost + 1.3

    merged["threshold"] = merged.apply(calc_threshold, axis=1)

    return merged

# ------------------- API -------------------

@app.route("/api/high-cost-ads", methods=["GET"])
def analyze():
    try:
        df = process_data()

        if df.empty:
            return jsonify({"data": [], "message": "No data found"})

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
            "count": len(result),
            "data": result.to_dict(orient="records")
        })

    except Exception as e:
        return jsonify({
            "error": str(e),
            "trace": traceback.format_exc()
        })

# ------------------- تشغيل -------------------

if __name__ == "__main__":
    app.run(debug=True, port=5001)