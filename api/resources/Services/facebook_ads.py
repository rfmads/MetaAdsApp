import json
import time
import uuid
import logging
from flask import Blueprint, g, request, jsonify
from threading import Thread
from db.db import execute, query_dict
from db.config_store import get_config

# def format_to_dataslayer(rows):
#     if not rows:
#         return {"result": []}

#     # Extract headers from keys
#     headers = list(rows[0].keys())

#     data = [headers]

#     for row in rows:
#         data.append([row[col] for col in headers])

#     return {"result": data}
def format_to_dataslayer(rows):
    headers = [
        "Account name",
        "Account id",
        "Account Currency",
        "Balance",
        "Account status",
        "Account amount spent",
        "Business country code",
        "Clicks",
        "Reach"
    ]

    data = [headers]

    for r in rows:
        data.append([
            r["Account name"],
            str(r["Account id"]),
            r["Account Currency"],
            str(r["Balance"] or 0),
            r["Account status"],
            str(r["Account amount spent"] or 0),
            r["Business country code"],
            int(r["Clicks"] or 0),
            int(r["Reach"] or 0)
        ])

    return {"result": data}

def fetch_account_metrics():
    return query_dict(""" 
       SELECT 
    a.name AS 'Account name',
    a.ad_account_id AS 'Account id',
    a.currency AS 'Account Currency',
    b.balance AS 'Balance',
    CASE
        WHEN b.account_status = 1 THEN 'ACTIVE'
        ELSE 'UNKNOWN'
    END AS 'Account status',
    b.amount_spent AS 'Account amount spent',
    'PS' AS 'Business country code',
    COALESCE(SUM(i.results), 0) AS 'Clicks',
    COALESCE(SUM(i.reach), 0) AS 'Reach'
FROM
    ad_accounts a
        INNER JOIN
    billing b ON b.ad_account_id = a.ad_account_id
        AND b.account_status = 1
        LEFT JOIN adsets s ON s.ad_account_id = a.ad_account_id
        LEFT JOIN ads ad ON ad.adset_id = s.adset_id
        LEFT JOIN ad_daily_insights i ON i.ad_id = ad.ad_id 
        GROUP BY a.ad_account_id
    """)