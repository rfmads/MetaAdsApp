import os
from flask import Flask
from api.highcostapi import high_cost_ads, health as high_health
from api.lowcostapi import low_cost_ads

app = Flask(__name__)

app.add_url_rule("/api/high-cost-ads", view_func=high_cost_ads, methods=["GET","POST"])
app.add_url_rule("/api/low-cost-ads", view_func=low_cost_ads, methods=["GET","POST"])
app.add_url_rule("/health", view_func=high_health, methods=["GET"])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
