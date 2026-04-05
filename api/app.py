''''
import os
from flask import Flask
from api.highcosttest import high_cost_ads, health as high_health
#from api.lowcostapi import low_cost_ad

app = Flask(__name__)

app.add_url_rule("/api/high-cost-ads", view_func=high_cost_ads, methods=["GET","POST"])
#app.add_url_rule("/api/low-cost-ads", view_func=low_cost_ads, methods=["GET","POST"])
app.add_url_rule("/health", view_func=high_health, methods=["GET"])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5001")), debug=False)
'''

import os
from flask import Flask

from api.highcosttest import high_cost_ads
from api.lowcosttest import low_cost_ads
from api.highcosttest import health

app = Flask(__name__)

# endpoints
app.add_url_rule("/api/high-cost-ads", view_func=high_cost_ads, methods=["GET"])
app.add_url_rule("/api/low-cost-ads", view_func=low_cost_ads, methods=["GET"])
app.add_url_rule("/health", view_func=health, methods=["GET"])


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)