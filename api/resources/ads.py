from flask_smorest import Blueprint
from api.highcosttest import high_cost_ads
from api.lowcosttest import low_cost_ads

from api.schemas.common import AdAccountSchema

blp = Blueprint(
    "ads",
    "ads",
    url_prefix="/api",
    description="Ads Analytics APIs"
)

# =========================
# High Cost Ads (WITH INPUT)
# =========================
@blp.route("/high-cost-ads", methods=["GET"])
@blp.arguments(AdAccountSchema, location="query")
def high_cost(args):
    ad_account_id = args["ad_account_id"]
    return high_cost_ads(ad_account_id)


# =========================
# Low Cost Ads (WITH INPUT)
# =========================
@blp.route("/low-cost-ads", methods=["GET"])
@blp.arguments(AdAccountSchema, location="query")
def low_cost(args):
    ad_account_id = args["ad_account_id"]
    return low_cost_ads(ad_account_id)