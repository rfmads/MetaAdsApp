from flask import Blueprint, request, jsonify
from api.highcosttest import high_cost_ads
from api.lowcosttest import low_cost_ads

# Standard Blueprint without smorest
ads_bp = Blueprint("ads", __name__, url_prefix="/api")

# =========================
# High Cost Ads
# =========================
@ads_bp.route("/high-cost-ads", methods=["GET"])
def high_cost():
    # Manual extraction of ad_account_id from query string
    ad_account_id = request.args.get("ad_account_id")
    
    if not ad_account_id:
        return jsonify({"error": "Missing parameter: ad_account_id"}), 400
        
    # Calling your existing high_cost_ads logic
    return high_cost_ads(ad_account_id)


# =========================
# Low Cost Ads
# =========================
@ads_bp.route("/low-cost-ads", methods=["GET"])
def low_cost():
    # Manual extraction of ad_account_id from query string
    ad_account_id = request.args.get("ad_account_id")
    
    if not ad_account_id:
        return jsonify({"error": "Missing parameter: ad_account_id"}), 400
        
    # Calling your existing low_cost_ads logic
    return low_cost_ads(ad_account_id)



# from flask_smorest import Blueprint
# from api.highcosttest import high_cost_ads
# from api.lowcosttest import low_cost_ads

# from api.schemas.common import AdAccountSchema

# blp = Blueprint(
#     "ads",
#     "ads",
#     url_prefix="/api",
#     description="Ads Analytics APIs"
# )

# # =========================
# # High Cost Ads (WITH INPUT)
# # =========================
# @blp.route("/high-cost-ads", methods=["GET"])
# @blp.arguments(AdAccountSchema, location="query")
# def high_cost(args):
#     ad_account_id = args["ad_account_id"]
#     return high_cost_ads(ad_account_id)


# # =========================
# # Low Cost Ads (WITH INPUT)
# # =========================
# @blp.route("/low-cost-ads", methods=["GET"])
# @blp.arguments(AdAccountSchema, location="query")
# def low_cost(args):
#     ad_account_id = args["ad_account_id"]
#     return low_cost_ads(ad_account_id)