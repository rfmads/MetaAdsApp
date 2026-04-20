from flask import Blueprint, jsonify
from api.health_service import get_health_status 

# 1. Create a standard Flask Blueprint
# We can set the url_prefix here to match your previous logic
health_bp = Blueprint("health", __name__, url_prefix="/health")

# 2. Use the standard route decorator
# Since the prefix is "/health", this route serves at "/health/"
@health_bp.route("/", methods=["GET"])
def health():
    # 3. Use jsonify for the response
    return jsonify(get_health_status())


# from flask_smorest import Blueprint
# from flask import jsonify

# from api.health_service import get_health_status

# blp = Blueprint(
#     "health",
#     "health",
#     url_prefix="/health",
#     description="System Health"
# )


# @blp.route("/", methods=["GET"])
# def health():
#     return jsonify(get_health_status())