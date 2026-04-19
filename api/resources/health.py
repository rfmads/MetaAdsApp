from flask_smorest import Blueprint
from flask import jsonify

from api.health_service import get_health_status

blp = Blueprint(
    "health",
    "health",
    url_prefix="/health",
    description="System Health"
)


@blp.route("/", methods=["GET"])
def health():
    return jsonify(get_health_status())