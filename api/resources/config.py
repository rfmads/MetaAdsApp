from flask_smorest import Blueprint
from marshmallow import Schema, fields
from werkzeug.exceptions import BadRequest

from db.config_store import get_config, set_config, is_token_valid
from api.schemas.common import ConfigUpdateSchema

blp = Blueprint(
    "config",
    "config",
    url_prefix="/api",
    description="Configuration APIs"
)

# =========================
# CONFIG STATUS (NO INPUTS)
# =========================
@blp.route("/config-status")
def config_status():
    if not is_token_valid(get_config("META_USER_TOKEN")):
        raise BadRequest("Token validation failed")

    return {
        "status": "success",
        "detail": "Token is valid"
    }

# =========================
# CONFIG UPDATE (WITH INPUTS)
# =========================
@blp.route("/config-update")
@blp.arguments(ConfigUpdateSchema, location="query")
@blp.response(200)
def config_update(args):

    key = args["key"]
    value = args["value"]

    if key == "META_USER_TOKEN" and not is_token_valid(value):
        raise BadRequest("Invalid token")

    set_config(key, value)

    return {
        "status": "success",
        "message": f"{key} updated",
        "value": "********" if "TOKEN" in key else value
    }

# =========================
# CONFIG ALL (NO INPUTS)
# =========================
@blp.route("/config-all")
def config_all():
    return {
        "META_USER_TOKEN_SET": bool(get_config("META_USER_TOKEN")),
        "META_GRAPH_VERSION": get_config("META_GRAPH_VERSION"),
        "PAGE_ID": get_config("PAGE_ID")
    }