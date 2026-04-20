from flask import Blueprint, request, jsonify, abort
from db.config_store import get_config, set_config, is_token_valid

# Standard Blueprint
config_bp = Blueprint("config", __name__, url_prefix="/api")

# =========================
# CONFIG STATUS
# =========================
@config_bp.route("/config-status")
def config_status():
    token = get_config("META_USER_TOKEN")
    
    if not is_token_valid(token):
        # abort(400) is the built-in Flask way to throw errors
        return jsonify({"error": "Token validation failed"}), 400

    return jsonify({
        "status": "success",
        "detail": "Token is valid"
    })

# =========================
# CONFIG UPDATE
# =========================
@config_bp.route("/config-update")
def config_update():
    # Pure Flask: pull from request.args
    key = request.args.get("key")
    value = request.args.get("value")

    if not key or not value:
        return jsonify({"error": "Missing key or value"}), 400

    if key == "META_USER_TOKEN" and not is_token_valid(value):
        return jsonify({"error": "Invalid token"}), 400

    try:
        set_config(key, value)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({
        "status": "success",
        "message": f"{key} updated",
        "value": "********" if "TOKEN" in key else value
    })

# =========================
# CONFIG ALL
# =========================
@config_bp.route("/config-all")
def config_all():
    # Native Python dictionary to JSON
    return jsonify({
        "META_USER_TOKEN_SET": bool(get_config("META_USER_TOKEN")),
        "META_GRAPH_VERSION": get_config("META_GRAPH_VERSION"),
        "PAGE_ID": get_config("PAGE_ID")
    })



# from flask_smorest import Blueprint
# from marshmallow import Schema, fields
# from werkzeug.exceptions import BadRequest

# from db.config_store import get_config, set_config, is_token_valid
# from api.schemas.common import ConfigUpdateSchema

# blp = Blueprint(
#     "config",
#     "config",
#     url_prefix="/api",
#     description="Configuration APIs"
# )

# # =========================
# # CONFIG STATUS (NO INPUTS)
# # =========================
# @blp.route("/config-status")
# def config_status():
#     if not is_token_valid(get_config("META_USER_TOKEN")):
#         raise BadRequest("Token validation failed")

#     return {
#         "status": "success",
#         "detail": "Token is valid"
#     }

# # =========================
# # CONFIG UPDATE (WITH INPUTS)
# # =========================
# @blp.route("/config-update")
# @blp.arguments(ConfigUpdateSchema, location="query")
# @blp.response(200)
# def config_update(args):

#     key = args["key"]
#     value = args["value"]

#     if key == "META_USER_TOKEN" and not is_token_valid(value):
#         raise BadRequest("Invalid token")

#     set_config(key, value)

#     return {
#         "status": "success",
#         "message": f"{key} updated",
#         "value": "********" if "TOKEN" in key else value
#     }

# # =========================
# # CONFIG ALL (NO INPUTS)
# # =========================
# @blp.route("/config-all")
# def config_all():
#     return {
#         "META_USER_TOKEN_SET": bool(get_config("META_USER_TOKEN")),
#         "META_GRAPH_VERSION": get_config("META_GRAPH_VERSION"),
#         "PAGE_ID": get_config("PAGE_ID")
#     }