from threading import Thread
from flask import Blueprint, request, jsonify
from api.resources.Services.facebook_ads import fetch_account_metrics, format_to_dataslayer
from api.resources.Services.facebook_insights import format_posts_to_dataslayer, fetch_facebook_insights
from db.db import query_dict, execute

# Standard Flask Blueprint
rfmdata = Blueprint("rfmdata", __name__, url_prefix="/api")

@rfmdata.route("/get_facebook_metrics/", defaults={"token": None}, methods=["GET"])
@rfmdata.route("/get_facebook_metrics/<path:token>", methods=["GET"])
def get_facebook_metrics(token):
    include_static = request.args.get("include_static", "false").lower() == "true"
    data = format_to_dataslayer(fetch_account_metrics())
    return jsonify(data), 200

@rfmdata.route("/get_facebook_insights/", defaults={"token": None}, methods=["GET"])
@rfmdata.route("/get_facebook_insights/<path:token>", methods=["GET"])
def get_facebook_insights(token):
    include_static = request.args.get("include_static", "false").lower() == "true"
    data = format_posts_to_dataslayer(fetch_facebook_insights())
    return jsonify(data), 200