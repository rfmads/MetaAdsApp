from threading import Thread
from flask import Blueprint, request, jsonify
from api.resources.Services.facebook_ads import fetch_account_metrics, format_to_dataslayer
from api.resources.Services.facebook_insights import format_posts_to_dataslayer, fetch_facebook_insights
from api.resources.Services.instagram_ads import fetch_instagram_insights
from db.db import query_dict, execute
from logs import logger
from services.job_service import create_job, get_running_job, update_job_status
from services.pipeline_runner import run_pipeline_job

# Standard Flask Blueprint
rfmdata = Blueprint("rfmdata", __name__, url_prefix="/api")

@rfmdata.route("/get_facebook_metrics/", defaults={"token": None}, methods=["GET"])
@rfmdata.route("/get_facebook_metrics/<path:token>", methods=["GET"])
def get_facebook_metrics(token):
    # 1. Parse arguments
    include_static = None

    # 2. Check for an existing running job
    running_job = get_running_job()

    if not running_job:
        # 3. No job is running, so create and start one in the background
        job_id = create_job(include_static=include_static)
        update_job_status(job_id, "RUNNING")

        job_context = {
            "id": job_id,
            "include_static": include_static
        }

        # Fire and forget: the thread runs independently of this request
        Thread(
            target=run_pipeline_job,
            args=(job_context,),
            daemon=True
        ).start()
        
        logger.info(f"🚀 No job was active. Started new background job ID: {job_id}")
    else:
        logger.info(f"🔄 Job {running_job['id']} is already running. Skipping trigger.")

    # 4. IMMEDIATELY return existing data
    # We do NOT wait for the thread. We return what is currently in the DB.
    try:
        raw_data = fetch_account_metrics()
        data = format_to_dataslayer(raw_data)
        return jsonify(data), 200
    except Exception as e:
        logger.error(f"❌ Error fetching existing metrics: {e}")
        return jsonify({"error": "Failed to fetch existing data", "details": str(e)}), 500
    
@rfmdata.route("/get_facebook_insights/", defaults={"token": None}, methods=["GET"])
@rfmdata.route("/get_facebook_insights/<path:token>", methods=["GET"])
def get_facebook_insights(token):
    include_static = request.args.get("include_static", "false").lower() == "true"
    data = format_posts_to_dataslayer(fetch_facebook_insights())
    return jsonify(data), 200

@rfmdata.route("/get_facebook_insights_videos/", defaults={"token": None}, methods=["GET"])
@rfmdata.route("/get_facebook_insights_videos/<path:token>", methods=["GET"])
def get_facebook_insights_videos(token):
    include_static = request.args.get("include_static", "false").lower() == "true"
    data = format_posts_to_dataslayer(fetch_facebook_insights())
    return jsonify(data), 200

@rfmdata.route("/get_instagram_insights/", defaults={"token": None}, methods=["GET"])
@rfmdata.route("/get_instagram_insights/<path:token>", methods=["GET"])
def get_instagram_insights(token):
    include_static = request.args.get("include_static", "false").lower() == "true"
    data = format_posts_to_dataslayer(fetch_instagram_insights())
    return jsonify(data), 200