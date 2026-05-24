from threading import Thread
from flask import Blueprint, request, jsonify
from api.resources.Services.facebook_ads import fetch_account_metrics, format_to_dataslayer
from api.resources.Services.facebook_insights import format_posts_to_dataslayer, fetch_facebook_insights
from api.resources.Services.instagram_ads import fetch_instagram_insights, format_instagram_to_dataslayer
from db.db import query_dict, execute
from services.job_service import cleanup_stuck_jobs, create_job, get_running_job, update_job_status
from services.pipeline_runner import run_pipeline_job

# Standard Flask Blueprint
rfmdata = Blueprint("rfmdata", __name__, url_prefix="/api")

@rfmdata.route("/get_facebook_metrics/", defaults={"token": None}, methods=["GET"])
@rfmdata.route("/get_facebook_metrics/<path:token>", methods=["GET"])
def get_facebook_metrics(token):

    include_static = None

    # Cleanup stuck jobs
    cleanup_stuck_jobs()

    # Check if a job is already running
    running_job = get_running_job()

    if not running_job:

        job_id = create_job(include_static=include_static)

        job_context = {
            "id": job_id,
            "include_static": include_static
        }

        try:
            Thread(
                target=run_pipeline_job,
                args=(job_context,),
                daemon=True
            ).start()

        except Exception as e:
            update_job_status(job_id, "FAILED", str(e))
            raise

    # Return existing data immediately
    try:
        raw_data = fetch_account_metrics()
        data = format_to_dataslayer(raw_data)
        return jsonify(data), 200

    except Exception as e:
        return jsonify({
            "error": "Failed to fetch existing data",
            "details": str(e)
        }), 500
    
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
    data = format_instagram_to_dataslayer(fetch_instagram_insights())
    return jsonify(data), 200