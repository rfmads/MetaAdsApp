from flask import Blueprint, request, jsonify
from threading import Thread

from db.db import execute, query_dict
from db.config_store import get_config
from services.job_service import create_job, get_running_job, update_job_status
from services.pipeline_runner import run_pipeline_job

# Standard Flask Blueprint
pipeline_bp = Blueprint("pipeline", __name__, url_prefix="/api")

# =========================
# RUN PIPELINE
# =========================
@pipeline_bp.route("/run-pipeline", methods=["GET"])
def run_pipeline():
    # 1. Manual parsing of include_static
    # Note: request.args.get returns a string, so we convert it to a boolean
    include_static_raw = request.args.get("include_static")
    include_static = None
    if include_static_raw is not None:
        include_static = include_static_raw.lower() == "true"

    # Check for concurrency (already running job)
    running_job = get_running_job()
    if running_job:
        return jsonify({
            "status": "blocked",
            "running_job_id": running_job["id"]
        }), 409

    # Create and start the job
    job_id = create_job(include_static=include_static)
    update_job_status(job_id, "RUNNING")

    job = {
        "id": job_id,
        "include_static": include_static
    }

    # Start background execution
    Thread(
        target=run_pipeline_job,
        args=(job,),
        daemon=True
    ).start()

    return jsonify({
        "status": "running",
        "job_id": job_id
    })


# =========================
# STOP JOB
# =========================
@pipeline_bp.route("/stop-job", methods=["GET"])
def stop_job():
    # 2. Manual extraction of job_id
    job_id = request.args.get("job_id")

    if not job_id:
        running = get_running_job()
        if not running:
            return jsonify({"message": "No active job"}), 404
        job_id = running["id"]

    update_job_status(job_id, "STOPPED")

    return jsonify({
        "status": "success",
        "job_id": job_id
    })