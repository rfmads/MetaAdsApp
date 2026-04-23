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
    include_static_raw = request.args.get("include_static")
    include_static = None
    if include_static_raw is not None:
        include_static = include_static_raw.lower() == "true"

    running_job = get_running_job()
    if running_job:
        return jsonify({
            "error": {
                "message": "Another job is already running",
                "code": 409
            }
        }), 409

    job_id = create_job(include_static=include_static)
    update_job_status(job_id, "RUNNING")

    job = {
        "id": job_id,
        "include_static": include_static
    }

    Thread(
        target=run_pipeline_job,
        args=(job,),
        daemon=True
    ).start()

    return jsonify({
        "job_id": job_id
    }), 202

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

@pipeline_bp.route("/get_results", methods=["GET"])
def get_results():
    include_static_raw = request.args.get("include_static")
    include_static = None
    if include_static_raw is not None:
        include_static = include_static_raw.lower() == "true"

    # check if there is already a completed recent job
    job = get_latest_completed_job(include_static)

    if not job:
        # trigger job silently
        job_id = create_job(include_static=include_static)
        update_job_status(job_id, "RUNNING")

        Thread(
            target=run_pipeline_job,
            args=({"id": job_id, "include_static": include_static},),
            daemon=True
        ).start()

        # ⚠️ IMPORTANT: Dataslayer systems often expect 200, not 202
        return jsonify([]), 200

    # return actual results
    results = query_dict(
        "SELECT * FROM pipeline_results WHERE job_id=%s",
        (job["id"],)
    )

    return jsonify(results), 200

def get_latest_completed_job(include_static=None):
    if include_static is None:
        query = """
            SELECT *
            FROM pipeline_jobs
            WHERE status = 'COMPLETED'
            ORDER BY created_at DESC
            LIMIT 1
        """
        params = ()
    else:
        query = """
            SELECT *
            FROM pipeline_jobs
            WHERE status = 'COMPLETED'
            AND include_static = %s
            ORDER BY created_at DESC
            LIMIT 1
        """
        params = (include_static,)

    result = query_dict(query, params)

    return result[0] if result else None