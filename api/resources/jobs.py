from threading import Thread
from flask import Blueprint, request, jsonify
from db.db import query_dict, execute
from services.job_service import create_job, get_running_job, update_job_status
from services.pipeline_runner import run_pipeline_job

# Standard Flask Blueprint
jobs_bp = Blueprint("jobs", __name__, url_prefix="/api")

# =========================
# GET JOB
# =========================
@jobs_bp.route("/job", methods=["GET"])
def get_job():
    # Manual extraction of job_id from query params
    job_id = request.args.get("job_id")

    if job_id:
        job_data = query_dict(
            "SELECT * FROM pipeline_jobs WHERE id=%s",
            (job_id,)
        )

        if not job_data:
            return jsonify({"message": "Job not found"}), 404

        job = job_data[0]
    else:
        # Get the most recent job if no ID is provided
        recent_jobs = query_dict("""
            SELECT * FROM pipeline_jobs 
            ORDER BY created_at DESC 
            LIMIT 1
        """)
        
        if not recent_jobs:
            return jsonify({"message": "No jobs found in database"}), 404
            
        job = recent_jobs[0]
        job_id = job["id"]

    logs = query_dict(
        "SELECT * FROM pipeline_job_logs WHERE job_id=%s ORDER BY id",
        (job_id,)
    )

    return jsonify({
        "job": job,
        "logs": logs
    })

# =========================
# JOB HEALTH
# =========================
@jobs_bp.route("/job-health", methods=["GET"])
def job_health():
    stuck = query_dict("""
        SELECT id, status, updated_at
        FROM pipeline_jobs
        WHERE status='RUNNING'
        AND updated_at < NOW() - INTERVAL 15 MINUTE
    """)

    return jsonify({
        "status": "warning" if stuck else "healthy",
        "stuck_jobs": stuck
    })

# =========================
# CLEANUP
# =========================
@jobs_bp.route("/cleanup-stuck-jobs", methods=["GET"])
def cleanup():
    execute("""
        UPDATE pipeline_jobs
        SET status='FAILED',
            error_message='Timeout'
        WHERE status='RUNNING'
        AND updated_at < NOW() - INTERVAL 20 MINUTE
    """)

    return jsonify({
        "message": "cleanup done"
    })

# =========================
# STOP JOB
# =========================
@jobs_bp.route("/stop-job", methods=["GET"])
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

# =========================
# RUN Job
# =========================
@jobs_bp.route("/run-job", methods=["GET"])
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