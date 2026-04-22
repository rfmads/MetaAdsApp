from flask import Blueprint, request, jsonify
from db.db import query_dict, execute

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

@jobs_bp.route("/get_results", methods=["GET"])
def get_results():
    job_id = request.args.get("job_id")

    if not job_id:
        return jsonify({
            "error": {
                "message": "job_id is required"
            }
        }), 400

    job_data = query_dict(
        "SELECT * FROM pipeline_jobs WHERE id=%s",
        (job_id,)
    )

    if not job_data:
        return jsonify({
            "error": {
                "message": "Job not found"
            }
        }), 404

    job = job_data[0]

    # ⏳ Still running
    if job["status"] != "COMPLETED":
        return jsonify({
            "status": job["status"]
        }), 202

    # ✅ THIS IS THE MOST IMPORTANT PART
    results = query_dict(
        "SELECT * FROM pipeline_results WHERE job_id=%s",
        (job_id,)
    )

    # 🚨 Return RAW DATA ONLY (no wrapper)
    return jsonify(results), 200
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

# from flask_smorest import Blueprint
# from db.db import query_dict, execute
# from api.schemas.common import JobQuerySchema

# blp = Blueprint(
#     "jobs",
#     "jobs",
#     url_prefix="/api",
#     description="Jobs APIs"
# )

# # =========================
# # GET JOB (WITH INPUT)
# # =========================
# @blp.route("/job")
# @blp.arguments(JobQuerySchema, location="query")
# def get_job(args):

#     job_id = args.get("job_id")

#     if job_id:
#         job = query_dict(
#             "SELECT * FROM pipeline_jobs WHERE id=%s",
#             (job_id,)
#         )

#         if not job:
#             return {"message": "Job not found"}, 404

#         job = job[0]
#     else:
#         job = query_dict("""
#             SELECT * FROM pipeline_jobs
#             ORDER BY created_at DESC
#             LIMIT 1
#         """)[0]

#         job_id = job["id"]

#     logs = query_dict(
#         "SELECT * FROM pipeline_job_logs WHERE job_id=%s ORDER BY id",
#         (job_id,)
#     )

#     return {
#         "job": job,
#         "logs": logs
#     }

# # =========================
# # JOB HEALTH (NO INPUT)
# # =========================
# @blp.route("/job-health")
# def job_health():

#     stuck = query_dict("""
#         SELECT id, status, updated_at
#         FROM pipeline_jobs
#         WHERE status='RUNNING'
#         AND updated_at < NOW() - INTERVAL 15 MINUTE
#     """)

#     return {
#         "status": "warning" if stuck else "healthy",
#         "stuck_jobs": stuck
#     }

# # =========================
# # CLEANUP (NO INPUT)
# # =========================
# @blp.route("/cleanup-stuck-jobs")
# def cleanup():

#     execute("""
#         UPDATE pipeline_jobs
#         SET status='FAILED',
#             error_message='Timeout'
#         WHERE status='RUNNING'
#         AND updated_at < NOW() - INTERVAL 20 MINUTE
#     """)

#     return {
#         "message": "cleanup done"
#     }