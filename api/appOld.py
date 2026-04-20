from http.client import HTTPException
import threading
from flask import Flask, jsonify, request
from api.highcosttest import high_cost_ads
from api.lowcosttest import low_cost_ads
from api.health_service import health
from db.config_store import get_config
from db.db import execute, query_dict
from services.job_service import create_job, get_running_job, update_job_status
from services.pipeline_runner import run_pipeline_job
from db.config_store import get_config, set_config, is_token_valid

app = Flask(__name__)
# existing APIs
app.add_url_rule("/api/high-cost-ads", view_func=high_cost_ads, methods=["GET"])

app.add_url_rule("/api/low-cost-ads", view_func=low_cost_ads, methods=["GET"])

app.add_url_rule("/health", view_func=health, methods=["GET"])

@app.route("/api/config-status", methods=["GET"])
def config_status():
    # 1. Validation check
    if not is_token_valid(get_config("META_USER_TOKEN")):
        # Raising an exception is fine as the framework handles it
        raise HTTPException(
            status_code=400, 
            detail="Token validation failed. The provided token is invalid or expired."
        )
    
    # 2. Success path (MUST USE RETURN)
    return jsonify({
        "status": "success",
        "detail": "Token is Valid"
    }), 200

@app.route("/api/config-update", methods=["GET"])
def update_config():

    key = request.args.get("key")
    value = request.args.get("value")
    """
    Updates a value in sys_config and refreshes the internal cache.
    If key is META_USER_TOKEN, it validates the token before saving.
    """
    # Optional Validation for Tokens
    if key == "META_USER_TOKEN" :
        if not is_token_valid(value):
            raise HTTPException(
                status_code=400, 
                detail="Token validation failed. The provided token is invalid or expired."
            )

    try:
        set_config(key, value)
        return {
            "status": "success",
            "message": f"Updated {key} successfully.",
            "new_value": value if "TOKEN" not in key else "********" # Hide token in response
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database update failed: {str(e)}")

@app.route("/api/config-all", methods=["GET"])
def list_all_configs():
    """Returns a view of all current configurations (IDs, Versions, etc.)"""
    return {
        "META_USER_TOKEN_SET": bool(get_config("META_USER_TOKEN")),
        "META_GRAPH_VERSION": get_config("META_GRAPH_VERSION"),
        "PAGE_ID": get_config("PAGE_ID")
    }

@app.route("/api/job-health", methods=["GET"])
def get_job_health():
    # Detects jobs that are 'RUNNING' but the DB row hasn't been touched in 15 mins
    stuck_jobs = query_dict("""
        SELECT id, status, started_at, updated_at,
               TIMESTAMPDIFF(MINUTE, updated_at, NOW()) as minutes_since_update
        FROM pipeline_jobs
        WHERE status = 'RUNNING'
        AND updated_at < NOW() - INTERVAL 15 MINUTE
    """)

    if not stuck_jobs:
        return jsonify({"status": "healthy", "stuck_jobs": []})

    return jsonify({
        "status": "warning",
        "message": f"Found {len(stuck_jobs)} stuck jobs",
        "stuck_jobs": stuck_jobs
    })

@app.route("/api/cleanup-stuck-jobs", methods=["GET"])
def cleanup_jobs():
    # Move anything 'RUNNING' and silent for 20 mins to 'FAILED'
    execute("""
        UPDATE pipeline_jobs 
        SET status = 'FAILED', 
            error_message = 'Job timed out or connection lost (Watchdog Cleanup)'
        WHERE status = 'RUNNING' 
        AND id IN (
            SELECT id FROM (
                SELECT j.id FROM pipeline_jobs j
                LEFT JOIN pipeline_job_logs l ON j.id = l.job_id
                GROUP BY j.id
                HAVING MAX(l.created_at) < NOW() - INTERVAL 20 MINUTE
            ) as stuck
        )
    """)
    return jsonify({"message": "Cleanup complete. Stuck jobs moved to FAILED."})

@app.route("/api/stop-job", methods=["GET"])
def stop_job():
    job_id = request.args.get("job_id")
    
    # If no job_id provided, find the one currently RUNNING
    if not job_id:
        running = get_running_job()
        if not running:
            return jsonify({"message": "No active job found to stop"}), 404
        job_id = running["id"]

    # Change status to STOPPED
    update_job_status(job_id, "STOPPED")
    
    return jsonify({
        "status": "success",
        "message": f"Signal sent to stop job {job_id}",
        "job_id": job_id
    })

@app.route("/api/run-pipeline", methods=["GET"])
def run_pipeline_api():

    include_static_raw = request.args.get("include_static")

    if include_static_raw is None:
        include_static = None
    elif include_static_raw.lower() == "true":
        include_static = True
    elif include_static_raw.lower() == "false":
        include_static = False
    else:
        include_static = None

    # 🚫 BLOCK if job already running
    running_job = get_running_job()
    if running_job:
        return jsonify({
            "status": "blocked",
            "message": "A pipeline job is already running",
            "running_job_id": running_job["id"]
        }), 409
    # ✅ create new job
    job_id = create_job(include_static=include_static)

    job = {
        "id": job_id,
        "include_static": include_static,
        "retries": 0,
        "max_retries": 3
    }

    # mark running
    update_job_status(job_id, "RUNNING")

    # run async
    thread = threading.Thread(
        target=run_pipeline_job,
        args=(job,),
        daemon=True
    )
    thread.start()

    return jsonify({
        "status": "running",
        "job_id": job_id
    })

@app.route("/api/job", methods=["GET"])
def get_job():

    job_id = request.args.get("job_id")

    # ✅ Case 1: job_id provided
    if job_id:
        job_rows = query_dict(
            "SELECT * FROM pipeline_jobs WHERE id=%s",
            (job_id,)
        )

        if not job_rows:
            return jsonify({
                "job": None,
                "logs": [],
                "message": "Job not found"
            }), 404

        job = job_rows[0]

    # ✅ Case 2: NO job_id → get last RUNNING job
    else:
        job_rows = query_dict("""
            SELECT * FROM pipeline_jobs
            WHERE status = 'RUNNING'
            ORDER BY created_at DESC
            LIMIT 1
        """)

        if not job_rows:
            job_rows = query_dict("""
                SELECT * FROM pipeline_jobs
                ORDER BY created_at DESC
                LIMIT 1
            """)

        job = job_rows[0]
        job_id = job["id"]

    # ✅ get logs
    logs = query_dict(
        "SELECT * FROM pipeline_job_logs WHERE job_id=%s ORDER BY id",
        (job_id,)
    )

    return jsonify({
        "job": job,
        "logs": logs
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)