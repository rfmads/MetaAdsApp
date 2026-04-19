import threading
import os
from flask import Flask, jsonify, request
from flasgger import Swagger
from werkzeug.exceptions import BadRequest, InternalServerError

# Your existing imports
from api.highcosttest import high_cost_ads
from api.lowcosttest import low_cost_ads
from api.health_service import health
from db.config_store import get_config, set_config, is_token_valid
from db.db import execute, query_dict
from services.job_service import create_job, get_running_job, update_job_status
from services.pipeline_runner import run_pipeline_job

app = Flask(__name__)

# ✅ Load OpenAPI file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

swagger = Swagger(
    app,
    template_file=os.path.join(BASE_DIR, "docs", "openapi.yaml")
)

# =========================
# Existing APIs
# =========================
@app.route("/api/high-cost-ads", methods=["GET"])
def high_cost_ads_route():
    return high_cost_ads()


@app.route("/api/low-cost-ads", methods=["GET"])
def low_cost_ads_route():
    return low_cost_ads()


@app.route("/health", methods=["GET"])
def health_route():
    return health()


# =========================
# Config APIs
# =========================

@app.route("/api/config-status", methods=["GET"])
def config_status():
    if not is_token_valid(get_config("META_USER_TOKEN")):
        raise BadRequest("Token validation failed.")

    return jsonify({
        "status": "success",
        "detail": "Token is Valid"
    })


@app.route("/api/config-update", methods=["GET"])
def update_config():
    key = request.args.get("key")
    value = request.args.get("value")

    if not key or not value:
        raise BadRequest("key and value are required")

    if key == "META_USER_TOKEN" and not is_token_valid(value):
        raise BadRequest("Invalid token.")

    try:
        set_config(key, value)
        return jsonify({
            "status": "success",
            "message": f"Updated {key} successfully.",
            "new_value": value if "TOKEN" not in key else "********"
        })
    except Exception as e:
        raise InternalServerError(str(e))


@app.route("/api/config-all", methods=["GET"])
def list_all_configs():
    return jsonify({
        "META_USER_TOKEN_SET": bool(get_config("META_USER_TOKEN")),
        "META_GRAPH_VERSION": get_config("META_GRAPH_VERSION"),
        "PAGE_ID": get_config("PAGE_ID")
    })


# =========================
# Pipeline APIs
# =========================

@app.route("/api/run-pipeline", methods=["GET"])
def run_pipeline_api():
    include_static_raw = request.args.get("include_static")
    include_static = include_static_raw.lower() == "true" if include_static_raw else None

    running_job = get_running_job()
    if running_job:
        return jsonify({
            "status": "blocked",
            "running_job_id": running_job["id"]
        }), 409

    job_id = create_job(include_static=include_static)
    job = {"id": job_id, "include_static": include_static, "retries": 0, "max_retries": 3}

    update_job_status(job_id, "RUNNING")
    threading.Thread(target=run_pipeline_job, args=(job,), daemon=True).start()

    return jsonify({
        "status": "running",
        "job_id": job_id
    })


@app.route("/api/stop-job", methods=["GET"])
def stop_job():
    job_id = request.args.get("job_id") or (get_running_job() or {}).get("id")

    if not job_id:
        return jsonify({"message": "No active job found"}), 404

    update_job_status(job_id, "STOPPED")

    return jsonify({
        "status": "success",
        "job_id": job_id
    })


# =========================
# Monitoring APIs
# =========================

@app.route("/api/job", methods=["GET"])
def get_job():
    job_id = request.args.get("job_id")

    if job_id:
        job_rows = query_dict("SELECT * FROM pipeline_jobs WHERE id=%s", (job_id,))
        if not job_rows:
            return jsonify({"message": "Job not found"}), 404
        job = job_rows[0]
    else:
        job = query_dict("SELECT * FROM pipeline_jobs ORDER BY created_at DESC LIMIT 1")[0]
        job_id = job["id"]

    logs = query_dict("SELECT * FROM pipeline_job_logs WHERE job_id=%s ORDER BY id", (job_id,))

    return jsonify({
        "job": job,
        "logs": logs
    })


@app.route("/api/job-health", methods=["GET"])
def get_job_health():
    stuck_jobs = query_dict("""
        SELECT id, status, updated_at,
        TIMESTAMPDIFF(MINUTE, updated_at, NOW()) as minutes_since_update
        FROM pipeline_jobs
        WHERE status = 'RUNNING'
        AND updated_at < NOW() - INTERVAL 15 MINUTE
    """)

    return jsonify({
        "status": "healthy" if not stuck_jobs else "warning",
        "stuck_jobs": stuck_jobs
    })


@app.route("/api/cleanup-stuck-jobs", methods=["GET"])
def cleanup_jobs():
    execute("""
        UPDATE pipeline_jobs
        SET status = 'FAILED', error_message = 'Timeout'
        WHERE status = 'RUNNING'
        AND updated_at < NOW() - INTERVAL 20 MINUTE
    """)

    return jsonify({"message": "Cleanup complete."})


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5001, debug=True)