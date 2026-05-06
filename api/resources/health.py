from flask import Blueprint, jsonify
from datetime import datetime
from db.db import query_dict

# 1. Create a standard Flask Blueprint
# We can set the url_prefix here to match your previous logic
health_bp = Blueprint("health", __name__, url_prefix="/health")

# 2. Use the standard route decorator
# Since the prefix is "/health", this route serves at "/health/"
@health_bp.route("/", methods=["GET"])
def health():
    # 3. Use jsonify for the response
    return jsonify(get_health_status())

def get_health_status():
    try:
        query_dict("SELECT 1")

        running_job = query_dict("""
            SELECT id, started_at
            FROM pipeline_jobs
            WHERE status = 'RUNNING'
            ORDER BY started_at DESC
            LIMIT 1
        """)

        return {
            "status": "ok",
            "time": datetime.utcnow().isoformat(),
            "database": "connected",
            "pipeline": {
                "running": bool(running_job),
                "job_id": running_job[0]["id"] if running_job else None
            }
        }

    except Exception as e:
        return {
            "status": "error",
            "time": datetime.utcnow().isoformat(),
            "error": str(e)
        }