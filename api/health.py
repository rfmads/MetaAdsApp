from datetime import datetime
from flask import jsonify
from db.db import query_dict

def health():
    try:
        # ✅ check DB connection
        query_dict("SELECT 1")

        # ✅ check if pipeline stuck
        running_job = query_dict("""
            SELECT id, started_at
            FROM pipeline_jobs
            WHERE status = 'RUNNING'
            ORDER BY started_at DESC
            LIMIT 1
        """)

        return jsonify({
            "status": "ok",
            "time": datetime.utcnow().isoformat(),
            "database": "connected",
            "pipeline": {
                "running": bool(running_job),
                "job_id": running_job[0]["id"] if running_job else None
            }
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "time": datetime.utcnow().isoformat(),
            "error": str(e)
        }), 500