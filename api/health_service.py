from datetime import datetime
from db.db import query_dict


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