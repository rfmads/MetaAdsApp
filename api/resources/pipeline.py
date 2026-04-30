import time
import uuid
import logging
from flask import Blueprint, g, request, jsonify
from threading import Thread
from db.db import execute, query_dict
from db.config_store import get_config
from logs import logger
from services.job_service import create_job, get_running_job, update_job_status
from services.pipeline_runner import run_pipeline_job

# Standard Flask Blueprint
pipeline_bp = Blueprint("pipeline", __name__, url_prefix="/api")
@pipeline_bp.before_request
def set_request_id():
    g.request_id = str(uuid.uuid4())
    
logger = logging.getLogger("api_logger")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)s | %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)    
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

@pipeline_bp.route("/get_results/", defaults={"token": None}, methods=["GET"])
@pipeline_bp.route("/get_results/<path:token>", methods=["GET"])
def get_results(token):
    start_time = time.time()
    job_id = None
    status = "SUCCESS"
    response_code = 200
    error_message = None

    try:
        include_static = request.args.get("include_static", "false").lower() == "true"

        logger.info(f"[{g.request_id}] START /get_results | include_static={include_static}")

        job = get_valid_job(include_static)

        if job:
            job_id = job.get("id") if isinstance(job, dict) else None

            data = format_to_dataslayer(fetch_account_metrics())

            return jsonify(data), 200

        job_id = create_job(include_static=include_static)
        logger.info(f"[{g.request_id}] Created job {job_id}")

        update_job_status(job_id, "RUNNING")

        run_pipeline_job({
            "id": job_id,
            "include_static": include_static
        })

        data = format_to_dataslayer(fetch_account_metrics())

        return jsonify(data), 200

    except Exception as e:
        status = "ERROR"
        response_code = 500
        error_message = str(e)

        logger.error(f"[{getattr(g, 'request_id', None)}] ERROR: {error_message}", exc_info=True)

        return jsonify({"error": "Internal Server Error"}), 500

    finally:
        duration = int((time.time() - start_time) * 1000)

        log_to_db({
            "request_id": getattr(g, "request_id", None),
            "endpoint": "/get_results",
            "method": "GET",
            "query_params": request.args.to_dict(),
            "job_id": job_id,
            "status": status,
            "response_code": response_code,
            "duration_ms": duration,
            "error_message": error_message
        })

        logger.info(
            f"[{getattr(g, 'request_id', None)}] END "
            f"| status={status} | duration={duration}ms | job_id={job_id}"
        )
    
# @pipeline_bp.route("/get_results/", defaults={"token": None}, methods=["GET"])
# @pipeline_bp.route("/get_results/<path:token>", methods=["GET"])
# def get_results(token):
#     include_static = request.args.get("include_static", "false").lower() == "true"
    
#     job = get_valid_job(include_static)
#     if job:
#         return jsonify(format_to_dataslayer(fetch_account_metrics())), 200

#     job_id = create_job(include_static=include_static)
#     update_job_status(job_id, "RUNNING")
    
#     # Ensure this is blocking if you want the data in the same request
#     run_pipeline_job({"id": job_id, "include_static": include_static})

#     return jsonify(format_to_dataslayer(fetch_account_metrics())), 200

def fetch_account_metrics():
    return query_dict(""" 
       SELECT 
    a.name AS 'Account name',
    a.ad_account_id AS 'Account id',
    a.currency AS 'Account Currency',
    b.balance AS 'Balance',
    CASE
        WHEN b.account_status = 1 THEN 'ACTIVE'
        ELSE 'UNKNOWN'
    END AS 'Account status',
    b.amount_spent AS 'Account amount spent',
    'PS' AS 'Business country code',
    COALESCE(SUM(i.results), 0) AS 'Clicks',
    COALESCE(SUM(i.reach), 0) AS 'Reach'
FROM
    ad_accounts a
        INNER JOIN
    billing b ON b.ad_account_id = a.ad_account_id
        AND b.account_status = 1
        LEFT JOIN adsets s ON s.ad_account_id = a.ad_account_id
        LEFT JOIN ads ad ON ad.adset_id = s.adset_id
        LEFT JOIN ad_daily_insights i ON i.ad_id = ad.ad_id 
        GROUP BY a.ad_account_id
    """)
def format_to_dataslayer(rows):
    if not rows:
        return {"result": []}

    # Extract headers from keys
    headers = list(rows[0].keys())

    data = [headers]

    for row in rows:
        data.append([row[col] for col in headers])

    return {"result": data}

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
def log_to_db(data):
    try:
        query = """
        INSERT INTO api_logs (
            request_id,
            endpoint,
            method,
            query_params,
            job_id,
            status,
            response_code,
            duration_ms,
            error_message
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        values = (
            data.get("request_id"),
            data.get("endpoint"),
            data.get("method"),
            str(data.get("query_params")),
            data.get("job_id"),
            data.get("status"),
            data.get("response_code"),
            data.get("duration_ms"),
            data.get("error_message")
        )

        execute(query, values)   # <-- your DB function

    except Exception as e:
        logger.error(f"DB logging failed: {str(e)}", exc_info=True)
def get_valid_job(include_static=None):
    query = """
        SELECT *
        FROM pipeline_jobs
        WHERE (status = 'RUNNING'
               OR (status = 'SUCCESS' AND finished_at >= NOW() - INTERVAL 10 HOUR))
        ORDER BY created_at DESC
        LIMIT 1
    """

    result = query_dict(query)

    return result[0] if result else None

def format_to_dataslayer(rows):
    headers = [
        "Account name",
        "Account id",
        "Account Currency",
        "Balance",
        "Account status",
        "Account amount spent",
        "Business country code",
        "Clicks",
        "Reach"
    ]

    data = [headers]

    for r in rows:
        data.append([
            r["Account name"],
            str(r["Account id"]),
            r["Account Currency"],
            str(r["Balance"] or 0),
            r["Account status"],
            str(r["Account amount spent"] or 0),
            r["Business country code"],
            int(r["Clicks"] or 0),
            int(r["Reach"] or 0)
        ])

    return {"result": data}