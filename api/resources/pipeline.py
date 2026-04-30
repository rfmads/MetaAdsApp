import json
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

logger = logging.getLogger("api_logger")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)s | %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)    

@pipeline_bp.before_request
def before_request():

    g.request_id = str(uuid.uuid4())
    g.start_time = time.time()

    # 🔥 LOG TO DB (BEFORE EXECUTION)
    log_to_db({
        "request_id": g.request_id,
        "endpoint": request.path,
        "method": request.method,
        "query_params": request.args.to_dict(),
        "job_id": None,
        "status": "STARTED",
        "response_code": None,
        "duration_ms": None,
        "error_message": None,
        "ip_address": request.remote_addr,
        "user_agent": request.headers.get("User-Agent"),
        "headers": json.dumps(dict(request.headers))
    })

    logger.info(
        f"[{g.request_id}] START {request.method} {request.path} "
        f"| IP={request.remote_addr}"
    )
@pipeline_bp.after_request
def after_request(response):

    duration = int((time.time() - g.start_time) * 1000)

    log_to_db({
        "request_id": g.request_id,
        "endpoint": request.path,
        "method": request.method,
        "query_params": request.args.to_dict(),
        "job_id": getattr(g, "job_id", None),
        "status": "SUCCESS",
        "response_code": response.status_code,
        "duration_ms": duration,
        "error_message": None,
        "ip_address": request.remote_addr,
        "user_agent": request.headers.get("User-Agent"),
        "headers": json.dumps(dict(request.headers))
    })

    logger.info(
        f"[{g.request_id}] END | {response.status_code} | {duration}ms"
    )
    return response    

@pipeline_bp.errorhandler(Exception)
def handle_error(e):

    duration = int((time.time() - g.start_time) * 1000)

    log_to_db({
        "request_id": g.request_id,
        "endpoint": request.path,
        "method": request.method,
        "query_params": request.args.to_dict(),
        "job_id": getattr(g, "job_id", None),
        "status": "ERROR",
        "response_code": 500,
        "duration_ms": duration,
        "error_message": str(e),
        "ip_address": request.remote_addr,
        "user_agent": request.headers.get("User-Agent"),
        "headers": json.dumps(dict(request.headers))
    })

    logger.error(f"[{g.request_id}] ERROR: {str(e)}", exc_info=True)

    return jsonify({"error": "Internal Server Error"}), 500

@pipeline_bp.route("/get_results/", defaults={"token": None}, methods=["GET"])
@pipeline_bp.route("/get_results/<path:token>", methods=["GET"])
def get_results(token):

    include_static = request.args.get("include_static", "false").lower() == "true"

    job = get_valid_job(include_static)

    if job:
        data = format_to_dataslayer(fetch_account_metrics())
        return jsonify(data), 200

    job_id = create_job(include_static=include_static)
    g.job_id = job_id

    update_job_status(job_id, "RUNNING")

    run_pipeline_job({
        "id": job_id,
        "include_static": include_static
    })

    data = format_to_dataslayer(fetch_account_metrics())
    return jsonify(data), 200
    

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
            error_message,
            ip_address,
            user_agent,
            headers
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        values = (
            data.get("request_id"),
            data.get("endpoint"),
            data.get("method"),
            json.dumps(data.get("query_params")),
            data.get("job_id"),
            data.get("status"),
            data.get("response_code"),
            data.get("duration_ms"),
            data.get("error_message"),
            data.get("ip_address"),
            data.get("user_agent"),
            json.dumps(data.get("headers"))
        )

        execute(query, values)

    except Exception as e:
        logger.error(f"DB logging failed: {str(e)}", exc_info=True)

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
