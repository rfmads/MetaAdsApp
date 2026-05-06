# import json
# import time
# import uuid
# import logging
# from flask import Blueprint, g, request, jsonify
# from threading import Thread
# from db.db import execute, query_dict
# from db.config_store import get_config
# from logs import logger
# from services.job_service import create_job, get_running_job, update_job_status
# from services.pipeline_runner import run_pipeline_job

# # Standard Flask Blueprint
# pipeline_bp = Blueprint("pipeline", __name__, url_prefix="/api")

# logger = logging.getLogger("api_logger")
# logger.setLevel(logging.INFO)

# if not logger.handlers:
#     handler = logging.StreamHandler()
#     formatter = logging.Formatter(
#         '%(asctime)s | %(levelname)s | %(message)s'
#     )
#     handler.setFormatter(formatter)
#     logger.addHandler(handler)    

# @pipeline_bp.before_request
# def before_request():

#     g.request_id = str(uuid.uuid4())
#     g.start_time = time.time()

#     # 🔥 LOG TO DB (BEFORE EXECUTION)
#     log_to_db({
#         "request_id": g.request_id,
#         "endpoint": request.path,
#         "method": request.method,
#         "query_params": request.args.to_dict(),
#         "job_id": None,
#         "status": "STARTED",
#         "response_code": None,
#         "duration_ms": None,
#         "error_message": None,
#         "ip_address": request.remote_addr,
#         "user_agent": request.headers.get("User-Agent"),
#         "headers": json.dumps(dict(request.headers))
#     })

#     logger.info(
#         f"[{g.request_id}] START {request.method} {request.path} "
#         f"| IP={request.remote_addr}"
#     )
# @pipeline_bp.after_request
# def after_request(response):

#     duration = int((time.time() - g.start_time) * 1000)

#     log_to_db({
#         "request_id": g.request_id,
#         "endpoint": request.path,
#         "method": request.method,
#         "query_params": request.args.to_dict(),
#         "job_id": getattr(g, "job_id", None),
#         "status": "SUCCESS",
#         "response_code": response.status_code,
#         "duration_ms": duration,
#         "error_message": None,
#         "ip_address": request.remote_addr,
#         "user_agent": request.headers.get("User-Agent"),
#         "headers": json.dumps(dict(request.headers))
#     })

#     logger.info(
#         f"[{g.request_id}] END | {response.status_code} | {duration}ms"
#     )
#     return response    
# @pipeline_bp.errorhandler(Exception)
# def handle_error(e):

#     duration = int((time.time() - g.start_time) * 1000)

#     log_to_db({
#         "request_id": g.request_id,
#         "endpoint": request.path,
#         "method": request.method,
#         "query_params": request.args.to_dict(),
#         "job_id": getattr(g, "job_id", None),
#         "status": "ERROR",
#         "response_code": 500,
#         "duration_ms": duration,
#         "error_message": str(e),
#         "ip_address": request.remote_addr,
#         "user_agent": request.headers.get("User-Agent"),
#         "headers": json.dumps(dict(request.headers))
#     })

#     logger.error(f"[{g.request_id}] ERROR: {str(e)}", exc_info=True)

#     return jsonify({"error": "Internal Server Error"}), 500

# # @pipeline_bp.route("/get_results/", defaults={"token": None}, methods=["GET"])
# # @pipeline_bp.route("/get_results/<path:token>", methods=["GET"])
# # def get_results(token):

# #     include_static = request.args.get("include_static", "false").lower() == "true"

# #     job = get_valid_job(include_static)

# #     if job:
# #         data = format_to_dataslayer(fetch_account_metrics())
# #         return jsonify(data), 200

# #     job_id = create_job(include_static=include_static)
# #     g.job_id = job_id

# #     update_job_status(job_id, "RUNNING")

# #     run_pipeline_job({
# #         "id": job_id,
# #         "include_static": include_static
# #     })

# #     data = format_to_dataslayer(fetch_account_metrics())
# #     return jsonify(data), 200
    

# def log_to_db(data):
#     try:
#         query = """
#         INSERT INTO api_logs (
#             request_id,
#             endpoint,
#             method,
#             query_params,
#             job_id,
#             status,
#             response_code,
#             duration_ms,
#             error_message,
#             ip_address,
#             user_agent,
#             headers
#         )
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#         """

#         values = (
#             data.get("request_id"),
#             data.get("endpoint"),
#             data.get("method"),
#             json.dumps(data.get("query_params")),
#             data.get("job_id"),
#             data.get("status"),
#             data.get("response_code"),
#             data.get("duration_ms"),
#             data.get("error_message"),
#             data.get("ip_address"),
#             data.get("user_agent"),
#             json.dumps(data.get("headers"))
#         )

#         execute(query, values)

#     except Exception as e:
#         logger.error(f"DB logging failed: {str(e)}", exc_info=True)

