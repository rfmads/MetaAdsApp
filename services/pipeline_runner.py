from datetime import datetime
from db.db import execute, query_dict
from logs.logger import logger

from workers import (
    ad_accounts_worker,
    pages_worker,
    entities_worker,
    posts_worker,
    insights_worker,
    billing_worker,
    creative_worker,
)

from services.job_service import update_job_status, log_step

def run_pipeline_job(job):
    job_id = job["id"]
    include_static = job.get("include_static")

    # Ensure the DB knows we are starting
    update_job_status(job_id, "RUNNING")

    steps = []

    # 1. Define steps based on include_static logic
    if include_static is True:
        steps = [
            ("ad_accounts", ad_accounts_worker.run),
            ("pages", pages_worker.run),
        ]
    elif include_static is None:
        steps = [
            ("ad_accounts", ad_accounts_worker.run),
            ("pages", pages_worker.run),
            ("entities", entities_worker.run),
            ("posts", posts_worker.run),
            ("creatives", creative_worker.run),
            ("insights", insights_worker.run),
            ("billing", billing_worker.run),
        ]
    else:  # include_static is False
        steps = [
            ("entities", entities_worker.run),
            ("posts", posts_worker.run),
            ("creatives", creative_worker.run),
            ("insights", insights_worker.run),
            ("billing", billing_worker.run),
        ]

    # 2. Execution Engine
    try:
        for name, func in steps:
            # 🛑 STOP CHECK: Re-query DB to see if status was changed to 'STOPPED'
            status_check = query_dict("SELECT status FROM pipeline_jobs WHERE id=%s", (job_id,))
            if status_check and status_check[0]["status"] == "STOPPED":
                logger.warning(f"🛑 Manual stop detected for job {job_id} before starting {name}.")
                log_step(job_id, name, "STOPPED", "Execution terminated by user.")
                return  # Exit the thread immediately
            execute("UPDATE pipeline_jobs SET updated_at = NOW() WHERE id = %s", (job_id,))
            # Normal Step Start
            log_step(job_id, name, "START", "started")
            start_time = datetime.now()
            
            # Run the worker function
            # result = func()
# ⭐ PASS JOB_ID HERE
            try:
                # Try passing the job_id (needed for entities_worker heartbeat)
                result = func(job_id=job_id)
            except TypeError:
                # If the worker function doesn't accept job_id yet, call it normally
                result = func()
            log_step(job_id, name, "SUCCESS", str(result))
            logger.info(f"✅ {name} done in {(datetime.now() - start_time).seconds}s")

        # If we finished the loop without being stopped or failing
        update_job_status(job_id, "SUCCESS")

    except Exception as e:
        # 🛑 STOP CHECK (Inside Exception): 
        # If the worker failed because we were trying to stop it, just exit.
        status_check = query_dict("SELECT status FROM pipeline_jobs WHERE id=%s", (job_id,))
        if status_check and status_check[0]["status"] == "STOPPED":
            logger.info(f"🛑 Job {job_id} stopped during exception handling.")
            return

        logger.error(f"❌ pipeline failed at step {name}: {e}")
        update_job_status(job_id, "FAILED", str(e))
        log_step(job_id, name, "FAILED", str(e))

        # 3. Retry logic: Only trigger if not manually stopped
        max_retries = job.get("max_retries", 3)
        current_retries = job.get("retries", 0)

        if current_retries < max_retries:
            logger.info(f"🔄 Queuing retry {current_retries + 1}/{max_retries} for job {job_id}")
            execute("""
                UPDATE pipeline_jobs
                SET status='PENDING',
                    retries=retries+1
                WHERE id=%s
            """, (job_id,))
            
# def run_pipeline_job(job):
#     job_id = job["id"]
#     include_static = job.get("include_static")

#     update_job_status(job_id, "RUNNING")

#     steps = []

#     # =========================
#     # CASE 1: ONLY STATIC
#     # =========================
#     if include_static is True:
#         steps = [
#             ("ad_accounts", ad_accounts_worker.run),
#             ("pages", pages_worker.run),
#         ]

#     # =========================
#     # CASE 2: FULL PIPELINE
#     # include_static is None
#     # =========================
#     elif include_static is None:
#         steps = [
#             ("ad_accounts", ad_accounts_worker.run),
#             ("pages", pages_worker.run),
#             ("entities", entities_worker.run),
#             ("posts", posts_worker.run),
#             ("creatives", creative_worker.run),
#             ("insights", insights_worker.run),
#             ("billing", billing_worker.run),
#         ]

#     # =========================
#     # CASE 3: EVERYTHING EXCEPT STATIC
#     # include_static is False
#     # =========================
#     else:
#         steps = [
#             ("entities", entities_worker.run),
#             ("posts", posts_worker.run),
#             ("creatives", creative_worker.run),
#             ("insights", insights_worker.run),
#             ("billing", billing_worker.run),
#         ]

#     # =========================
#     # EXECUTION ENGINE
#     # =========================
#     try:
#         for name, func in steps:
#             log_step(job_id, name, "START", "started")

#             start = datetime.now()
#             result = func()

#             log_step(job_id, name, "SUCCESS", str(result))

#             logger.info(f"✅ {name} done in {(datetime.now() - start).seconds}s")

#         update_job_status(job_id, "SUCCESS")

#     except Exception as e:
#         logger.error(f"❌ pipeline failed: {e}")

#         update_job_status(job_id, "FAILED", str(e))

#         log_step(job_id, name, "FAILED", str(e))

#         # retry logic
#         if job.get("retries", 0) < job.get("max_retries", 3):
#             execute("""
#                 UPDATE pipeline_jobs
#                 SET status='PENDING',
#                     retries=retries+1
#                 WHERE id=%s
#             """, (job_id,))