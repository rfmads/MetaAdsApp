from datetime import datetime
from db.db import execute, query_dict
from logs.logger import logger

from workers import (
    ad_accounts_worker,
    page_ad_account_worker,
    ad_posts_worker,
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

    update_job_status(job_id, "RUNNING")

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
            ("ad_posts", ad_posts_worker.run),
            ("page_ad_account", page_ad_account_worker.run),
        ]
    else:
        steps = [
            ("entities", entities_worker.run),
            ("posts", posts_worker.run),
            ("creatives", creative_worker.run),
            ("insights", insights_worker.run),
            ("billing", billing_worker.run),
            ("ad_posts", ad_posts_worker.run),
            ("page_ad_account", page_ad_account_worker.run),
        ]

    try:
        for name, func in steps:

            # STOP CHECK
            status_check = query_dict(
                "SELECT status FROM pipeline_jobs WHERE id=%s",
                (job_id,)
            )

            if status_check and status_check[0]["status"] == "STOPPED":
                logger.warning(f"🛑 Job stopped before {name}")
                log_step(job_id, name, "STOPPED", "User stopped job")
                update_job_status(job_id, "STOPPED")
                return

            log_step(job_id, name, "START", "started")
            start_time = datetime.now()

            try:
                result = func(job_id=job_id)
            except TypeError:
                result = func()

            # -----------------------------
            # STEP FAILED DETECTION
            # -----------------------------
            if isinstance(result, dict) and result.get("ok") is False:
                raise Exception(result.get("error", f"{name} failed"))

            log_step(job_id, name, "SUCCESS", str(result))

            logger.info(
                f"✅ {name} done in {(datetime.now() - start_time).seconds}s"
            )

        # FINAL SUCCESS
        update_job_status(job_id, "SUCCESS")

    except Exception as e:
        logger.error(f"❌ pipeline failed at step {name}: {e}")

        log_step(job_id, name, "FAILED", str(e))

        # ⭐ CRITICAL FIX: always mark job failed
        update_job_status(job_id, "FAILED", str(e))

        # retry logic
        max_retries = job.get("max_retries", 3)
        current_retries = job.get("retries", 0)

        if current_retries < max_retries:
            logger.info(
                f"🔄 retry {current_retries+1}/{max_retries} job {job_id}"
            )

            execute("""
                UPDATE pipeline_jobs
                SET status='PENDING',
                    retries=retries+1
                WHERE id=%s
            """, (job_id,))