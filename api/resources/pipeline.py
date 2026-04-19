from flask_smorest import Blueprint
from threading import Thread

from db.db import execute, query_dict
from db.config_store import get_config
from services.job_service import create_job, get_running_job, update_job_status
from services.pipeline_runner import run_pipeline_job

from api.schemas.common import RunPipelineSchema, StopJobSchema


blp = Blueprint(
    "pipeline",
    "pipeline",
    url_prefix="/api",
    description="Pipeline APIs"
)


# =========================
# RUN PIPELINE (WITH INPUTS IN SWAGGER)
# =========================
@blp.arguments(RunPipelineSchema, location="query")
@blp.response(200)
@blp.route("/run-pipeline")
def run_pipeline(args):

    include_static = args.get("include_static")

    running_job = get_running_job()
    if running_job:
        return {
            "status": "blocked",
            "running_job_id": running_job["id"]
        }, 409

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

    return {
        "status": "running",
        "job_id": job_id
    }


# =========================
# STOP JOB (WITH INPUT IN SWAGGER)
# =========================
@blp.arguments(StopJobSchema, location="query")
@blp.response(200)
@blp.route("/stop-job")
def stop_job(args):

    job_id = args.get("job_id")

    if not job_id:
        running = get_running_job()
        if not running:
            return {"message": "No active job"}, 404
        job_id = running["id"]

    update_job_status(job_id, "STOPPED")

    return {
        "status": "success",
        "job_id": job_id
    }