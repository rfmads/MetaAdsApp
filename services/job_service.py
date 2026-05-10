from db.db import execute, get_connection, query_dict


def create_job(include_static=None, include_insights=True):
    sql = """
    INSERT INTO pipeline_jobs (job_type, include_static, include_insights)
    VALUES ('full_pipeline', %(include_static)s, %(include_insights)s)
    """

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(sql, {
        "include_static": include_static,
        "include_insights": include_insights
    })

    conn.commit()

    return cursor.lastrowid   # ⭐ THIS IS THE FIX

def get_pending_jobs(limit=1):
    return query_dict("""
        SELECT * FROM pipeline_jobs
        WHERE status = 'PENDING'
        ORDER BY created_at ASC
        LIMIT %s
    """, (limit,))


# def update_job_status(job_id, status, error=None):
#     execute("""
#         UPDATE pipeline_jobs
#         SET status=%(status)s,
#             error_message=%(error)s,
#             started_at=IF(%(status)s='RUNNING', NOW(), started_at),
#             finished_at=IF(%(status)s IN ('SUCCESS','FAILED'), NOW(), finished_at)
#         WHERE id=%(job_id)s
#     """, {
#         "job_id": job_id,
#         "status": status,
#         "error": error
#     })
def update_job_status(job_id, status, error=None):
    execute("""
        UPDATE pipeline_jobs
        SET status=%(status)s,
            error_message=%(error)s,
            started_at=IF(%(status)s='RUNNING', NOW(), started_at),
            finished_at=IF(%(status)s IN ('SUCCESS','FAILED','STOPPED'), NOW(), finished_at)
        WHERE id=%(job_id)s
    """, {
        "job_id": job_id,
        "status": status,
        "error": error
    })

def get_running_job():
    rows = query_dict("""
        SELECT * FROM pipeline_jobs
        WHERE status IN ('RUNNING', 'PENDING')
        ORDER BY created_at DESC
        LIMIT 1
    """)
    return rows[0] if rows else None

def cleanup_stuck_jobs():
    execute("""
        UPDATE pipeline_jobs
        SET status='FAILED',
            error_message='Timeout'
        WHERE status IN('RUNNING','PENDING')
        AND updated_at < NOW() - INTERVAL 60 MINUTE
    """)


def log_step(job_id, step, status, message=""):
    execute("""
        INSERT INTO pipeline_job_logs (job_id, step_name, status, message)
        VALUES (%(job_id)s, %(step)s, %(status)s, %(message)s)
    """, {
        "job_id": job_id,
        "step": step,
        "status": status,
        "message": message
    })
def heartbeat(job_id):
    execute("UPDATE pipeline_jobs SET updated_at = NOW() WHERE id = %s", (job_id,))    

def log_error(job_id, step, ad_account_id, error_message):
    execute("""
        INSERT INTO pipeline_job_logs
        (job_id, step_name, status, message)
        VALUES (%s, %s, 'FAILED', %s)
    """, (
        job_id,
        f"{step}:act_{ad_account_id}",
        error_message
    ))    