from db.db import execute
from logs.logger import logger


def batch_execute(sql: str, rows: list[dict], name: str = ""):
    if not rows:
        return

    try:
        execute(sql, rows)
    except Exception as e:
        logger.error(f"❌ batch insert failed [{name}]: {e}")
        raise