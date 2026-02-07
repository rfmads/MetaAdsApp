# db/db.py
from typing import Any, Dict, List, Optional, Iterable
import mysql.connector
from mysql.connector import pooling

from config.config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
from logs.logger import logger

_POOL: Optional[pooling.MySQLConnectionPool] = None


def _get_pool() -> pooling.MySQLConnectionPool:
    global _POOL
    if _POOL is None:
        _POOL = pooling.MySQLConnectionPool(
            pool_name="metaads_pool",
            pool_size=5,
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            autocommit=True,
            pool_reset_session=True,
        )
    return _POOL


def execute(sql: str, params: Optional[Dict[str, Any]] = None) -> int:
    """
    Execute INSERT/UPDATE/DELETE and return affected rows.
    """
    pool = _get_pool()
    conn = pool.get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or {})
        affected = cur.rowcount
        cur.close()
        return affected
    except mysql.connector.Error as e:
        logger.error(f"DB execute error: {e}")
        raise
    finally:
        conn.close()


def execute_many(sql: str, rows: Iterable[Dict[str, Any]]) -> int:
    """
    Execute batch insert/update using executemany.
    Returns total affected rows (best-effort, depends on connector).
    """
    pool = _get_pool()
    conn = pool.get_connection()
    try:
        cur = conn.cursor()
        cur.executemany(sql, list(rows))
        affected = cur.rowcount
        cur.close()
        return affected
    except mysql.connector.Error as e:
        logger.error(f"DB execute_many error: {e}")
        raise
    finally:
        conn.close()


def query_dict(sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Execute SELECT and return list of dict rows.
    """
    pool = _get_pool()
    conn = pool.get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params or {})
        rows = cur.fetchall()
        cur.close()
        return rows
    except mysql.connector.Error as e:
        logger.error(f"DB query_dict error: {e}")
        raise
    finally:
        conn.close()


def query_one(sql: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """
    Execute SELECT and return single row as dict or None.
    """
    pool = _get_pool()
    conn = pool.get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params or {})
        row = cur.fetchone()
        cur.close()
        return row
    except mysql.connector.Error as e:
        logger.error(f"DB query_one error: {e}")
        raise
    finally:
        conn.close()


def query_scalar(sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """
    Execute SELECT that returns single value (e.g., COUNT(*)).
    """
    pool = _get_pool()
    conn = pool.get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or {})
        row = cur.fetchone()
        cur.close()
        return row[0] if row else None
    except mysql.connector.Error as e:
        logger.error(f"DB query_scalar error: {e}")
        raise
    finally:
        conn.close()
