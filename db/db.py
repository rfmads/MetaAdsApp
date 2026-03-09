# db/db.py
from typing import Any, Dict, List, Optional, Iterable, Union, Sequence
import mysql.connector
from mysql.connector import pooling
from mysql.connector.connection import MySQLConnection

from config.config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
from logs.logger import logger

ParamsType = Optional[Union[Dict[str, Any], Sequence[Any]]]

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
        logger.info("MySQL connection pool initialized successfully.")

    return _POOL


def get_connection() -> MySQLConnection:
    """
    Return a live connection from the pool.
    """
    pool = _get_pool()
    return pool.get_connection()


def test_connection() -> bool:
    """
    Test DB connection and return True if success.
    """
    conn = None
    try:
        conn = get_connection()
        if conn.is_connected():
            logger.info("Database connection successful.")
            return True
        return False
    except mysql.connector.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise
    finally:
        if conn:
            conn.close()


def execute(sql: str, params: ParamsType = None) -> int:
    """
    Execute INSERT/UPDATE/DELETE and return affected rows.
    Supports tuple/list/dict params.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        affected = cur.rowcount
        cur.close()
        return affected
    except mysql.connector.Error as e:
        logger.error(f"DB execute error: {e} | SQL: {sql} | Params: {params}")
        raise
    finally:
        conn.close()


def execute_many(sql: str, rows: Iterable[Union[Dict[str, Any], Sequence[Any]]]) -> int:
    """
    Execute batch insert/update using executemany.
    Returns total affected rows.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.executemany(sql, list(rows))
        affected = cur.rowcount
        cur.close()
        return affected
    except mysql.connector.Error as e:
        logger.error(f"DB execute_many error: {e} | SQL: {sql}")
        raise
    finally:
        conn.close()


def query_dict(sql: str, params: ParamsType = None) -> List[Dict[str, Any]]:
    """
    Execute SELECT and return list of dict rows.
    Supports tuple/list/dict params.
    """
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
        return rows
    except mysql.connector.Error as e:
        logger.error(f"DB query_dict error: {e} | SQL: {sql} | Params: {params}")
        raise
    finally:
        conn.close()


def query_one(sql: str, params: ParamsType = None) -> Optional[Dict[str, Any]]:
    """
    Execute SELECT and return single row as dict or None.
    Supports tuple/list/dict params.
    """
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params)
        row = cur.fetchone()
        cur.close()
        return row
    except mysql.connector.Error as e:
        logger.error(f"DB query_one error: {e} | SQL: {sql} | Params: {params}")
        raise
    finally:
        conn.close()


def query_scalar(sql: str, params: ParamsType = None) -> Any:
    """
    Execute SELECT that returns a single value.
    Supports tuple/list/dict params.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        cur.close()
        return row[0] if row else None
    except mysql.connector.Error as e:
        logger.error(f"DB query_scalar error: {e} | SQL: {sql} | Params: {params}")
        raise
    finally:
        conn.close()