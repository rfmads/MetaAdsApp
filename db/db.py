from typing import Any, Dict, List, Optional, Iterable, Union, Sequence
import mysql.connector
from mysql.connector import pooling
from mysql.connector.connection import MySQLConnection
from mysql.connector import errors
from config.config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
from logs.logger import logger

ParamsType = Optional[Union[Dict[str, Any], Sequence[Any]]]

_POOL: Optional[pooling.MySQLConnectionPool] = None

def _get_pool() -> pooling.MySQLConnectionPool:
    global _POOL
    if _POOL is None:
        # Increased pool_size to 32 to handle threaded operations
        _POOL = pooling.MySQLConnectionPool(
            pool_name="metaads_pool",
            pool_size=32, 
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            autocommit=True,
            pool_reset_session=True,
        )
        logger.info("MySQL connection pool initialized successfully (size=32).")
    return _POOL

def get_connection() -> MySQLConnection:
    """Return a live connection from the pool with a health check."""
    conn = _get_pool().get_connection()
    try:
        # Reconnect=True tells the driver to try and restore the 
        # connection if the ping fails.
        conn.ping(reconnect=True, attempts=3, delay=1)
    except errors.Error as e:
        logger.error(f"Failed to ping MySQL connection: {e}")
        # If the connection is totally dead, try to get a fresh one once
        conn = _get_pool().get_connection()
    
    return conn
def test_connection() -> bool:
    """Test DB connection and return True if success."""
    conn = None
    try:
        conn = get_connection()
        return conn.is_connected()
    except mysql.connector.Error as e:
        logger.error(f"Database connection test failed: {e}")
        return False
    finally:
        if conn:
            conn.close()

def execute(sql: str, params: ParamsType = None) -> int:
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(sql, params or {})
        conn.commit()
        return cur.rowcount
    except mysql.connector.Error as e:
        logger.error(f"DB execute error: {e} | SQL: {sql}")
        raise
    finally:
        if cur: cur.close()
        conn.close()

def execute_many(sql: str, rows: Iterable[Union[Dict[str, Any], Sequence[Any]]]) -> int:
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.executemany(sql, list(rows))
        conn.commit()
        return cur.rowcount
    except mysql.connector.Error as e:
        logger.error(f"DB execute_many error: {e} | SQL: {sql}")
        raise
    finally:
        if cur: cur.close()
        conn.close()

def query_dict(sql: str, params: ParamsType = None) -> List[Dict[str, Any]]:
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params or {})
        return cur.fetchall()
    except mysql.connector.Error as e:
        logger.error(f"DB query_dict error: {e} | SQL: {sql}")
        raise
    finally:
        if cur: cur.close()
        conn.close()

def query_one(sql: str, params: ParamsType = None) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params or {})
        row = cur.fetchone()
        # Ensure result set is cleared before closing
        cur.fetchall() 
        return row
    except mysql.connector.Error as e:
        logger.error(f"DB query_one error: {e} | SQL: {sql}")
        raise
    finally:
        if cur: cur.close()
        conn.close()

def query_scalar(sql: str, params: ParamsType = None) -> Any:
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(sql, params or {})
        row = cur.fetchone()
        return row[0] if row else None
    except mysql.connector.Error as e:
        logger.error(f"DB query_scalar error: {e} | SQL: {sql}")
        raise
    finally:
        if cur: cur.close()
        conn.close()