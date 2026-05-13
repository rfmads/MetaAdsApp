from typing import Any, Dict, List, Optional, Iterable, Union, Sequence
import mysql.connector
from mysql.connector import pooling
from mysql.connector.connection import MySQLConnection
from mysql.connector import errors
from config.config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
from logs.logger import logger

ParamsType = Optional[Union[Dict[str, Any], Sequence[Any]]]

_POOL: Optional[pooling.MySQLConnectionPool] = None


# =========================
# POOL INITIALIZATION
# =========================
def _get_pool() -> pooling.MySQLConnectionPool:
    global _POOL

    logger.info(f"_get_pool called, pool is None? {_POOL is None}")

    if _POOL is None:
        try:
            logger.info(
                f"Creating MySQL pool -> host={DB_HOST}, port={DB_PORT}, db={DB_NAME}, user={DB_USER}"
            )

            _POOL = pooling.MySQLConnectionPool(
                pool_name="metaads_pool",
                pool_size=100,
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                autocommit=True,
                pool_reset_session=True,
            )

            logger.info("MySQL connection pool initialized successfully (size=32).")

        except Exception as e:
            logger.error("❌ Failed to initialize MySQL pool", exc_info=True)
            raise

    return _POOL


# =========================
# CONNECTION HANDLING
# =========================
import time

def get_connection(retries=5, delay=0.2) -> MySQLConnection:
    last_error = None

    for attempt in range(retries):
        try:
            logger.debug("Acquiring DB connection")

            conn = _get_pool().get_connection()

            if not conn.is_connected():
                conn.reconnect(attempts=2, delay=1)

            return conn

        except errors.PoolError as e:
            last_error = e

            logger.warning(
                f"Pool exhausted "
                f"(attempt {attempt + 1}/{retries})"
            )

            time.sleep(delay)

        except Exception as e:
            logger.error(
                f"❌ Failed to get DB connection: {e}",
                exc_info=True
            )
            raise

    logger.error(f"❌ Pool exhausted after retries: {last_error}")

    raise Exception("Database is busy. Try again later.")


# =========================
# CONNECTION TEST
# =========================
def test_connection() -> bool:
    conn = None
    try:
        conn = get_connection()
        return conn.is_connected()
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False
    finally:
        if conn:
            conn.close()


# =========================
# EXECUTE (INSERT/UPDATE/DELETE)
# =========================
def execute(sql: str, params: ParamsType = None) -> int:
    conn = get_connection()
    cur = None

    try:
        cur = conn.cursor()
        cur.execute(sql, params or {})
        conn.commit()
        return cur.rowcount

    except mysql.connector.Error as e:
        logger.error(f"DB execute error: {e} | SQL: {sql}", exc_info=True)
        raise

    finally:
        if cur:
            cur.close()
        conn.close()


# =========================
# EXECUTE MANY
# =========================
def execute_many(sql: str, rows: Iterable[Union[Dict[str, Any], Sequence[Any]]]) -> int:
    conn = get_connection()
    cur = None

    try:
        cur = conn.cursor()
        cur.executemany(sql, list(rows))
        conn.commit()
        return cur.rowcount

    except mysql.connector.Error as e:
        logger.error(f"DB execute_many error: {e} | SQL: {sql}", exc_info=True)
        raise

    finally:
        if cur:
            cur.close()
        conn.close()


# =========================
# QUERY MANY (DICT)
# =========================
def query_dict(sql: str, params: ParamsType = None) -> List[Dict[str, Any]]:
    conn = get_connection()
    cur = None

    try:
        cur = conn.cursor(dictionary=True, buffered=True)
        cur.execute(sql, params or {})
        return cur.fetchall()

    except mysql.connector.Error as e:
        logger.error(f"DB query_dict error: {e} | SQL: {sql}", exc_info=True)
        raise

    finally:
        if cur:
            cur.close()
        conn.close()


# =========================
# QUERY ONE
# =========================
def query_one(sql: str, params: ParamsType = None) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    cur = None

    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params or {})
        return cur.fetchone()

    except mysql.connector.Error as e:
        logger.error(f"DB query_one error: {e} | SQL: {sql}", exc_info=True)
        raise

    finally:
        if cur:
            cur.close()
        conn.close()


# =========================
# QUERY SCALAR
# =========================
def query_scalar(sql: str, params: ParamsType = None) -> Any:
    conn = get_connection()
    cur = None

    try:
        cur = conn.cursor()
        cur.execute(sql, params or {})
        row = cur.fetchone()
        return row[0] if row else None

    except mysql.connector.Error as e:
        logger.error(f"DB query_scalar error: {e} | SQL: {sql}", exc_info=True)
        raise

    finally:
        if cur:
            cur.close()
        conn.close()

# from typing import Any, Dict, List, Optional, Iterable, Union, Sequence
# import mysql.connector
# from mysql.connector import Error
# from config.config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
# from logs.logger import logger

# ParamsType = Optional[Union[Dict[str, Any], Sequence[Any]]]


# # =========================
# # SINGLE CONNECTION FACTORY
# # =========================
# def get_connection():
#     try:
#         conn = mysql.connector.connect(
#             host=DB_HOST,
#             port=DB_PORT,
#             user=DB_USER,
#             password=DB_PASSWORD,
#             database=DB_NAME,

#             connection_timeout=30,
#             connect_timeout=30,
#             read_timeout=60,
#             write_timeout=60,

#             autocommit=True,
#             use_pure=True
#         )

#         return conn

#     except Exception as e:
#         logger.error(f"DB connection failed: {e}", exc_info=True)
#         raise


# # =========================
# # EXECUTE
# # =========================
# def execute(sql: str, params: ParamsType = None) -> int:
#     conn = get_connection()
#     cur = None

#     try:
#         cur = conn.cursor()
#         cur.execute(sql, params or {})
#         conn.commit()
#         return cur.rowcount

#     except Error as e:
#         logger.error(f"DB execute error: {e} | SQL: {sql}", exc_info=True)
#         raise

#     finally:
#         if cur:
#             cur.close()
#         conn.close()


# # =========================
# # QUERY DICT
# # =========================
# def query_dict(sql: str, params: ParamsType = None) -> List[Dict[str, Any]]:
#     conn = get_connection()
#     cur = None

#     try:
#         cur = conn.cursor(dictionary=True)
#         cur.execute(sql, params or {})
#         return cur.fetchall()

#     except Error as e:
#         logger.error(f"DB query error: {e} | SQL: {sql}", exc_info=True)
#         raise

#     finally:
#         if cur:
#             cur.close()
#         conn.close()


# # =========================
# # QUERY ONE
# # =========================
# def query_one(sql: str, params: ParamsType = None) -> Optional[Dict[str, Any]]:
#     conn = get_connection()
#     cur = None

#     try:
#         cur = conn.cursor(dictionary=True)
#         cur.execute(sql, params or {})
#         return cur.fetchone()

#     except Error as e:
#         logger.error(f"DB query_one error: {e} | SQL: {sql}", exc_info=True)
#         raise

#     finally:
#         if cur:
#             cur.close()
#         conn.close()