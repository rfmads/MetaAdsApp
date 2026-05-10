# entities_worker.py

import os
import time
import random

from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

from logs.logger import logger
from db.db import query_dict, execute
from integrations.meta_graph_client import MetaGraphClient

from services.campaigns_service import sync_campaigns_for_account
from services.adsets_service import sync_adsets_for_account
from services.ads_service import sync_ads_for_account

from db.config_store import get_config
from services.job_service import heartbeat


# =========================================================
# ACCOUNT-LEVEL LOCKS (NOT GLOBAL)
# =========================================================

account_locks = {}
account_locks_guard = Lock()


def get_account_lock(ad_account_id):
    with account_locks_guard:
        if ad_account_id not in account_locks:
            account_locks[ad_account_id] = Lock()

        return account_locks[ad_account_id]


# =========================================================
# MYSQL DEADLOCK RETRY
# =========================================================

def retry_deadlock(fn, retries=5, base_delay=0.5):

    for attempt in range(retries):

        try:
            return fn()

        except Exception as e:

            msg = str(e).lower()

            deadlock = (
                "deadlock" in msg
                or "1213" in msg
                or "lock wait timeout" in msg
            )

            if not deadlock:
                raise

            if attempt >= retries - 1:
                raise

            sleep_time = (base_delay * (2 ** attempt)) + random.uniform(0, 0.3)

            logger.warning(
                f"⚠️ Deadlock retry "
                f"{attempt + 1}/{retries} "
                f"sleep={sleep_time:.2f}s "
                f"error={e}"
            )

            time.sleep(sleep_time)


# =========================================================
# META RATE LIMIT RETRY
# =========================================================

def retry_meta(fn, retries=5):

    for attempt in range(retries):

        try:
            return fn()

        except Exception as e:

            msg = str(e).lower()

            rate_limited = (
                "rate limit" in msg
                or "80004" in msg
                or "17" in msg
                or "4" in msg
            )

            if not rate_limited:
                raise

            sleep_time = min(
                60,
                (2 ** attempt) + random.uniform(0, 1)
            )

            logger.warning(
                f"⚠️ Meta rate limit retry "
                f"{attempt + 1}/{retries} "
                f"sleep={sleep_time:.2f}s "
                f"error={e}"
            )

            time.sleep(sleep_time)

    raise Exception("Meta retry failed")


# =========================================================
# DATABASE ERROR LOGGER
# =========================================================

def log_error_to_db(job_id, step, ad_account_id, error_message):

    if not job_id:
        return

    try:

        execute(
            """
            INSERT INTO pipeline_job_logs
            (
                job_id,
                step_name,
                status,
                message
            )
            VALUES
            (
                %s,
                %s,
                'FAILED',
                %s
            )
            """,
            (
                job_id,
                f"{step}:act_{ad_account_id}",
                error_message[:1000]
            )
        )

    except Exception as e:

        logger.error(f"⚠️ Failed to write log to DB: {e}")


# =========================================================
# ACCOUNT WORKER
# =========================================================

def _process_account(
    user_token: str,
    ad_account_id: int,
    portfolio_code: str,
    job_id=None
):

    act = f"act_{ad_account_id}"

    logger.info(f"🧵 START {act} portfolio={portfolio_code}")

    client = MetaGraphClient(user_token)

    result = {
        "ad_account_id": ad_account_id,
        "portfolio_code": portfolio_code,
        "campaigns": {"saved": 0},
        "adsets": {"saved": 0},
        "ads": {"saved": 0},
        "errors": [],
    }

    try:

        has_campaigns = query_dict(
            """
            SELECT 1
            FROM campaigns
            WHERE ad_account_id=%(id)s
            LIMIT 1
            """,
            {"id": ad_account_id},
        )

        first_time = not bool(has_campaigns)

        mode = "full" if first_time else "incremental"

        sync_days = 90 if first_time else 14

        # =====================================================
        # CAMPAIGNS
        # =====================================================

        try:

            with get_account_lock(ad_account_id):

                result["campaigns"] = retry_deadlock(
                    lambda: retry_meta(
                        lambda: sync_campaigns_for_account(
                            client=client,
                            ad_account_id=ad_account_id,
                            mode=mode,
                            days=sync_days
                        )
                    )
                )

        except Exception as e:

            err = f"Campaigns failed: {e}"

            logger.exception(f"🔥 {act} {err}")

            result["errors"].append(err)

            log_error_to_db(
                job_id,
                "Campaigns",
                ad_account_id,
                str(e)
            )

            return result

        # =====================================================
        # ADSETS
        # =====================================================

        try:

            result["adsets"] = retry_deadlock(
                lambda: retry_meta(
                    lambda: sync_adsets_for_account(
                        client=client,
                        ad_account_id=ad_account_id,
                        mode=mode,
                        days=sync_days
                    )
                )
            )

        except Exception as e:

            err = f"Adsets failed: {e}"

            logger.exception(f"🔥 {act} {err}")

            result["errors"].append(err)

            log_error_to_db(
                job_id,
                "Adsets",
                ad_account_id,
                str(e)
            )

            return result

        # =====================================================
        # ADS
        # =====================================================

        try:

            result["ads"] = retry_deadlock(
                lambda: retry_meta(
                    lambda: sync_ads_for_account(
                        client=client,
                        ad_account_id=ad_account_id,
                        mode=mode,
                        days=sync_days
                    )
                )
            )

        except Exception as e:

            err = f"Ads failed: {e}"

            logger.exception(f"🔥 {act} {err}")

            result["errors"].append(err)

            log_error_to_db(
                job_id,
                "Ads",
                ad_account_id,
                str(e)
            )

            return result

    except Exception as e:

        msg = f"Account Global Failure: {e}"

        logger.exception(f"🔥 {act} crashed")

        result["errors"].append(msg)

        log_error_to_db(
            job_id,
            "AccountGlobal",
            ad_account_id,
            msg
        )

    logger.info(
        f"🧵 DONE {act} | "
        f"C={result['campaigns'].get('saved',0)} "
        f"A={result['adsets'].get('saved',0)} "
        f"D={result['ads'].get('saved',0)}"
    )

    return result


# =========================================================
# MAIN RUN LOOP
# =========================================================

def run(job_id=None):

    user_token = get_config("META_USER_TOKEN")

    if not user_token:

        logger.error("❌ META_USER_TOKEN missing")

        return {
            "ok": False,
            "error": "Missing Token"
        }

    max_workers = int(os.getenv("SYNC_WORKERS", "2"))

    accounts = query_dict(
        """
        SELECT
            a.ad_account_id,
            p.code AS portfolio_code
        FROM ad_accounts a
        JOIN portfolios p
            ON p.id = a.portfolio_id
        WHERE p.code IN ('RFM','MAGIC_EXTREME')
        """
    )

    total_synced = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        future_to_acc = {

            executor.submit(
                _process_account,
                user_token,
                acc["ad_account_id"],
                acc["portfolio_code"],
                job_id
            ): acc

            for acc in accounts
        }

        for future in as_completed(future_to_acc):

            if job_id:
                heartbeat(job_id)

            acc_info = future_to_acc[future]

            try:

                res = future.result()

                for level in ["campaigns", "adsets", "ads"]:

                    data = res.get(level)

                    if isinstance(data, dict):

                        total_synced += data.get("saved", 0)

            except Exception as e:

                err_msg = f"Critical thread crash: {e}"

                logger.exception(
                    f"🔥 {err_msg} "
                    f"for {acc_info['ad_account_id']}"
                )

                log_error_to_db(
                    job_id,
                    "ThreadCrash",
                    acc_info["ad_account_id"],
                    err_msg
                )

    logger.info(
        f"🚀 FINISHED. "
        f"Total entities saved: {total_synced}"
    )

    return {
        "ok": True,
        "total_saved": total_synced
    }