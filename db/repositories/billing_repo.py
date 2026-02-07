# app/db/repositories/billing_repo.py

from db.db import execute


def upsert_billing(record: dict) -> None:
    """
    Upsert billing (latest state) for one ad account.
    billing has UNIQUE(ad_account_id)
    """
    sql = """
        INSERT INTO billing (
            ad_account_id,
            account_status,
            disable_reason,
            balance,
            amount_spent,
            spend_cap,
            is_prepay,
            checked_at,
            created_at,
            updated_at
        ) VALUES (
            %(ad_account_id)s,
            %(account_status)s,
            %(disable_reason)s,
            %(balance)s,
            %(amount_spent)s,
            %(spend_cap)s,
            %(is_prepay)s,
            %(checked_at)s,
            NOW(),
            NOW()
        )
        ON DUPLICATE KEY UPDATE
            account_status=VALUES(account_status),
            disable_reason=VALUES(disable_reason),
            balance=VALUES(balance),
            amount_spent=VALUES(amount_spent),
            spend_cap=VALUES(spend_cap),
            is_prepay=VALUES(is_prepay),
            checked_at=VALUES(checked_at),
            updated_at=NOW();
    """
    execute(sql, record)
