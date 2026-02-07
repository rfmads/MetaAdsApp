# db/repositories/ad_accounts_repo.py
from db.db import execute


def upsert_ad_account(r: dict) -> None:
    """
    Insert / Update ad account record.
    - Uses INSERT ... AS new to avoid deprecated VALUES()
    - Updates last_seen_at on every sync
    - Keeps existing values if incoming value is NULL
    """

    sql = """
    INSERT INTO ad_accounts (
        ad_account_id,
        name,
        currency,
        account_creation_date,
        timezone,
        portfolio_id,
        first_seen_at,
        last_seen_at
    )
    VALUES (
        %(ad_account_id)s,
        %(name)s,
        %(currency)s,
        %(account_creation_date)s,
        %(timezone)s,
        %(portfolio_id)s,
        NOW(),
        NOW()
    ) AS new
    ON DUPLICATE KEY UPDATE
        name = COALESCE(new.name, ad_accounts.name),
        currency = COALESCE(new.currency, ad_accounts.currency),
        account_creation_date = COALESCE(
            new.account_creation_date,
            ad_accounts.account_creation_date
        ),
        timezone = COALESCE(new.timezone, ad_accounts.timezone),
        portfolio_id = COALESCE(new.portfolio_id, ad_accounts.portfolio_id),
        last_seen_at = NOW();
    """

    execute(sql, r)
