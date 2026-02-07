# db/repositories/portfolios_repo.py
from db.db import query_dict, execute

def get_or_create_portfolio(code: str, name: str, description: str | None = None) -> int:
    # MySQL 8.0.20+ : avoid VALUES() deprecation using alias
    sql = """
    INSERT INTO portfolios (code, name, description)
    VALUES (%(code)s, %(name)s, %(description)s) AS new
    ON DUPLICATE KEY UPDATE
        name = new.name,
        description = new.description;
    """
    execute(sql, {"code": code, "name": name, "description": description})

    row = query_dict("SELECT id FROM portfolios WHERE code=%(code)s LIMIT 1", {"code": code})
    return int(row[0]["id"])