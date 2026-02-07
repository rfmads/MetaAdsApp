# db/repositories/sync_checkpoints_repo.py
from typing import Optional
from db.db import query_dict, execute

def get_last_success(entity: str, scope_key: str) -> Optional[str]:
    rows = query_dict(
        """
        SELECT last_success_at
        FROM sync_checkpoints
        WHERE entity=%(entity)s AND scope_key=%(scope_key)s
        LIMIT 1
        """,
        {"entity": entity, "scope_key": scope_key},
    )
    if not rows or not rows[0]["last_success_at"]:
        return None
    # رجّعها كسلسلة "YYYY-MM-DD HH:MM:SS"
    return str(rows[0]["last_success_at"])

def set_last_success(entity: str, scope_key: str) -> None:
    execute(
        """
        INSERT INTO sync_checkpoints (entity, scope_key, last_success_at)
        VALUES (%(entity)s, %(scope_key)s, UTC_TIMESTAMP())
        ON DUPLICATE KEY UPDATE last_success_at=UTC_TIMESTAMP()
        """,
        {"entity": entity, "scope_key": scope_key},
    )
