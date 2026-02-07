# db/repositories/sync_state_repo.py
from __future__ import annotations

from datetime import datetime
from typing import Optional, Dict, Any

from db.db import query_one, execute


def get_last_synced(sync_key: str) -> Optional[datetime]:
    row = query_one(
        """
        SELECT last_synced_at
        FROM sync_state
        WHERE sync_key = %(sync_key)s
        """,
        {"sync_key": sync_key},
    )
    if not row:
        return None
    return row.get("last_synced_at")


def set_last_synced(sync_key: str, last_synced_at: datetime) -> None:
    # MySQL DATETIME: الأفضل نخزّن naive UTC أو server time (حسب نظامك)
    execute(
        """
        INSERT INTO sync_state (sync_key, last_synced_at)
        VALUES (%(sync_key)s, %(last_synced_at)s)
        ON DUPLICATE KEY UPDATE
            last_synced_at = VALUES(last_synced_at),
            updated_at = CURRENT_TIMESTAMP
        """,
        {"sync_key": sync_key, "last_synced_at": last_synced_at},
    )
