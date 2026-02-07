# utils/datetime_utils.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


def utc_now() -> datetime:
    """Timezone-aware UTC now."""
    return datetime.now(timezone.utc)


def to_utc(dt: datetime) -> datetime:
    """Ensure datetime is timezone-aware UTC."""
    if dt.tzinfo is None:
        # treat naive as UTC
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_mysql_naive_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """Convert aware UTC datetime -> naive (for MySQL DATETIME)."""
    if dt is None:
        return None
    dt_utc = to_utc(dt)
    return dt_utc.replace(tzinfo=None)


def parse_meta_datetime(value: Optional[str]) -> Optional[datetime]:
    """
    Returns timezone-aware UTC datetime.

    Meta examples:
      - 2010-09-04T20:25:22+0200
      - 2025-12-26T19:10:00+0000
      - 2025-12-26T19:10:00Z
    """
    if not value:
        return None

    # Normalize Z
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+0000"

    for fmt in ("%Y-%m-%dT%H:%M:%S%z",):
        try:
            dt = datetime.strptime(v, fmt)
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue

    # fallback: try first 19 as naive (assume UTC)
    try:
        dt = datetime.strptime(value[:19].replace("T", " "), "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None
