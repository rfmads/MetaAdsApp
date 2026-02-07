from datetime import datetime, timezone, timedelta

def utc_now():
    return datetime.now(timezone.utc)

def cutoff_days(days: int):
    return utc_now() - timedelta(days=days)
