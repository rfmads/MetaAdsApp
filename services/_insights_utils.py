# services/_insights_utils.py
from typing import Any, Dict, Optional


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None or x == "":
            return default
        return int(float(x))
    except Exception:
        return default


def extract_results_and_cpr(row: Dict[str, Any]) -> tuple[int, Optional[float]]:
    """
    Meta insights sometimes return:
    - results, cost_per_result (not always)
    or
    - actions, cost_per_action_type (common)

    This function tries best-effort:
    1) use results/cost_per_result if present
    2) else use first action value and first cost_per_action_type as fallback
    """
    results = _safe_int(row.get("results"), 0)
    cpr = _safe_float(row.get("cost_per_result"))

    if results or cpr is not None:
        return results, cpr

    actions = row.get("actions") or []
    if isinstance(actions, list) and actions:
        # take sum of all action values as "results" fallback
        total = 0
        for a in actions:
            total += _safe_int(a.get("value"), 0)
        results = total

    cpat = row.get("cost_per_action_type") or []
    if isinstance(cpat, list) and cpat:
        cpr = _safe_float(cpat[0].get("value"))

    return results, cpr
