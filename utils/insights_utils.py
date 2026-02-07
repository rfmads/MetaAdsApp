# utils/insights_utils.py
from decimal import Decimal, InvalidOperation

def _to_int(x, default=0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default

def _to_dec(x) -> Decimal | None:
    if x is None or x == "":
        return None
    try:
        return Decimal(str(x))
    except (InvalidOperation, Exception):
        return None

def compute_results_and_cpr(insight_row: dict) -> tuple[int, Decimal | None]:
    """
    Meta API لا تعطي "results" جاهز بشكل ثابت لكل الأهداف.
    هنا بنعمل حل عام:
      - results = مجموع كل actions.value
      - cost_per_result = spend/results إذا results>0
    (حل عام مناسب كبداية، وبعدين بنخصصه حسب objective/optimization_goal)
    """
    actions = insight_row.get("actions") or []
    results = 0
    for a in actions:
        results += _to_int(a.get("value"), 0)

    spend = _to_dec(insight_row.get("spend"))
    if results > 0 and spend is not None:
        return results, (spend / Decimal(results))

    return results, None

