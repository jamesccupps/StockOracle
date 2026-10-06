"""Small helpers shared by the terminal modules."""
import math


def plain(obj):
    """Recursively convert to JSON-safe Python types: numpy scalars/arrays to
    Python numbers/lists, NaN/inf to None, dates to ISO strings."""
    if obj is None or isinstance(obj, (str, bool, int)):
        return obj
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {str(k): plain(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [plain(v) for v in obj]
    item = getattr(obj, "item", None)
    if callable(item) and not hasattr(obj, "__len__"):
        try:
            return plain(item())
        except Exception:
            pass
    tolist = getattr(obj, "tolist", None)
    if callable(tolist):
        try:
            return plain(tolist())
        except Exception:
            pass
    if hasattr(obj, "isoformat"):
        try:
            return obj.isoformat()
        except Exception:
            pass
    return str(obj)
