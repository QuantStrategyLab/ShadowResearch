"""Canonical serialization for FUSE-1 reference engine.

Per §5.2 and §5.3: identical inputs produce byte-identical outputs under
canonical JSON serialization (sorted keys, compact separators, no trailing whitespace).
"""

import json
import math
from typing import Any, Optional


def finite_float(value: Any) -> Optional[float]:
    """Convert non-bool numeric value to finite float, else return None."""
    if value is None or isinstance(value, bool):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(num) or math.isinf(num):
        return None
    return num


def canonical_json(obj: Any) -> str:
    """Serialize object to canonical JSON format.

    Uses sorted keys and compact separators for deterministic serialization.
    Per §5.2: identical inputs must produce byte-identical canonical JSON.
    """
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def canonical_hash_input(obj: Any) -> bytes:
    """Convert object to bytes for hashing via canonical JSON."""
    return canonical_json(obj).encode('utf-8')
