"""
General-purpose utility functions.

These have no dependency on the wire format and can be used independently
of the codec layer.
"""

import math
import sys
from typing import Any

__version__ = "1.0.0"
__edition__ = "LUMEN V1"


def fnv1a(data: bytes) -> int:
    """
    FNV-1a 32-bit hash.

    Fast, non-cryptographic, deterministic. Used internally for hashing
    string keys when building the string pool.
    """
    h = 0x811C9DC5
    for b in data:
        h ^= b
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h


def fnv1a_str(s: str) -> int:
    """FNV-1a hash of a UTF-8 encoded string."""
    return fnv1a(s.encode())


def estimate_tokens(text: str) -> int:
    """
    Rough token count estimate for LLM context budgeting.

    Uses the common approximation of 1 token per 4 characters.
    Returns 0 for empty input, minimum 1 for any non-empty input.
    """
    if not text:
        return 0
    return max(1, (len(text) + 3) // 4)


def deep_size(obj: Any, _seen: set = None) -> int:
    """
    Recursively compute the total memory footprint of an object in bytes.

    Traverses dicts, lists, tuples, sets, and frozensets. Cycle-safe via
    the _seen identity set so circular references are counted once only.
    """
    if _seen is None:
        _seen = set()
    obj_id = id(obj)
    if obj_id in _seen:
        return 0
    _seen.add(obj_id)
    size = sys.getsizeof(obj)
    if isinstance(obj, dict):
        size += sum(
            deep_size(k, _seen) + deep_size(v, _seen)
            for k, v in obj.items()
        )
    elif isinstance(obj, (list, tuple, set, frozenset)):
        size += sum(deep_size(item, _seen) for item in obj)
    return size


def deep_eq(a: Any, b: Any) -> bool:
    """
    Deep structural equality with correct NaN and signed-infinity handling.

    Standard == returns False for NaN == NaN but this returns True.
    Mixed int/float comparisons use float promotion so deep_eq(1, 1.0) is True.
    """
    if type(a) != type(b):
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            return float(a) == float(b)
        return False
    if isinstance(a, float):
        if math.isnan(a) and math.isnan(b):
            return True
        if math.isinf(a) and math.isinf(b):
            return math.copysign(1, a) == math.copysign(1, b)
        return a == b
    if isinstance(a, dict):
        if set(a.keys()) != set(b.keys()):
            return False
        return all(deep_eq(a[k], b[k]) for k in a)
    if isinstance(a, (list, tuple)):
        if len(a) != len(b):
            return False
        return all(deep_eq(x, y) for x, y in zip(a, b))
    return a == b
