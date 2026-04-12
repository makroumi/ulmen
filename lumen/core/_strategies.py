"""
Column strategy selection and compression savings analysis.

The strategy layer sits above raw primitives and decides how to encode
each column in a T_MATRIX payload. It also exposes diagnostic functions
for measuring how much each strategy saves.
"""

from collections import Counter
from typing import Any

from lumen.core._primitives import encode_zigzag, pack_bits, pack_delta_raw, pack_rle


def detect_column_strategy(values: list) -> str:
    """
    Analyse a column of values and return the best encoding strategy.

    Decision tree:
        empty                          -> 'raw'
        all None                       -> 'rle'
        all bool (non-null)            -> 'bits'
        all int (non-bool, non-null)   -> 'delta' if saves bytes, else 'raw'
        all str, low cardinality       -> 'pool'
        run ratio < 0.6                -> 'rle'
        fallthrough                    -> 'raw'

    Note: floats and mixed-type columns are never pooled. Pool references
    only work for strings in the binary format; applying pool to floats
    was a historical bug and is explicitly excluded.
    """
    if not values:
        return 'raw'

    non_null = [v for v in values if v is not None]

    if not non_null:
        return 'rle'

    if all(isinstance(v, bool) for v in non_null):
        return 'bits'

    if all(isinstance(v, int) and not isinstance(v, bool) for v in non_null):
        if len(non_null) >= 2:
            deltas    = [non_null[i] - non_null[i - 1] for i in range(1, len(non_null))]
            raw_cost  = sum(len(encode_zigzag(v)) for v in non_null)
            delta_cost = (
                len(encode_zigzag(non_null[0]))
                + sum(len(encode_zigzag(d)) for d in deltas)
            )
            if delta_cost < raw_cost:
                return 'delta'
        return 'raw'

    if all(isinstance(v, str) for v in non_null):
        cnt    = Counter(non_null)
        n      = len(non_null)
        unique = len(cnt)
        if unique <= max(8, n // 10) and n > 4:
            return 'pool'

    runs = sum(1 for i in range(1, len(values)) if values[i] != values[i - 1]) + 1
    if runs < len(values) * 0.6:
        return 'rle'

    return 'raw'


def compute_delta_savings(values: list) -> dict:
    """
    Compare raw zigzag encoding cost vs delta encoding cost.

    Returns a dict with keys: raw, delta, saving, pct.
    Returns all-zero dict for empty or non-integer input (including booleans).
    """
    if not values or not all(
        isinstance(v, int) and not isinstance(v, bool) for v in values
    ):
        return {'raw': 0, 'delta': 0, 'saving': 0, 'pct': 0.0}

    raw    = sum(1 + len(encode_zigzag(v)) for v in values)
    delta  = len(pack_delta_raw(values))
    saving = raw - delta
    pct    = 100.0 * saving / raw if raw else 0.0
    return {'raw': raw, 'delta': delta, 'saving': saving, 'pct': pct}


def compute_rle_savings(values: list) -> dict:
    """
    Compare tab-separated raw text size vs RLE binary size.

    Returns a dict with keys: raw, rle, saving.
    """
    raw_str = '\t'.join(str(v) for v in values)
    raw     = len(raw_str.encode())
    rle     = len(pack_rle(values))
    return {'raw': raw, 'rle': rle, 'saving': raw - rle}


def compute_bits_savings(bools: list) -> dict:
    """
    Compare naive boolean storage (2 bytes per bool) vs bitpacked storage.

    Returns a dict with keys: raw, bits, saving.
    """
    raw    = len(bools) * 2
    packed = len(pack_bits(bools))
    return {'raw': raw, 'bits': packed, 'saving': raw - packed}


def build_pool(records: list, max_pool: int = 64) -> tuple:
    """
    Build a string interning pool from a list of records.

    Recursively scans all string values and dict keys, counts their
    frequencies, then selects the strings that yield the greatest byte
    savings when replaced by compact pool references.

    A string is worth pooling if:
        frequency * (len(string) - ref_cost) > 0

    where ref_cost is 2 bytes for pools with <= 9 entries, else 4 bytes.

    Returns:
        pool     -- ordered list of pooled strings
        pool_map -- mapping from string to its pool index
    """
    freq: Counter = Counter()

    def _scan(obj: Any) -> None:
        if isinstance(obj, str) and obj != '' and len(obj) > 1:
            freq[obj] += 1
        elif isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(k, str) and len(k) > 1:
                    freq[k] += 1
                _scan(v)
        elif isinstance(obj, (list, tuple)):
            for item in obj:
                _scan(item)

    for record in records:
        _scan(record)

    def _score(s: str, f: int) -> int:
        ref_cost = 2 if len(freq) <= 9 else 4
        return f * (len(s) - ref_cost)

    candidates = sorted(
        [(s, f) for s, f in freq.items() if f >= 2],
        key=lambda x: -_score(x[0], x[1]),
    )

    pool: list = []
    pool_map: dict = {}
    for s, f in candidates[:max_pool]:
        if _score(s, f) > 0:
            pool_map[s] = len(pool)
            pool.append(s)
        if len(pool) >= max_pool:
            break

    return pool, pool_map
