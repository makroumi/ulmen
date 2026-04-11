"""
LUMEN binary codec -- encoder and decoder for the compact binary format.

Binary format layout:

    MAGIC (4 bytes)          'LUMB'
    VERSION (2 bytes)        major.minor
    [T_POOL_DEF block]       optional, if pool is non-empty
    [payload]                T_MATRIX for multi-dict records, T_LIST otherwise

Column encoding in T_MATRIX:

    Header per column:       pack_string(name) + strategy_byte
    Data per column:         encoded according to strategy byte
        S_BITS   -> T_BITS bitfield
        S_DELTA  -> T_DELTA_RAW sequence
        S_RLE    -> T_RLE sequence
        S_RAW    -> T_LIST of individually encoded values
        S_POOL   -> T_LIST of individually encoded values with pool refs inline
"""

import struct
from typing import Any

from lumen.core._constants import (
    MAGIC, VERSION,
    T_STR_TINY, T_STR, T_INT, T_FLOAT, T_BOOL, T_NULL,
    T_LIST, T_MAP, T_POOL_DEF, T_POOL_REF, T_MATRIX,
    T_BITS, T_DELTA_RAW, T_RLE,
    S_BITS, S_DELTA, S_RLE,
    STRATEGY_BYTE,
)
from lumen.core._primitives import (
    encode_varint, decode_varint,
    decode_zigzag,
    pack_string, pack_int, pack_float, pack_bool, pack_null, pack_pool_ref,
    pack_bits, unpack_bits,
    pack_delta_raw, unpack_delta_raw,
    pack_rle,
)
from lumen.core._strategies import detect_column_strategy


def _encode_value_binary(v: Any, pool_map: dict) -> bytes:
    """
    Encode a single value to binary recursively.

    Handles None, bool, int, float, str, list, tuple, and dict.
    Strings are replaced with pool references when available.
    All other types are coerced to string as a last resort.
    """
    if v is None:
        return pack_null()
    if isinstance(v, bool):
        return pack_bool(v)
    if isinstance(v, int):
        return pack_int(v)
    if isinstance(v, float):
        return pack_float(v)
    if isinstance(v, str):
        if v in pool_map:
            return pack_pool_ref(pool_map[v])
        return pack_string(v)
    if isinstance(v, (list, tuple)):
        out = bytearray([T_LIST])
        out += encode_varint(len(v))
        for item in v:
            out += _encode_value_binary(item, pool_map)
        return bytes(out)
    if isinstance(v, dict):
        out = bytearray([T_MAP])
        out += encode_varint(len(v))
        for k, val in v.items():
            out += _encode_value_binary(k, pool_map)
            out += _encode_value_binary(val, pool_map)
        return bytes(out)
    return pack_string(str(v))


def _encode_matrix_binary(
    records: list,
    pool_map: dict,
    use_strategies: bool,
) -> bytearray:
    """Encode multiple dict records into a T_MATRIX columnar block."""
    all_keys: list = []
    seen: set = set()
    for r in records:
        for k in r:
            if k not in seen:
                all_keys.append(k)
                seen.add(k)

    out = bytearray([T_MATRIX])
    out += encode_varint(len(records))
    out += encode_varint(len(all_keys))

    col_data:  dict = {}
    col_strat: dict = {}
    for col in all_keys:
        vals           = [r.get(col) for r in records]
        col_data[col]  = vals
        strategy_name  = detect_column_strategy(vals) if use_strategies else 'raw'
        s_byte         = STRATEGY_BYTE.get(strategy_name, 0x00)
        col_strat[col] = s_byte
        out += pack_string(col)
        out.append(s_byte)

    for col in all_keys:
        vals  = col_data[col]
        strat = col_strat[col]
        if strat == S_BITS:
            out += pack_bits([bool(v) if v is not None else False for v in vals])
        elif strat == S_DELTA:
            out += pack_delta_raw([v if v is not None else 0 for v in vals])
        elif strat == S_RLE:
            out += pack_rle(vals)
        else:
            out.append(T_LIST)
            out += encode_varint(len(vals))
            for v in vals:
                out += _encode_value_binary(v, pool_map)

    return out


def _encode_list_binary(records: list, pool_map: dict) -> bytearray:
    """Encode records as a flat T_LIST for single record or non-dict items."""
    out = bytearray([T_LIST])
    out += encode_varint(len(records))
    for r in records:
        if isinstance(r, dict):
            out.append(T_MAP)
            out += encode_varint(len(r))
            for k, v in r.items():
                out += _encode_value_binary(k, pool_map)
                out += _encode_value_binary(v, pool_map)
        else:
            out += _encode_value_binary(r, pool_map)
    return out


def encode_binary_records(
    records: list,
    pool: list,
    pool_map: dict,
    use_strategies: bool = True,
) -> bytes:
    """
    Encode a list of records to LUMEN binary format.

    Parameters
    ----------
    records:        list of dicts, scalars, or mixed values
    pool:           ordered list of pooled strings, may be empty
    pool_map:       mapping from string to pool index
    use_strategies: if True select the optimal column encoding per column;
                    if False all columns are stored raw as T_LIST

    Returns the complete binary payload including MAGIC and VERSION.
    """
    out = bytearray(MAGIC + VERSION)

    if pool:
        out.append(T_POOL_DEF)
        out += encode_varint(len(pool))
        for s in pool:
            out += pack_string(s)

    if not records:
        return bytes(out)

    is_matrix = all(isinstance(r, dict) for r in records)
    if is_matrix and len(records) > 1:
        out += _encode_matrix_binary(records, pool_map, use_strategies)
    else:
        out += _encode_list_binary(records, pool_map)

    return bytes(out)


def decode_binary_records(data: bytes) -> list:
    """
    Decode a LUMEN binary payload.

    Always returns a list. Single-record payloads return a one-element list.
    T_MATRIX payloads return a list of dicts.
    Raises ValueError for bad magic or unknown tags.
    """
    if data[:4] != MAGIC:
        raise ValueError("Not LUMEN binary (bad magic)")

    pos   = 6
    pool: list = []

    def _decode_string(buf: bytes, p: int) -> tuple:
        tag = buf[p]; p += 1
        if tag == T_STR_TINY:
            L = buf[p]; p += 1
            return buf[p:p + L].decode('utf-8'), p + L
        if tag == T_STR:
            L, p = decode_varint(buf, p)
            return buf[p:p + L].decode('utf-8'), p + L
        raise ValueError(f"Expected string tag, got 0x{tag:02x}")

    def _decode_value(buf: bytes, p: int) -> tuple:
        tag = buf[p]; p += 1
        if tag == T_STR_TINY:
            L = buf[p]; p += 1
            return buf[p:p + L].decode('utf-8'), p + L
        if tag == T_STR:
            L, p = decode_varint(buf, p)
            return buf[p:p + L].decode('utf-8'), p + L
        if tag == T_INT:
            return decode_zigzag(buf, p)
        if tag == T_FLOAT:
            return struct.unpack('>d', buf[p:p + 8])[0], p + 8
        if tag == T_BOOL:
            return bool(buf[p]), p + 1
        if tag == T_NULL:
            return None, p
        if tag == T_POOL_REF:
            idx, p = decode_varint(buf, p)
            return (pool[idx] if idx < len(pool) else None), p
        if tag == T_LIST:
            n, p = decode_varint(buf, p)
            lst  = []
            for _ in range(n):
                v, p = _decode_value(buf, p)
                lst.append(v)
            return lst, p
        if tag == T_MAP:
            n, p = decode_varint(buf, p)
            d    = {}
            for _ in range(n):
                k, p = _decode_value(buf, p)
                v, p = _decode_value(buf, p)
                d[k] = v
            return d, p
        if tag == T_BITS:
            return unpack_bits(buf, p)
        if tag == T_DELTA_RAW:
            return unpack_delta_raw(buf, p)
        if tag == T_RLE:
            n_runs, p = decode_varint(buf, p)
            result    = []
            for _ in range(n_runs):
                v, p   = _decode_value(buf, p)
                run, p = decode_varint(buf, p)
                result.extend([v] * run)
            return result, p
        raise ValueError(f"Unknown tag 0x{tag:02x} at pos {p - 1}")

    def _decode_matrix(buf: bytes, p: int) -> tuple:
        n_rows, p = decode_varint(buf, p)
        n_cols, p = decode_varint(buf, p)

        col_names:  list = []
        col_strats: list = []
        for _ in range(n_cols):
            cname, p = _decode_string(buf, p)
            strat     = buf[p]; p += 1
            col_names.append(cname)
            col_strats.append(strat)

        col_values: dict = {}
        for ci, col in enumerate(col_names):
            strat = col_strats[ci]
            if strat == S_BITS:
                if buf[p] == T_BITS:
                    p += 1
                    vals, p = unpack_bits(buf, p)
                else:
                    vals, p = _decode_value(buf, p)
            elif strat == S_DELTA:
                if buf[p] == T_DELTA_RAW:
                    p += 1
                    vals, p = unpack_delta_raw(buf, p)
                else:
                    vals, p = _decode_value(buf, p)
            elif strat == S_RLE:
                if buf[p] == T_RLE:
                    p += 1
                    n_runs, p = decode_varint(buf, p)
                    vals      = []
                    for _ in range(n_runs):
                        v, p   = _decode_value(buf, p)
                        run, p = decode_varint(buf, p)
                        vals.extend([v] * run)
                else:
                    vals, p = _decode_value(buf, p)
            else:
                vals, p = _decode_value(buf, p)
            col_values[col] = vals

        rows = []
        for ri in range(n_rows):
            rec = {}
            for col in col_names:
                cv       = col_values[col]
                rec[col] = cv[ri] if isinstance(cv, list) and ri < len(cv) else None
            rows.append(rec)

        return rows, p

    results = []
    while pos < len(data):
        tag = data[pos]
        if tag == T_POOL_DEF:
            pos += 1
            n, pos = decode_varint(data, pos)
            for _ in range(n):
                s, pos = _decode_string(data, pos)
                pool.append(s)
        elif tag == T_MATRIX:
            pos += 1
            rows, pos = _decode_matrix(data, pos)
            return rows
        elif tag == T_LIST:
            pos += 1
            n, pos = decode_varint(data, pos)
            lst = []
            for _ in range(n):
                v, pos = _decode_value(data, pos)
                lst.append(v)
            results.extend(lst)
        else:
            v, pos = _decode_value(data, pos)
            results.append(v)

    return results
