"""
Low-level wire-format primitives.

All functions in this module are pure: they take Python values and return
bytes, or take bytes and a position and return (value, new_position).
No state, no side-effects.

Encoding atoms (in dependency order):
    varint / zigzag      -- variable-length integer encoding
    pack_string          -- UTF-8 string with tiny/normal tag split
    pack_int             -- signed integer via zigzag
    pack_float           -- IEEE-754 double, big-endian
    pack_bool            -- single-byte boolean
    pack_null            -- zero-payload null marker
    pack_pool_ref        -- interned string reference
    pack_bits            -- bitpacked boolean column
    pack_delta_raw       -- delta-encoded integer column
    pack_rle             -- run-length encoded column
"""

import struct
from typing import Tuple

from lumen.core._constants import (
    T_STR_TINY, T_STR,
    T_INT, T_FLOAT, T_BOOL, T_NULL,
    T_POOL_REF, T_BITS, T_DELTA_RAW, T_RLE,
)


# ---------------------------------------------------------------------------
# Varint -- variable-length unsigned integer encoding
# ---------------------------------------------------------------------------

def encode_varint(n: int) -> bytes:
    """
    Encode a non-negative integer as a variable-length byte sequence.

    Each byte uses 7 data bits. The high bit signals continuation.
    Values 0-127 encode to a single byte; larger values use more.
    Raises ValueError for negative input.
    """
    if n < 0:
        raise ValueError(f"varint expects non-negative, got {n}")
    parts = []
    while True:
        byte = n & 0x7F
        n >>= 7
        if n:
            parts.append(byte | 0x80)
        else:
            parts.append(byte)
            break
    return bytes(parts)


def decode_varint(buf: bytes, pos: int) -> Tuple[int, int]:
    """Decode a varint starting at buf[pos]. Returns (value, new_pos)."""
    result = shift = 0
    while True:
        b = buf[pos]; pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
    return result, pos


# ---------------------------------------------------------------------------
# Zigzag -- signed integer encoding layered on top of varint
# ---------------------------------------------------------------------------

def encode_zigzag(n: int) -> bytes:
    """
    Zigzag-encode a signed integer then varint-encode the result.

    Mapping: 0->0, -1->1, 1->2, -2->3, ...
    This ensures small negative numbers (e.g., -1) encode compactly.
    """
    zz = (n << 1) ^ (n >> 63) if n < 0 else n << 1
    return encode_varint(zz)


def decode_zigzag(buf: bytes, pos: int) -> Tuple[int, int]:
    """Decode a zigzag-varint from buf[pos]. Returns (value, new_pos)."""
    zz, pos = decode_varint(buf, pos)
    return (zz >> 1) ^ -(zz & 1), pos


# ---------------------------------------------------------------------------
# Scalar pack functions
# ---------------------------------------------------------------------------

def pack_string(s: str) -> bytes:
    """
    Encode a UTF-8 string.

    Strings with at most 3 UTF-8 bytes use T_STR_TINY, which stores the
    length as a plain byte rather than a varint (saves 1 byte for short keys).
    Strings of 4 or more bytes use T_STR with a varint length prefix.
    """
    b = s.encode('utf-8')
    L = len(b)
    if L <= 3:
        return bytes([T_STR_TINY, L]) + b
    return bytes([T_STR]) + encode_varint(L) + b


def pack_int(n: int) -> bytes:
    """Encode a signed integer as T_INT + zigzag-varint."""
    return bytes([T_INT]) + encode_zigzag(n)


def pack_float(f: float) -> bytes:
    """Encode an IEEE-754 double as T_FLOAT + 8 bytes big-endian."""
    return bytes([T_FLOAT]) + struct.pack('>d', f)


def pack_bool(v: bool) -> bytes:
    """Encode a boolean as T_BOOL followed by 0x01 (True) or 0x00 (False)."""
    return bytes([T_BOOL, 1 if v else 0])


def pack_null() -> bytes:
    """Encode None as a single T_NULL byte (no payload)."""
    return bytes([T_NULL])


def pack_pool_ref(idx: int) -> bytes:
    """Encode a string pool reference as T_POOL_REF + varint index."""
    return bytes([T_POOL_REF]) + encode_varint(idx)


# ---------------------------------------------------------------------------
# Column pack functions -- bulk encoding for columnar (matrix) storage
# ---------------------------------------------------------------------------

def pack_bits(bools: list) -> bytes:
    """
    Pack a boolean list into a compact bitfield (LSB-first per byte).

    Format: T_BITS + varint(n) + ceil(n/8) bytes.
    Achieves 8x density vs storing each bool as T_BOOL.
    """
    n = len(bools)
    n_bytes = (n + 7) // 8
    arr = bytearray(n_bytes)
    for i, v in enumerate(bools):
        if v:
            arr[i >> 3] |= (1 << (i & 7))
    return bytes([T_BITS]) + encode_varint(n) + bytes(arr)


def unpack_bits(buf: bytes, pos: int) -> Tuple[list, int]:
    """Decode a T_BITS bitfield. Returns (list[bool], new_pos)."""
    n, pos = decode_varint(buf, pos)
    n_bytes = (n + 7) // 8
    arr = buf[pos:pos + n_bytes]
    pos += n_bytes
    return [bool(arr[i >> 3] & (1 << (i & 7))) for i in range(n)], pos


def pack_delta_raw(ints: list) -> bytes:
    """
    Delta-encode an integer list.

    Stores the first value as-is, then each subsequent value as a zigzag
    difference from the previous. Highly effective for monotonic sequences
    such as auto-increment IDs.

    Format: T_DELTA_RAW + varint(n) + zigzag(base) + zigzag(delta) x (n-1).
    """
    if not ints:
        return bytes([T_DELTA_RAW]) + encode_varint(0)
    out = bytearray([T_DELTA_RAW])
    out += encode_varint(len(ints))
    out += encode_zigzag(ints[0])
    for i in range(1, len(ints)):
        out += encode_zigzag(ints[i] - ints[i - 1])
    return bytes(out)


def unpack_delta_raw(buf: bytes, pos: int) -> Tuple[list, int]:
    """Decode a T_DELTA_RAW sequence. Returns (list[int], new_pos)."""
    n, pos = decode_varint(buf, pos)
    if n == 0:
        return [], pos
    base, pos = decode_zigzag(buf, pos)
    result = [base]
    for _ in range(n - 1):
        d, pos = decode_zigzag(buf, pos)
        result.append(result[-1] + d)
    return result, pos


def pack_rle(values: list) -> bytes:
    """
    Run-length encode a list of scalar values.

    Collapses consecutive equal values into (value, count) pairs.
    Supports None, bool, int, float, and str run values.
    Effective when columns have long constant stretches or low cardinality.

    Format: T_RLE + varint(n_runs) + (encoded_value + varint(count)) x n_runs.
    """
    if not values:
        return bytes([T_RLE]) + encode_varint(0)

    runs = []
    cur = values[0]
    cnt = 1
    for v in values[1:]:
        if v == cur:
            cnt += 1
        else:
            runs.append((cur, cnt))
            cur = v
            cnt = 1
    runs.append((cur, cnt))

    out = bytearray([T_RLE])
    out += encode_varint(len(runs))
    for val, run in runs:
        if val is None:
            out.append(T_NULL)
        elif isinstance(val, bool):
            out += pack_bool(val)
        elif isinstance(val, int):
            out += pack_int(val)
        elif isinstance(val, float):
            out += pack_float(val)
        elif isinstance(val, str):
            out += pack_string(val)
        out += encode_varint(run)
    return bytes(out)
