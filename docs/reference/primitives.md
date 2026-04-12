# Primitives Reference

Low-level codec functions for building custom encoders and decoders.
All functions are pure: no state, no side effects.

```python
from lumen import (
    encode_varint, decode_varint,
    encode_zigzag, decode_zigzag,
    pack_string, pack_int, pack_float, pack_bool, pack_null, pack_pool_ref,
    pack_bits, unpack_bits,
    pack_delta_raw, unpack_delta_raw,
    pack_rle,
)
```

---

## Varint

Variable-length unsigned integer encoding. 7 data bits per byte.
High bit set means more bytes follow.

```Python
def encode_varint(n: int) -> bytes
```
Encode a non-negative integer. Raises ValueError for negative input.

```Python
def decode_varint(buf: bytes, pos: int) -> tuple[int, int]
```
Decode from `buf` starting at `pos`. Returns `(value, new_pos)`.

Examples:

| Value | Encoded bytes |
|---|---|
| 0 | '0x00' |
| 127 | '0x7F' |
| 128 | '0x80 0x01' |
| 300 | '0xAC 0x02' |
| 16383 | '0xFF 0x7F' |

---

## Zigzag

Signed integer encoding built on top of varint.
Maps signed integers to unsigned so small negative numbers encode compactly.

```Python
def encode_zigzag(n: int) -> bytes
```

```Python
def decode_zigzag(buf: bytes, pos: int) -> tuple[int, int]
```

Mapping:
| Input | Unsigned |
|---|---|
| 0 | 0 |
| -1 | 1 |
| 1 | 2 |
| -2 | 3 |
| n | (n << 1) ^ (n >> 63) |

---

## Scolar Packers
```Python
def pack_string(s: str) -> bytes
```
Encode a UTF-8 string. Strings 0-3 bytes use 'T_STR_TINY' (saves 1 varint byte).
Strings 4+ bytes use 'T_STR' with a varint length prefix.

```Python
def pack_int(n: int) -> bytes
```
Encode a signed integer as 'T_INT' + zigzag-varint.

```Python
def pack_float(f: float) -> bytes
```
Encode a double as 'T_FLOAT' + 8 bytes big-endian.

```Python
def pack_bool(v: bool) -> bytes
``` 
Encode a boolean as 'T_BOOL' + 0x01 (True) or 0x00 (False).

```Python
def pack_null() -> bytes
```
Encode None as a single 'T_NULL' byte with no payload.

```Python
def pack_pool_ref(idx: int) -> bytes
```
Encode a pool index as 'T_POOL_REF' + varint.

---

## Boolean Bitpacking
```Python
def pack_bits(bools: list) -> bytes
```
Pack a boolean list into a compact bitfield, LSB-first per byte.
Format: 'T_BITS' + 'varint(n)' + 'ceil(n/8) bytes'.
8x denser than one byte per bool.

```Python
def unpack_bits(buf: bytes, pos: int) -> tuple[list, int]
```
Decode a T_BITS bitfield. Returns (list[bool], new_pos).

Example:

```Python
packed = pack_bits([True, False, True, True])
# T_BITS, 0x04, 0x0D  (binary 00001101, bits 0,2,3 set)
```

---

## Delta Encoding

```Python
def pack_delta_raw(ints: list) -> bytes
```
Delta-encode an integer list.
Stores the first value as-is then each subsequent value as a signed difference.
Format: 'T_DELTA_RAW' + 'varint(n)' + 'zigzag(base)' + 'zigzag(delta) x (n-1)'.

```Python
def unpack_delta_raw(buf: bytes, pos: int) -> tuple[list, int]
```
Decode a T_DELTA_RAW sequence. Returns (list[int], new_pos).

Example:

```Python
pack_delta_raw([1000, 1001, 1002, 1003])
# Stores: 1000, +1, +1, +1  -- much smaller than four raw ints
```

---

## Run-Length Encoding
```Python
def pack_rle(values: list) -> bytes
```
Collapse consecutive equal values into (value, count) pairs.
Format: 'T_RLE' + varint(n_runs) + (encoded_value + varint(count)) x n_runs.

Supports None, bool, int, float, and str run values.

Example:
```Python
pack_rle(["active"] * 500 + ["inactive"] * 500)
# Stores only 2 runs instead of 1000 values
```

