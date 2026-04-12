# API Reference

The public API of **LUMEN** is securely exposed through the `lumen` package. Below is a concise reference for the most essential classes, functions, and constants.

## Classes

| Class | Description |
|-------|-------------|
| `LumenDict` | Pure Python reference implementation for column aware serialization. Completely supports `encode_text()`, `encode_binary_pooled()`, `encode_binary_zlib()`, and all decoding helpers. |
| `LumenDictFull` | Specialized variant of `LumenDict` that always enables column strategies and natively allows a larger string pool limit of 256. |
| `LumenDictRust` | Rust accelerated drop in replacement tailored for `LumenDict`. Exposes the identical API while automatically falling back to the pure Python shim if the Rust extension fails to build. |
| `LumenDictFullRust` | Rust accelerated high capacity version of `LumenDictFull`. |

## Functions

| Function | Description |
|----------|-------------|
| `decode_text_records(text: str) -> list` | Accurately decode a LUMEN text representation securely back into a list of Python objects. |
| `decode_binary_records(data: bytes) -> object` | Decodes a LUMEN binary blob directly back to complex Python structures. |
| `encode_text_records(records: list, pool: list, pool_map: dict, schema: dict = None, matrix_mode: bool = True) -> str` | Highly optimized low level text encoder utilized exclusively internally. |
| `encode_binary_records(records: list, pool: list, pool_map: dict, use_strategies: bool = True) -> bytes` | Low level binary memory encoder utilized exclusively internally. |
| `build_pool(records: list, max_pool: int = 64) -> tuple` | Build an optimal string pool quickly from a list of records. |
| `detect_column_strategy(values: list) -> str` | Detect the most efficient compression strategy for any given column. |
| `compute_delta_savings(values: list)` | Helper function carefully reporting the potential byte savings achievable using delta strategy. |
| `compute_rle_savings(values: list)` | Helper function carefully reporting the potential byte savings achievable using RLE strategy. |
| `compute_bits_savings(bools: list)` | Helper function carefully reporting the potential byte savings achievable using bits strategy. |
| `fnv1a(data: bytes) -> int` | Lightning fast 32 bit FNV hash deployed for complex pool reference encoding. |
| `fnv1a_str(s: str) -> int` | String native fast 32 bit FNV hash used tightly for pool lookup. |
| `estimate_tokens(text: str) -> int` | Rough token count capability used actively for LLM cost estimation. |
| `deep_eq(a, b) -> bool` | Precise deep equality comparison that systematically handles NaN, infinities, and highly nested complex structures. |
| `deep_size(obj) -> int` | Compute the approximate memory footprint of an active Python object graph. |

## Primitive Encoders and Decoders

* `encode_varint(n: int) -> bytes` / `decode_varint(buf: bytes, pos: int)`: Variable length integer fast encoding.
* `encode_zigzag(n: int) -> bytes` / `decode_zigzag(buf: bytes, pos: int)`: Zig zag signed integer optimal encoding.
* `pack_string(s: str) -> bytes`, `pack_int(n: int) -> bytes`, `pack_float(f: float) -> bytes`, `pack_bool(v: bool) -> bytes`, `pack_null() -> bytes`: Low level binary hardware primitives.
* `pack_bits(bools: list) -> bytes`, `unpack_bits(buf: bytes, pos: int)`: Highly advanced bit packing for massive boolean columns.
* `pack_delta_raw(ints: list) -> bytes`, `unpack_delta_raw(buf: bytes, pos: int)`: Delta level memory encoding for strictly monotonic integer columns.
* `pack_rle(values: list) -> bytes`: Run length encoding efficiently shrinking any column type.

## Constants

* Type tags: `T_STR_TINY`, `T_STR`, `T_INT`, `T_FLOAT`, `T_BOOL`, `T_NULL`, `T_LIST`, `T_MAP`, `T_POOL_DEF`, `T_POOL_REF`, `T_MATRIX`, `T_DELTA_RAW`, `T_BITS`, `T_RLE`.
* Strategy bytes: `S_RAW`, `S_DELTA`, `S_RLE`, `S_BITS`, `S_POOL`.
* Magic header: `MAGIC = b"LUMB"`, `VERSION = bytes([3, 3])`.
* Version strings: `__version__ = "3.3.1"`, `__edition__ = "Lumex Ultra Absolute 1"`.

## Runtime Flag

* `RUST_AVAILABLE`: Boolean logic flag indicating clearly whether the required Rust extension was successfully fully loaded. The local module automatically cleanly falls safely back to purely pure Python shims when absolutely False.

All structural symbols cleanly listed above are solidly exported via `lumen.__all__` and logically imported explicitly:

```python
from lumen import LumenDict, LumenDictRust, decode_binary_records, RUST_AVAILABLE
```

Please refer precisely back to the core logic code inside `lumen/__init__.py` and `lumen/core.py` for comprehensive deep technical architectural details.
