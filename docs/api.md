# API Reference

The public API of **LUMEN** is exposed through the `lumen` package. Below is a concise reference of the most important classes, functions, and constants.

## Classes

| Class | Description |
|-------|-------------|
| `LumenDict` | Pure‑Python reference implementation for column‑aware serialization. Supports `encode_text()`, `encode_binary_pooled()`, `encode_binary_zlib()`, and decoding helpers. |
| `LumenDictFull` | Variant of `LumenDict` that always enables column strategies and allows a larger string pool (default limit 256). |
| `LumenDictRust` | Rust‑accelerated drop‑in replacement for `LumenDict`. Identical API; automatically falls back to the Python shim if the Rust extension is not built. |
| `LumenDictFullRust` | Rust‑accelerated version of `LumenDictFull`. |

## Functions

| Function | Description |
|----------|-------------|
| `decode_text_records(text: str) -> list` | Decode a LUMEN text representation back to a list of Python objects. |
| `decode_binary_records(data: bytes) -> object` | Decode a LUMEN binary blob (pooled or raw) back to Python structures. |
| `encode_text_records(records: list, pool: list, pool_map: dict, schema: dict = None, matrix_mode: bool = True) -> str` | Low‑level text encoder used internally. |
| `encode_binary_records(records: list, pool: list, pool_map: dict, use_strategies: bool = True) -> bytes` | Low‑level binary encoder used internally. |
| `build_pool(records: list, max_pool: int = 64) -> tuple` | Build a string pool from a list of records, returning `(pool, pool_map)`. |
| `detect_column_strategy(values: list) -> str` | Detect the best compression strategy for a column (`raw`, `bits`, `delta`, `rle`, `pool`). |
| `compute_delta_savings(values: list)`, `compute_rle_savings(values: list)`, `compute_bits_savings(bools: list)` | Helper functions that report potential byte savings for each strategy. |
| `fnv1a(data: bytes) -> int`, `fnv1a_str(s: str) -> int` | 32‑bit FNV‑1a hash used for pool reference encoding. |
| `estimate_tokens(text: str) -> int` | Rough token count estimator (used for LLM cost estimation). |
| `deep_eq(a, b) -> bool` | Deep equality comparison that correctly handles NaN, infinities, and nested structures. |
| `deep_size(obj) -> int` | Approximate memory footprint of a Python object graph. |

## Primitive Encoders / Decoders

- `encode_varint(n: int) -> bytes` / `decode_varint(buf: bytes, pos: int)` – Variable‑length integer encoding.
- `encode_zigzag(n: int) -> bytes` / `decode_zigzag(buf: bytes, pos: int)` – Zig‑zag signed integer encoding.
- `pack_string(s: str) -> bytes`, `pack_int(n: int) -> bytes`, `pack_float(f: float) -> bytes`, `pack_bool(v: bool) -> bytes`, `pack_null() -> bytes` – Low‑level binary primitives.
- `pack_bits(bools: list) -> bytes`, `unpack_bits(buf: bytes, pos: int)` – Bit‑packing for boolean columns.
- `pack_delta_raw(ints: list) -> bytes`, `unpack_delta_raw(buf: bytes, pos: int)` – Delta encoding for monotonic integer columns.
- `pack_rle(values: list) -> bytes` – Run‑length encoding for any column type.

## Constants

- Type tags: `T_STR_TINY`, `T_STR`, `T_INT`, `T_FLOAT`, `T_BOOL`, `T_NULL`, `T_LIST`, `T_MAP`, `T_POOL_DEF`, `T_POOL_REF`, `T_MATRIX`, `T_DELTA_RAW`, `T_BITS`, `T_RLE`.
- Strategy bytes: `S_RAW`, `S_DELTA`, `S_RLE`, `S_BITS`, `S_POOL`.
- Magic header: `MAGIC = b"LUMB"`, `VERSION = bytes([3, 3])`.
- Version strings: `__version__ = "3.3.1"`, `__edition__ = "Lumex Ultra Absolute #1"`.

## Runtime Flag

- `RUST_AVAILABLE` – Boolean flag indicating whether the Rust extension was successfully loaded. The module automatically falls back to the pure‑Python shims when `False`.

All symbols listed above are exported via `lumen.__all__` and can be imported directly:

```python
from lumen import LumenDict, LumenDictRust, decode_binary_records, RUST_AVAILABLE
```

Refer to the source code in `lumen/__init__.py` and `lumen/core.py` for full implementation details.
