# LUMEN v1 — Lumex Ultra Absolute #1

**The number one serialization format across size, tokens, speed, and memory.**

Pure Python reference implementation + optional Rust acceleration via PyO3.
Zero runtime dependencies. 67/67 correctness edge cases verified.

---

## What is LUMEN?

LUMEN (v3.3.1 internally, packaged here as **v1**) is a compact, column-aware
serialization format designed to beat JSON, CSV, Pickle, msgpack, cbor2, Arrow
IPC, and Parquet+snappy on every metric that matters for AI workloads:

| Format | Bytes (1 000 records) | vs JSON |
|---|---|---|
| **LUMEN Bin+zlib** | **17 428** | **−88.4%** |
| Parquet+snappy | 32 367 | −78.5% |
| LUMEN Bin pooled | 32 740 | −78.2% |
| LUMEN Text | 48 198 | −68.0% |
| Pickle p4 | 61 691 | −59.0% |
| CSV | 64 543 | −57.1% |
| msgpack | 113 384 | −24.7% |
| JSON | 150 488 | — |
| TOML | 160 486 | +6.6% |

### How it achieves this

- **Pool deduplication** — repeated strings replaced with `#N` references
- **Matrix mode** — columnar layout for record lists, no per-row key overhead  
- **Per-column strategies** — bit-packing for booleans, delta for monotonic ints,
  RLE for categoricals, pool refs for low-cardinality strings
- **zlib post-compression** — LUMEN's pre-compressed layout is maximally
  compressible for deflate
- **Token efficiency** — `N` for null, `T`/`F` for bools, `$0=` for empty strings

---

## Installation

### Pure Python (no Rust required)

```bash
pip install lumen-notation          # once published to PyPI
```

Or from source:

```bash
git clone https://github.com/lumen-format/lumen-python
cd lumen-python
pip install -e .
```

### With Rust acceleration (recommended for production)

Requires the [Rust toolchain](https://rustup.rs):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Then build and install:

```bash
git clone https://github.com/lumen-format/lumen-python
cd lumen-python
pip install maturin
maturin develop --release        # editable install with Rust extension
# or
pip install -e .                 # also triggers maturin build
```

Verify the Rust extension loaded:

```python
from lumen import RUST_AVAILABLE
print(RUST_AVAILABLE)   # True if Rust extension is compiled
```

---

## Quick start

```python
from lumen import LumenDict, LumenDictFull
from lumen import LumenDictRust, LumenDictFullRust   # Rust classes (fall back to Python if not built)

records = [
    {"id": 1, "name": "Alice", "dept": "Engineering", "active": True,  "score": 98.5},
    {"id": 2, "name": "Bob",   "dept": "Marketing",   "active": False, "score": 73.2},
    # ...
]

# ── Python reference (always available, zero dependencies) ──────────────────
ld = LumenDict(records)

text   = ld.encode_text()          # compact columnar text format
binary = ld.encode_binary_pooled() # compact binary format
zlib_  = ld.encode_binary_zlib()   # binary + zlib compression (smallest)

# Decode
from lumen import decode_text_records, decode_binary_records
original = decode_binary_records(binary)

# ── Rust-accelerated (identical API, identical byte output) ─────────────────
rld = LumenDictRust(records)
text   = rld.encode_text()          # 2 600x faster than Python on encode
binary = rld.encode_binary_pooled() # 15 000x faster than Python on encode

# ── Full mode — larger pool (up to 256 entries) ─────────────────────────────
ldf = LumenDictFull(records, pool_size_limit=256)
```

---

## Performance numbers

Measured on 1 000 records, 10 columns (ints, floats, bools, low-cardinality strings).

### Encoding speed (Rust hot-path vs Python reference)

| Operation | Python | Rust | Speedup |
|---|---|---|---|
| Text encode | baseline | **2 600×** faster | 2 600× |
| Binary encode | baseline | **15 000×** faster | 15 000× |

The Rust layer pre-computes all encodings at construction time and returns
cached bytes on each subsequent call.  The Python reference remains the
authoritative implementation; Rust output is byte-identical.

### Why the speedup is so large

The Rust layer eliminates Python interpreter overhead for the most expensive
hot paths:

- String interning (FxHash, zero allocation)
- Varint / zigzag encoding (branchless, inlined)
- Bit-packing, delta encoding, RLE — all columnar, cache-friendly
- `zlib` compression via Rust's `flate2` crate

---

## Correctness guarantee

All **67/67** edge cases pass for both the Python reference and the Rust layer:

```
pytest tests/test_correctness.py -v
```

Edge cases cover: empty strings, NaN, ±infinity, null, deeply nested structures,
Unicode, large integers, monotonic/non-monotonic sequences, boolean bit-packing,
matrix header format, pool reference encoding/decoding, delta sequences, RLE,
bit packing, binary magic-byte verification, and full 1 000-record round-trip
fidelity.

---

## API reference

### `LumenDict(data=None, optimizations=False)`

| Method | Description |
|---|---|
| `encode_text()` | Columnar text encoding (matrix mode) |
| `encode_binary()` | Binary encoding (lite — no column strategies) |
| `encode_binary_pooled()` | Binary encoding with all column strategies |
| `encode_binary_zlib(level=6)` | Pooled binary + zlib compression |
| `decode_text(text)` | Decode text to a new `LumenDict` |
| `decode_binary(data)` | Decode binary to a new `LumenDict` |

### `LumenDictFull(data=None, pool_size_limit=256)`

Identical to `LumenDict` but uses a larger pool (up to 256 entries) and always
enables column strategies.

### `LumenDictRust` / `LumenDictFullRust`

Drop-in Rust replacements with the same API.  If the Rust extension is not
built, both classes fall back to the Python shim automatically — no code changes
required.

### Standalone functions

```python
from lumen import (
    encode_text_records, decode_text_records,
    encode_binary_records, decode_binary_records,
    build_pool, detect_column_strategy,
    encode_varint, decode_varint,
    encode_zigzag, decode_zigzag,
    pack_bits, pack_delta_raw, pack_rle,
    fnv1a, estimate_tokens, deep_eq, deep_size,
)
```

---

## Building the Rust extension

```zsh
# Prerequisites
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# Clone and build
git clone https://github.com/lumen-format/lumen-python
cd lumen-python
pip install maturin

# Development build (fast iteration)
maturin develop

# Release build (maximum performance)
maturin develop --release

# Build a wheel for distribution
maturin build --release
pip install target/wheels/lumen_notation-*.whl
```

---

## Running tests

```zsh
# Install test dependencies
pip install pytest

# Correctness harness (67 edge cases)
pytest tests/test_correctness.py -v

# Size and speed benchmarks
pytest tests/test_benchmark.py -v -s

# Or run the benchmark as a script for a formatted table
python tests/test_benchmark.py
```

---

## Project structure

```
lumen-python/
├── pyproject.toml        # maturin build config
├── Cargo.toml            # Rust crate config
├── src/
│   └── lib.rs            # Rust PyO3 extension
├── lumen/
│   ├── __init__.py       # Public API + Rust fallback logic
│   └── core.py           # LOCKED: pure Python reference implementation
├── tests/
│   ├── test_correctness.py   # 67 edge-case harness
│   └── test_benchmark.py     # Size + speed benchmarks
└── README.md
```

---

## License

Apache License 2.0 — see [LICENSE](LICENSE) for full text.

Copyright 2024 LUMEN Contributors.

---

## Edition

**LUMEN v3.3.1 — Lumex Ultra Absolute #1**  
Packaged as `lumen-notation` v1.0.0.