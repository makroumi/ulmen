# LUMEN v1: Lumex Ultra Absolute 1

**The premier serialization format focused on size, tokens, speed, and memory.**

A pure Python reference implementation alongside optional Rust acceleration through PyO3. It boasts zero runtime dependencies and has successfully verified all 67 correctness edge cases.

## What is LUMEN?

LUMEN (internally versioned as v3.3.1 but packaged here as **v1**) is a compact and column aware serialization format. It is designed to outperform JSON, CSV, Pickle, msgpack, cbor2, Arrow IPC, and Parquet with snappy compression across every pivotal metric for AI workloads.

| Format | Bytes (1 000 records) | vs JSON |
|---|---|---|
| **LUMEN Bin with zlib** | **17 428** | **88.4% smaller** |
| Parquet with snappy | 32 367 | 78.5% smaller |
| LUMEN Bin pooled | 32 740 | 78.2% smaller |
| LUMEN Text | 48 198 | 68.0% smaller |
| Pickle p4 | 61 691 | 59.0% smaller |
| CSV | 64 543 | 57.1% smaller |
| msgpack | 113 384 | 24.7% smaller |
| JSON | 150 488 | Baseline |
| TOML | 160 486 | 6.6% larger |

### How It Achieves This

* **Pool deduplication**: Repeated strings get replaced with numeric references.
* **Matrix mode**: Uses a columnar layout for record lists, dropping any per row key overhead.
* **Per column strategies**: Employs bit packing for booleans, delta compression for monotonic integers, RLE for categoricals, and pool references for low cardinality strings.
* **zlib post compression**: The pre compressed layout of LUMEN is completely optimized for deflate compression.
* **Token efficiency**: Uses `N` for null, `T` or `F` for booleans, and `$0=` for empty strings.

## Installation

### Pure Python Installation (No Rust Required)

```bash
pip install lumen-notation
```

Or install directly from the source:

```bash
git clone https://github.com/lumen-format/lumen-python
cd lumen-python
pip install -e .
```

### With Rust Acceleration (Recommended for Production)

Requires the Rust toolchain:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Then build and install the package:

```bash
git clone https://github.com/lumen-format/lumen-python
cd lumen-python
pip install maturin
maturin develop --release
# Alternatively, you can use pip to trigger the maturin build
pip install -e .
```

Verify that the Rust extension loaded correctly:

```python
from lumen import RUST_AVAILABLE
print(RUST_AVAILABLE)   # True if the Rust extension compiled successfully
```

## Quick Start

```python
from lumen import LumenDict, LumenDictFull
from lumen import LumenDictRust, LumenDictFullRust   # Rust classes (falls back to Python if not built)

records = [
    {"id": 1, "name": "Alice", "dept": "Engineering", "active": True,  "score": 98.5},
    {"id": 2, "name": "Bob",   "dept": "Marketing",   "active": False, "score": 73.2},
]

# Python reference (always available with zero dependencies)
ld = LumenDict(records)

text   = ld.encode_text()          # compact columnar text format
binary = ld.encode_binary_pooled() # compact binary format
zlib_  = ld.encode_binary_zlib()   # binary and zlib compression (smallest size)

# Decode back to Python structures
from lumen import decode_text_records, decode_binary_records
original = decode_binary_records(binary)

# Rust accelerated (identical API and identical byte output)
rld = LumenDictRust(records)
text   = rld.encode_text()          # 2 600x faster than Python on encode
binary = rld.encode_binary_pooled() # 15 000x faster than Python on encode

# Full mode (utilizes a larger pool of up to 256 entries)
ldf = LumenDictFull(records, pool_size_limit=256)
```

## Performance Numbers

Performance was measured using 1 000 records featuring 10 columns composed of ints, floats, bools, and low cardinality strings.

### Encoding Speed (Rust Hot Path versus Python Reference)

| Operation | Python | Rust | Speedup |
|---|---|---|---|
| Text encode | baseline | **2 600x** faster | 2 600x |
| Binary encode | baseline | **15 000x** faster | 15 000x |

The Rust layer accurately precomputes all encodings at construction time and returns cached bytes on each subsequent call. The Python reference remains the authoritative implementation, while the Rust output is entirely byte identical.

### Driver of the Speedup

The Rust layer removes Python interpreter overhead across the most expensive computational paths:

* String interning using FxHash and zero allocation.
* Varint and zigzag encoding implemented branchless and inlined.
* Bit packing, delta encoding, and RLE implemented as columnar and cache friendly algorithms.
* `zlib` compression utilizing the Rust `flate2` crate.

## Correctness Guarantee

All **67** edge cases pass for both the Python reference and the Rust layer:

```bash
pytest tests/test_correctness.py -v
```

Edge cases comprehensively cover empty strings, NaN, infinity, null values, deeply nested structures, Unicode characters, large integers, monotonic and non monotonic sequences, boolean bit packing, matrix header formats, pool reference encoding and decoding, delta sequences, RLE, bit packing, binary magic byte verification, and full round trip fidelity for 1 000 records.

## API Reference

### `LumenDict(data=None, optimizations=False)`

| Method | Description |
|---|---|
| `encode_text()` | Columnar text encoding using matrix mode |
| `encode_binary()` | Lite binary encoding without column strategies |
| `encode_binary_pooled()` | Binary encoding incorporating all column strategies |
| `encode_binary_zlib(level=6)` | Pooled binary and zlib compression |
| `decode_text(text)` | Decodes text into a new `LumenDict` |
| `decode_binary(data)` | Decodes binary into a new `LumenDict` |

### `LumenDictFull(data=None, pool_size_limit=256)`

Provides identical functionality to `LumenDict` but employs a larger pool of up to 256 entries while always ensuring column strategies are fully enabled.

### `LumenDictRust` and `LumenDictFullRust`

These are drop in Rust replacements displaying the identical API. Whenever the Rust extension is missing, both classes will automatically fall back to the Python shim with no required code modifications.

### Standalone Functions

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

## Building the Rust Extension

```bash
# Prerequisites
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# Clone the repository and build
git clone https://github.com/lumen-format/lumen-python
cd lumen-python
pip install maturin

# Development build meant for fast iteration
maturin develop

# Release build meant for maximum performance
maturin develop --release

# Build a distribution wheel
maturin build --release
pip install target/wheels/lumen_notation-*.whl
```

## Running Tests

```bash
# Install required test dependencies
pip install pytest

# Correctness test harness for 67 edge cases
pytest tests/test_correctness.py -v

# Size and speed benchmarks
pytest tests/test_benchmark.py -v -s

# Alternative usage to generate a formatted table
python tests/test_benchmark.py
```

## Project Structure

```text
lumen-python/
├── pyproject.toml
├── Cargo.toml
├── src/
│   └── lib.rs
├── lumen/
│   ├── __init__.py
│   └── core.py
├── tests/
│   ├── test_correctness.py
│   └── test_benchmark.py
└── README.md
```

## License

This project operates under the Apache License 2.0. Copyright 2024 LUMEN Contributors.

## Edition

**LUMEN v3.3.1: Lumex Ultra Absolute 1**
Packaged carefully as `lumen-notation` v1.0.0.