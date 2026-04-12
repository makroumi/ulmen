# LUMEN V1
**Lightweight Universal Minimal Encoding Notation**

Copyright (c) El Mehdi Makroumi. All rights reserved.
Proprietary and confidential.

---

LUMEN is a serialization format engineered to be the smallest, fastest,
and most token-efficient way to move structured data - between services,
into storage, and through language model context windows.

It ships as a pure Python library with an optional Rust acceleration layer
that is drop-in compatible and byte-identical in output.

---

## Table of Contents

- [Benchmarks](#benchmarks)
- [At a Glance](#at-a-glance)
- [Surfaces](#surfaces)
  - [Binary](#binary)
  - [Text](#text)
  - [LUMIA](#lumia)
  - [LUMEN-AGENT](#lumen-agent)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [Wire Format Constants](#wire-format-constants)
- [Utilities](#utilities)
- [Architecture](#architecture)
- [Running Tests](#running-tests)
- [Format Specification](#format-specification)
- [Versioning](#versioning)

## Benchmarks

Measured on 1,000 records, 10 mixed-type columns (int, float, str, bool).
Speed = median of 50 runs, full construction included (pool build + encode).
Machine: x86_64 Linux, Python 3.12, rustc 1.92.

### Size

| Format | Bytes | vs JSON |
|---|---:|---:|
| JSON | 145,664 | 100.0% |
| Pickle protocol 4 | 62,177 | 42.7% |
| CSV | 61,717 | 42.4% |
| LUMIA text | 57,403 | 39.4% |
| LUMEN text | 46,779 | 32.1% |
| LUMEN binary | 32,701 | 22.4% |
| LUMEN zlib-6 | 2,453 | **1.7%** |
| LUMEN zlib-9 | 2,450 | **1.7%** |

Python and Rust produce byte-identical output.

### Speed - Encode (median ms, 1,000 records)

| Format | Encode ms | Decode ms |
|---|---:|---:|
| JSON | 1.264 | 1.849 |
| Pickle protocol 4 | 0.369 | 0.497 |
| LUMEN text (Python) | 12.005 | - |
| LUMEN binary (Python) | 13.579 | 0.479 |
| LUMEN zlib-6 (Python) | 13.693 | - |
| LUMIA (Python) | 1.511 | 0.833 |
| LUMEN text (Rust) | **1.018** | - |
| LUMEN binary (Rust) | **1.013** | 0.475 |
| LUMEN zlib-6 (Rust) | **1.243** | - |
| LUMIA (Rust) | 3.497 | 0.830 |

Rust acceleration: **13.4× faster** binary encode, **11.8× faster** text encode vs Python.

LUMEN binary (Rust) encode is comparable to JSON encode while producing
output **4.5× smaller**.

---

## At a Glance

| | LUMEN binary | LUMEN text | LUMIA | JSON |
|---|---|---|---|---|
| Size vs JSON | **22.4%** | 32.1% | 39.4% | 100% |
| Zlib compressed | **1.7%** | - | - | - |
| Rust encode (ms) | **1.013** | 1.018 | 3.497 | 1.264 |
| Python encode (ms) | 13.579 | 12.005 | 1.511 | 1.264 |
| Self-describing | ✓ | ✓ | ✓ | ✓ |
| LLM-generatable | - | - | **✓** | partial |
| Round-trip exact | ✓ | ✓ | ✓ | ✗ NaN/inf |

---

## Surfaces

LUMEN exposes three surfaces over a single data model:

### Binary:`LUMB` prefix
Columnar binary format. Smallest on wire. Designed for storage and IPC.
Supports delta encoding, bitpacking, RLE, string pooling, and zlib.

### Text: `records[N]:` prefix
Line-oriented, diff-friendly, human-readable. Compatible with standard
text tools. Uses the same pool and strategy system as binary.

### LUMIA: `L|` prefix
LLM-native CSV surface. Every payload is self-describing via a typed
header line. Language models can read and generate LUMIA without
special training or prompt engineering.

### LUMEN-AGENT
Structured protocol for agentic AI communication. Typed record schemas
for messages, tool calls, results, plans, observations, errors, memory,
RAG chunks, hypotheses, and chain-of-thought steps.

---

## Installation

### From source (development)

```bash
# Requires: Rust toolchain + maturin
git clone https://github.com/makroumi/lumen
cd lumen
pip install maturin
maturin develop --release
```
---

## Surfaces

LUMEN exposes three surfaces over a single data model:

### Binary - `LUMB` prefix
Columnar binary format. Smallest on wire. Designed for storage and IPC.
Supports delta encoding, bitpacking, RLE, string pooling, and zlib.

### Text - `records[N]:` prefix
Line-oriented, diff-friendly, human-readable. Compatible with standard
text tools. Uses the same pool and strategy system as binary.

### LUMIA - `L|` prefix
LLM-native CSV surface. Every payload is self-describing via a typed
header line. Language models can read and generate LUMIA without
special training or prompt engineering.

### LUMEN-AGENT
Structured protocol for agentic AI communication. Typed record schemas
for messages, tool calls, results, plans, observations, errors, memory,
RAG chunks, hypotheses, and chain-of-thought steps.

---

## Installation

### From source (development)

```bash
# Requires: Rust toolchain + maturin
git clone https://github.com/makroumi/lumen
cd lumen
pip install maturin
maturin develop --release
```
Python only (no Rust)

```Bash

pip install -e .
The library detects automatically whether the Rust extension is available
and falls back to the pure Python reference implementation silently.
```

Quick Start
```Python

from lumen import LumenDict, LumenDictRust, encode_lumen_llm, decode_lumen_llm

records = [
    {"id": 1, "name": "Alice", "city": "London", "score": 98.5, "active": True},
    {"id": 2, "name": "Bob",   "city": "London", "score": 91.0, "active": False},
    {"id": 3, "name": "Carol", "city": "Paris",  "score": 87.3, "active": True},
]

# --- Binary (smallest) ---
ld = LumenDict(records)
binary = ld.encode_binary_pooled()   # columnar binary with pool + strategies
zlib_  = ld.encode_binary_zlib()     # binary + zlib compression

# --- Text (human-readable) ---
text = ld.encode_text()

# --- LUMIA (LLM-native) ---
lumia = encode_lumen_llm(records)
back  = decode_lumen_llm(lumia)      # exact round-trip

# --- Rust acceleration (drop-in) ---
ld_rust = LumenDictRust(records)
binary  = ld_rust.encode_binary_pooled()
text    = ld_rust.encode_text()
lumia   = ld_rust.encode_lumen_llm()
```
API Reference
LumenDict(data=None, optimizations=False)
Pure Python record container. Zero dependencies.

```Python

ld = LumenDict(records)

ld.encode_text()              # → str   LUMEN text format
ld.encode_binary()            # → bytes raw binary (no strategies unless optimizations=True)
ld.encode_binary_pooled()     # → bytes binary with full strategy selection
ld.encode_binary_zlib(level=6)# → bytes binary + zlib, level 0–9
ld.encode_lumen_llm()         # → str   LUMIA format

ld.decode_text(text)          # → LumenDict
ld.decode_binary(data)        # → LumenDict
ld.decode_lumen_llm(text)     # → LumenDict

ld.to_json()                  # → str   standard JSON (NaN/inf → null)
ld.append(record)             # mutate - rebuilds pool, invalidates cache

len(ld)                       # number of records
ld.pool_size                  # number of interned strings
ld[0]                         # direct index access
LumenDictFull(data=None, pool_size_limit=256)
```
Extended pool variant. Strategies always enabled. Best compression
for large repetitive datasets.

```Python

ldf = LumenDictFull(records, pool_size_limit=256)
ldf.encode_binary()           # always uses full strategy selection
ldf.encode_text()
ldf.encode_lumen_llm()
```
LumenDictRust / LumenDictFullRust
Rust-accelerated drop-in replacements. Byte-identical output.
Available when the Rust extension is compiled.

```Python

from lumen import LumenDictRust, LumenDictFullRust, RUST_AVAILABLE

print(RUST_AVAILABLE)         # True if Rust extension is compiled

ld = LumenDictRust(records, optimizations=False, pool_size_limit=64)
ld.encode_text()
ld.encode_binary_pooled()
ld.encode_binary_zlib(level=6)
ld.encode_lumen_llm()
```
Module-level functions
```Python

from lumen import (
    encode_lumen_llm,       # records → LUMIA str
    decode_lumen_llm,       # LUMIA str → records
    encode_binary_records,  # records, pool, pool_map → bytes
    decode_binary_records,  # bytes → records
    encode_text_records,    # records, pool, pool_map → str
    decode_text_records,    # str → records
    build_pool,             # records → (pool, pool_map)
    detect_column_strategy, # values → 'raw'|'delta'|'rle'|'bits'|'pool'
)
```
LUMIA Format
LUMIA is designed so a language model can produce valid output by filling
in column values - no grammar to memorize, no pointers to resolve.

```text

L|id:d,name:s,city:s,score:f,active:b
1,Alice,London,98.5,T
2,Bob,London,91.0,F
3,Carol,Paris,87.3,T
Header line: L| magic + comma-separated column:type specs.
Data rows: one comma-separated row per record.

Type hints: d int · f float · b bool · s string · n null · m mixed

Special tokens: N null · T true · F false · $0= empty string ·
nan · inf · -inf

Strings containing , " { } [ ] | : are RFC 4180 quoted.
```
LUMEN-AGENT Protocol
Structured wire format for multi-agent AI systems.

```Python

from lumen import encode_agent_payload, decode_agent_payload, validate_agent_payload

records = [
    {
        "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
        "role": "user", "turn": 1, "content": "Hello", "tokens": 5, "flagged": False,
    },
    {
        "type": "tool", "id": "tc1", "thread_id": "t1", "step": 2,
        "name": "search", "args": '{"q":"lumen"}', "status": "done",
    },
]

payload = encode_agent_payload(records)
decoded = decode_agent_payload(payload)
ok, err = validate_agent_payload(payload)
```
Record types: msg tool res plan obs err mem rag hyp cot

Wire format:

```text

LUMEN-AGENT v1
records: N
type|id|thread_id|step|field|field|...
```
Primitives
Low-level codec functions for building custom encoders:

```Python

from lumen import (
    encode_varint, decode_varint,      # variable-length unsigned int
    encode_zigzag, decode_zigzag,      # signed int via zigzag mapping
    pack_string, pack_int, pack_float, # scalar packers
    pack_bool, pack_null, pack_pool_ref,
    pack_bits, unpack_bits,            # boolean bitpacking
    pack_delta_raw, unpack_delta_raw,  # delta encoding
    pack_rle,                          # run-length encoding
)
```
Wire Format Constants
```Python

from lumen import (
    MAGIC,    # b'LUMB'
    VERSION,  # bytes([3, 3])
    # Type tags
    T_STR_TINY, T_STR, T_INT, T_FLOAT, T_BOOL, T_NULL,
    T_LIST, T_MAP, T_POOL_DEF, T_POOL_REF, T_MATRIX,
    T_DELTA_RAW, T_BITS, T_RLE,
    # Strategy bytes
    S_RAW, S_DELTA, S_RLE, S_BITS, S_POOL,
)
```
Utilities
```Python

from lumen import (
    estimate_tokens,   # rough LLM token count (len / 4)
    deep_size,         # recursive memory footprint in bytes
    deep_eq,           # structural equality handling NaN and inf
    fnv1a, fnv1a_str,  # FNV-1a 32-bit hash
)
```
Architecture
```text

lumen/
├── Cargo.lock
├── Cargo.toml
├── pyproject.toml
├── README.md
├── SPEC.md
├── .gitignore
├── .github/
│   └── workflows/
│       └── ci.yml
├── src/
│   └── lib.rs
├── lumen/
│   ├── __init__.py
│   ├── core.py
│   └── core/
│       ├── __init__.py
│       ├── _constants.py
│       ├── _primitives.py
│       ├── _strategies.py
│       ├── _text.py
│       ├── _binary.py
│       ├── _lumen_llm.py
│       ├── _agent.py
│       └── _api.py
├── tests/
│   ├── conftest.py
│   ├── integration/
│   │   ├── test_edge_cases.py
│   │   ├── test_init_coverage.py
│   │   └── test_rust_layer.py
│   ├── perf/
│   │   ├── test_benchmark.py
│   │   ├── test_size.py
│   │   └── test_speed.py
│   └── unit/
│       ├── test_agent.py
│       ├── test_core_coverage.py
│       ├── test_encoders.py
│       ├── test_lumendict.py
│       ├── test_lumen_llm.py
│       ├── test_primitives.py
│       └── test_strategies.py
└── docs/
    ├── index.md
    ├── getting-started/
    │   ├── installation.md
    │   └── quickstart.md
    ├── guides/
    │   ├── binary-format.md
    │   ├── text-format.md
    │   ├── lumia.md
    │   └── compression.md
    ├── reference/
    │   ├── api.md
    │   ├── constants.md
    │   ├── primitives.md
    │   └── benchmarks.md
    ├── agent/
    │   ├── overview.md
    │   ├── spec.md
    │   └── system-prompt.md
    └── internals/
        ├── architecture.md
        └── wire-format.md
```
Design principle: the Python layer is the normative specification.
The Rust layer is an optimization - same output, faster execution.
All encode results are cached after the first call and invalidated on mutation.

Running Tests
```Bash

pytest tests/ -v
```
862 tests across unit, integration, and performance suites.
All tests pass with and without the Rust extension.

Format Specification
See SPEC.md for the complete wire format specification
including all tag values, encoding rules, strategy selection logic,
and LUMIA/LUMEN-AGENT protocol details.

Versioning
1.0.0
