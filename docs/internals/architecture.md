# Architecture

This document describes the codebase structure and the responsibilities
of each module.

---

## Repository Layout

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

## Module Responsibilities

### `lumen/__init__.py`

The single public entry point. Responsibilities:

1. Import all public symbols from `lumen.core`
2. Attempt to import the Rust extension `lumen._lumen_rust`
3. If Rust is available, override `decode_binary_records`, `encode_lumen_llm`,
   `decode_lumen_llm` with the Rust implementations
4. If Rust is unavailable, provide Python shims for `LumenDictRust` and
   `LumenDictFullRust` that satisfy the same interface
5. Export `RUST_AVAILABLE` flag

### `lumen/core/_constants.py`

Pure constants. No functions, no imports from other lumen modules.
Defines all type tags, strategy bytes, MAGIC, and VERSION.
Every other module imports from here.

### `lumen/core/_primitives.py`

Pure functions. Input values, output bytes or (value, position) tuples.
No state, no side effects. Implements varint, zigzag, and all scalar
pack and unpack operations.

### `lumen/core/_strategies.py`

Strategy selection and pool building. Depends on `_primitives` only.
`detect_column_strategy` is the normative strategy oracle.
`build_pool` is the normative pool builder.
Both Python and Rust implementations must produce identical results.

### `lumen/core/_text.py`

Text encoder and decoder. Depends on `_strategies` and `_primitives`.
Implements the full text format including POOL lines, SCHEMA lines,
matrix mode, inline columns, and value tokenization.

### `lumen/core/_binary.py`

Binary encoder and decoder. Depends on `_constants`, `_primitives`,
and `_strategies`. Implements the T_MATRIX columnar format, pool block,
and all column strategy encoders.

### `lumen/core/_lumen_llm.py`

LUMIA encoder and decoder. Self-contained. Implements the `L|` format
with typed headers, RFC 4180 quoting, and nested value encoding.

### `lumen/core/_agent.py`

LUMEN-AGENT protocol. Self-contained. Implements all 10 record type
schemas, pipe-delimited encoding, RFC 4180 quoting, validation, and
subgraph extraction.

### `lumen/core/_api.py`

`LumenDict` and `LumenDictFull` classes. Thin stateful wrappers around
the stateless codec functions. Manages pool state, encode caches, and
provides the high-level user API.

### `src/lib.rs`

Rust acceleration layer. Registered as `lumen._lumen_rust` via PyO3.
Exposes `LumenDictRust`, `LumenDictFullRust`, `decode_binary_records_rust`,
`encode_lumen_llm_rust`, `decode_lumen_llm_rust`.

Output is byte-identical to the Python reference for all surfaces.
The Python layer is the normative specification.

---

## Design Principles

**Python is normative.** The Python implementation defines correct behavior.
The Rust layer is an optimization that must match Python output exactly.

**Stateless codecs.** All encode and decode functions are pure. State lives
only in `LumenDict` and `LumenDictFull`.

**Cached encode results.** All encode methods cache their result on the
first call and return the cached value on subsequent calls. The cache is
invalidated on `append`.

**Explicit fallback.** `RUST_AVAILABLE` is always importable. Code that
needs to know whether Rust is active can check it explicitly.

**Zero runtime dependencies.** The pure Python implementation imports only
from the standard library. No third-party packages required at runtime.

---

## Rust Layer Internal Structure

```text
FxHasher fast non-cryptographic hash for string interning
StringTable O(1) string intern lookup with stable indices
ColVal stack-allocated column value enum (no heap per cell)
LumenCore all precomputed encodings, lazy zlib
LumenDictRust Python-exposed class wrapping LumenCore
LumenDictFullRust Python-exposed class wrapping LumenCore
Cursor bounds-safe binary reader for decode
```

`LumenCore::build` performs all encoding at construction time.
Zlib compression is deferred until first call to `encode_binary_zlib`.

---

## Adding a New Surface

1. Add encoder and decoder functions in a new `lumen/core/_mysurface.py`
2. Export them from `lumen/core/__init__.py`
3. Export them from `lumen/__init__.py`
4. Add the Rust implementation in `src/lib.rs` if performance-critical
5. Wire the Rust override in the `try` block in `lumen/__init__.py`
6. Add tests in `tests/unit/test_mysurface.py`