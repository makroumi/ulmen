# Architecture

This document describes the codebase structure and the responsibilities of each module.

---

## Repository Layout

```text
lumen/
lumen/
├── Cargo.lock
├── Cargo.toml
├── pyproject.toml
├── README.md
├── SPEC.md
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
│       ├── _api.py
│       ├── _repair.py
│       ├── _replay.py
│       ├── _routing.py
│       ├── _threading.py
│       ├── _tokens.py
│       └── _msgpack_compat.py
├── tests/
│   ├── conftest.py
│   ├── smoke_test_comprehensive.py
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
│       ├── test_msgpack_compat.py
│       ├── test_primitives.py
│       ├── test_repair.py
│       ├── test_replay.py
│       ├── test_routing.py
│       ├── test_strategies.py
│       ├── test_threading.py
│       └── test_tokens.py
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

LUMEN-AGENT protocol. Self-contained. Implements:

- All 10 record type schemas with typed field validation
- Pipe-delimited encoding with RFC 4180 quoting for unsafe characters
- Header parsing with forward-compatible unknown-line ignoring
- AgentHeader dataclass with all optional fields
- Structured validation returning `ValidationError` objects
- ContextBudgetExceededError for budget enforcement
- Context compression: `completed_sequences`, `keep_types`, `sliding_window`
- Memory deduplication: `dedup_mem`, `get_latest_mem`
- Unlimited context: `chunk_payload`, `merge_chunks`, `build_summary_chain`
- Subgraph extraction: `extract_subgraph`, `extract_subgraph_payload`
- Streaming decode: `decode_agent_stream`
- Context usage estimation: `estimate_context_usage`
- LUMIA bridge: `convert_agent_to_lumia`, `convert_lumia_to_agent`
- System prompt generation: `generate_system_prompt`
- Validation error payload: `make_validation_error`

### `lumen/core/_api.py`

`LumenDict` and `LumenDictFull` classes. Thin stateful wrappers around the stateless codec functions. Manages pool state, encode caches, and provides the high-level user API.

### `lumen/core/_repair.py`
LLM output parser and auto-repair engine. Accepts raw LLM text that may contain markdown fences, wrong record counts, blank lines, or unknown record types and returns a valid LUMEN-AGENT v1 payload. Applies repair in seven passes. Falls back to row-by-row re-encode on first-pass failure.
Returns a validation error payload when repair is not possible.
'strict=True' raises 'ValueError' instead of returning an error payload.

### `lumen/core/_replay.py`
Append-only ReplayLog for audit trails. Each event is a dict appended in insertion order. The log supports no mutation or deletion. all() returns all events in insertion order.

### `lumen/core/_routing.py`
'AgentRouter' for multi-agent message dispatch. Handlers are registered by (from_agent, to_agent) pair. dispatch routes a list of records to matching handlers and returns results. dispatch_one routes a single record. validate_routing_consistency checks from_agent and to_agent field consistency across a record list.

### `lumen/core/_threading.py`
'ThreadRegistry' for cross-payload thread tracking. 'add_payload'
registers all records from a payload keyed by payload_id. get_threads returns a dict mapping thread_id to all records seen across all registered payloads. merge_threads takes a list of record lists and returns a unified thread dict.

### 'lumen/core/_tokens.py'
Exact BPE token counting using the cl100k_base tokenizer (GPT-4 and
Claude compatible). 'count_tokens_exact' counts tokens in a string.
count_tokens_exact_records counts tokens across a list of agent records by encoding each record and summing. Falls back to character-based estimation when tiktoken is not installed.

### 'lumen/core/_msgpack_compat.py'
MessagePack compatibility layer. encode_msgpack and decode_msgpack
provide a MessagePack-compatible wire format for interoperability with systems that consume MessagePack.

### `lumen/core/_streaming.py`
Streaming binary encode surface. `LumenStreamEncoder` accumulates records
via `feed` / `feed_many` then yields bytes chunks from `flush` / `finish`.
Automatically selects the Rust backend when available (`_RUST_STREAM`).
`stream_encode` is the one-shot helper. `stream_encode_windowed` processes
fixed-size windows into independent decodable sub-payloads for truly
unbounded streams. `stream_encode_lumia` streams the LUMIA text format.

### `src/lib.rs`

Rust acceleration layer. Registered as `lumen._lumen_rust` via PyO3.
Exposes `LumenDictRust`, `LumenDictFullRust`, `LumenStreamEncoder`,
`decode_binary_records_rust`, `encode_lumen_llm_rust`,
`decode_lumen_llm_rust`, `encode_binary_stream_chunked`.

Output is byte-identical to the Python reference for all surfaces.
The Python layer is the normative specification.

---

## Design Principles

**Python is normative.** The Python implementation defines correct behavior.
The Rust layer is an optimization that must match Python output exactly.

**Stateless codecs.** All encode and decode functions are pure. State lives only in `LumenDict` and `LumenDictFull`.

**Cached encode results.** All encode methods cache their result on the first call and return the cached value on subsequent calls. The cache is invalidated on `append`.

**Explicit fallback.** `RUST_AVAILABLE` is always importable. Code that needs to know whether Rust is active can check it explicitly.

**Zero runtime dependencies.** The pure Python implementation imports only from the standard library. No third-party packages required at runtime.

**Forward compatible headers.** Unknown header lines in LUMEN-AGENT payloads are silently ignored, allowing future protocol versions to add new fields without breaking existing parsers.

**All-or-nothing validation.** LUMEN-AGENT validation is strict: one invalid row rejects the entire payload. There is no partial acceptance.

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
2. Export them from `lumen/core/__init__.py` and add to `__all__`
3. Export them from `lumen/__init__.py` and add to `__all__`
4. Add the Rust implementation in `src/lib.rs` if performance-critical
5. Wire the Rust override in the `try` block in `lumen/__init__.py`
6. Add tests in `tests/unit/test_mysurface.py` targeting 100% coverage