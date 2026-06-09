# Architecture

This document describes the codebase structure and the responsibilities of each module.

---

## Repository Layout

```text
ulmen/
├── Cargo.lock
├── Cargo.toml              # Rust PyO3 extension configuration
├── pyproject.toml
├── README.md
├── SPEC.md
├── src/
│   └── lib.rs              # PyO3 Rust acceleration layer
├── wasm/                   # Separate WASM crate for JavaScript
│   ├── Cargo.toml          # wasm-bindgen configuration
│   ├── src/
│   │   └── lib.rs          # WASM exports, same core algorithms
│   └── pkg/                # Generated WASM output (gitignored)
├── js/                     # JavaScript/TypeScript npm package
│   ├── package.json
│   ├── tsconfig.json
│   ├── README.md
│   ├── src/
│   │   └── index.ts        # TypeScript wrapper and Node.js/browser compat
│   └── dist/               # Built npm package (gitignored)
├── ulmen/
│   ├── __init__.py
│   ├── core.py
│   └── core/
│       ├── __init__.py
│       ├── _constants.py
│       ├── _primitives.py
│       ├── _strategies.py
│       ├── _text.py
│       ├── _binary.py
│       ├── _ulmen_llm.py
│       ├── _agent.py
│       ├── _api.py
│       ├── _repair.py
│       ├── _replay.py
│       ├── _routing.py
│       ├── _threading.py
│       ├── _tokens.py
│       ├── _msgpack_compat.py
│       └── _streaming.py
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
│       ├── test_ulmendict.py
│       ├── test_ulmen_llm.py
│       ├── test_msgpack_compat.py
│       ├── test_primitives.py
│       ├── test_repair.py
│       ├── test_replay.py
│       ├── test_routing.py
│       ├── test_strategies.py
│       ├── test_streaming.py
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
    │   ├── ulmen.md
    │   ├── compression.md
    │   └── javascript.md
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

### `ulmen/__init__.py`

The single public entry point. Responsibilities:

1. Import all public symbols from `ulmen.core`
2. Attempt to import the Rust extension `ulmen._ulmen_rust`
3. If Rust is available, override `decode_binary_records`, `encode_ulmen_llm`,
   `decode_ulmen_llm` with the Rust implementations
4. If Rust is unavailable, provide Python shims for `UlmenDictRust` and
   `UlmenDictFullRust` that satisfy the same interface
5. Export `RUST_AVAILABLE` flag

### `ulmen/core/_constants.py`

Pure constants. No functions, no imports from other ulmen modules.
Defines all type tags, strategy bytes, MAGIC, and VERSION.
Every other module imports from here.

### `ulmen/core/_primitives.py`

Pure functions. Input values, output bytes or (value, position) tuples.
No state, no side effects. Implements varint, zigzag, and all scalar
pack and unpack operations.

### `ulmen/core/_strategies.py`

Strategy selection and pool building. Depends on `_primitives` only.
`detect_column_strategy` is the normative strategy oracle.
`build_pool` is the normative pool builder.
Both Python and Rust implementations must produce identical results.

### `ulmen/core/_text.py`

Text encoder and decoder. Depends on `_strategies` and `_primitives`.
Implements the full text format including POOL lines, SCHEMA lines,
matrix mode, inline columns, and value tokenization.

### `ulmen/core/_binary.py`

Binary encoder and decoder. Depends on `_constants`, `_primitives`,
and `_strategies`. Implements the T_MATRIX columnar format, pool block,
and all column strategy encoders.

### `ulmen/core/_ulmen_llm.py`

ULMEN encoder and decoder. Self-contained. Implements the `L|` format
with typed headers, RFC 4180 quoting, and nested value encoding.

### `ulmen/core/_agent.py`

ULMEN-AGENT protocol. Self-contained. Implements:

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
- ULMEN bridge: `convert_agent_to_ulmen`, `convert_ulmen_to_agent`
- System prompt generation: `generate_system_prompt`
- Validation error payload: `make_validation_error`

### `ulmen/core/_api.py`

`UlmenDict` and `UlmenDictFull` classes. Thin stateful wrappers around the stateless codec functions. Manages pool state, encode caches, and provides the high-level user API.

### `ulmen/core/_repair.py`
LLM output parser and auto-repair engine. Accepts raw LLM text that may contain markdown fences, wrong record counts, blank lines, or unknown record types and returns a valid ULMEN-AGENT v1 payload. Applies repair in seven passes. Falls back to row-by-row re-encode on first-pass failure.
Returns a validation error payload when repair is not possible.
'strict=True' raises 'ValueError' instead of returning an error payload.

### `ulmen/core/_replay.py`
Append-only ReplayLog for audit trails. Each event is a dict appended in insertion order. The log supports no mutation or deletion. all() returns all events in insertion order.

### `ulmen/core/_routing.py`
'AgentRouter' for multi-agent message dispatch. Handlers are registered by (from_agent, to_agent) pair. dispatch routes a list of records to matching handlers and returns results. dispatch_one routes a single record. validate_routing_consistency checks from_agent and to_agent field consistency across a record list.

### `ulmen/core/_threading.py`
'ThreadRegistry' for cross-payload thread tracking. 'add_payload'
registers all records from a payload keyed by payload_id. get_threads returns a dict mapping thread_id to all records seen across all registered payloads. merge_threads takes a list of record lists and returns a unified thread dict.

### 'ulmen/core/_tokens.py'
Exact BPE token counting using the cl100k_base tokenizer (GPT-4 and
Claude compatible). 'count_tokens_exact' counts tokens in a string.
count_tokens_exact_records counts tokens across a list of agent records by encoding each record and summing. Falls back to character-based estimation when tiktoken is not installed.

### 'ulmen/core/_msgpack_compat.py'
MessagePack compatibility layer. encode_msgpack and decode_msgpack
provide a MessagePack-compatible wire format for interoperability with systems that consume MessagePack.

### `ulmen/core/_streaming.py`
Streaming binary encode surface. `UlmenStreamEncoder` accumulates records
via `feed` / `feed_many` then yields bytes chunks from `flush` / `finish`.
Automatically selects the Rust backend when available (`_RUST_STREAM`).
`stream_encode` is the one-shot helper. `stream_encode_windowed` processes
fixed-size windows into independent decodable sub-payloads for truly
unbounded streams. `stream_encode_ulmen` streams the ULMEN text format.

### `src/lib.rs`

Rust acceleration layer. Registered as `ulmen._ulmen_rust` via PyO3.
Exposes `UlmenDictRust`, `UlmenDictFullRust`, `UlmenStreamEncoder`,
`decode_binary_records_rust`, `encode_ulmen_llm_rust`,
`decode_ulmen_llm_rust`, `encode_binary_stream_chunked`.

Output is byte-identical to the Python reference for all surfaces.
The Python layer is the normative specification.

---

## Design Principles

**Python is normative.** The Python implementation defines correct behavior.
The Rust layer is an optimization that must match Python output exactly.

**Stateless codecs.** All encode and decode functions are pure. State lives only in `UlmenDict` and `UlmenDictFull`.

**Cached encode results.** All encode methods cache their result on the first call and return the cached value on subsequent calls. The cache is invalidated on `append`.

**Explicit fallback.** `RUST_AVAILABLE` is always importable. Code that needs to know whether Rust is active can check it explicitly.

**Zero runtime dependencies.** The pure Python implementation imports only from the standard library. No third-party packages required at runtime.

**Forward compatible headers.** Unknown header lines in ULMEN-AGENT payloads are silently ignored, allowing future protocol versions to add new fields without breaking existing parsers.

**All-or-nothing validation.** ULMEN-AGENT validation is strict: one invalid row rejects the entire payload. There is no partial acceptance.

---

## Rust Layer Internal Structure

```text
FxHasher fast non-cryptographic hash for string interning
StringTable O(1) string intern lookup with stable indices
ColVal stack-allocated column value enum (no heap per cell)
UlmenCore all precomputed encodings, lazy zlib
UlmenDictRust Python-exposed class wrapping UlmenCore
UlmenDictFullRust Python-exposed class wrapping UlmenCore
Cursor bounds-safe binary reader for decode
```

`UlmenCore::build` performs all encoding at construction time.
Zlib compression is deferred until first call to `encode_binary_zlib`.

---

## Adding a New Surface

1. Add encoder and decoder functions in a new `ulmen/core/_mysurface.py`
2. Export them from `ulmen/core/__init__.py` and add to `__all__`
3. Export them from `ulmen/__init__.py` and add to `__all__`
4. Add the Rust implementation in `src/lib.rs` if performance-critical
5. Wire the Rust override in the `try` block in `ulmen/__init__.py`
6. Add tests in `tests/unit/test_mysurface.py` targeting 100% coverage