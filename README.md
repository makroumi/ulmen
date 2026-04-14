# LUMEN V1
**Lightweight Universal Minimal Encoding Notation**

Copyright (c) El Mehdi Makroumi. All rights reserved.
Proprietary and confidential.

---

LUMEN is a serialization format engineered to be the smallest, fastest,
and most token-efficient way to move structured data between services,
into storage, and through language model context windows.

It ships as a pure Python library with an optional Rust acceleration layer
that is drop-in compatible and byte-identical in output.

---

## Table of Contents

- [Benchmarks](#benchmarks)
- [At a Glance](#at-a-glance)
- [Surfaces](#surfaces)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [Wire Format Constants](#wire-format-constants)
- [Utilities](#utilities)
- [Architecture](#architecture)
- [Running Tests](#running-tests)
- [Format Specification](#format-specification)
- [Versioning](#versioning)

---

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
| LUMEN zlib-6 | 2,453 | 1.7% |
| LUMEN zlib-9 | 2,450 | 1.7% |

Python and Rust produce byte-identical output.

### Speed - Encode (median ms, 1,000 records)

| Format | Encode ms | Decode ms |
|---|---:|---:|
| JSON | 2.137 | 2.771 |
| Pickle protocol 4 | 0.644 | 0.807 |
| LUMEN text (Python) | 19.707 | - |
| LUMEN binary (Python) | 22.008 | 0.736 |
| LUMEN zlib-6 (Python) | 22.251 | - |
| LUMIA (Python) | 2.433 | 1.405 |
| LUMEN text (Rust) | 1.726 | - |
| LUMEN binary (Rust) | 1.721 | 0.738 |
| LUMEN zlib-6 (Rust) | 2.109 | - |
| LUMIA (Rust) | 5.697 | 1.397 |

Rust acceleration: 12.8x faster binary encode, 11.4x faster text encode vs Python.

LUMEN binary (Rust) encode is comparable to JSON encode while producing
output 4.5x smaller.

### Streaming (median 50 runs, 1,000 records)

| Surface | Encode ms | Decode ms | MB/s |
|---|---:|---:|---:|
| `stream_encode` | 2.298 | 0.726 | 14.2 |
| `stream_encode_windowed` (ws=100) | 1.660 | — | — |

Wire format identical to batch encode. Rust backend selected automatically.

---

## At a Glance

| | LUMEN binary | LUMEN text | LUMIA | JSON |
|---|---|---|---|---|
| Size vs JSON | 22.4% | 32.1% | 39.4% | 100% |
| Zlib compressed | 1.7% | - | - | - |
| Rust encode (ms) | 1.721 | 1.726 | 5.697 | 2.137 |
| Python encode (ms) | 22.008 | 19.707 | 2.433 | 2.137 |
| Self-describing | yes | yes | yes | yes |
| LLM-generatable | - | - | yes | partial |
| Round-trip exact | yes | yes | yes | no (NaN/inf) |

---

## Surfaces

LUMEN exposes four surfaces over a single data model:

### Binary: `LUMB` prefix
Columnar binary format. Smallest on wire. Designed for storage and IPC.
Supports delta encoding, bitpacking, RLE, string pooling, and zlib.

### Text: `records[N]:` prefix
Line-oriented, diff-friendly, human-readable. Compatible with standard
text tools. Uses the same pool and strategy system as binary.

### LUMIA: `L|` prefix
LLM-native CSV surface. Every payload is self-describing via a typed
header line. Language models can read and generate LUMIA without
special training or prompt engineering.

### Streaming: `LumenStreamEncoder` / `stream_encode`
Zero-materialisation streaming encode surface. Feed records one at a time
or in batches, then flush to an iterator of bytes chunks. The Rust backend
is selected automatically. Wire format is identical to batch binary encode —
every chunk is independently decodable. For truly unbounded streams use
`stream_encode_windowed` which encodes fixed-size windows into independent
sub-payloads, each decodable standalone.

### LUMEN-AGENT: `LUMEN-AGENT v1` prefix
Structured protocol for agentic AI communication. Typed record schemas
for messages, tool calls, results, plans, observations, errors, memory,
RAG chunks, hypotheses, and chain-of-thought steps.

Extended capabilities:
- Extended header fields: payload_id, parent_payload_id, agent_id,
  session_id, schema_version, context_window, context_used, meta_fields
- Meta fields appended to every row: parent_id, from_agent, to_agent, priority
- Context compression: completed_sequences, keep_types, sliding_window
- Priority-based retention: MUST_KEEP, KEEP_IF_ROOM, COMPRESSIBLE
- Unlimited context via chunk_payload, merge_chunks, build_summary_chain
- LLM output auto-repair via parse_llm_output
- Exact BPE token counting via count_tokens_exact (cl100k_base)
- Multi-agent routing via AgentRouter
- Cross-payload thread tracking via ThreadRegistry
- Append-only audit trail via ReplayLog
- Programmatic system prompt generation via generate_system_prompt
- LUMIA bridge: convert_agent_to_lumia, convert_lumia_to_agent
- Structured validation errors via ValidationError
- Context budget enforcement via ContextBudgetExceededError
- Streaming decode via decode_agent_stream
- Subgraph extraction by thread, step range, type
- Memory deduplication via dedup_mem, get_latest_mem
- MessagePack compatibility via encode_msgpack, decode_msgpack

---

## Installation

### From source (with Rust acceleration)

```bash
git clone https://github.com/makroumi/lumen
cd lumen
pip install maturin
maturin develop --release
```

### Python only (no Rust required)

```bash
pip install -e .
```
The library detects automatically whether the Rust extension is available and falls back to the pure Python implementation silently.

---

## Quick Start

```Python
from lumen import LumenDict, LumenDictRust, encode_lumen_llm, decode_lumen_llm

records = [
    {"id": 1, "name": "Alice", "city": "London", "score": 98.5, "active": True},
    {"id": 2, "name": "Bob",   "city": "London", "score": 91.0, "active": False},
    {"id": 3, "name": "Carol", "city": "Paris",  "score": 87.3, "active": True},
]

# Binary (smallest)
ld     = LumenDict(records)
binary = ld.encode_binary_pooled()
zlib_  = ld.encode_binary_zlib()

# Text (human-readable)
text = ld.encode_text()

# LUMIA (LLM-native)
lumia = encode_lumen_llm(records)
back  = decode_lumen_llm(lumia)

# Rust acceleration (drop-in, byte-identical)
ld_rust = LumenDictRust(records)
binary  = ld_rust.encode_binary_pooled()
text    = ld_rust.encode_text()
lumia   = ld_rust.encode_lumen_llm()
```

## LUMEN-AGENT

```python
from lumen import (
    encode_agent_payload,
    decode_agent_payload,
    decode_agent_payload_full,
    validate_agent_payload,
    compress_context,
    chunk_payload,
    merge_chunks,
    build_summary_chain,
    parse_llm_output,
    count_tokens_exact,
    AgentRouter,
    ThreadRegistry,
    ReplayLog,
    generate_system_prompt,
    convert_agent_to_lumia,
    convert_lumia_to_agent,
    dedup_mem,
    get_latest_mem,
    estimate_context_usage,
    extract_subgraph,
    extract_subgraph_payload,
    make_validation_error,
    AgentHeader,
    ValidationError,
    ContextBudgetExceededError,
)

records = [
    {
        "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
        "role": "user", "turn": 1, "content": "Hello", "tokens": 5,
        "flagged": False,
    },
    {
        "type": "tool", "id": "tc1", "thread_id": "t1", "step": 2,
        "name": "search", "args": '{"q":"lumen"}', "status": "pending",
    },
    {
        "type": "res", "id": "tc1", "thread_id": "t1", "step": 3,
        "name": "search", "data": "LUMEN is fast", "status": "done",
        "latency_ms": 42,
    },
]

# Encode with extended header fields
payload = encode_agent_payload(
    records,
    thread_id="t1",
    context_window=8000,
    payload_id="uuid-abc",
    parent_payload_id="uuid-prev",
    agent_id="agent-alpha",
    session_id="sess-001",
    schema_version="1.0.0",
    auto_context=True,
    auto_payload_id=False,
    enforce_budget=False,
)

# Decode (records only)
decoded = decode_agent_payload(payload)

# Decode (records + parsed header)
records_out, header = decode_agent_payload_full(payload)
print(header.payload_id)
print(header.context_used)

# Validate
ok, err = validate_agent_payload(payload)

# Validate with structured error object
ok, err = validate_agent_payload(payload, structured=True)
if not ok:
    print(err.message, err.row, err.field, err.suggestion)

# Stream decode one record at a time
from lumen import decode_agent_stream
for rec in decode_agent_stream(iter(payload.splitlines(keepends=True))):
    print(rec["type"])

# Context compression
from lumen.core._agent import COMPRESS_COMPLETED_SEQUENCES
compressed = compress_context(
    records,
    strategy=COMPRESS_COMPLETED_SEQUENCES,
    preserve_cot=True,
)

# Memory deduplication
clean = dedup_mem(records)
latest = get_latest_mem(records, key="user_pref")

# Context usage estimation
usage = estimate_context_usage(records)
print(usage["tokens"], usage["by_type"])

# Chunking for unlimited context
chunks = chunk_payload(records, token_budget=2000, thread_id="t1", overlap=1)
merged = merge_chunks(chunks)

# Summary chain for unlimited context
chain = build_summary_chain(records, token_budget=2000, thread_id="t1")

# LLM output auto-repair
repaired = parse_llm_output(raw_llm_text)
repaired = parse_llm_output(raw_llm_text, strict=True)

# Exact token counting
n_tokens = count_tokens_exact(payload)

# Subgraph extraction
filtered = extract_subgraph(records, thread_id="t1", step_min=2, types=["tool","res"])
filtered_payload = extract_subgraph_payload(payload, types=["cot"])

# Multi-agent routing
router = AgentRouter()
router.register("planner", "executor", lambda rec: print(rec))
router.dispatch(records)

# Cross-payload thread tracking
registry = ThreadRegistry()
registry.add_payload("pid-1", records)

# Audit trail
log = ReplayLog()
log.append({"event": "encode", "payload_id": "pid-1"})

# System prompt generation
prompt = generate_system_prompt(include_examples=True, include_validation=True)

# LUMIA bridge
lumia   = convert_agent_to_lumia(payload)
payload2 = convert_lumia_to_agent(lumia, thread_id="t1")

# Validation error payload
err_payload = make_validation_error("bad step", thread_id="t1")

# Context budget enforcement
try:
    encode_agent_payload(records, context_window=10, enforce_budget=True)
except ContextBudgetExceededError as e:
    print(e.overage)
```

---

## API Reference
### LumenDict
Pure Python record container. Zero runtime dependencies.

```python
ld = LumenDict(records)

ld.encode_text()               # str   LUMEN text format
ld.encode_binary()             # bytes raw binary
ld.encode_binary_pooled()      # bytes binary with full strategy selection
ld.encode_binary_zlib(level=6) # bytes binary + zlib, level 0-9
ld.encode_lumen_llm()          # str   LUMIA format

ld.decode_text(text)           # LumenDict
ld.decode_binary(data)         # LumenDict
ld.decode_lumen_llm(text)      # LumenDict

ld.to_json()                   # str standard JSON (NaN/inf replaced with null)
ld.append(record)              # mutate, rebuilds pool, invalidates cache

len(ld)                        # number of records
ld.pool_size                   # number of interned strings
ld[0]                          # direct index access
```

### LumenDictRust
Extended pool variant. Strategies always enabled.

```python
ldf = LumenDictFull(records, pool_size_limit=256)
ldf.encode_binary()
ldf.encode_text()
ldf.encode_lumen_llm()
```

### LumenDictRust / LumenDictFullRust
Rust-accelerated drop-in replacements. Byte-identical output.

```python
from lumen import LumenDictRust, LumenDictFullRust, RUST_AVAILABLE

print(RUST_AVAILABLE)
ld = LumenDictRust(records, optimizations=False, pool_size_limit=64)
ld.encode_text()
ld.encode_binary_pooled()
ld.encode_binary_zlib(level=6)
ld.encode_lumen_llm()
```
### Streaming encode

See `lumen.core._streaming` for full API.

    from lumen import LumenStreamEncoder, stream_encode, stream_encode_windowed

    # One-shot
    for chunk in stream_encode(records, chunk_size=65536):
        socket.sendall(chunk)

    # Stateful
    enc = LumenStreamEncoder(pool_size_limit=64, chunk_size=65536)
    enc.feed(record)
    enc.feed_many(records)
    for chunk in enc.flush():
        sink.write(chunk)
    print(enc.rust_backed)  # True when Rust extension active

    # Unbounded windowed
    for chunk in stream_encode_windowed(records, window_size=1000):
        decode_binary_records(chunk)

### Model-level encode/decode

```python
from lumen import (
    encode_lumen_llm,
    decode_lumen_llm,
    encode_binary_records,
    decode_binary_records,
    encode_text_records,
    decode_text_records,
    build_pool,
    detect_column_strategy,
)
```

### LUMEN-AGENT core
```python
from lumen import (
    encode_agent_payload,
    decode_agent_payload,
    decode_agent_payload_full,
    decode_agent_record,
    encode_agent_record,
    decode_agent_stream,
    validate_agent_payload,
    make_validation_error,
    extract_subgraph,
    extract_subgraph_payload,
    AgentHeader,
    ValidationError,
    ContextBudgetExceededError,
)
```

'encode_agent_payload' parameters:

| Parameter | Type | Description |
|-----------|------|-------------|
| **records** | list[dict] | Records to encode |
| **thread_id** | str or None | Written to header |
| **context_window** | int or None | Token budget declared in header |
| **meta_fields** | tuple | Extra fields appended to every row |
| **auto_context** | bool | Compute context_used automatically |
| **enforce_budget** | bool | Raise ContextBudgetExceededError if over budget |
| **payload_id** | str or None | Unique ID for this payload |
| **parent_payload_id** | str or None | Links to prior payload in chain |
| **agent_id** | str or None | ID of the producing agent |
| **session_id** | str or None | Session this payload belongs to |
| **schema_version** | str or None | Protocol version for negotiation |
| **auto_payload_id** | bool | Generate a UUID payload_id automatically |

### Context compression

```python
from lumen import compress_context, dedup_mem, get_latest_mem, estimate_context_usage
from lumen.core._agent import (
    COMPRESS_COMPLETED_SEQUENCES,
    COMPRESS_KEEP_TYPES,
    COMPRESS_SLIDING_WINDOW,
    PRIORITY_MUST_KEEP,
    PRIORITY_KEEP_IF_ROOM,
    PRIORITY_COMPRESSIBLE,
)

compressed = compress_context(
    records,
    strategy=COMPRESS_COMPLETED_SEQUENCES,
    keep_priority=PRIORITY_KEEP_IF_ROOM,
    preserve_cot=True,
)

clean  = dedup_mem(records)
latest = get_latest_mem(records, key="pref")
usage  = estimate_context_usage(records)
```

Strategies:
- **completed_sequences**: replace completed tool+res pairs with mem summaries
- **keep_types**: keep only specified record types
- **sliding_window**: keep recent records verbatim, summarize older ones

### Unlimited context

```python
from lumen import chunk_payload, merge_chunks, build_summary_chain

chunks = chunk_payload(
    records,
    token_budget=4000,
    thread_id="t1",
    overlap=2,
    parent_payload_id="prev-id",
    session_id="sess-1",
)
merged = merge_chunks(chunks)

chain = build_summary_chain(
    records,
    token_budget=4000,
    thread_id="t1",
    session_id="sess-1",
)
```

### LLM output repair

```python
from lumen import parse_llm_output

repaired = parse_llm_output(raw_text)
repaired = parse_llm_output(raw_text, thread_id="t1", strict=True)
```
Uses cl100k_base BPE (GPT-4 / Claude compatible).
Falls back to character estimate when tiktoken is unavailable.

### Multi-agent routing

```python
from lumen import AgentRouter, validate_routing_consistency

router = AgentRouter()
router.register("agent_a", "agent_b", handler_fn)
router.dispatch(records)
router.dispatch_one(record)

ok, err = validate_routing_consistency(records)
```

### Cross-payload thread tracking

```python
from lumen import ThreadRegistry, merge_threads

registry = ThreadRegistry()
registry.add_payload("pid-1", records)
threads  = registry.get_threads()

merged = merge_threads([payload1_records, payload2_records])
```

### Audit trail

```python
from lumen import ReplayLog

log    = ReplayLog()
log.append({"event": "encode", "ts": 1234})
events = log.all()
```

### System prompt generation

```python
from lumen import generate_system_prompt

prompt = generate_system_prompt(
    include_examples=True,
    include_validation=True,
)
```

### LUMIA bridge

```python
from lumen import convert_agent_to_lumia, convert_lumia_to_agent

lumia   = convert_agent_to_lumia(agent_payload)
payload = convert_lumia_to_agent(lumia, thread_id="t1")
```

### MessagePack compatibility

```python
from lumen.core._msgpack_compat import encode_msgpack, decode_msgpack

packed   = encode_msgpack(records)
unpacked = decode_msgpack(packed)
```

---

## Wire Format Constants

```python
from lumen import (
    MAGIC,    # b'LUMB'
    VERSION,  # bytes([3, 3])
    T_STR_TINY, T_STR, T_INT, T_FLOAT, T_BOOL, T_NULL,
    T_LIST, T_MAP, T_POOL_DEF, T_POOL_REF, T_MATRIX,
    T_DELTA_RAW, T_BITS, T_RLE,
    S_RAW, S_DELTA, S_RLE, S_BITS, S_POOL,
    AGENT_MAGIC,   # "LUMEN-AGENT v1"
    AGENT_VERSION, # "1.0.0"
    RECORD_TYPES,  # frozenset of 10 type tags
    FIELD_COUNTS,  # dict[type -> total field count per row including common fields]
    META_FIELDS,   # ("parent_id", "from_agent", "to_agent", "priority")
    COMPRESS_COMPLETED_SEQUENCES,
    COMPRESS_KEEP_TYPES,
    COMPRESS_SLIDING_WINDOW,
    PRIORITY_MUST_KEEP,    # 1
    PRIORITY_KEEP_IF_ROOM, # 2
    PRIORITY_COMPRESSIBLE, # 3
)
```

---

## Utilities

```python
from lumen import (
    estimate_tokens,   # rough LLM token count (chars / 4)
    deep_size,         # recursive memory footprint in bytes
    deep_eq,           # structural equality handling NaN and inf
    fnv1a, fnv1a_str,  # FNV-1a 32-bit hash
)
```

---

## Architecture

```text
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
│       ├── test_lumendict.py
│       ├── test_lumen_llm.py
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
The Rust layer is an optimization producing identical output at higher speed.
All encode results are cached after the first call and invalidated on mutation.

---

## Running Tests

```bash
pytest tests/ -v
pytest tests/ --cov=lumen --cov-report=term-missing
```
1,364 tests across unit, integration, performance, and smoke suites.
100% statement coverage across all modules.
All tests pass with and without the Rust extension.

---

## Format Specification
See SPEC.md for the complete wire format specification including all tag
values, encoding rules, strategy selection logic, and full LUMIA and
LUMEN-AGENT protocol details.

---

## Versioning
1.0.0

