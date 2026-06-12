<div align="center">

# ULMEN

**The serialization engine built for AI agent infrastructure.**

[![Rust Tests](https://img.shields.io/badge/rust_tests-96-brightgreen)]()
[![Python Tests](https://img.shields.io/badge/python_tests-1393-brightgreen)]()
[![License](https://img.shields.io/badge/license-BSL--1.1-blue)]()
[![Zero Dependencies](https://img.shields.io/badge/ulmen--core-zero_deps-orange)]()

[Benchmarks](#benchmarks) | [Quick Start](#quick-start) | [Architecture](#architecture) | [Agent Protocol](#ulmen-agent-protocol) | [API Reference](#api-reference)

</div>

---

ULMEN is a pure Rust serialization and agent protocol engine.
It encodes structured data **8x faster than JSON** while producing payloads **55-97% smaller**.

Built as the data layer for agentic AI infrastructure. Used by [uldb](https://github.com/makroumi/uldb) (storage), [ulmp](https://github.com/makroumi/ulmp) (protocol), and ulflow (orchestration).

```rust
use ulmen_core::*;

let record = AgentRecord {
    record_type: RecordType::Msg,
    id: "m1".into(),
    thread_id: "t1".into(),
    step: 1,
    fields: vec![
        FieldValue::Str("user".into()),
        FieldValue::Int(1),
        FieldValue::Str("What is 2+2?".into()),
        FieldValue::Int(5),
        FieldValue::Bool(false),
    ],
    meta: MetaFields::default(),
};

let payload = AgentPayload {
    header: AgentHeader { record_count: 1, ..Default::default() },
    records: vec![record],
};

let encoded = payload.encode();   // 0.22 us/record
let decoded = AgentPayload::decode(&encoded).unwrap();
```

``` python
import ulmen

# Encode
payload = ulmen.encode_agent_payload([
    {"type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
     "role": "user", "turn": 1, "content": "hello", "tokens": 1, "flagged": False},
    {"type": "tool", "id": "t1", "thread_id": "t1", "step": 2,
     "name": "search", "args": "{}", "status": "done"},
    {"type": "res", "id": "t1", "thread_id": "t1", "step": 3,
     "name": "search", "data": "results", "status": "done", "latency_ms": 42},
])

# Decode
records = ulmen.decode_agent_payload(payload)

# Validate
ok, err = ulmen.validate_agent_payload(payload)

# Compress
compressed = ulmen.compress_context(records, strategy="completed_sequences")

# Chunk for unlimited context
chunks = ulmen.chunk_payload(records, token_budget=4096)

# Repair malformed LLM output
fixed = ulmen.parse_llm_output(raw_llm_text)

# JSON bridge
json_str = ulmen.to_json(payload, pretty=True)
payload2 = ulmen.from_json(json_str)
```

## Features

``` text
ulmen-core/        Pure Rust crate, zero dependencies
  agent.rs         AgentRecord, AgentPayload, encode/decode
  validate.rs      Semantic validation (thread monotonicity, tool/res pairing, enums)
  compress.rs      Context compression (3 strategies), dedup, subgraph extraction
  chunk.rs         Chunking with tool+res atomicity, merge, error payloads
  repair.rs        LLM output repair (strip fences, fix counts, skip bad rows)
  tokens.rs        BPE token counting (cl100k_base approximation)

ulmen-python/      PyO3 wrapper (thin client for Python community)
  lib.rs           Converts Python dicts to native Rust types

ulmen/             Python package (unchanged API, Rust-accelerated)
```

### Ecosystem

``` text
ulmen-core    Serialization + agent protocol    (this crate)
uldb          Storage, indexing, caching         depends on ulmen-core
ulmp          Wire protocol, networking          depends on ulmen-core
ulflow        Agent orchestration                depends on all three
```

---
## UlMEN-AGENT Protocol
ULMEN-AGENT v1 is a strict typed pipe-delimited format for AI agent communication.

### Wire Format
``` text
ULMEN-AGENT v1
thread: session_001
context_window: 4096
records: 3
msg|m1|session_001|1|user|1|Search for auth best practices|8|F
tool|t1|session_001|2|web_search|{"q": "auth"}|done
res|t1|session_001|3|web_search|results here|done|42
```

### Record Types

Type|Fields|Purpose
---|---|---
`msg`|role, turn, content, tokens, flagged|User/assistant messages
`tool`|name, args, status|Tool invocations
`res`|name, data, status, latency_ms|Tool results
`plan`|index, description, status|Planning steps
`obs`|source, content, confidence|Observations
`err`|code, message, source, recoverable|Errors
`mem`|key, value, confidence, ttl|Memory/state
`rag`|rank, score, source, chunk, used|RAG retrieval
`hyp`|statement, evidence, score, accepted|Hypotheses
`cot`|index, cot_type, text, confidence|Chain of thought

## Validation Rules

1. Field count must exactly match the schema for the record type
2. Required fields must not be null
3. step must be a positive integer, non-decreasing within each thread
4. Every res id must match a prior tool id
5. records: N must equal the exact number of data rows
6. Enum fields must contain valid values (e.g., role: user/assistant/system)

### Context Compression
Three strategies for managing large context windows:

1. **completed_sequences**: Replace finished tool+res pairs with mem summaries
2. **keep_types**: Keep only specified record types (e.g., msg, err, mem)
3. **sliding_window**: Keep recent N records, summarize older ones

### Unlimited Context
`chunk_payload` splits large record sets into multiple payloads linked via `payload_id / parent_payload_id`. Tool+res pairs are always kept atomic within the same chunk. `merge_chunks` reassembles them with deduplication.

---
## API Reference

### Rust (`ulmen-core`)

```rust
// Types
AgentRecord, AgentPayload, AgentHeader, MetaFields
RecordType, FieldValue, AgentError

// Encode/Decode
AgentPayload::encode(&self) -> String
AgentPayload::decode(text: &str) -> Result<AgentPayload, AgentError>

// Validate
validate_payload(payload: &AgentPayload) -> Result<(), ValidationError>
validate_payload_str(text: &str) -> (bool, Option<ValidationError>)

// Compress
compress_context(records, strategy, keep_priority, ...) -> Vec<AgentRecord>
dedup_mem(records) -> Vec<AgentRecord>
get_latest_mem(records, key) -> Option<&AgentRecord>
extract_subgraph(records, thread_id, step_min, step_max, types) -> Vec<AgentRecord>
summarize_as_mem(records) -> Vec<AgentRecord>

// Chunk
chunk_payload(records, budget, ...) -> Vec<AgentPayload>
merge_chunks(payloads) -> Vec<AgentRecord>
make_error_payload(msg, thread_id) -> AgentPayload

// Repair
parse_llm_output(raw_text, thread_id, strict) -> Result<AgentPayload, String>

// Tokens
count_tokens(text) -> usize
count_tokens_with_overhead(text, per_record_overhead) -> usize
estimate_tokens(text) -> usize
```

### Python 
``` python
# Encode/Decode
ulmen.encode_agent_payload(records, thread_id=None, context_window=None, ...)
ulmen.decode_agent_payload(text) -> list[dict]
ulmen.encode_agent_record(record, meta_fields=())
ulmen.decode_agent_record(line, meta_fields=())

# Validate
ulmen.validate_agent_payload(text, structured=False) -> (bool, error)

# Compress
ulmen.compress_context(records, strategy="completed_sequences", ...)
ulmen.dedup_mem(records) -> list[dict]
ulmen.get_latest_mem(records, key) -> dict | None

# Chunk
ulmen.chunk_payload(records, token_budget, ...)
ulmen.merge_chunks(payloads) -> list[dict]

# Repair
ulmen.parse_llm_output(raw_text, thread_id=None, strict=False)

# Tokens
ulmen.count_tokens_exact(text) -> int
ulmen.estimate_tokens(text) -> int

# JSON bridge
ulmen.from_json(json_str, thread_id=None, context_window=None) -> str
ulmen.to_json(payload, pretty=False) -> str
ulmen.compare_sizes(json_str) -> (int, int, float)

# Data surfaces (UlmenDict)
ulmen.UlmenDictRust(records).encode_text()
ulmen.UlmenDictRust(records).encode_binary_pooled()
ulmen.UlmenDictRust(records).encode_binary_zlib()
ulmen.UlmenDictRust(records).encode_ulmen_llm()
```
---
## Running tests
```Bash
# Rust tests (96 tests)
cargo test --workspace

# Python tests (1393 tests)
python -m pytest tests/ -q

# Benchmarks
cargo run --release --example bench -p ulmen-core
python -m pytest tests/perf/test_benchmark.py -v -s
```

---

## Wire Format
ULMEN supports four encoding surfaces over a single data model:

Surface                                 |  Use Case                              |  Size                                 | Speed
----------------------------------------|----------------------------------------|----------------------------------------|----------------------------------------
ULMEN-AGENT                             | Agent communication, LLM context       | 55% smaller than JSON                | 6x faster
Binary                                  | Storage, wire transport                | 59% smaller                          | 6x faster
Binary + zlib                           | Archival, bandwidth-constrained        | 97% smaller                          | 5x faster
Text                                    | Human-readable, diffs, debugging       | 57% smaller                          | 6x faster
ULMEN LLM                               | LLM-native typed CSV                   | 55% smaller                          | comparable

See [`SPEC.md`](./SPEC.md) for the complete wire format specification.

---
## Lisence
Business Source License 1.1. See [LICENSE](./LICENSE).

Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.






