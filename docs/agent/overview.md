# ULMEN-AGENT Overview

ULMEN-AGENT v1 is a structured wire format for agentic AI systems.
It replaces free-form JSON and prose with a strict, typed, pipe-delimited protocol that is both token-efficient and zero-hallucination by design.

---

## Problem It Solves

Language models communicating with tools and other agents typically use JSON.
JSON has three problems for agentic workloads:

1. Verbose. Schema keys repeat on every record. No column compression.
2. Untyped. The model must infer types from context or schema definitions.
3. Hallucination-prone. Free-form structure gives models too much latitude.

ULMEN-AGENT fixes all three: typed rows, fixed schemas per record type, and strict validation that rejects any malformed payload atomically.

---

## Design Principles

**Self-describing.** Every row carries its type tag. A consumer can parse
any row without reading a header first.

**Typed.** Every field has a declared type. The parser enforces types.
A field with the wrong type causes the entire payload to be rejected.

**Atomic validation.** One invalid row rejects the entire payload.
There is no partial acceptance. Either the full payload is valid or it is not.

**Token-efficient.** 63% fewer tokens than JSON for typical agentic payloads
on the 14-row reference example.

**Streamable.** Each row is independently parseable. A consumer can process
rows as they arrive without buffering the full payload.

**Forward compatible.** Unknown header lines are silently ignored, so future protocol versions can add new header fields without breaking
existing parsers.

---

## Record Types

| Type | Purpose |
|---|---|
| `msg` | Conversation message (user, assistant, system) |
| `tool` | Tool call request |
| `res` | Tool call result |
| `plan` | Agent planning step |
| `obs` | Observation or retrieved fact |
| `err` | Error state |
| `mem` | Persistent memory or fact |
| `rag` | RAG retrieval citation |
| `hyp` | Hypothesis under evaluation |
| `cot` | Chain-of-thought reasoning step |

---

## Quick Example

```python
from ulmen import encode_agent_payload, decode_agent_payload, validate_agent_payload

records = [
    {
        "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
        "role": "user", "turn": 1,
        "content": "What is the capital of France?",
        "tokens": 7, "flagged": False,
    },
    {
        "type": "tool", "id": "tc1", "thread_id": "t1", "step": 2,
        "name": "web_search", "args": '{"query":"capital of France"}',
        "status": "pending",
    },
    {
        "type": "res", "id": "tc1", "thread_id": "t1", "step": 3,
        "name": "web_search", "data": "Paris", "status": "done",
        "latency_ms": 142,
    },
    {
        "type": "msg", "id": "m2", "thread_id": "t1", "step": 4,
        "role": "assistant", "turn": 2,
        "content": "The capital of France is Paris.",
        "tokens": 7, "flagged": False,
    },
]

payload = encode_agent_payload(records)
decoded = decode_agent_payload(payload)
ok, err = validate_agent_payload(payload)
```

Output:
```text
ULMEN-AGENT v1
records: 4
msg|m1|t1|1|user|1|What is the capital of France?|7|F
tool|tc1|t1|2|web_search|{"query":"capital of France"}|pending
res|tc1|t1|3|web_search|Paris|done|142
msg|m2|t1|4|assistant|2|The capital of France is Paris.|7|F
```

Validate:
```Python
decoded = decode_agent_payload(payload)
print(decoded[0]["role"])   # "user"
print(decoded[2]["status"]) # "done"
```

---

## Extemded Header Fields
Payloads can carry optional metadata in the header:

```python
from ulmen import encode_agent_payload, decode_agent_payload_full

payload = encode_agent_payload(
    records,
    thread_id="t1",
    context_window=8000,
    auto_context=True,
    payload_id="uuid-abc",
    parent_payload_id="uuid-prev",
    agent_id="agent-alpha",
    session_id="sess-001",
    schema_version="1.0.0",
)

decoded_records, header = decode_agent_payload_full(payload)
print(header.thread_id)
print(header.context_window)
print(header.context_used)
print(header.payload_id)
print(header.parent_payload_id)
print(header.agent_id)
print(header.session_id)
print(header.schema_version)
```

Wire output:
```text
ULMEN-AGENT v1
thread: t1
context_window: 8000
context_used: 42
payload_id: uuid-abc
parent_payload_id: uuid-prev
agent_id: agent-alpha
session_id: sess-001
schema_version: 1.0.0
records: 4
...
```

---

## Meta Fields
Meta fields are declared in the header and appended to every data row.
Valid names: 'parent_id', 'from_agent', 'to_agent', 'priority'.

```python
rec = {
    "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
    "role": "user", "turn": 1, "content": "hi", "tokens": 1,
    "flagged": False,
    "from_agent": "planner", "to_agent": "executor",
    "priority": 1, "parent_id": None,
}

payload = encode_agent_payload(
    [rec],
    meta_fields=("parent_id", "from_agent", "to_agent", "priority"),
)
```

---

## Structured Validation Errors
```python
ok, err = validate_agent_payload(payload, structured=True)
if not ok:
    print(err.message)
    print(err.row)
    print(err.field)
    print(err.expected)
    print(err.got)
    print(err.suggestion)
```
`ValidationError` is always falsy: `bool(err)` is `False`.

---

## Context Budget Enforcement

```python
from ulmen import ContextBudgetExceededError

try:
    payload = encode_agent_payload(
        records,
        context_window=100,
        auto_context=True,
        enforce_budget=True,
    )
except ContextBudgetExceededError as e:
    print(e.context_window)
    print(e.context_used)
    print(e.overage)
```

---

## Context Compression
When a conversation grows long, compress it before encoding:

```python
from ulmen import compress_context
from ulmen.core._agent import (
    COMPRESS_COMPLETED_SEQUENCES,
    COMPRESS_KEEP_TYPES,
    COMPRESS_SLIDING_WINDOW,
    PRIORITY_MUST_KEEP,
    PRIORITY_KEEP_IF_ROOM,
    PRIORITY_COMPRESSIBLE,
)

# Replace completed tool+res pairs with mem summaries
compressed = compress_context(
    records,
    strategy=COMPRESS_COMPLETED_SEQUENCES,
    keep_priority=PRIORITY_KEEP_IF_ROOM,
    preserve_cot=True,
)

# Keep only specified types
compressed = compress_context(
    records,
    strategy=COMPRESS_KEEP_TYPES,
    keep_types=["msg", "mem", "err"],
)

# Keep recent records, summarize older ones
compressed = compress_context(
    records,
    strategy=COMPRESS_SLIDING_WINDOW,
    window_size=20,
)
```

Records with priority = 'PRIORITY_MUST_KEEP' are never removed by any strategy

---

## Memory Deduplication

```python
from ulmen import dedup_mem, get_latest_mem

# Keep only the most recent mem record per (thread_id, key)
clean = dedup_mem(records)

# Get the most recent mem record for a specific key
latest = get_latest_mem(records, key="user_preference")
if latest:
    print(latest["value"])
```

---

# Context Usage Estimation

```python
from ulmen import chunk_payload, merge_chunks, build_summary_chain

# Split into chunks, each fitting within token_budget
chunks = chunk_payload(
    records,
    token_budget=4000,
    thread_id="t1",
    overlap=2,
    parent_payload_id="prev-chain-id",
    session_id="sess-001",
)

# Every chunk is independently valid
for chunk in chunks:
    ok, err = validate_agent_payload(chunk)
    assert ok

# Merge chunks back into a flat record list (deduplicates by id+thread+step)
merged = merge_chunks(chunks)

# Summary chain: older records compressed to mem, recent kept verbatim
chain = build_summary_chain(
    records,
    token_budget=4000,
    thread_id="t1",
    session_id="sess-001",
)
# Feed chain[-1] to the LLM; it references prior summaries via parent_payload_id
```

---

## LLM Output Repair
When an LLM produces a malformed payload:

```python
from ulmen import parse_llm_output

# Non-strict: returns error payload if repair fails
repaired = parse_llm_output(raw_llm_text)

# Strict: raises ValueError if repair fails
repaired = parse_llm_output(raw_llm_text, strict=True)

# Override thread_id on all records
repaired = parse_llm_output(raw_llm_text, thread_id="t1")
```
Repairs applied automatically in order:

1. Strip markdown fences
2. Locate the magic line, discard everything before it
3. Fix wrong records: N count
4. Remove blank lines
5. Skip lines with unknown record types
6. Re-encode surviving decodable rows if first-pass validation fails

---

## Exact Token Counting

```python
from ulmen import count_tokens_exact, count_tokens_exact_records

n = count_tokens_exact(payload)
n = count_tokens_exact_records(records)
```
Uses cl100k_base BPE (GPT-4 / Claude compatible).
Falls back to character estimate when tiktoken is unavailable.

---

## Streaming Decode

```python
from ulmen import decode_agent_stream

with open("payload.txt") as f:
    for rec in decode_agent_stream(f):
        process(rec)
```
Header is buffered until 'records: N' is found. Data rows are decoded
and yielded one at a time. Blank lines are skipped. Unknown header lines
are ignored.

---

## Subgraph Extraction
Filter a payload by thread, step range, or record type:

```python
from ulmen import extract_subgraph, extract_subgraph_payload

# From decoded records
filtered = extract_subgraph(
    records,
    thread_id="t1",
    step_min=2,
    step_max=5,
    types=["tool", "res", "err"],
)

# From raw payload string, returns valid payload string
filtered_payload = extract_subgraph_payload(
    payload,
    thread_id="t1",
    types=["cot"],
)
```

---

## Multi-Agent Routing

```python
from ulmen import AgentRouter, validate_routing_consistency

router = AgentRouter()
router.register("planner", "executor", handle_task)
router.register("executor", "planner", handle_result)

router.dispatch(records)
router.dispatch_one(record)

ok, err = validate_routing_consistency(records)
```

---

## Cross-Payloadd Thread Tracking

```python
from ulmen import ThreadRegistry, merge_threads

registry = ThreadRegistry()
registry.add_payload("pid-1", records_from_payload_1)
registry.add_payload("pid-2", records_from_payload_2)
threads = registry.get_threads()

# Merge thread records from multiple payloads
merged = merge_threads([records_1, records_2, records_3])
```

---

## Audit Trail

```python
from ulmen import ReplayLog

log = ReplayLog()
log.append({"event": "encode", "payload_id": "pid-1", "ts": 1234567890})
log.append({"event": "validate", "ok": True})

for event in log.all():
    print(event)
```

---

## System Prompt Generation
```python
from ulmen import generate_system_prompt

prompt = generate_system_prompt(
    include_examples=True,
    include_validation=True,
)
```
The prompt is generated from the live schema and always reflects the
current record types, field names, enum values, and encoding rules.

---

## ULMEN Bridge

```python
from ulmen import convert_agent_to_ulmen, convert_ulmen_to_agent

# ULMEN-AGENT to ULMEN
ulmen = convert_agent_to_ulmen(agent_payload)

# ULMEN to ULMEN-AGENT
payload = convert_ulmen_to_agent(ulmen, thread_id="t1")
```

---

## MessagePack Compatibility

```python
from ulmen.core._msgpack_compat import encode_msgpack, decode_msgpack

packed   = encode_msgpack(records)
unpacked = decode_msgpack(packed)
```
---

## Next Steps
- [Specification](internals/wire-format.md): complete wire format and validation rules
- [System Prompt](system-prompt.md): ready-to-use LLM system prompt