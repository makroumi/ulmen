# LUMEN-AGENT Overview

LUMEN-AGENT v1 is a structured wire format for agentic AI systems.
It replaces free-form JSON and prose with a strict, typed, pipe-delimited
protocol that is both token-efficient and zero-hallucination by design.

---

## Problem It Solves

Language models communicating with tools and other agents typically use JSON.
JSON has three problems for agentic workloads:

1. Verbose. Schema keys repeat on every record. No column compression.
2. Untyped. The model must infer types from context or schema definitions.
3. Hallucination-prone. Free-form structure gives models too much latitude.

LUMEN-AGENT fixes all three: typed rows, fixed schemas per record type,
and strict validation that rejects any malformed payload atomically.

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
from lumen import encode_agent_payload, decode_agent_payload, validate_agent_payload

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
print(payload)
```

Output:
```text
LUMEN-AGENT v1
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

## Subgraph Extraction
Filter a payload by thread, step range, or record type:

```python
from lumen import extract_subgraph_payload

filtered = extract_subgraph_payload(
    payload,
    thread_id="t1",
    step_min=2,
    step_max=3,
    types=["tool", "res"],
)
```

---

## Next Steps
- [Specification](internals/wire-format.md): complete wire format and validation rules
- [System Prompt](system-prompt.md): ready-to-use LLM system prompt