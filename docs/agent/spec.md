# ULMEN-AGENT v1 Specification

Version: 1.0.0
Status: Stable

---

## 1. Payload Structure

```text
ULMEN-AGENT v1
records: N
<row>
<row>
...
```

Line 1: exactly the string `ULMEN-AGENT v1`. No trailing whitespace.
Line 2: exactly `records: N` where N is a non-negative integer.
Lines 3 to N+2: data rows, one per line.

Rules:
- Every line ends with `\n` (LF). No `\r\n`.
- No blank lines between rows.
- No trailing whitespace on any line.
- Payload ends with exactly one `\n` after the last row.
- N must equal the exact number of data rows. Mismatch rejects the payload.

---

## 2. Row Format
```text
type|id|thread_id|step|field|field|...
```


- Delimiter: `|` (U+007C)
- First field is always the record type tag
- Positions 2, 3, 4 are always `id`, `thread_id`, `step`
- Remaining fields are type-specific and positional
- Field count must match the schema for that type exactly
- No spaces around `|`

---

## 3. Field Value Encoding

### Sentinels

| Token | Meaning |
|---|---|
| `N` | null / None / absent |
| `T` | True |
| `F` | False |
| `$0=` | empty string "" |

### Integers

Plain decimal. No quotes. No leading zeros except `0` itself.
Range: -2^63 to 2^63-1.

Examples: `0` `42` `-7` `1000000`

### Floats

Python repr format. Special values: `nan` `inf` `-inf`.

Examples: `3.14` `0.5` `1e-10` `nan` `inf`

### Strings

Safe strings (no `|` `\n` `\r` `\` `"`) are written as-is.

Unsafe strings are RFC 4180 quoted: wrap in `"..."`, double internal quotes.

Empty string: use `$0=` not `""`.

Examples:
```text
hello world -> hello world
say "hi" -> "say ""hi"""
pipe|here -> "pipe|here"
line1 -> "line1\nline2"
(empty) -> $0=
```

### Null

Use `N`. No distinction between null and missing in v1.

---

## 4. Common Fields

Every row has these fields at positions 1-4 (after the type tag):

| Position | Field | Type | Required | Description |
|---|---|---|---|---|
| 1 | type | tag | YES | Record type |
| 2 | id | s | YES | Unique record ID |
| 3 | thread_id | s | YES | Groups records in one conversation or task |
| 4 | step | d | YES | Sequential step number, starts at 1 |

Rules:
- `id` must be non-empty, no spaces
- `thread_id` must be non-empty, no spaces
- `step` must be a positive integer >= 1
- `step` values within a thread must be non-decreasing

---

## 5. Record Type Schemas

### msg: Conversation message
```text
msg|id|thread_id|step|role|turn|content|tokens|flagged
```

| Field | Type | Required | Values |
|---|---|---|---|
| role | s | YES | `user` `assistant` `system` |
| turn | d | YES | Turn number, starts at 1 |
| content | s | YES | Message text |
| tokens | d | YES | Token count, >= 0 |
| flagged | b | YES | T = flagged for review |

### tool: Tool call request
```text
res|id|thread_id|step|name|data|status|latency_ms
```

| Field | Type | Required | Values |
|---|---|---|---|
| name | s | YES | Tool name, must match the tool row |
| data | s | YES | Result data, `N` if error |
| status | s | YES | `done` `error` `timeout` |
| latency_ms | d | YES | Wall-clock latency in milliseconds |

The `id` must match the tool row that initiated this call.

### plan: Planning step
```text
plan|id|thread_id|step|index|description|status
```

| Field | Type | Required | Values |
|---|---|---|---|
| index | d | YES | Plan step index, starts at 1 |
| description | s | YES | What this plan step does |
| status | s | YES | `pending` `active` `done` `skipped` |

### obs: Observation
```text
obs|id|thread_id|step|source|content|confidence
```

| Field | Type | Required | Values |
|---|---|---|---|
| source | s | YES | Origin: tool name, memory id, or `derived` |
| content | s | YES | The observed fact or value |
| confidence | f | YES | 0.0 to 1.0 |

### err: Error state
```text
err|id|thread_id|step|code|message|source|recoverable
```

| Field | Type | Required | Values |
|---|---|---|---|
| code | s | YES | Short error code |
| message | s | YES | Human-readable description |
| source | s | YES | What produced the error |
| recoverable | b | YES | T = agent can retry, F = fatal |

### mem: Persistent memory
```text
mem|id|thread_id|step|key|value|confidence|ttl
```

| Field | Type | Required | Values |
|---|---|---|---|
| key | s | YES | Unique memory key |
| value | s | YES | Stored fact or value |
| confidence | f | YES | 0.0 to 1.0 |
| ttl | d | NO | Time-to-live in seconds, N = permanent |

### rag: RAG citation
```text
rag|id|thread_id|step|rank|score|source|chunk|used
```

| Field | Type | Required | Values |
|---|---|---|---|
| rank | d | YES | Retrieval rank, 1 = most relevant |
| score | f | YES | Similarity score, 0.0 to 1.0 |
| source | s | YES | Document ID, URL, or source name |
| chunk | s | YES | Retrieved text chunk |
| used | b | YES | T = included in context |

### hyp: Hypothesis
```text
hyp|id|thread_id|step|statement|evidence|score|accepted
```

| Field | Type | Required | Values |
|---|---|---|---|
| statement | s | YES | The hypothesis being evaluated |
| evidence | s | YES | Evidence supporting or refuting it |
| score | f | YES | Confidence, 0.0 to 1.0 |
| accepted | b | YES | T = accepted, F = rejected |

### cot: Chain-of-thought step
```text
cot|id|thread_id|step|index|cot_type|text|confidence
```

| Field | Type | Required | Values |
|---|---|---|---|
| index | d | YES | CoT step index within this block, starts at 1 |
| cot_type | s | YES | `observe` `plan` `compute` `verify` `conclude` |
| text | s | YES | The reasoning text |
| confidence | f | YES | 0.0 to 1.0 |

---

## 6. Validation Rules

Validation is strict and all-or-nothing. One invalid row rejects the entire payload.

### Payload level

1. Line 1 must be exactly `ULMEN-AGENT v1`
2. Line 2 must match `records: N` where N is a non-negative integer
3. Actual row count must equal N
4. No blank lines
5. Lines end with `\n` only

### Row level

1. First field must be one of the 10 defined type tags
2. Field count must exactly match the schema for that type
3. `id` must be non-empty
4. `thread_id` must be non-empty
5. `step` must be a positive integer
6. Typed fields must match their declared type
7. Enum fields must contain one of the declared values
8. Required fields must not be `N`

### Thread level

1. `step` values within a thread must be non-decreasing
2. Every `res` row `id` must match a prior `tool` row `id`

### On validation failure

Emit one `err` record describing the failure:
```text
ULMEN-AGENT v1
records: 1
err|er_val_001|INVALID|1|VALIDATION_FAILED|<description>|validator|F
```

---

## 7. Subgraph Extraction

Filter a payload by thread, step range, or record type.
All filters combine with AND. Result is a valid ULMEN-AGENT v1 payload.

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

# From raw payload string
filtered_payload = extract_subgraph_payload(
    payload,
    thread_id="t1",
    types=["tool", "res"],
)
```

---

## 8. Full Examples

```text
ULMEN-AGENT v1
records: 10
msg|msg_001|th_42|1|user|1|What is the population density of Paris?|9|F
plan|pl_001|th_42|2|1|Search for Paris population|done
plan|pl_002|th_42|2|2|Search for Paris area|done
plan|pl_003|th_42|2|3|Compute density|done
tool|tc_001|th_42|3|web_search|{"query":"Paris population 2024"}|done
res|tc_001|th_42|4|web_search|2.1 million as of 2024|done|198
tool|tc_002|th_42|5|web_search|{"query":"Paris area square kilometers"}|done
res|tc_002|th_42|6|web_search|105.4 km2|done|211
cot|ct_001|th_42|7|1|compute|2100000 / 105.4 = 19924 per km2|1.0
msg|msg_002|th_42|8|assistant|2|The population density of Paris is approximately 20000 people per km2.|14|F
```

---

## 9. Out of Scope for v1
- Chunked binary streaming protocol
- Image or file binary sections
- Cryptographic signing or encryption
- Parent ID tree structure
- Custom record types beyond the 10 defined types
- Schema negotiation between agents

