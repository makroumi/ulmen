# LUMEN V1 - Wire Format Specification
## Table of Contents

- [LUMEN V1 - Wire Format Specification](#lumen-v1---wire-format-specification)
- [1. Overview](#1-overview)
- [2. Type System](#2-type-system)
- [3. Binary Format](#3-binary-format)
  - [3.1 File Layout](#31-file-layout)
  - [3.2 Type Tags](#32-type-tags)
  - [3.3 Varint Encoding](#33-varint-encoding)
  - [3.4 Zigzag Encoding](#34-zigzag-encoding)
  - [3.5 String Encoding](#35-string-encoding)
  - [3.6 String Pool](#36-string-pool)
  - [3.7 T_MATRIX - Columnar Record Set](#37-t_matrix---columnar-record-set)
    - [Column Strategy Bytes](#column-strategy-bytes)
    - [Strategy Selection Rules](#strategy-selection-rules)
    - [T_DELTA_RAW Body](#t_delta_raw-body)
    - [T_BITS Body](#t_bits-body)
    - [T_RLE Body](#t_rle-body)
  - [3.8 Single Record](#38-single-record)
- [4. Text Format](#4-text-format)
  - [4.1 Token Vocabulary](#41-token-vocabulary)
  - [4.2 Multi-Record Layout (Matrix)](#42-multi-record-layout-matrix)
  - [4.3 Single-Record Layout](#43-single-record-layout)
  - [4.4 Non-Dict Records](#44-non-dict-records)
- [5. LUMIA - LLM-Native Format](#5-lumia---llm-native-format)
  - [5.1 Layout](#51-layout)
  - [5.2 Type Hints](#52-type-hints)
  - [5.3 Value Encoding](#53-value-encoding)
  - [5.4 Quoting](#54-quoting)
- [6. LUMEN-AGENT Protocol](#6-lumen-agent-protocol)
  - [6.1 Layout](#61-layout)
  - [6.2 Record Types](#62-record-types)
  - [6.3 Field Encoding](#63-field-encoding)
  - [6.4 Validation Rules](#64-validation-rules)
- [7. Size Guarantees](#7-size-guarantees)
- [8. Implementation Notes](#8-implementation-notes)


**Lightweight Universal Minimal Encoding Notation**
Copyright (c) El Mehdi Makroumi. All rights reserved.

Version: 1.0.0
Format version: 3.3 (MAGIC + VERSION bytes)

---

## 1. Overview

LUMEN defines three surfaces over a single data model:

| Surface       | Prefix     | Use case                              |
|---------------|------------|---------------------------------------|
| Binary        | `LUMB`     | Storage, IPC, network transport       |
| Text          | `records[` | Human-readable, diff-friendly         |
| LUMIA         | `L|`       | LLM-native, token-efficient CSV       |
| LUMEN-AGENT   | `LUMEN-AGENT v1` | Structured agentic protocol    |

All surfaces share the same type system and round-trip losslessly.

---

## 2. Type System

Every value maps to exactly one type tag:

| Type     | Python       | Encoding                          |
|----------|-------------|-----------------------------------|
| null     | `None`      | `T_NULL` (tag only, no payload)   |
| bool     | `bool`      | `T_BOOL` + `0x00` or `0x01`      |
| int      | `int`       | `T_INT` + zigzag-varint           |
| float    | `float`     | `T_FLOAT` + 8 bytes big-endian    |
| string   | `str`       | `T_STR_TINY` or `T_STR` + UTF-8  |
| list     | `list`      | `T_LIST` + n + n × value          |
| map      | `dict`      | `T_MAP` + n + n × (key + value)   |

`bool` is checked before `int` because `bool` is a subtype of `int` in Python.

---

## 3. Binary Format

### 3.1 File Layout
```text
MAGIC (4 bytes) 0x4C 0x55 0x4D 0x42 "LUMB"
VERSION (2 bytes) 0x03 0x03 major=3, minor=3
[POOL] (optional) T_POOL_DEF block
[PAYLOAD] T_MATRIX | T_LIST
```

### 3.2 Type Tags
```text
T_STR_TINY = 0x01 string, 0–3 UTF-8 bytes
T_STR = 0x02 string, 4+ UTF-8 bytes
T_INT = 0x03 signed integer
T_FLOAT = 0x04 IEEE-754 double
T_BOOL = 0x05 boolean
T_NULL = 0x06 null / None
T_LIST = 0x07 heterogeneous list
T_MAP = 0x08 key-value map
T_POOL_DEF = 0x09 string pool definition
T_POOL_REF = 0x0A pool index reference
T_MATRIX = 0x0B columnar record set
T_DELTA_RAW = 0x0C delta-encoded integer column
T_BITS = 0x0E bitpacked boolean column
T_RLE = 0x0F run-length encoded column
```

### 3.3 Varint Encoding
```text 
7 bits of data per byte. High bit = continuation.
0x00–0x7F → 1 byte
0x80–0x3FFF → 2 bytes
```

Example: `300` = `0xAC 0x02`

### 3.4 Zigzag Encoding

Maps signed integers to unsigned for efficient varint encoding:
```text
0 → 0
-1 → 1
1 → 2
-2 → 3
n → (n << 1) ^ (n >> 63)
```

### 3.5 String Encoding

Strings with 0–3 UTF-8 bytes use `T_STR_TINY` (saves 1 varint byte):
```text
T_STR_TINY len_byte utf8_bytes
```

Strings with 4+ UTF-8 bytes use `T_STR`:
```text
T_STR varint(len) utf8_bytes
```

### 3.6 String Pool

When repeated strings are detected, a pool block precedes the payload:
```text
T_POOL_DEF varint(n) string × n
```

Pool references replace inline strings:
```text
T_POOL_REF varint(index)
```


A string enters the pool when:
```text
frequency × (len(string) − ref_cost) > 0
```

where `ref_cost = 2` for pools ≤ 9 entries, `4` otherwise.

Pool size is capped at 64 (LumenDict) or up to 256 (LumenDictFull).

### 3.7 T_MATRIX - Columnar Record Set

Used for 2+ homogeneous dict records. Stores data column-by-column
for maximum compression opportunity.
```text
T_MATRIX
varint(n_rows)
varint(n_cols)
[per column: pack_string(name) + strategy_byte] × n_cols
[column_body] × n_cols
```

#### Column Strategy Bytes
```text
S_RAW = 0x00 T_LIST of individually encoded values
S_DELTA = 0x01 T_DELTA_RAW - delta-encoded integers
S_RLE = 0x02 T_RLE - run-length encoded
S_BITS = 0x03 T_BITS - bitpacked booleans
S_POOL = 0x04 T_LIST with pool references inline
```


#### Strategy Selection Rules

| Condition                              | Strategy |
|----------------------------------------|----------|
| All values null                        | RLE      |
| All values bool (non-null)             | BITS     |
| All values int, delta saves bytes      | DELTA    |
| All values int, delta does not save    | RAW      |
| All values str, unique ≤ max(8, n/10)  | POOL     |
| Run ratio < 0.6                        | RLE      |
| Default                                | RAW      |

#### T_DELTA_RAW Body
```text
T_DELTA_RAW varint(n) zigzag(base) zigzag(delta) × (n−1)
```


Reconstruction: `v[0] = base`, `v[i] = v[i−1] + delta[i]`

#### T_BITS Body
```text
T_BITS varint(n) ceil(n/8) bytes
```

Bit `i` is at `byte[i >> 3]`, bit position `i & 7` (LSB-first).

#### T_RLE Body
```text
T_RLE varint(n_runs) (encoded_value + varint(count)) × n_runs
```

### 3.8 Single Record

A single dict record is stored as `T_LIST(1)` wrapping `T_MAP`:
```text
T_LIST varint(1)
T_MAP varint(n_keys)
[pack_string(key) + encoded_value] × n_keys
```

---

## 4. Text Format

Line-oriented, UTF-8, newline-delimited.

### 4.1 Token Vocabulary

| Token    | Python value          |
|----------|-----------------------|
| `N`      | `None`                |
| `T`      | `True`                |
| `F`      | `False`               |
| `$0=`    | `""` (empty string)   |
| `nan`    | `float('nan')`        |
| `inf`    | `float('inf')`        |
| `-inf`   | `float('-inf')`       |
| `#N`     | pool ref, index N ≤ 9 |
| `#{N}`   | pool ref, index N > 9 |
| integer  | `int`                 |
| float    | `float`               |
| other    | escaped string        |

String escapes: `\\` `\t` `\n` `\r`

### 4.2 Multi-Record Layout (Matrix)
```text
POOL:s1,s2,... optional pool line
records[N]:col:type,... matrix header
@col=v1;v2;... inline column (RLE or POOL strategy)
v1 TAB v2 TAB ... data row for non-inline columns
```

Type chars: `b` bool, `d` int, `f` float, `s` string, `n` null

### 4.3 Single-Record Layout
```text
POOL:s1,s2,... optional
SCHEMA:col:type,... header
v1 TAB v2 TAB ... single data row
```


### 4.4 Non-Dict Records

One encoded value per line. Nested dicts use `{k:v,k:v}`, nested
lists use `[v,v,v]`.

---

## 5. LUMIA - LLM-Native Format

LUMIA is a header-prefixed CSV designed for zero-hallucination
generation and parsing by language models.

### 5.1 Layout
```text
L|col:type,... header line
v1,v2,... data row (one per record)
```


Empty dataset: `L|`
All-empty-dict: `L|{}`

### 5.2 Type Hints

| Char | Type    |
|------|---------|
| `d`  | integer |
| `f`  | float   |
| `b`  | boolean |
| `s`  | string  |
| `n`  | null    |
| `m`  | mixed   |

Type is inferred from the first non-null value in each column.
Conflicts promote to `m`.

### 5.3 Value Encoding

| Value      | Token              |
|------------|--------------------|
| `None`     | `N`                |
| `True`     | `T`                |
| `False`    | `F`                |
| `""`       | `$0=`              |
| `float('nan')` | `nan`          |
| `float('inf')` | `inf`          |
| `float('-inf')` | `-inf`        |
| string with `,` `"` `{` `}` `[` `]` `\|` `:` | RFC 4180 quoted |
| other string | literal          |
| dict       | `{k:v,k:v}`        |
| list       | `[v\|v\|v]`        |

### 5.4 Quoting

Strings containing `,` `"` `\n` `\r` `{` `}` `[` `]` `|` `:` are
wrapped in double quotes. Internal double quotes are doubled (`""`).

---

## 6. LUMEN-AGENT Protocol

Structured protocol for agentic AI communication.

### 6.1 Payload Layout
```text
[thread: <thread_id>]
[context_window: <n>]
[context_used: <n>]
[payload_id: <id>]
[parent_payload_id: <id>]
[agent_id: <id>]
[session_id: <id>]
[schema_version: <version>]
[meta: field1,field2,...]
records: N
type|id|thread_id|step|field...|[meta_fields...]
```


Line 1 is exactly `LUMEN-AGENT v1`. Optional header lines follow in any
order. The `records: N` line must appear before any data rows. N must
equal the exact number of data rows that follow.

Unknown header lines are silently ignored for forward compatibility.
This allows future protocol versions to add new header fields without
breaking existing parsers.

### 6.2 Header Fields

| Field | Type | Description |
|---|---|---|
| `thread_id` | string | Groups all rows in one task or conversation |
| `context_window` | int | Token budget declared for this payload |
| `context_used` | int | Actual token count computed at encode time |
| `payload_id` | string | Unique identifier for this payload |
| `parent_payload_id` | string | Links to prior payload in a chain |
| `agent_id` | string | Identifier of the agent that produced this payload |
| `session_id` | string | Session this payload belongs to |
| `schema_version` | string | Protocol version for negotiation |
| `meta` | string | Comma-separated list of extra fields on every row |

All header fields are optional. A minimal valid payload has only
`records: N` as a header line.

### 6.3 Row Format

```text
type|id|thread_id|step|field1|field2|...|[meta_field1|meta_field2|...]
```

- `type` is one of the record types defined below.
- `id` is a unique identifier for this record within the payload.
- `thread_id` is the identifier of the thread this record belongs to.
- `step` is the step number of the record within the thread.
- `field1`, `field2`, ... are the fields of the record.
- `meta_field1`, `meta_field2`, ... are the meta fields of the record.

Delimiter: `|` (pipe). No spaces around `|`.
Field count must match the schema for the type exactly, plus the number
of declared meta fields.

### 6.4 Record Types

| Type | Fields beyond id/thread_id/step |
|---|---|
| `msg` | role, turn, content, tokens, flagged |
| `tool` | name, args, status |
| `res` | name, data, status, latency_ms |
| `plan` | index, description, status |
| `obs` | source, content, confidence |
| `err` | code, message, source, recoverable |
| `mem` | key, value, confidence, ttl |
| `rag` | rank, score, source, chunk, used |
| `hyp` | statement, evidence, score, accepted |
| `cot` | index, cot_type, text, confidence |

Total field count per row = 4 (type + id + thread_id + step) + type-specific fields + meta fields.

### 6.5 Enum Constraints

| Field | Allowed values |
|---|---|
| `msg.role` | `user` `assistant` `system` |
| `msg.flagged` | `T` `F` |
| `tool.status` | `pending` `running` `done` `error` |
| `res.status` | `done` `error` `timeout` |
| `plan.status` | `pending` `active` `done` `skipped` |
| `err.recoverable` | `T` `F` |
| `rag.used` | `T` `F` |
| `hyp.accepted` | `T` `F` |
| `cot.cot_type` | `observe` `plan` `compute` `verify` `conclude` |

### 6.6 Field Value Encoding

| Value | Token |
|---|---|
| `None` | `N` |
| `True` | `T` |
| `False` | `F` |
| `""` | `$0=` |
| string with `\|` `"` `\` `\n` `\r` | RFC 4180 quoted |
| integer | plain decimal |
| float | Python repr, `nan`, `inf`, `-inf` |
| safe string | literal |

Quoting: wrap in `"..."`, double internal quotes to `""`,
escape `\n` as `\n` and `\r` as `\r` literal sequences.

### 6.7 Meta Fields

Meta fields are declared in the header and appended to every data row:
```text
meta: parent_id,from_agent,to_agent,priority
```

Valid meta field names: `parent_id` `from_agent` `to_agent` `priority`

`priority` is decoded as integer. All others are decoded as string.

### 6.8 Priority Values

| Constant | Value | Meaning |
|---|---|---|
| `PRIORITY_MUST_KEEP` | `1` | Never removed by any compression strategy |
| `PRIORITY_KEEP_IF_ROOM` | `2` | Kept unless budget is exhausted |
| `PRIORITY_COMPRESSIBLE` | `3` | First candidate for compression |

Records without a priority field default to `PRIORITY_COMPRESSIBLE`.

### 6.9 Validation Rules

Validation is strict and all-or-nothing. One invalid row rejects the
entire payload.

Payload level:
1. Line 1 must be exactly `LUMEN-AGENT v1`
2. `records: N` must appear before data rows
3. Actual row count must equal N
4. No blank lines in the data section

Row level:
1. First field must be one of the 10 defined type tags
2. Field count must exactly match the schema plus declared meta fields
3. `id` must be non-empty
4. `thread_id` must be non-empty
5. `step` must be a positive integer >= 1
6. Required fields must not be `N`
7. Enum fields must contain one of the declared values

Thread level:
1. `step` values within a thread must be non-decreasing
2. Every `res` row `id` must match a prior `tool` row `id`

On validation failure with `structured=True`, a `ValidationError` object
is returned with fields: `message`, `row`, `field`, `expected`, `got`,
`suggestion`. `ValidationError` is always falsy.

### 6.10 Context Compression

Three strategies compress a record list to reduce token usage:

`completed_sequences`: replaces each completed tool+res pair with a
single `mem` record summarizing the result. `cot` records are converted
to `mem` when `preserve_cot=True`, or dropped when `preserve_cot=False`.
Records with `priority <= keep_priority` are never touched.

`keep_types`: retains only records whose type appears in `keep_types`.
Records with `priority <= keep_priority` are always retained regardless
of type.

`sliding_window`: keeps the most recent `window_size` records verbatim.
All earlier records are summarized as one `mem` record per thread.

### 6.11 Unlimited Context via Chunking

`chunk_payload` splits a large record list into multiple payloads each
fitting within `token_budget` tokens. Tool+res pairs are kept atomic:
they always land in the same chunk so every chunk independently passes
`validate_agent_payload`. Consecutive chunks are linked via
`parent_payload_id`. The `overlap` parameter repeats atomic units at
the start of the next chunk for context continuity.

`merge_chunks` decodes multiple payloads and merges records into a flat
list, deduplicating by `(id, thread_id, step)`.

`build_summary_chain` produces a chain of payloads where older records
are progressively compressed into `mem` summaries and the most recent
records are kept verbatim. Each payload in the chain is independently
valid. Feed the last payload to the LLM; it carries `parent_payload_id`
references to prior summaries.

### 6.12 LLM Output Repair

`parse_llm_output` accepts raw text from an LLM that may contain
formatting errors and returns a valid payload. Repair passes:

1. Strip leading/trailing whitespace and markdown code fences
2. Locate the `LUMEN-AGENT v1` magic line, discard everything before it
3. Separate header lines from data lines
4. Fix the `records: N` count to match actual data lines
5. Reassemble and validate; return if valid
6. Last-resort: decode each row individually, collect rows that parse,
   re-encode only the valid rows, validate the result
7. If no rows survive or re-encode fails, return a validation error payload

With `strict=True`, raises `ValueError` instead of returning an error
payload at steps 2, 7.

### 6.13 Token Counting

`count_tokens_exact` uses the cl100k_base BPE tokenizer compatible with
GPT-4 and Claude. It handles Unicode by splitting text into BPE-safe
chunks and counting each chunk independently.

`estimate_tokens` uses a character-based approximation (chars / 4).
Suitable for rough budgeting when tiktoken is not installed.

Both functions are used by `encode_agent_payload` when `auto_context=True`
to compute `context_used` written to the header.

`ContextBudgetExceededError` is raised by `encode_agent_payload` when
`enforce_budget=True` and `context_used > context_window`. It carries
`context_window`, `context_used`, and `overage` attributes.

### 6.14 Streaming Decode

`decode_agent_stream` accepts an `Iterator[str]` and yields decoded
record dicts one at a time without buffering the full payload. The header
is accumulated line by line until `records: N` is found. Unknown header
lines are silently ignored. Blank lines in the data section are skipped.
The iterator stops after exactly N records.

### 6.15 Subgraph Extraction

`extract_subgraph` filters a record list by `thread_id`, `step_min`,
`step_max`, and/or `types`. All filters combine with AND.

`extract_subgraph_payload` applies the same filters to a raw payload
string and returns a valid LUMEN-AGENT v1 payload.

### 6.16 Memory Operations

`get_latest_mem` returns the `mem` record with the given key that has
the highest `step` value. Returns `None` when no matching record exists.

`dedup_mem` retains only the highest-step `mem` record per
`(thread_id, key)` pair. All non-mem records are preserved unchanged.

### 6.17 Multi-Agent Routing

`AgentRouter` dispatches records by `(from_agent, to_agent)` pair.
Handlers are registered per pair. `dispatch` routes a list of records
to matching handlers. `dispatch_one` routes a single record.

`validate_routing_consistency` checks that `from_agent` and `to_agent`
fields are consistent and non-empty in every record that carries them.

### 6.18 Cross-Payload Thread Tracking

`ThreadRegistry` tracks records by thread_id across multiple payload
boundaries. `add_payload` registers all records from a payload.
`get_threads` returns a dict mapping thread_id to all records seen.

`merge_threads` takes a list of record lists (one per payload) and
returns a unified dict of thread_id to merged record list.

### 6.19 LUMIA Bridge

`convert_agent_to_lumia` decodes a LUMEN-AGENT payload and re-encodes
all records as LUMIA format for LLM consumption.

`convert_lumia_to_agent` decodes a LUMIA payload and re-encodes records
as LUMEN-AGENT format, assigning `thread_id`, `id`, and `step` to any
record that is missing them. Records with types not in `RECORD_TYPES`
are skipped.

### 6.20 System Prompt Generation

`generate_system_prompt` produces a complete LLM system prompt from the
live schema. It always reflects the current record types, field names,
enum values, and encoding rules. Parameters: `include_examples` (bool),
`include_validation` (bool).

---

## 7. Size Guarantees

On a 1,000-record realistic dataset (10 mixed-type columns):

| Format | Relative size |
|---|---|
| JSON | 1.00x (baseline) |
| CSV | ~0.85x |
| LUMEN text | ~0.55x |
| LUMEN binary | ~0.30x |
| LUMEN zlib | ~0.12x |
| LUMIA | ~0.45x tokens vs JSON |
| LUMEN-AGENT | ~0.38x tokens vs JSON |

---

## 8. Implementation Notes

The Python reference implementation in `lumen/core/` is the normative
specification. The Rust layer in `src/lib.rs` is byte-identical for all
outputs.

Pool ordering: strings are ranked by `frequency x (len - ref_cost)`,
ties broken by string table insertion order.

Boolean values must be checked before integer values in type dispatch
because `bool` is a subclass of `int` in Python.

Float specials (`nan`, `inf`, `-inf`) round-trip through all surfaces.
JSON output replaces them with `null`.

The overflow slice in `decode_agent_stream` (`raw_header_lines[1+consumed:]`)
is structurally always empty: `_parse_header` returns immediately on
finding `records:`, so the buffer has exactly `1 + consumed` entries at
that point. The implementation uses `continue` directly after header
construction with no overflow loop.

tiktoken is an optional dependency used only by `_tokens.py`. When it is
not installed, `count_tokens_exact` falls back to the character-based
`estimate_tokens` approximation and all other functionality is unaffected.
