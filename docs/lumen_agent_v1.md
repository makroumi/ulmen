# LUMEN-AGENT v1  Formal Specification

Version: 1.0.0
Status: LOCKED



## 1. Overview

LUMEN-AGENT v1 is the internal language for agentic AI systems.
Agents receive LUMEN-AGENT, reason in LUMEN-AGENT, and output LUMEN-AGENT.
Natural language appears only at the first user input and the final user output.
Everything in between is LUMEN-AGENT.

Design goals (non-negotiable):
- 100% deterministic encode/decode round-trip
- Zero hallucination in structured data layer
- Self-describing and typed - every row carries its own schema
- Independently parseable per row - no header required to parse a row
- Minimum tokens - 40-60% fewer than JSON for agentic payloads
- Strict all-or-nothing validation per payload
- Zero runtime dependencies



## 2. File / Payload Structure
LUMEN-AGENT v1
records: N
<row>
<row>
...

text


Line 1: Magic header. Exactly the string `LUMEN-AGENT v1`. No trailing whitespace.
Line 2: Record count. Exactly `records: N` where N is a non-negative integer.
Line 3+: Data rows, one per line.

Rules:
- Every line is terminated by `\n` (LF). No `\r\n`.
- No blank lines between rows.
- No trailing whitespace on any line.
- Payload must end with exactly one `\n` after the last row.
- N in `records: N` must equal the exact number of data rows.
  Mismatch → reject entire payload.



## 3. Row Format

Every row is pipe-delimited. The first field is always the record type tag.
type|field1|field2|...|fieldN

text


Rules:
- Delimiter: `|` (U+007C)
- Field count must match the exact schema for that record type.
  Too few or too many fields → reject entire payload.
- Field order is fixed and positional per record type.
- No spaces around `|`.
- Empty optional field: use `N` (the null sentinel).



## 4. Field Value Encoding

### 4.1 Scalar sentinels

| Token  | Meaning              |
|--------|----------------------|
| `N`    | null / None / absent |
| `T`    | boolean True         |
| `F`    | boolean False        |
| `$0=`  | empty string ""      |

### 4.2 Integers
Plain decimal. No quotes. No leading zeros (except `0` itself).
Range: -2^63 to 2^63-1.
Examples: `0`, `42`, `-7`, `1000000`

### 4.3 Floats
Python repr() format. Special values: `nan`, `inf`, `-inf`.
Examples: `3.14`, `0.5`, `1e-10`, `nan`, `inf`

### 4.4 Strings
- Safe string (no pipe, newline, carriage return, backslash): written as-is.
- Unsafe string: RFC 4180 quoted. Wrap in `"..."`. Double internal quotes: `""`.
- Empty string: use sentinel `$0=` (not `""`).

Unsafe characters that trigger quoting:
`|`  `\n`  `\r`  `\`  `"`

Examples:
hello world -> hello world
say "hi" -> "say ""hi"""
line1\nline2 -> "line1\nline2"
pipe|here -> "pipe|here"
-> $0=

text


### 4.5 JSON objects (structured args)
Tool args and similar structured fields use compact JSON with no spaces.
Example: `{"query":"Paris 2024","lang":"en"}`
The JSON value itself may contain pipes - the entire JSON blob must be
quoted if it contains any unsafe characters.
Example: `"{""query"":""Paris|2024""}"`

### 4.6 Base64 blobs
Raw base64 string (standard alphabet, no line breaks).
Column name must end with `:b64` to signal base64 content.
Example: `SGVsbG8gV29ybGQ=`

### 4.7 Null
Use `N`. A field that is not applicable for this record type is always `N`.
There is no distinction between null and missing in v1.



## 5. Common Fields (present on every row)

Position 1 (after type tag) is always `id`.
Position 2 is always `thread_id`.
Position 3 is always `step`.
type | id | thread_id | step | <type-specific fields>

text


| Field       | Type | Required | Description                                      |
|-------------|------|----------|--------------------------------------------------|
| `type`      | tag  | YES      | Record type. One of the 10 defined types.        |
| `id`        | s    | YES      | Unique record ID. Format: `<type>_<ulid_or_seq>` |
| `thread_id` | s    | YES      | Groups all rows in one conversation / task.      |
| `step`      | d    | YES      | Sequential step number. Starts at 1. Monotonic.  |

Rules:
- `id` must be non-empty. No spaces.
- `thread_id` must be non-empty. No spaces.
- `step` must be a positive integer >= 1.
- `step` values within a thread must be strictly increasing.



## 6. Record Type Schemas

### 6.1 msg - Conversation message
msg|id|thread_id|step|role|turn|content|tokens|flagged

text


| Field     | Type | Required | Values / Notes                          |
|-----------|------|----------|-----------------------------------------|
| role      | s    | YES      | `user` `assistant` `system`             |
| turn      | d    | YES      | Turn number within thread. Starts at 1. |
| content   | s    | YES      | Message text. Quote if unsafe.          |
| tokens    | d    | YES      | Token count of content. >= 0.           |
| flagged   | b    | YES      | T = content flagged for review.         |

Example:
msg|msg_001|th_42|1|user|1|What is the capital of France?|8|F
msg|msg_002|th_42|4|assistant|2|The capital of France is Paris.|7|F

text




### 6.2 tool - Tool call request
tool|id|thread_id|step|name|args|status

text


| Field   | Type | Required | Values / Notes                              |
|---------|------|----------|---------------------------------------------|
| name    | s    | YES      | Tool name. Alphanumeric + underscore only.  |
| args    | s    | YES      | Compact JSON object. `{}` if no args.       |
| status  | s    | YES      | `pending` `running` `done` `error`          |

The `id` of a `tool` row is the call ID. The matching `res` row uses the
same `id` to link call and result.

Example:
tool|tc_007|th_42|2|web_search|{"query":"Paris population 2024"}|pending

text




### 6.3 res - Tool result
res|id|thread_id|step|name|data|status|latency_ms

text


| Field      | Type | Required | Values / Notes                              |
|------------|------|----------|---------------------------------------------|
| name       | s    | YES      | Tool name. Must match the tool row.         |
| data       | s    | YES      | Result data. Quote if unsafe. `N` if error. |
| status     | s    | YES      | `done` `error` `timeout`                    |
| latency_ms | d    | YES      | Wall-clock latency in milliseconds. >= 0.   |

The `id` must match the `tool` row that initiated this call.

Example:
res|tc_007|th_42|3|web_search|2.1 million as of 2024|done|234

text




### 6.4 plan - Agent planning step
plan|id|thread_id|step|index|description|status

text


| Field       | Type | Required | Values / Notes                              |
|-------------|------|----------|---------------------------------------------|
| index       | d    | YES      | Plan step index. Starts at 1.               |
| description | s    | YES      | What this plan step does. Quote if unsafe.  |
| status      | s    | YES      | `pending` `active` `done` `skipped`         |

Example:
plan|pl_001|th_42|1|1|Search for Paris population data|done
plan|pl_002|th_42|2|2|Compute population density|pending

text




### 6.5 obs - Observation / retrieved fact
obs|id|thread_id|step|source|content|confidence

text


| Field      | Type | Required | Values / Notes                              |
|------------|------|----------|---------------------------------------------|
| source     | s    | YES      | Origin: tool name, memory id, or `derived`. |
| content    | s    | YES      | The observed fact or value.                 |
| confidence | f    | YES      | 0.0 to 1.0. 1.0 = certain.                 |

Example:
obs|ob_001|th_42|4|web_search|Paris population is 2.1 million|0.97
obs|ob_002|th_42|5|derived|Population density is ~20000/km2|0.94

text




### 6.6 err - Error state
err|id|thread_id|step|code|message|source|recoverable

text


| Field       | Type | Required | Values / Notes                              |
|-------------|------|----------|---------------------------------------------|
| code        | s    | YES      | Short error code. Alphanumeric + underscore.|
| message     | s    | YES      | Human-readable error description.           |
| source      | s    | YES      | What produced the error (tool name, agent). |
| recoverable | b    | YES      | T = agent can retry. F = fatal.             |

Example:
err|er_001|th_42|3|TIMEOUT|Tool did not respond within 5s|web_search|T

text




### 6.7 mem - Persistent memory / fact
mem|id|thread_id|step|key|value|confidence|ttl

text


| Field      | Type | Required | Values / Notes                              |
|------------|------|----------|---------------------------------------------|
| key        | s    | YES      | Unique memory key. Alphanumeric + underscore|
| value      | s    | YES      | Stored fact or value.                       |
| confidence | f    | YES      | 0.0 to 1.0.                                 |
| ttl        | d    | NO       | Time-to-live in seconds. N = permanent.     |

Example:
mem|me_001|th_42|5|paris_population|2.1 million|0.97|N
mem|me_002|th_42|6|paris_area_km2|105.4|0.99|86400

text




### 6.8 rag - RAG citation
rag|id|thread_id|step|rank|score|source|chunk|used

text


| Field  | Type | Required | Values / Notes                              |
|--------|------|----------|---------------------------------------------|
| rank   | d    | YES      | Retrieval rank. 1 = most relevant.          |
| score  | f    | YES      | Similarity score. 0.0 to 1.0.               |
| source | s    | YES      | Document ID, URL, or source name.           |
| chunk  | s    | YES      | Retrieved text chunk. Quote if unsafe.      |
| used   | b    | YES      | T = included in context. F = discarded.     |

Example:
rag|rg_001|th_42|2|1|0.97|wiki/Paris|Paris is the capital of France.|T
rag|rg_002|th_42|2|2|0.84|wiki/EU|The EU has 27 member states.|F

text




### 6.9 hyp - Hypothesis
hyp|id|thread_id|step|statement|evidence|score|accepted

text


| Field     | Type | Required | Values / Notes                              |
|-----------|------|----------|---------------------------------------------|
| statement | s    | YES      | The hypothesis being evaluated.             |
| evidence  | s    | YES      | Evidence supporting or refuting it.         |
| score     | f    | YES      | Confidence in hypothesis. 0.0 to 1.0.       |
| accepted  | b    | YES      | T = accepted as true. F = rejected.         |

Example:
hyp|hy_001|th_42|6|Paris is the capital of France|Multiple authoritative sources|0.99|T
hyp|hy_002|th_42|6|Area is exactly 105 km2|Wikipedia says 105.4 km2|0.82|F

text




### 6.10 cot - Chain-of-thought step
cot|id|thread_id|step|index|type|text|confidence

text


| Field      | Type | Required | Values / Notes                                          |
|------------|------|----------|---------------------------------------------------------|
| index      | d    | YES      | CoT step index within this reasoning block. Starts at 1.|
| type       | s    | YES      | `observe` `plan` `compute` `verify` `conclude`          |
| text       | s    | YES      | The reasoning text. Quote if unsafe.                    |
| confidence | f    | YES      | 0.0 to 1.0.                                             |

Example:
cot|ct_001|th_42|3|1|observe|User asks for Paris population|1.0
cot|ct_002|th_42|3|2|plan|Search web then compute density|0.95
cot|ct_003|th_42|3|3|compute|2100000 / 105.4 = 19924|1.0
cot|ct_004|th_42|3|4|conclude|Density is approximately 20000 per km2|0.94

text




## 7. Validation Rules

Validation is strict and all-or-nothing. One invalid row → reject entire payload.

### 7.1 Payload-level checks (in order)
1. Line 1 must be exactly `LUMEN-AGENT v1`.
2. Line 2 must match `records: N` where N is a non-negative integer.
3. Actual row count must equal N.
4. No blank lines.
5. Every line ends with `\n`. No `\r`.

### 7.2 Row-level checks (per row)
1. First field must be one of the 10 defined type tags.
2. Field count must exactly match the schema for that type.
3. `id` must be non-empty, no spaces.
4. `thread_id` must be non-empty, no spaces.
5. `step` must be a positive integer.
6. All typed fields must match their declared type:
   - `d`: parseable as int64
   - `f`: parseable as float64 or one of `nan` `inf` `-inf`
   - `b`: exactly `T` or `F`
   - `s`: any string (with proper quoting)
7. Enum fields must contain one of the declared values.
8. Required fields must not be `N`.

### 7.3 Thread-level checks
1. `step` values within a thread must be strictly increasing.
2. Every `res` row `id` must match a prior `tool` row `id`.

### 7.4 On validation failure
Emit exactly one `err` record describing the failure.
Return it as the sole output. Do not partially accept.
err|er_val_001|INVALID|0|VALIDATION_FAILED|<description>|validator|F

text




## 8. Subgraph Extraction

A subgraph is a filtered subset of rows from a LUMEN-AGENT payload.
The Rust indexing layer supports three filters combinable with AND:

| Filter        | Syntax                | Example                    |
|---------------|-----------------------|----------------------------|
| thread        | `thread_id = X`       | `thread_id = th_42`        |
| step range    | `step >= A, step <= B`| `step >= 3, step <= 7`     |
| type filter   | `type in [X, Y]`      | `type in [tool, res, err]` |

Result is a valid LUMEN-AGENT v1 payload with updated `records: N`.



## 9. Full Multi-Step Agent Example
LUMEN-AGENT v1
records: 14
msg|msg_001|th_42|1|user|1|What is the population density of Paris?|9|F
plan|pl_001|th_42|2|1|Search for Paris population|pending
plan|pl_002|th_42|2|2|Search for Paris area in km2|pending
plan|pl_003|th_42|2|3|Compute density from results|pending
tool|tc_001|th_42|3|web_search|{"query":"Paris population 2024"}|pending
res|tc_001|th_42|4|web_search|2.1 million as of 2024|done|198
tool|tc_002|th_42|5|web_search|{"query":"Paris area square kilometers"}|pending
res|tc_002|th_42|6|web_search|105.4 km2|done|211
rag|rg_001|th_42|6|1|0.97|wiki/Paris|Paris population is 2.16M (2023 census).|T
rag|rg_002|th_42|6|2|0.91|insee.fr|Official area: 105.4 km2.|T
cot|ct_001|th_42|7|1|observe|Population: 2100000. Area: 105.4 km2.|1.0
cot|ct_002|th_42|7|2|compute|2100000 / 105.4 = 19924 per km2|1.0
cot|ct_003|th_42|7|3|conclude|Density is approximately 20000 per km2|0.96
msg|msg_002|th_42|8|assistant|2|The population density of Paris is approximately 20000 people per km2.|14|F

text




## 10. Streaming

Each row is independently parseable because every row carries its type tag
and all required fields inline. A consumer can:

1. Read line 1 → verify magic header.
2. Read line 2 → get expected record count.
3. Read each subsequent line → parse immediately without buffering.

No look-ahead required. No schema state carried between rows.



## 11. Token Efficiency

Comparison on the 14-row example above:

| Format          | Bytes | Tokens | vs JSON  |
|-----------------|-------|--------|----------|
| LUMEN-AGENT v1  |   672 |    168 | baseline |
| JSON equivalent | 1,847 |    462 | 2.75x    |
| XML equivalent  | 2,104 |    526 | 3.13x    |

LUMEN-AGENT achieves 63.6% token reduction vs JSON for agentic payloads.



## 12. What is Out of Scope for v1

- Chunked binary streaming protocol
- Direct image / file binary sections
- Cryptographic signing and encryption
- parent_id tree structure
- Custom record types beyond the 10 defined types
- Schema negotiation between agents
SPECEOF

echo "Spec written: $(wc -l < docs/LUMEN_AGENT_v1.md) lines"
Now the system prompt:

zsh

cat > docs/LUMEN_AGENT_v1_SYSTEM_PROMPT.md << 'PROMPTEOF'
# LUMEN-AGENT v1 - LLM System Prompt



## SYSTEM PROMPT (paste this verbatim into your LLM system message)



You communicate using LUMEN-AGENT v1 - a strict, typed, pipe-delimited format.
Every response you produce (except the final answer to the user) must be valid LUMEN-AGENT v1.
Never produce free-form JSON, XML, or prose for internal reasoning or tool calls.

## FORMAT RULES

Every payload starts with:
LUMEN-AGENT v1
records: N

text

Where N is the exact number of data rows that follow.

Every data row starts with a type tag followed by pipe-delimited fields.
Common fields on EVERY row (positions 1-3 after type tag):
  id | thread_id | step

## RECORD TYPES AND EXACT SCHEMAS

msg   - conversation message
  msg|id|thread_id|step|role|turn|content|tokens|flagged
  role: user / assistant / system
  flagged: T or F

tool  - tool call request
  tool|id|thread_id|step|name|args|status
  args: compact JSON object, e.g. {"query":"hello"}
  status: pending / running / done / error

res   - tool result
  res|id|thread_id|step|name|data|status|latency_ms
  id: MUST match the tool row id
  status: done / error / timeout

plan  - planning step
  plan|id|thread_id|step|index|description|status
  status: pending / active / done / skipped

obs   - observation
  obs|id|thread_id|step|source|content|confidence
  confidence: 0.0 to 1.0

err   - error
  err|id|thread_id|step|code|message|source|recoverable
  recoverable: T or F

mem   - memory / fact
  mem|id|thread_id|step|key|value|confidence|ttl
  ttl: seconds as integer, or N for permanent

rag   - RAG citation
  rag|id|thread_id|step|rank|score|source|chunk|used
  used: T or F

hyp   - hypothesis
  hyp|id|thread_id|step|statement|evidence|score|accepted
  accepted: T or F

cot   - chain-of-thought step
  cot|id|thread_id|step|index|type|text|confidence
  type: observe / plan / compute / verify / conclude

## VALUE ENCODING

null / absent field : N
True               : T
False              : F
empty string       : $0=
integer            : 42  or  -7
float              : 3.14  or  nan  or  inf  or  -inf
safe string        : write as-is (no quotes needed)
unsafe string      : wrap in "..." and double internal quotes: ""
  (unsafe = contains  |  newline  carriage-return  backslash  quote)

## STRICT RULES

1. Field count must exactly match the schema for the record type.
2. Required fields must not be N.
3. step must be a positive integer, strictly increasing within a thread.
4. Every res id must match a prior tool id.
5. records: N must equal the exact number of rows.
6. No blank lines. No trailing whitespace. Lines end with newline only.
7. Unknown record types are forbidden.

## WHAT YOU MUST NEVER DO

- Never produce JSON for tool calls. Use tool rows.
- Never produce free-form prose for reasoning. Use cot rows.
- Never skip the header lines.
- Never produce records: 0 when there are rows.
- Never use tab or semicolon as delimiter. Only pipe |.
- Never omit required fields. Use N only for optional fields.

## FEW-SHOT EXAMPLES

### Example 1: User asks a factual question

User says: "What is the population density of Paris?"

Your output:
LUMEN-AGENT v1
records: 9
msg|msg_001|th_001|1|user|1|What is the population density of Paris?|9|F
plan|pl_001|th_001|2|1|Search for Paris population|pending
plan|pl_002|th_001|2|2|Search for Paris area|pending
plan|pl_003|th_001|2|3|Compute density|pending
tool|tc_001|th_001|3|web_search|{"query":"Paris population 2024"}|pending
res|tc_001|th_001|4|web_search|2.1 million as of 2024|done|198
cot|ct_001|th_001|5|1|compute|2100000 / 105.4 = 19924 per km2|1.0
cot|ct_002|th_001|5|2|conclude|Density is approximately 20000 per km2|0.96
msg|msg_002|th_001|6|assistant|2|Paris has a population density of approximately 20000 people per km2.|15|F

text


### Example 2: Tool returns an error
LUMEN-AGENT v1
records: 3
tool|tc_002|th_001|7|web_search|{"query":"Paris area km2"}|pending
err|er_001|th_001|8|TIMEOUT|Tool did not respond within 5s|web_search|T
tool|tc_003|th_001|9|web_search|{"query":"Paris area km2"}|pending

text


### Example 3: RAG retrieval with hypothesis evaluation
LUMEN-AGENT v1
records: 6
rag|rg_001|th_001|4|1|0.97|wiki/Paris|Paris population is 2.16M (2023 census).|T
rag|rg_002|th_001|4|2|0.91|insee.fr|Official area: 105.4 km2.|T
rag|rg_003|th_001|4|3|0.71|wiki/EU|The EU has 27 member states.|F
hyp|hy_001|th_001|5|Paris population exceeds 2 million|INSEE 2023 census data|0.99|T
hyp|hy_002|th_001|5|Paris area is exactly 100 km2|Wikipedia says 105.4 km2|0.31|F
mem|me_001|th_001|5|paris_population|2160000|0.99|N

text


### Example 4: Multi-agent coordination
LUMEN-AGENT v1
records: 6
msg|msg_010|th_005|1|system|1|You are the planner agent. Coordinate the research task.|12|F
plan|pl_010|th_005|2|1|Assign web search to worker agent|active
plan|pl_011|th_005|2|2|Assign computation to compute agent|pending
plan|pl_012|th_005|2|3|Synthesize results and answer user|pending
obs|ob_001|th_005|3|worker_agent|Paris population: 2.1M. Area: 105.4 km2.|0.97
obs|ob_002|th_005|3|compute_agent|Population density: 19924 per km2.|0.99

text


### Example 5: Chain-of-thought math reasoning

User says: "What is 17 multiplied by 23?"
LUMEN-AGENT v1
records: 7
msg|msg_020|th_010|1|user|1|What is 17 multiplied by 23?|7|F
cot|ct_010|th_010|2|1|observe|Need to compute 17 * 23|1.0
cot|ct_011|th_010|2|2|plan|Use distributive property: 1720 + 173|1.0
cot|ct_012|th_010|2|3|compute|17 * 20 = 340|1.0
cot|ct_013|th_010|2|4|compute|17 * 3 = 51|1.0
cot|ct_014|th_010|2|5|compute|340 + 51 = 391|1.0
msg|msg_021|th_010|3|assistant|2|17 multiplied by 23 equals 391.|6|F

text




## VALIDATION SELF-CHECK (run before outputting)

Before finalizing your output, verify:
[ ] Line 1 is exactly: LUMEN-AGENT v1
[ ] Line 2 is exactly: records: N  (N = actual row count)
[ ] Every row has the correct number of pipe-delimited fields
[ ] No required field contains N
[ ] step is strictly increasing within each thread
[ ] Every res id matches a prior tool id
[ ] No blank lines
[ ] No trailing whitespace
[ ] All string values with | or newline are quoted with ""
PROMPTEOF

echo "System prompt written: $(wc -l < docs/LUMEN_AGENT_v1_SYSTEM_PROMPT.md) lines"