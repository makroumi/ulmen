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
MAGIC (4 bytes) 0x4C 0x55 0x4D 0x42 "LUMB"
VERSION (2 bytes) 0x03 0x03 major=3, minor=3
[POOL] (optional) T_POOL_DEF block
[PAYLOAD] T_MATRIX | T_LIST

text


### 3.2 Type Tags
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

text


### 3.3 Varint Encoding

7 bits of data per byte. High bit = continuation.
0x00–0x7F → 1 byte
0x80–0x3FFF → 2 bytes
...

text


Example: `300` = `0xAC 0x02`

### 3.4 Zigzag Encoding

Maps signed integers to unsigned for efficient varint encoding:
0 → 0
-1 → 1
1 → 2
-2 → 3
n → (n << 1) ^ (n >> 63)

text


### 3.5 String Encoding

Strings with 0–3 UTF-8 bytes use `T_STR_TINY` (saves 1 varint byte):
T_STR_TINY len_byte utf8_bytes

text


Strings with 4+ UTF-8 bytes use `T_STR`:
T_STR varint(len) utf8_bytes

text


### 3.6 String Pool

When repeated strings are detected, a pool block precedes the payload:
T_POOL_DEF varint(n) string × n

text


Pool references replace inline strings:
T_POOL_REF varint(index)

text


A string enters the pool when:
frequency × (len(string) − ref_cost) > 0

text

where `ref_cost = 2` for pools ≤ 9 entries, `4` otherwise.

Pool size is capped at 64 (LumenDict) or up to 256 (LumenDictFull).

### 3.7 T_MATRIX - Columnar Record Set

Used for 2+ homogeneous dict records. Stores data column-by-column
for maximum compression opportunity.
T_MATRIX
varint(n_rows)
varint(n_cols)
[per column: pack_string(name) + strategy_byte] × n_cols
[column_body] × n_cols

text


#### Column Strategy Bytes
S_RAW = 0x00 T_LIST of individually encoded values
S_DELTA = 0x01 T_DELTA_RAW - delta-encoded integers
S_RLE = 0x02 T_RLE - run-length encoded
S_BITS = 0x03 T_BITS - bitpacked booleans
S_POOL = 0x04 T_LIST with pool references inline

text


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
T_DELTA_RAW varint(n) zigzag(base) zigzag(delta) × (n−1)

text


Reconstruction: `v[0] = base`, `v[i] = v[i−1] + delta[i]`

#### T_BITS Body
T_BITS varint(n) ceil(n/8) bytes

text


Bit `i` is at `byte[i >> 3]`, bit position `i & 7` (LSB-first).

#### T_RLE Body
T_RLE varint(n_runs) (encoded_value + varint(count)) × n_runs

text


### 3.8 Single Record

A single dict record is stored as `T_LIST(1)` wrapping `T_MAP`:
T_LIST varint(1)
T_MAP varint(n_keys)
[pack_string(key) + encoded_value] × n_keys

text


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
POOL:s1,s2,... optional pool line
records[N]:col:type,... matrix header
@col=v1;v2;... inline column (RLE or POOL strategy)
v1 TAB v2 TAB ... data row for non-inline columns

text


Type chars: `b` bool, `d` int, `f` float, `s` string, `n` null

### 4.3 Single-Record Layout
POOL:s1,s2,... optional
SCHEMA:col:type,... header
v1 TAB v2 TAB ... single data row

text


### 4.4 Non-Dict Records

One encoded value per line. Nested dicts use `{k:v,k:v}`, nested
lists use `[v,v,v]`.

---

## 5. LUMIA - LLM-Native Format

LUMIA is a header-prefixed CSV designed for zero-hallucination
generation and parsing by language models.

### 5.1 Layout
L|col:type,... header line
v1,v2,... data row (one per record)

text


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

### 6.1 Layout
LUMEN-AGENT v1
records: N
type|id|thread_id|step|field...|field...

text


### 6.2 Record Types

| Type  | Fields (beyond id/thread_id/step)                       |
|-------|---------------------------------------------------------|
| `msg` | role, turn, content, tokens, flagged                    |
| `tool`| name, args, status                                      |
| `res` | name, data, status, latency_ms                          |
| `plan`| index, description, status                              |
| `obs` | source, content, confidence                             |
| `err` | code, message, source, recoverable                      |
| `mem` | key, value, confidence, ttl                             |
| `rag` | rank, score, source, chunk, used                        |
| `hyp` | statement, evidence, score, accepted                    |
| `cot` | index, cot_type, text, confidence                       |

### 6.3 Field Encoding

| Value    | Token  |
|----------|--------|
| `None`   | `N`    |
| `True`   | `T`    |
| `False`  | `F`    |
| `""`     | `$0=`  |
| string with `|` `"` `\` `\n` `\r` | RFC 4180 quoted |

Delimiter: `|` (pipe)
Quoting: double-quote wrapping with `""` for internal quotes,
`\n` and `\r` escaped as literal backslash sequences.

### 6.4 Validation Rules

- `thread_id` must be non-empty
- `id` must be non-empty
- `step` must be a positive integer
- Steps must be non-decreasing within a thread
- Every `res` record must have a matching `tool` record by `id`
- Enum fields validated against allowed value sets

---

## 7. Size Guarantees

On a 1,000-record realistic dataset (10 mixed-type columns):

| Format        | Relative size |
|---------------|---------------|
| JSON          | 1.00× (baseline) |
| CSV           | ~0.85×        |
| TOML          | ~1.20×        |
| Pickle (p4)   | ~0.90×        |
| LUMEN text    | ~0.55×        |
| LUMEN binary  | ~0.30×        |
| LUMEN zlib    | ~0.12×        |
| LUMIA         | ~0.45× tokens |

---

## 8. Implementation Notes

- The Python reference implementation in `lumen/core/` is the
  normative specification. The Rust layer in `src/lib.rs` is
  byte-identical for all outputs.
- Pool ordering: strings are ranked by `frequency × (len − ref_cost)`,
  ties broken by string table insertion order.
- Boolean values must be checked before integer values in type
  dispatch because `bool` is a subclass of `int` in Python.
- Float specials (`nan`, `inf`, `-inf`) round-trip through all
  surfaces. JSON output replaces them with `null`.
EOF