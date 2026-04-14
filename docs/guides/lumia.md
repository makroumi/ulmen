# ULMEN Guide

ULMEN is the LLM-native text surface of ULMEN. It is a typed, header-prefixed
CSV format designed so language models can read and generate it without
special training or prompt engineering.

---

## Design Goals

- Self-describing: every payload carries its schema in the header line
- Zero indirection: no pool references, no index counting, no pointers
- LLM-generatable: an LLM fills in rows by following the header
- Compact: fewer tokens than JSON for typical record datasets
- Round-trip exact: every Python value survives encode and decode unchanged

---

## Format
```text
L|col:type,col:type,... header line
v1,v2,... data row, one per record
```
The `L|` prefix is the magic identifier. It is always the first two
characters of a ULMEN payload.

---

## Example

```python
from ulmen import encode_ulmen_llm, decode_ulmen_llm

records = [
    {"id": 1, "name": "Alice", "city": "London", "score": 98.5, "active": True},
    {"id": 2, "name": "Bob",   "city": "London", "score": 91.0, "active": False},
    {"id": 3, "name": "Carol", "city": "Paris",  "score": 87.3, "active": True},
]

ulmen = encode_ulmen_llm(records)
print(ulmen)
```
Output:
```text
L|id:d,name:s,city:s,score:f,active:b
1,Alice,London,98.5,T
2,Bob,London,91.0,F
3,Carol,Paris,87.3,T
```
Decode:
```Python
back = decode_ulmen_llm(ulmen)
assert back == records
```

---

## Type Hints

| Char | Python type | Notes |
|------|-------------|-------|
| d | int | decimal integer |
| f | float | includes nan, inf, -inf |
| b | bool | T or F only |
| s | str | with quoting if needed |
| n | None | always null column |
| m | mixed | multiple types in column |

Type is inferred from the first non-null value in each column.
If a later row has a conflicting type the column is promoted to 'm'.

---

## Special Tokens

| Token | Value |
|-------|-------|
| N | None |
| T | True |
| F | False |
| $0= | "" (empty string) |
| nan | float("nan") |
| inf | float("inf") |
| -inf | float("-inf") |

---

## String Quoting

Strings containing any of ',' '"' '{' '}' '[' ']' '|' ':' '\n' '\r'
are wrapped in double quotes. Internal double quotes are doubled.

```Python
encode_ulmen_llm([{"note": 'say "hello"'}])
# L|note:s
# "say ""hello"""
```
Safe strings are written as-is with no quoting overhead.

---

## Nested Values
Dicts and lists are supported as cell values:
```text
dict  ->  {k:v,k:v}
list  ->  [v|v|v]
```
Lists use `|` as separator inside `[]` to avoid collision with the
comma row delimiter.

```Python
encode_ulmen_llm([{"tags": ["a", "b", "c"]}])
# L|tags:m
# [a|b|c]
```

---

## Empty Payloads

| Case | Output |
|------|--------|
| Empty list | `L|` |
| List of empty dicts | `L|` |

---

## Rust Acceleration
```Python
from ulmen import UlmenDictRust

ld    = UlmenDictRust(records)
ulmen = ld.encode_ulmen_llm()
```
The Rust encoder produces byte-identical output to the Python encoder.

---

## For Language Models
An LLM receiving ULMEN should:

1. Read the header line starting with L|
2. Parse column names and type hints from the header
3. Read each subsequent line as a comma-separated row
4. Decode each cell using the type hint for that column

An LLM generating ULMEN should:

1. Write L| followed by comma-separated name:type specs
2. Write one comma-separated row per record
3. Use the special tokens for null, bool, and empty string
4. Quote any string containing a comma or double quote

See [ULMEN-AGENT System Prompt](ULMEN-AGENT.md) for a
complete LLM system prompt that includes ULMEN instructions.