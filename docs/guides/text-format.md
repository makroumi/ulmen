# Text Format Guide

The LUMEN text format is a line-oriented, human-readable serialization
format. It uses the same pool and strategy system as binary but outputs
plain UTF-8 text.

---

## When to Use Text

Use text when:
- Output needs to be readable and diffable by humans
- Debugging or inspecting serialized data
- Integrating with line-based tools (grep, awk, diff)

Use binary for production data transfer where size matters.
Use LUMIA when the consumer is a language model.

---

## Basic Usage

```python
from lumen import LumenDict, LumenDictRust, decode_text_records

records = [
    {"id": 1, "city": "London", "active": True},
    {"id": 2, "city": "Paris",  "active": False},
    {"id": 3, "city": "London", "active": True},
]

ld   = LumenDict(records)
text = ld.encode_text()
print(text)
```

Output:
```text
POOL:London
records[3]:id:d,city:s,active:b
@city=#0;#0;Paris
1	T
2	F
3	T
```

Decode back:
```Python
back = decode_text_records(text)
print(back[0])  # {"id": 1, "city": "London", "active": True}
```

---

## Format Structure
### Multi-Record (Matrix Mode)
For two or more dict records:

```text
POOL:s1,s2,...              optional, present when pool is non-empty
records[N]:col:type,...     matrix header with row count and column types
@col=v1;v2;...              inline column (RLE or POOL strategy columns)
v1 TAB v2 TAB ...           data row for non-inline columns
```

### Single Record
```text
POOL:s1,s2,...              optional
SCHEMA:col:type,...         schema header
v1 TAB v2 TAB ...           single data row
```

### Non-Dict Records
```text
encoded_value               one line per record
```

---

## Token Vocabulary

Token|Python value
N|None
T|True
F|False
$0=|"" (empty string)
nan|float("nan")
inf|float("inf")
-inf|float("-inf")
#N|pool reference, index N (single digit)
#{N}|pool reference, index N (multi digit)
integer literal|int
float literal|float
escaped string|str

---

## String Escaping
Strings in text format use backslash escaping:

Character|Escaped form
\|\\
tab|\t
newline|\n
carriage return|\r

---

## Column Type Characters
Char|Type
b|bool
d|int
f|float
s|string
n|null only

---

## Inline Columns
Columns with RLE or POOL strategy are written as inline column lines
instead of per-row values. This avoids repeating the column delimiter
on every row.

```text
@city=London;London;Paris;London
```
The remaining columns are written tab-delimited, one row per line.

---

## Low-Level Encoder
```Python
from lumen import build_pool, encode_text_records, decode_text_records

records  = [{"id": i, "city": "London"} for i in range(10)]
pool, pm = build_pool(records, max_pool=64)
text     = encode_text_records(records, pool, pm, matrix_mode=True)
back     = decode_text_records(text)
```
Set [matrix_mode](reference/api.md#lumen.encode_text_records) to force SCHEMA format even for multiple records.
