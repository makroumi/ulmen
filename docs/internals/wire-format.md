# Wire Format

Complete specification of the LUMEN binary and text wire formats.

---

## Binary Format

### File Layout

```text
MAGIC 4 bytes 0x4C 0x55 0x4D 0x42 "LUMB"
VERSION 2 bytes 0x03 0x03 major=3, minor=3
[POOL] optional T_POOL_DEF block
[PAYLOAD] T_MATRIX or T_LIST
```


### Varint Encoding

7 data bits per byte. High bit set means more bytes follow.

```text
value 0 -> 0x00
value 127 -> 0x7F
value 128 -> 0x80 0x01
value 300 -> 0xAC 0x02
value 16383 -> 0xFF 0x7F
```
Algorithm:
```text
while n >= 0x80:
emit (n & 0x7F) | 0x80
n >>= 7
emit n
```


### Zigzag Encoding

Maps signed integers to unsigned for compact varint encoding of negatives.

```text
encode: zz = (n << 1) ^ (n >> 63)
decode: n = (zz >> 1) ^ -(zz & 1)

0 -> 0
-1 -> 1
1 -> 2
-2 -> 3
```


### String Encoding

Strings 0 to 3 UTF-8 bytes:

```text
T_STR_TINY (0x01) len_byte utf8_bytes
```

Strings 4+ UTF-8 bytes:

```text
T_STR (0x02) varint(len) utf8_bytes
```

The tiny path saves one varint byte for short strings and keys.

### Type Tags
```text
T_STR_TINY 0x01 string 0-3 bytes
T_STR 0x02 string 4+ bytes
T_INT 0x03 signed integer, zigzag-varint
T_FLOAT 0x04 IEEE-754 double, 8 bytes big-endian
T_BOOL 0x05 boolean, 0x00 or 0x01
T_NULL 0x06 null, no payload
T_LIST 0x07 varint(n) + n values
T_MAP 0x08 varint(n) + n (key, value) pairs
T_POOL_DEF 0x09 varint(n) + n strings
T_POOL_REF 0x0A varint(index)
T_MATRIX 0x0B columnar record set
T_DELTA_RAW 0x0C delta-encoded integer column
T_BITS 0x0E bitpacked boolean column
T_RLE 0x0F run-length encoded column
```


### String Pool Block
```text
T_POOL_DEF varint(n) string * n
```

Pool references:
```text
T_POOL_REF varint(index)
```


Pool scoring: a string enters the pool when `frequency * (len - ref_cost) > 0`.
`ref_cost` is 2 for pools with 9 or fewer entries, 4 otherwise.
Pool is sorted by score descending, ties broken by insertion order.

### T_MATRIX -- Columnar Record Set

Used for 2 or more homogeneous dict records.
```text
T_MATRIX (0x0B)
varint(n_rows)
varint(n_cols)
[pack_string(col_name) + strategy_byte] * n_cols
[column_body] * n_cols
```

### Strategy Bytes
```text
S_RAW 0x00 T_LIST of individually encoded values
S_DELTA 0x01 T_DELTA_RAW sequence
S_RLE 0x02 T_RLE sequence
S_BITS 0x03 T_BITS bitfield
S_POOL 0x04 T_LIST with pool references inline
```

### Strategy Selection Rules

| Condition | Strategy |
|---|---|
| All values null | RLE |
| All values bool, no null | BITS |
| All values int, delta saves bytes | DELTA |
| All values int, delta does not save | RAW |
| All values str, unique count <= max(8, n/10), n > 4 | POOL |
| Run ratio below 0.6 | RLE |
| Default | RAW |

### T_DELTA_RAW Body
```text
T_DELTA_RAW (0x0C)
varint(n)
zigzag(base_value)
zigzag(delta) * (n-1)
```

Reconstruction: `v[0] = base`, `v[i] = v[i-1] + delta[i]`

### T_BITS Body
```text
T_BITS (0x0E)
varint(n)
ceil(n/8) bytes
```

Bit `i` is at byte `i >> 3`, bit position `i & 7` (LSB-first within byte).

### T_RLE Body
```text
T_RLE (0x0F)
varint(n_runs)
[encoded_value + varint(count)] * n_runs
```

### Single Record Encoding

A single dict record uses T_LIST wrapping T_MAP:
```text
T_LIST varint(1)
T_MAP varint(n_keys)
[pack_string(key) + encoded_value] * n_keys
```

---

## Text Format

### Multi-Record Layout
```text
POOL:s1,s2,... optional, when pool is non-empty
records[N]:col:type,... matrix header
@col=v1;v2;... inline column (RLE or POOL strategy)
v1 TAB v2 TAB ... data row for non-inline columns
```

### Single Record Layout
```text
POOL:s1,s2,... optional
SCHEMA:col:type,... schema header
v1 TAB v2 TAB ... single data row
```

### Non-Dict Records
```text
encoded_value one line per record
```

### Column Type Characters
```text
b bool
d int
f float
s str
n null only column
```

### Token Vocabulary
```text
N None
T True
F False
$0= "" (empty string)
nan float("nan")
inf float("inf")
-inf float("-inf")
#N pool reference, single digit index
#{N} pool reference, multi-digit index
```

### String Escaping
```text
\ -> \
TAB -> \t
LF -> \n
CR -> \r
```

All other characters written as-is.

### Inline Columns

Columns with RLE or POOL strategy are written as inline column lines:
```text
@col_name=v1;v2;v3;...
```

Remaining columns are written tab-delimited per row.
This avoids repeating the column value on every row line.

---

## LUMIA Format

### Layout
```text
L|col:type,col:type,... header line
v1,v2,... data row, one per record
```

### Special Cases
```text
L| empty dataset
L|{} all-empty-dict records, followed by {} per row
```

### Type Hint Characters
```text
d int
f float
b bool
s str
n null column
m mixed types
```

Type inferred from first non-null value. Conflicts promote to `m`.

### Value Encoding
```text
None N
True T
False F
"" $0=
float("nan") nan
float("inf") inf
float("-inf") -inf
dict {k:v,k:v}
list [v|v|v]
safe string literal
unsafe string RFC 4180 quoted: "..." with "" for internal quotes
```

Unsafe characters: `,` `"` `{` `}` `[` `]` `|` `:` `\n` `\r`

---

## Implementation Invariants

1. Bool is checked before int because bool is a subtype of int in Python.
2. Pool ordering: score descending, ties by insertion order. Both Python
   and Rust must produce the same pool for the same input.
3. Float specials (nan, inf, -inf) round-trip through all surfaces.
4. JSON output replaces float specials with null (JSON does not allow them).
5. The Python implementation is normative. Rust output must be byte-identical.






