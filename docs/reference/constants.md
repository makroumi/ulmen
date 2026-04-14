# Constants Reference

All constants are exported from the top-level `lumen` package.

```python
from lumen import MAGIC, VERSION, T_INT, S_DELTA
```

---

## Format Identifiers

| Constant | Value | Description |
|---|---|---|
| 'MAGIC' | 'b"LUMB"' | 4-byte magic number at the start of every binary payload |
| 'VERSION' | 'bytes([3, 3])' | Major and minor wire format version |
| 'LUMEN_LLM_MAGIC' | '"L|"' | LUMIA payload prefix |
| 'AGENT_MAGIC' | '"LUMEN-AGENT v1"' | LUMEN-AGENT payload first line |
| 'AGENT_VERSION' | '"1.0.0"' | LUMEN-AGENT protocol version |

---

## Type Tags
Single byte written before every encoded value.

| Constant | Value | Format |
|---|---|---|
| 'T_STR_TINY' | '0x01' | UTF-8 string, 0-3 bytes. tag + len_byte + bytes |
| 'T_STR' | '0x02' | UTF-8 string, 4+ bytes. tag + varint_len + bytes |
| 'T_INT' | '0x03' | Signed integer. tag + zigzag_varint |
| 'T_FLOAT' | '0x04' | IEEE-754 double. tag + 8 bytes big-endian |
| 'T_BOOL' | '0x05' | Boolean. tag + 0x00 or 0x01 |
| 'T_NULL' | '0x06' | Null. tag only, no payload |
| 'T_LIST' | '0x07' | Heterogeneous list. tag + varint_n + n values |
| 'T_MAP' | '0x08' | Key-value map. tag + varint_n + n (key, value) pairs |
| 'T_POOL_DEF' | '0x09' | String pool definition. tag + varint_n + n strings |
| 'T_POOL_REF' | '0x0A' | Pool index reference. tag + varint_index |
| 'T_MATRIX' | '0x0B' | Columnar record set. tag + rows + cols + headers + data |
| 'T_DELTA_RAW' | '0x0C' | Delta-encoded integer column |
| 'T_BITS' | '0x0E' | Packed boolean column |
| 'T_RLE' | '0x0F' | Run-length encoded column |

---

## Strategy Bytes

Written in the T_MATRIX column header, one byte per column.
| Constant | Value | Applied when |
|---|---|---|
| 'S_RAW' | '0x00' | Default, no pattern detected |
| 'S_DELTA' | '0x01' | Integer column where delta saves bytes |
| 'S_RLE' | '0x02' | Run ratio below 0.6, or all null |
| 'S_BITS' | '0x03' | All boolean values |
| 'S_POOL' | '0x04' | String column with low cardinality |

---

## Version Strings

| Constant | Value |
|---|---|
| '__version__' | '1.0.0' |
| '__edition__' | 'LUMEN V1' |

```Python
from lumen import RECORD_TYPES, FIELD_COUNTS

print(RECORD_TYPES)
# frozenset({"msg", "tool", "res", "plan", "obs", "err", "mem", "rag", "hyp", "cot"})

print(FIELD_COUNTS["msg"])   # 9
print(FIELD_COUNTS["tool"])  # 7
```

---

## LUMEN-AGENT Constants

### Priority Values

| Constant | Value | Meaning |
|---|---|---|
| `PRIORITY_MUST_KEEP` | `1` | Never removed by any compression strategy |
| `PRIORITY_KEEP_IF_ROOM` | `2` | Kept unless budget is exhausted |
| `PRIORITY_COMPRESSIBLE` | `3` | First candidate for compression |

Records without a priority field default to `PRIORITY_COMPRESSIBLE`.

### Compression Strategy Names

| Constant | Value |
|---|---|
| `COMPRESS_COMPLETED_SEQUENCES` | `"completed_sequences"` |
| `COMPRESS_KEEP_TYPES` | `"keep_types"` |
| `COMPRESS_SLIDING_WINDOW` | `"sliding_window"` |

### Meta Fields

```python
META_FIELDS = ("parent_id", "from_agent", "to_agent", "priority")
```
Valid names for the 'meta:' header line. 'priority' is decoded as integer.
All others are decoded as string.

### Record Type Field Counts
`FIELD_COUNTS` maps each record type to its total field count per row,
including the four common fields (type, id, thread_id, step) but
excluding any declared meta fields.

```python
from lumen import FIELD_COUNTS

FIELD_COUNTS["msg"]  # 9
FIELD_COUNTS["tool"] # 7
FIELD_COUNTS["res"]  # 8
FIELD_COUNTS["plan"] # 7
FIELD_COUNTS["obs"]  # 7
FIELD_COUNTS["err"]  # 8
FIELD_COUNTS["mem"]  # 8
FIELD_COUNTS["rag"]  # 9
FIELD_COUNTS["hyp"]  # 8
FIELD_COUNTS["cot"]  # 8
```