# Binary Format Guide

The LUMEN binary format is the primary wire format. It is designed to be
the smallest and fastest way to serialize structured records.

---

## When to Use Binary

Use binary when:
- Sending data over a network or between processes
- Writing to disk for later retrieval
- Size or parse speed is a constraint
- You do not need the output to be human-readable

Use text when you need human-readable, diffable output.
Use LUMIA when the consumer is a language model.

---

## Basic Usage

```python
from lumen import LumenDict, LumenDictRust, decode_binary_records

records = [{"id": i, "city": "London", "score": 9.5} for i in range(1000)]

# Python
ld     = LumenDict(records)
binary = ld.encode_binary_pooled()

# Rust (13x faster, byte-identical)
ld     = LumenDictRust(records)
binary = ld.encode_binary_pooled()

# Decode
back = decode_binary_records(binary)
```

---

## Encode Methods

Method|Strategies|Pool|Use case
encode_binary()|off by default|yes|raw storage without overhead
encode_binary_pooled()|on|yes|maximum compression
encode_binary_zlog(level=6)|on|yes|smallest possible output

optimizations=True on LumenDict turns strategies on for encode_binary().

---

## String Pool
The string pool replaces repeated strings with compact integer references.
A string is pooled when:
```text
frequency x (length - ref_cost) > 0
```
** where ** ref_cost is 2 bytes for pools with 9 or fewer entries, 4 bytes otherwise.

```Python

from lumen import build_pool

records = [{"city": "London"} for _ in range(1000)]
pool, pool_map = build_pool(records, max_pool=64)

print(pool)      # ["London"]
print(pool_map)  # {"London": 0}
```

'LumenDict' manages the pool automatically. Use 'build_pool' directly only
when calling the low-level encoder.

Pool size limit:

- 'LumenDict': 64 entries
- 'LumenDictFull': up to 256 entries (configurable)
- 'LumenDictRust': configurable via pool_size_limit parameter

---

## Column Strategies
When encoding multiple records, LUMEN stores data column by column
and selects the best encoding strategy per column.

Strategy|Byte|Applied when
RAW|0x00|Default, no pattern detected
DELTA|0x01|All integers, delta saves bytes vs raw
RLE|0x02|Run ratio below 0.6, or all null
BITS|0x03|All boolean values
POOL|0x04|All strings, unique count below threshold

Check what strategy a column will use:

```Python
from lumen import compute_delta_savings, compute_rle_savings, compute_bits_savings

compute_delta_savings([1, 2, 3, 4, 5])
# {"raw": 10, "delta": 7, "saving": 3, "pct": 30.0}

compute_bits_savings([True, False, True, True])
# {"raw": 8, "bits": 3, "saving": 5}
```

---

## Low-Level Encoder
For custom use cases, call the encoder directly:

```Python
from lumen import build_pool, encode_binary_records, decode_binary_records

records  = [{"id": i, "name": f"user_{i}"} for i in range(100)]
pool, pm = build_pool(records, max_pool=64)
data     = encode_binary_records(records, pool, pm, use_strategies=True)
back     = decode_binary_records(data)
```

---

## File Layout
Every binary payload starts with:

```text

LUMB  (4 bytes magic)
0x03 0x03  (2 bytes version)
[optional pool block]
[payload: T_MATRIX or T_LIST]
```

See 'Wire Format' for the complete binary
encoding specification.

---

##  Zlib Compression
```Python
ld = LumenDictRust(records)

zlib6 = ld.encode_binary_zlib(level=6)   # balanced
zlib9 = ld.encode_binary_zlib(level=9)   # smallest, slower

print(len(zlib6))   # 2,452 bytes for 1,000 records (vs 145,664 JSON)
```
Level 6 is the default and recommended for most use cases.
Level 9 provides marginal additional compression at higher CPU cost.

Decompress with standard zlib:
```Python
import zlib
raw = zlib.decompress(zlib6)
from lumen import decode_binary_records
back = decode_binary_records(raw)
```
