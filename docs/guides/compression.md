# Compression Guide

LUMEN provides multiple layers of compression. This guide explains each
layer, when to use it, and how to tune it.

---

## Compression Layers

| Layer | What it does | Where it applies |
|---|---|---|
| String pool | Replaces repeated strings with integer references | Binary and text |
| Column strategies | Selects optimal encoding per column | Binary only |
| Zlib | General-purpose byte compression | Binary only |

Each layer is independent. They stack: pool reduces repetition, strategies
reduce column entropy, zlib compresses the result.

---

## Layer 1: String Pool

The pool scans all string values and keys, counts frequencies, and
selects strings where replacing them with a 2-4 byte reference saves bytes.

A string is pooled when:
```text
frequency x (byte_length - ref_cost) > 0
```


`ref_cost` is 2 bytes for pools with 9 or fewer entries, 4 bytes otherwise.

```python
from lumen import build_pool

records  = [{"city": "London", "country": "UK"} for _ in range(1000)]
pool, pm = build_pool(records, max_pool=64)
print(pool)      # ["city", "country", "London", "UK"]
print(pool_map)  # {"city": 0, "country": 1, "London": 2, "UK": 3}
```

Pool size limits:

| Class | Default limit | Configurable |
|---|---|---|
| LumenDict | 64 | No |
| LumenDictFull | 256 | Yes, via pool_size_limit |
| LumenDictRust | 64 | Yes, via pool_size_limit |
| LumenDictFullRust | 256 | Yes, via pool_size_limit |

Larger pools save more bytes on highly repetitive string datasets but increase the pool header size and pool-build time.

---

## Layer 2: Column Strategies

Applied automatically when calling 'encode_binary_pooled()' or setting 'optimizations=True'. Strategies are selected per column based on the data pattern.

### BITS: boolean columns
Stores booleans as a bitfield. 8x denser than one byte per bool.

```Python
from lumen import compute_bits_savings

compute_bits_savings([True, False, True, True, False])
# {"raw": 10, "bits": 3, "saving": 7}
```

### DELTA: integer sequences
Stores the first value then differences. Effective for monotonic IDs.

```python
from lumen import compute_delta_savings

compute_delta_savings([1000, 1001, 1002, 1003, 1004])
# {"raw": 10, "delta": 6, "saving": 4, "pct": 40.0}
```

### RLE: repeated values
Stores runs as (value, count) pairs. Effective for low-cardinality columns
or columns with long constant stretches.

```Python
from lumen import compute_rle_savings

compute_rle_savings(["active"] * 500 + ["inactive"] * 500)
# {"raw": ..., "rle": ..., "saving": ...}
```

### POOL: repeated strings
Uses pool references for string columns with low cardinality.
Threshold: unique count must be below `max(8, n / 10)`.

#### Inspect strategy selection
```Python
from lumen import detect_column_strategy

detect_column_strategy([1, 2, 3, 4, 5])                      # "delta"
detect_column_strategy([True, False, True])                   # "bits"
detect_column_strategy(["London"] * 900 + ["Paris"] * 100)   # "pool"
detect_column_strategy([None] * 100)                          # "rle"
```

---

## Layer 3: Zlib Compression
Applied on top of the already-compressed binary payload.

```Python
ld = LumenDictRust(records)

level6 = ld.encode_binary_zlib(level=6)   # balanced, recommended
level9 = ld.encode_binary_zlib(level=9)   # smallest, higher CPU cost
level1 = ld.encode_binary_zlib(level=1)   # fastest, larger output
```

Measured on 1,000 records:

| Level | Size | vs no zlib |
|---|---|---|
| No zlib | 32,701 bytes | 100% |
| Level 1 | ~2,700 bytes | ~8% |
| Level 6 | 2,453 bytes | 7.5% |
| Level 9 | 2,450 bytes | 7.5% |

Level 9 provides negligible improvement over level 6 on LUMEN binary because the pool and strategies already remove most redundancy.

Decompress with standard Python zlib:
```Python
import zlib
from lumen import decode_binary_records

raw  = zlib.decompress(compressed)
back = decode_binary_records(raw)
```

---

## Choosing the Right Method

| Scenario | Recommended method |
|---|---|
| Maximum compatibility, no size concern | 'encode_binary()' |
| Production data transfer | 'encode_binary_pooled()' |
| Long-term storage, disk space critical | 'encode_binary_zlib(level=6)' |
| Large repetitive dataset | 'LumenDictFull.encode_binary()' |
| Fastest possible encode | 'LumenDictRust.encode_binary_pooled()' |

---

## Size Referance
Measured on 1,000 records, 10 mixed-type columns.

| Method | Bytes | vs JSON |
|---|---|---|
| JSON | 145,664 | 100.0% |
| Pickle protocol 4 | 62,177 | 42.7% |
| CSV | 61,717 | 42.4% |
| LUMEN text | 46,779 | 32.1% |
| LUMEN binary | 32,701 | 22.4% |
| LUMEN zlib-6 | 2,453 | 1.7% |
| LUMEN zlib-9 | 2,450 | 1.7% |
