# Quick Start

This page gets you from zero to working LUMEN encode and decode in five minutes.

---

## 1. Basic Encode and Decode

```python
from lumen import LumenDict, decode_binary_records, decode_text_records

records = [
    {"id": 1, "name": "Alice", "city": "London", "score": 98.5, "active": True},
    {"id": 2, "name": "Bob",   "city": "London", "score": 91.0, "active": False},
    {"id": 3, "name": "Carol", "city": "Paris",  "score": 87.3, "active": True},
]

ld = LumenDict(records)

# Binary -- smallest on wire
binary = ld.encode_binary_pooled()
back   = decode_binary_records(binary)
print(back[0])  # {"id": 1, "name": "Alice", ...}

# Text -- human-readable
text = ld.encode_text()
back = decode_text_records(text)

# Zlib -- smallest possible
compressed = ld.encode_binary_zlib(level=6)
```

---

## 2. Rust Acceleration
Swap LumenDict for LumenDictRust. The API is identical.
Output is byte-identical. Speed is 13x faster on binary encode.

```Python
from lumen import LumenDictRust, RUST_AVAILABLE

print(RUST_AVAILABLE)  # True if Rust extension is compiled

ld = LumenDictRust(records)
binary     = ld.encode_binary_pooled()
text       = ld.encode_text()
compressed = ld.encode_binary_zlib(level=6)
```

---

## 3. LUMIA: LLM-Native Format
LUMIA is a typed CSV format that language models can read and generate
without special training.

```Python
from lumen import encode_lumen_llm, decode_lumen_llm

lumia = encode_lumen_llm(records)
print(lumia)
```
Output:
```text
L|id:d,name:s,city:s,score:f,active:b
1,Alice,London,98.5,T
2,Bob,London,91.0,F
3,Carol,Paris,87.3,T
```

Decode back:
```Python

back = decode_lumen_llm(lumia)
print(back[0])  # {"id": 1, "name": "Alice", "city": "London", ...}
```

---

## 4. Extended Pool for Maximum Compression
LumenDictFull uses a pool of up to 256 strings instead of 64.
Strategies are always enabled. Best for large repetitive datasets.

```Python
from lumen.core import LumenDictFull

ldf    = LumenDictFull(records, pool_size_limit=256)
binary = ldf.encode_binary()
```

---

## 5. LUMEN-AGENT Protocol
Structured wire format for agentic AI systems.

```Python
from lumen import encode_agent_payload, decode_agent_payload, validate_agent_payload

records = [
    {
        "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
        "role": "user", "turn": 1,
        "content": "What is the capital of France?",
        "tokens": 7, "flagged": False,
    },
]

payload = encode_agent_payload(records)
decoded = decode_agent_payload(payload)
ok, err = validate_agent_payload(payload)
print(ok)   # True
```

---

## 6. Appending Records
```Python
ld = LumenDict([])
ld.append({"id": 1, "name": "Alice"})
ld.append({"id": 2, "name": "Bob"})
print(len(ld))        # 2
print(ld.pool_size)   # number of interned strings
```
The pool is rebuilt and all caches are invalidated on each 'append'.

---

## 7. JSON Compatibility
```Python
ld = LumenDict(records)
json_str = ld.to_json()
# NaN and inf are replaced with null in JSON output
```

---

## Next Steps
- [Binary Format Guide](binary.md): pool, strategies, columnar encoding
- [LUMIA Guide](lumia.md): typed CSV for language models
- [API Reference](api.md): every method documented
- [Benchmarks](benchmarks.md): real measured numbers