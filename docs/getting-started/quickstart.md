# Quick Start

This page gets you from zero to working ULMEN encode and decode in five minutes.

---

## 1. Basic Encode and Decode

```python
from ulmen import UlmenDict, decode_binary_records, decode_text_records

records = [
    {"id": 1, "name": "Alice", "city": "London", "score": 98.5, "active": True},
    {"id": 2, "name": "Bob",   "city": "London", "score": 91.0, "active": False},
    {"id": 3, "name": "Carol", "city": "Paris",  "score": 87.3, "active": True},
]

ld = UlmenDict(records)

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
Swap UlmenDict for UlmenDictRust. The API is identical.
Output is byte-identical. Speed is 13x faster on binary encode.

```Python
from ulmen import UlmenDictRust, RUST_AVAILABLE

print(RUST_AVAILABLE)  # True if Rust extension is compiled

ld = UlmenDictRust(records)
binary     = ld.encode_binary_pooled()
text       = ld.encode_text()
compressed = ld.encode_binary_zlib(level=6)
```

---

## 3. ULMEN: LLM-Native Format
ULMEN is a typed CSV format that language models can read and generate
without special training.

```Python
from ulmen import encode_ulmen_llm, decode_ulmen_llm

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

Decode back:
```Python

back = decode_ulmen_llm(ulmen)
print(back[0])  # {"id": 1, "name": "Alice", "city": "London", ...}
```

---

## 4. Extended Pool for Maximum Compression
UlmenDictFull uses a pool of up to 256 strings instead of 64.
Strategies are always enabled. Best for large repetitive datasets.

```Python
from ulmen.core import UlmenDictFull

ldf    = UlmenDictFull(records, pool_size_limit=256)
binary = ldf.encode_binary()
```

---

## 5. ULMEN-AGENT Protocol
Structured wire format for agentic AI systems.

```Python
from ulmen import encode_agent_payload, decode_agent_payload, validate_agent_payload

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
ld = UlmenDict([])
ld.append({"id": 1, "name": "Alice"})
ld.append({"id": 2, "name": "Bob"})
print(len(ld))        # 2
print(ld.pool_size)   # number of interned strings
```
The pool is rebuilt and all caches are invalidated on each 'append'.

---

## 7. JSON Compatibility
```Python
ld = UlmenDict(records)
json_str = ld.to_json()
# NaN and inf are replaced with null in JSON output
```

---

## Next Steps
- [Binary Format Guide](binary.md): pool, strategies, columnar encoding
- [ULMEN Guide](ulmen.md): typed CSV for language models
- [API Reference](api.md): every method documented
- [Benchmarks](benchmarks.md): real measured numbers