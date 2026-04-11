# Quick‑Start Guide

This guide shows how to serialize and deserialize data with LUMEN in just a few lines of Python.

## 1. Install the library

```bash
pip install lumen-notation
```

## 2. Basic usage (Python reference implementation)

```python
from lumen import LumenDict

# Sample records – a list of dictionaries
records = [
    {"id": 1, "name": "Alice", "dept": "Engineering", "active": True, "score": 98.5},
    {"id": 2, "name": "Bob",   "dept": "Marketing",   "active": False, "score": 73.2},
]

# Create a LumenDict instance
ld = LumenDict(records)

# Encode to the compact text format
text = ld.encode_text()
print("Text representation:\n", text)

# Encode to the binary format (pooled strategies)
binary = ld.encode_binary_pooled()
print("Binary size:", len(binary), "bytes")

# Decode back to Python objects
from lumen import decode_text_records, decode_binary_records

decoded_text = decode_text_records(text)
decoded_binary = decode_binary_records(binary)
print("Round‑trip equality:", decoded_text == decoded_binary == records)
```

## 3. Using the Rust‑accelerated implementation (optional)

If you have built the Rust extension (see the [Installation Guide](installation.md)) you can simply swap the class:

```python
from lumen import LumenDictRust

ld_rust = LumenDictRust(records)
print("Rust‑accelerated text size:", len(ld_rust.encode_text()))
```

The API is **identical** between the Python and Rust versions, so you can write code once and benefit from the speed boost when the Rust module is available.

## 4. Advanced options

- **Full mode** (`LumenDictFull`) – uses a larger string pool (up to 256 entries) for even better compression on datasets with many repeated strings.
- **Zlib compression** – call `encode_binary_zlib()` to get the smallest possible binary payload.

Enjoy fast, tiny serialization with LUMEN!
