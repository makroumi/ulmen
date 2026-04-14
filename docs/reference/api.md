# API Reference

All public symbols are exported from the top-level `ulmen` package.

```python
from ulmen import UlmenDict, UlmenDictRust, encode_ulmen_llm, RUST_AVAILABLE
```
---

# Classes

## UlmenDict
Pure Python record container. Zero runtime dependencies.
```Python
class UlmenDict:
    def __init__(self, data=None, optimizations: bool = False)
```

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `data` | list, dict, iterable, None | None | Records to initialise with. A single dict is wrapped in a list. |
| `optimizations` | bool | False | When True, `encode_binary()` applies column strategies. |

### Properties

| Property | Type | Description |
|---|---|---|
| `pool_size` | int | Number of strings currently in the interning pool |
| `record_count` | int | Number of records held |

### Methods

```Python
def encode_text(self, matrix_mode: bool = True) -> str
```
Encode to ULMEN text format. Result is cached until next mutation.

```Python
def encode_binary(self) -> bytes
```
Encode to ULMEN binary. Strategies applied only when `optimizations=True`.

```Python
def encode_binary_pooled(self) -> bytes
```
Encode to ULMEN binary with all column strategies enabled.

```Python
def encode_binary_zlib(self, level: int = 6) -> bytes
```
Encode with all strategies then compress with zlib. Level 0 to 9.

```Python
def encode_ulmen_llm(self) -> str
```
Encode to ULMEN format. Result is cached until next mutation.

```Python
def decode_text(self, text: str) -> UlmenDict
```
Decode a ULMEN text payload into a new UlmenDict.

```Python
def decode_binary(self, data: bytes) -> UlmenDict
```
Decode a ULMEN binary payload into a new UlmenDict.

```Python
def decode_ulmen_llm(self, text: str) -> UlmenDict
```
Decode a ULMEN payload into a new UlmenDict.

```Python
def to_json(self) -> str
```
Serialize to standard JSON. NaN and inf are replaced with null.

```Python
def append(self, record: Any) -> None
```
Append a record. Rebuilds the pool and invalidates all encode caches.

```Python
def __len__(self) -> int
def __getitem__(self, idx) -> Any
def __iter__(self)
```

---

## UlmenDictFull

Extends UlmenDict with a larger pool and strategies always enabled.

```Python
class UlmenDictFull(UlmenDict):
    def __init__(self, data=None, pool_size_limit: int = 256)
```

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `data` | list, dict, iterable, None | None | Records to initialise with |
| `pool_size_limit` | int | 256 | Maximum number of strings in the pool |

All 'UlmenDict' methods are available. 'encode_binary()' always uses
full strategy selection regardless of the optimizations flag.

---

## UlmenDictRust
Rust-accelerated drop-in replacement for 'UlmenDict'.
Available when the Rust extension is compiled. Falls back to a Python
shim with the same interface when Rust is unavailable.

```Python

class UlmenDictRust:
    def __init__(
        self,
        data: list = None,
        optimizations: bool = False,
        pool_size_limit: int = 64,
    )
```

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `data` | list, None | None | Records to initialise with |
| `optimizations` | bool | False | When True, 'encode_binary()' uses strategies |
| `pool_size_limit` | int | 64 | Maximum pool size |

### Methods

All methods from 'UlmenDict' are available plus:

```Python
def pool_size(self) -> int
def encode_ulmen_llm(self) -> str
def bench_encode_text_only(self, iters: int) -> int
def bench_encode_binary_only(self, iters: int) -> int
def bench_encode_text_clone(self, iters: int) -> int
def bench_encode_binary_clone(self, iters: int) -> int
```

---

## UlmenDictFullRust
Rust-accelerated drop-in replacement for 'UlmenDictFull'.

```Python

class UlmenDictFullRust(UlmenDictFull):
    def __init__(
        self,
        data: list = None,
        pool_size_limit: int = 256,
    )
```

Same methods as 'UlmenDictRust'. Strategies are always enabled.

---

## Module-Level Functions
### Encoding
```Python
def encode_ulmen_llm(records: list) -> str
```
Encode records to ULMEN format. Uses Rust when available.

```Python
def encode_text_records(
    records: list,
    pool: list,
    pool_map: dict,
    schema: dict = None,
    matrix_mode: bool = True,
) -> str
```
Low-level text encoder. Manages no pool state.

```Python
def encode_binary_records(
    records: list,
    pool: list,
    pool_map: dict,
    use_strategies: bool = True,
) -> bytes
```
Low-level binary encoder. Manages no pool state.

### Decoding
```Python
def decode_ulmen_llm(text: str) -> list
```
Decode ULMEN format. Uses Rust when available.

```Python
def decode_text_records(text: str) -> list
```
Decode ULMEN text format.

```Python
def decode_binary_records(data: bytes) -> list
```
Decode ULMEN binary format. Uses Rust when available.

### Pool and Strategy
```Python
def build_pool(records: list, max_pool: int = 64) -> tuple[list, dict]
```
Build a string interning pool. Returns (pool, pool_map).

```Python
def detect_column_strategy(values: list) -> str
```
Return the optimal strategy for a column.
Returns one of: "raw" "delta" "rle" "bits" "pool"

```Python
def compute_delta_savings(values: list) -> dict
```
Returns {"raw": int, "delta": int, "saving": int, "pct": float}.

```Python
def compute_rle_savings(values: list) -> dict
```
Returns {"raw": int, "rle": int, "saving": int}.

```Python
def compute_bits_savings(bools: list) -> dict
```
Returns {"raw": int, "bits": int, "saving": int}.

### ULMEN-AGENT
```Python
def encode_agent_payload(records: list[dict]) -> str
def decode_agent_payload(text: str) -> list[dict]
def validate_agent_payload(text: str) -> tuple[bool, str]
def encode_agent_record(rec: dict) -> str
def decode_agent_record(line: str) -> dict
def extract_subgraph(
    records: list[dict],
    thread_id: str = None,
    step_min: int = None,
    step_max: int = None,
    types: list[str] = None,
) -> list[dict]
def extract_subgraph_payload(
    text: str,
    thread_id: str = None,
    step_min: int = None,
    step_max: int = None,
    types: list[str] = None,
) -> str
def make_validation_error(error_msg: str, thread_id: str = "INVALID") -> str
```

### Utilities
```Python
def estimate_tokens(text: str) -> int
```
Rough LLM token count estimate. Uses 1 token per 4 characters.

```Python
def deep_size(obj: Any) -> int
```
Recursive memory footprint in bytes. Cycle-safe.

```Python
def deep_eq(a: Any, b: Any) -> bool
```
Structural equality. Handles NaN, signed infinity, and mixed int/float.

```Python
def fnv1a(data: bytes) -> int
def fnv1a_str(s: str) -> int
```
FNV-1a 32-bit hash.

---

## Runtime Flags
```Python
RUST_AVAILABLE: bool
```
True when the Rust extension is compiled and loaded successfully.
False when running on the pure Python implementation.

---

## Constants
See [Constants Reference](constants.md) for all wire-format tags,
strategy bytes, and magic values.
