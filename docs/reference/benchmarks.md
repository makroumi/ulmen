# Benchmarks

All numbers measured on x86_64 Linux, release builds.
Rust: rustc 1.86+, ulmen-core with zero external dependencies.
Python: 3.12+, PyO3 extension compiled with LTO.
Dataset: 1,000 typed agent records (msg type, 9 fields each).

## Native Rust (ulmen-core)

| Operation | Time | Per Record |
|---|--:|--:|
| Encode payload | 288 us | 0.29 us |
| Decode payload | 795 us | 0.80 us |
| Validate payload | 91 us | 0.09 us |
| Compress (completed_sequences) | 256 us | 0.26 us |
| Compress (sliding_window) | 81 us | 0.08 us |
| Chunk (budget=2000) | 568 us | 0.57 us |
| Token count (BPE) | 286 us | 0.29 us |
| Dedup mem | 249 us | 0.25 us |

## Python API (via PyO3)

| Operation | Time | vs json |
|---|--:|---|
| encode_agent_payload | 1,211 us | 1.4x faster |
| decode_agent_payload | 1,817 us | 1.4x faster |
| validate_agent_payload | 2,716 us | |
| json.dumps | 1,741 us | baseline |
| json.loads | 2,473 us | baseline |

## Size

| Format | Bytes | vs JSON |
|---|--:|---|
| JSON | 196,066 | baseline |
| ULMEN-AGENT | 88,095 | 55% smaller |
| ULMEN Text | 84,724 | 57% smaller |
| ULMEN Binary | 80,066 | 59% smaller |
| ULMEN Binary + zlib | 5,046 | 97% smaller |

## Run Yourself

```bash
# Native Rust benchmark
cargo run --release --example bench -p ulmen-core

# Python benchmark suite
python -m pytest tests/perf/test_benchmark.py -v -s
```

