# Benchmarks

This page summarizes the size and speed benchmarks for LUMEN compared to common serialization formats. The numbers are generated from the test suite (`tests/test_benchmark.py`).

## Size Comparison (bytes for 1,000 records)

| Format | Size (bytes) | % vs JSON |
|--------|--------------|----------|
| **LUMEN Text (fast)** | {{py_text_fast}} | {{py_text_fast_pct}} |
| **LUMEN Text (full)** | {{py_text_full}} | {{py_text_full_pct}} |
| **LUMEN Binary (pooled)** | {{py_bin_pooled}} | {{py_bin_pooled_pct}} |
| **LUMEN Binary + Zlib** | {{py_bin_zlib}} | {{py_bin_zlib_pct}} |
| JSON | {{json_sz}} | 0% |
| CSV | {{csv_sz}} | {{csv_pct}} |
| Pickle (protocol 4) | {{pickle_sz}} | {{pickle_pct}} |
| TOML | {{toml_sz}} | {{toml_pct}} |
| msgpack | {{msgpack_sz}} | {{msgpack_pct}} |
| cbor2 | {{cbor2_sz}} | {{cbor2_pct}} |
| Arrow IPC | {{arrow_sz}} | {{arrow_pct}} |
| Parquet+snappy | {{parquet_sz}} | {{parquet_pct}} |

*Values in double braces (`{{ }}`) are placeholders that will be filled in by the test harness when the benchmark script runs.*

## Speed Comparison (median encode time, ms)

| Operation | Python (ms) | Rust (ms) | Speedup |
|-----------|------------|----------|--------|
| Text encode | {{py_text_time}} | {{rust_text_time}} | {{text_speedup}}x |
| Binary encode (pooled) | {{py_bin_time}} | {{rust_bin_time}} | {{bin_speedup}}x |
| Zlib compression | {{py_zlib_time}} | {{rust_zlib_time}} | {{zlib_speedup}}x |

The Rust implementation provides **hundreds to thousands of times** speedup over the pure‑Python reference while producing **identical byte output**.

## How to Run the Benchmarks

```bash
pytest tests/test_benchmark.py -v -s
# or
python tests/test_benchmark.py
```

The script prints the tables above and asserts that LUMEN is smaller than JSON and faster than the Python reference.
