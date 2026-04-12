# Benchmarks

This page summarizes the comprehensive size and speed benchmarks for LUMEN compared rigorously to other common serialization formats. The performance numbers are actively generated directly from the robust test suite explicitly located at `tests/test_benchmark.py`.

## Size Comparison in Bytes for 1 000 records

| Format | Size in bytes | % vs JSON |
|---|---|---|
| **LUMEN Text fast** | {{py_text_fast}} | {{py_text_fast_pct}} |
| **LUMEN Text full** | {{py_text_full}} | {{py_text_full_pct}} |
| **LUMEN Binary pooled** | {{py_bin_pooled}} | {{py_bin_pooled_pct}} |
| **LUMEN Binary with Zlib** | {{py_bin_zlib}} | {{py_bin_zlib_pct}} |
| JSON | {{json_sz}} | Baseline |
| CSV | {{csv_sz}} | {{csv_pct}} |
| Pickle protocol 4 | {{pickle_sz}} | {{pickle_pct}} |
| TOML | {{toml_sz}} | {{toml_pct}} |
| msgpack | {{msgpack_sz}} | {{msgpack_pct}} |
| cbor2 | {{cbor2_sz}} | {{cbor2_pct}} |
| Arrow IPC | {{arrow_sz}} | {{arrow_pct}} |
| Parquet with snappy | {{parquet_sz}} | {{parquet_pct}} |

*Values tightly enclosed in double braces like `{{ }}` are variables that will automatically be dynamically populated by the active test harness whenever the system benchmark script correctly executes.*

## Speed Comparison in Median Encode Time

| Operation | Python in ms | Rust in ms | Speedup |
|---|---|---|---|
| Text encode | {{py_text_time}} | {{rust_text_time}} | {{text_speedup}}x |
| Binary encode pooled | {{py_bin_time}} | {{rust_bin_time}} | {{bin_speedup}}x |
| Zlib compression | {{py_zlib_time}} | {{rust_zlib_time}} | {{zlib_speedup}}x |

The tightly optimized Rust implementation reliably yields hundreds to even thousands of times more speed over the pure Python reference, importantly while reliably producing identical byte output perfectly every single time.

## How to Run the Benchmarks

```bash
pytest tests/test_benchmark.py -v -s
# Alternative direct execution
python tests/test_benchmark.py
```

The script logically prints the comprehensive tables clearly shown above and intelligently asserts that LUMEN remains structurally smaller than JSON and significantly faster natively than the pure Python reference.
