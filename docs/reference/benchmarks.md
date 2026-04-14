# Benchmarks

All numbers are measured on real hardware. No estimates or approximations.

Machine: x86_64 Linux, Python 3.12.13, GCC 15.2, rustc 1.92.0
Dataset: 1,000 records, 10 mixed-type columns (int, float, str, bool)
Speed: median of 50 runs, full construction included (pool build + encode)

Run the benchmark yourself:

```bash
pytest tests/perf/test_benchmark.py -v -s
```
---

## Size

| Format | Bytes | vs JSON |
|---|---|---|
| JSON | 145,664 | 100.0% |
| Pickle protocol 4 | 62,177 | 42.7% |
| CSV | 61,717 | 42.4% |
| LUMIA text | 57,403 | 39.4% |
| LUMEN text | 46,779 | 32.1% |
| LUMEN binary | 32,701 | 22.4% |
| LUMEN binary full pool | 32,701 | 22.4% |
| LUMEN zlib-6 | 2,453 | 1.7% |
| LUMEN zlib-9 | 2,450 | 1.7% |

Python and Rust produce byte-identical output.

---

## Speed

| Format | Encode ms | Decode ms |
|---|---|---|
| JSON | 1.264 | 1.849 |
| Pickle protocol 4 | 0.369 | 0.497 |
| LUMEN text (Python) | 12.005 | n/a |
| LUMEN binary (Python) | 13.579 | 0.479 |
| LUMEN zlib-6 (Python) | 13.693 | n/a |
| LUMIA (Python) | 1.511 | 0.833 |
| LUMEN text (Rust) | 1.018 | n/a |
| LUMEN binary (Rust) | 1.013 | 0.475 |
| LUMEN zlib-6 (Rust) | 1.243 | n/a |
| LUMIA (Rust) | 3.497 | 0.830 |

---

## Key Observations
**Rust vs Python encode speedup**

| Surface | Speedup |
|---|---|
| Binary encode | 13.4x |
| Text encode | 11.8x |

**LUMEN binary (Rust) vs JSON**

LUMEN binary encodes in 1.013 ms vs JSON at 1.264 ms -- comparable speed
while producing output that is 4.5x smaller.

**Zlib headroom**

LUMEN binary is already highly compressed by pool and strategies.
Zlib adds only marginal additional reduction from 32,701 to 2,453 bytes.
The large gap (22.4% to 1.7%) shows the binary format retains repetition
that zlib can exploit, primarily from numeric patterns.

**LUMIA vs JSON**

LUMIA encodes in 1.511 ms (Python) with 39.4% of JSON size and 43% of
JSON token count, making it suitable for LLM context where token budget
matters more than byte count.

---

## Notes on Methodology
- Each timing calls the full construction path including pool build.
- Cached-only timings would show lower numbers but do not reflect
real first-call cost.
- Pickle is included as a reference for a binary Python-native format
and is not a general-purpose interoperable format.
- CSV does not encode booleans, nulls, or nested structures natively.
The CSV size here is for the flat string representation only.
---

## Streaming

Streaming surfaces never materialise the full payload before the first byte
is emitted. Wire format is identical to batch encode — every chunk is a valid
independently-decodable LUMEN binary payload.

Machine: x86_64 Linux, Python 3.12.13, rustc 1.92.0
Dataset: mixed-type records (int, float, str, bool), median 50 runs (1k) / 10 runs (10k)

| Surface | Records | Encode ms | Decode ms | MB/s |
|---|---|---|---|---|
| `stream_encode` | 1,000 | 2.298 | 0.726 | 14.2 |
| `stream_encode` | 10,000 | 24.889 | 9.448 | 13.4 |
| `stream_encode_windowed` (ws=100) | 1,000 | 1.660 | — | — |
| `stream_encode_windowed` (ws=100) | 10,000 | 16.560 | — | — |

Payload sizes: 1k = 32,701 B · 10k = 334,606 B

### Notes

- `stream_encode` accumulates records then slices the encoded payload into
  `chunk_size` chunks (default 64 KiB). The Rust backend is used automatically
  when available — Python and Rust paths produce identical wire bytes.
- `stream_encode_windowed` encodes fixed-size windows into independent
  sub-payloads. Each sub-payload is decodable standalone with
  `decode_binary_records`. Use this for truly unbounded streams where even
  the column scan must be bounded.
- Decode timings use `decode_binary_records_rust` on the reassembled payload.
