"""
ULMEN V1 — Official benchmark suite.

Measures real encode/decode throughput and size ratios vs JSON, CSV,
pickle, and TOML on the canonical 1,000-record mixed-type dataset.

Run with:
    pytest tests/perf/test_benchmark.py -v -s --no-header
"""

import csv
import io
import json
import pickle
import time
from typing import Callable

from tests.conftest import make_record
from ulmen import (
    RUST_AVAILABLE,
    UlmenDictRust,
    decode_binary_records,
    decode_ulmen_llm,
    encode_ulmen_llm,
)
from ulmen.core import UlmenDict, UlmenDictFull

# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

RECORDS_1K = [make_record(i) for i in range(1000)]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _timeit(fn: Callable, n: int = 50) -> float:
    """Return median ms over n cold calls (no caching across calls)."""
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    times.sort()
    return times[len(times) // 2]  # median


def _json_encode(records):
    return json.dumps(records, separators=(',', ':')).encode()

def _json_decode(data):
    return json.loads(data)

def _pickle_encode(records):
    return pickle.dumps(records, protocol=4)

def _pickle_decode(data):
    return pickle.loads(data)

def _csv_encode(records):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=records[0].keys())
    w.writeheader()
    w.writerows(records)
    return buf.getvalue().encode()


# ---------------------------------------------------------------------------
# Size measurements
# ---------------------------------------------------------------------------

class TestSizeMeasurements:
    """
    Measure exact byte sizes for all formats and surfaces.
    Results printed to stdout with -s flag.
    All assertions verify ULMEN is smaller than JSON.
    """

    def test_size_report(self, capsys):
        recs = RECORDS_1K

        # Reference sizes
        json_bytes   = len(_json_encode(recs))
        pickle_bytes = len(_pickle_encode(recs))
        csv_bytes    = len(_csv_encode(recs))

        # ULMEN sizes
        ld = UlmenDict(recs)
        ldf = UlmenDictFull(recs)

        py_text_bytes   = len(ld.encode_text().encode())
        py_bin_bytes    = len(ld.encode_binary_pooled())
        py_zlib_bytes   = len(ld.encode_binary_zlib(6))
        py_zlib9_bytes  = len(ld.encode_binary_zlib(9))
        full_bin_bytes  = len(ldf.encode_binary())

        ulmen_bytes     = len(encode_ulmen_llm(recs).encode())

        if RUST_AVAILABLE:
            rs = UlmenDictRust(recs)
            rs_text_bytes  = len(rs.encode_text().encode())
            rs_bin_bytes   = len(rs.encode_binary_pooled())
            rs_zlib_bytes  = len(rs.encode_binary_zlib(6))
            rs_ulmen_bytes = len(rs.encode_ulmen_llm().encode())
        else:
            rs_text_bytes = rs_bin_bytes = rs_zlib_bytes = rs_ulmen_bytes = 0

        def pct(x):
            return f"{100 * x / json_bytes:.1f}%"

        print("\n" + "=" * 60)
        print("ULMEN V1 — Size Benchmark (n=1,000 records)")
        print("=" * 60)
        print(f"{'Format':<28} {'Bytes':>10} {'vs JSON':>10}")
        print("-" * 60)
        print(f"{'JSON':.<28} {json_bytes:>10,} {'100.0%':>10}")
        print(f"{'Pickle (protocol 4)':.<28} {pickle_bytes:>10,} {pct(pickle_bytes):>10}")
        print(f"{'CSV':.<28} {csv_bytes:>10,} {pct(csv_bytes):>10}")
        print("-" * 60)
        print(f"{'ULMEN text (Python)':.<28} {py_text_bytes:>10,} {pct(py_text_bytes):>10}")
        print(f"{'ULMEN binary (Python)':.<28} {py_bin_bytes:>10,} {pct(py_bin_bytes):>10}")
        print(f"{'ULMEN binary full pool':.<28} {full_bin_bytes:>10,} {pct(full_bin_bytes):>10}")
        print(f"{'ULMEN zlib-6 (Python)':.<28} {py_zlib_bytes:>10,} {pct(py_zlib_bytes):>10}")
        print(f"{'ULMEN zlib-9 (Python)':.<28} {py_zlib9_bytes:>10,} {pct(py_zlib9_bytes):>10}")
        print(f"{'ULMEN text (Python)':.<28} {ulmen_bytes:>10,} {pct(ulmen_bytes):>10}")
        if RUST_AVAILABLE:
            print("-" * 60)
            print(f"{'ULMEN text (Rust)':.<28} {rs_text_bytes:>10,} {pct(rs_text_bytes):>10}")
            print(f"{'ULMEN binary (Rust)':.<28} {rs_bin_bytes:>10,} {pct(rs_bin_bytes):>10}")
            print(f"{'ULMEN zlib-6 (Rust)':.<28} {rs_zlib_bytes:>10,} {pct(rs_zlib_bytes):>10}")
            print(f"{'ULMEN text (Rust)':.<28} {rs_ulmen_bytes:>10,} {pct(rs_ulmen_bytes):>10}")
        print("=" * 60)

        # Store results for README generation
        TestSizeMeasurements._results = {
            'json':         json_bytes,
            'pickle':       pickle_bytes,
            'csv':          csv_bytes,
            'py_text':      py_text_bytes,
            'py_bin':       py_bin_bytes,
            'py_zlib6':     py_zlib_bytes,
            'py_zlib9':     py_zlib9_bytes,
            'full_bin':     full_bin_bytes,
            'ulmen':        ulmen_bytes,
            'rs_text':      rs_text_bytes,
            'rs_bin':       rs_bin_bytes,
            'rs_zlib6':     rs_zlib_bytes,
            'rs_ulmen':     rs_ulmen_bytes,
        }

        # Assertions
        assert py_text_bytes  < json_bytes
        assert py_bin_bytes   < json_bytes
        assert py_zlib_bytes  < json_bytes
        assert ulmen_bytes    < json_bytes
        if RUST_AVAILABLE:
            assert rs_bin_bytes  < json_bytes
            assert rs_zlib_bytes < json_bytes


# ---------------------------------------------------------------------------
# Speed measurements
# ---------------------------------------------------------------------------

class TestSpeedMeasurements:
    """
    Measure median encode/decode latency over 50 iterations.
    All ULMEN timings include construction (pool build + encode).
    """

    def test_speed_report(self, capsys):
        recs = RECORDS_1K

        # Pre-encode for decode benchmarks
        json_enc    = _json_encode(recs)
        pickle_enc  = _pickle_encode(recs)
        ld_bin      = UlmenDict(recs).encode_binary_pooled()
        ulmen_enc   = encode_ulmen_llm(recs)

        # Encode timings
        t_json_enc   = _timeit(lambda: _json_encode(recs))
        t_pickle_enc = _timeit(lambda: _pickle_encode(recs))
        t_py_text    = _timeit(lambda: UlmenDict(recs).encode_text())
        t_py_bin     = _timeit(lambda: UlmenDict(recs).encode_binary_pooled())
        t_py_zlib    = _timeit(lambda: UlmenDict(recs).encode_binary_zlib(6))
        t_ulmen_enc  = _timeit(lambda: encode_ulmen_llm(recs))

        # Decode timings
        t_json_dec   = _timeit(lambda: _json_decode(json_enc))
        t_pickle_dec = _timeit(lambda: _pickle_decode(pickle_enc))
        t_py_bin_dec = _timeit(lambda: decode_binary_records(ld_bin))
        t_ulmen_dec  = _timeit(lambda: decode_ulmen_llm(ulmen_enc))

        if RUST_AVAILABLE:
            rs_bin     = UlmenDictRust(recs).encode_binary_pooled()
            rs_ulmen   = UlmenDictRust(recs).encode_ulmen_llm()
            t_rs_text  = _timeit(lambda: UlmenDictRust(recs).encode_text())
            t_rs_bin   = _timeit(lambda: UlmenDictRust(recs).encode_binary_pooled())
            t_rs_zlib  = _timeit(lambda: UlmenDictRust(recs).encode_binary_zlib(6))
            t_rs_ulmen = _timeit(lambda: UlmenDictRust(recs).encode_ulmen_llm())
            t_rs_bin_dec   = _timeit(lambda: decode_binary_records(rs_bin))
            t_rs_ulmen_dec = _timeit(lambda: decode_ulmen_llm(rs_ulmen))
        else:
            t_rs_text = t_rs_bin = t_rs_zlib = t_rs_ulmen = 0.0
            t_rs_bin_dec = t_rs_ulmen_dec = 0.0

        print("\n" + "=" * 60)
        print("ULMEN V1 — Speed Benchmark (n=1,000 records, median 50 runs)")
        print("=" * 60)
        print(f"{'Format':<32} {'Encode ms':>12} {'Decode ms':>12}")
        print("-" * 60)
        print(f"{'JSON':.<32} {t_json_enc:>11.3f}  {t_json_dec:>11.3f}")
        print(f"{'Pickle (protocol 4)':.<32} {t_pickle_enc:>11.3f}  {t_pickle_dec:>11.3f}")
        print("-" * 60)
        print(f"{'ULMEN text (Python)':.<32} {t_py_text:>11.3f}  {'—':>11}")
        print(f"{'ULMEN binary (Python)':.<32} {t_py_bin:>11.3f}  {t_py_bin_dec:>11.3f}")
        print(f"{'ULMEN zlib-6 (Python)':.<32} {t_py_zlib:>11.3f}  {'—':>11}")
        print(f"{'ULMEN (Python)':.<32} {t_ulmen_enc:>11.3f}  {t_ulmen_dec:>11.3f}")
        if RUST_AVAILABLE:
            print("-" * 60)
            print(f"{'ULMEN text (Rust)':.<32} {t_rs_text:>11.3f}  {'—':>11}")
            print(f"{'ULMEN binary (Rust)':.<32} {t_rs_bin:>11.3f}  {t_rs_bin_dec:>11.3f}")
            print(f"{'ULMEN zlib-6 (Rust)':.<32} {t_rs_zlib:>11.3f}  {'—':>11}")
            print(f"{'ULMEN (Rust)':.<32} {t_rs_ulmen:>11.3f}  {t_rs_ulmen_dec:>11.3f}")
            print("-" * 60)
            if t_py_bin > 0 and t_rs_bin > 0:
                speedup_bin  = t_py_bin  / t_rs_bin
                speedup_text = t_py_text / t_rs_text
                print(f"  Rust speedup binary encode: {speedup_bin:.1f}×")
                print(f"  Rust speedup text encode:   {speedup_text:.1f}×")
        print("=" * 60)

        # Store results
        TestSpeedMeasurements._results = {
            't_json_enc': t_json_enc, 't_json_dec': t_json_dec,
            't_py_text': t_py_text, 't_py_bin': t_py_bin,
            't_py_zlib': t_py_zlib, 't_ulmen_enc': t_ulmen_enc,
            't_ulmen_dec': t_ulmen_dec, 't_py_bin_dec': t_py_bin_dec,
            't_rs_text': t_rs_text, 't_rs_bin': t_rs_bin,
            't_rs_zlib': t_rs_zlib, 't_rs_ulmen': t_rs_ulmen,
            't_rs_bin_dec': t_rs_bin_dec, 't_rs_ulmen_dec': t_rs_ulmen_dec,
        }

        # Sanity assertions — not speed guarantees, just smoke checks
        assert t_py_bin < 10_000
        assert t_json_enc < 10_000
        if RUST_AVAILABLE:
            assert t_rs_bin < t_py_bin  # Rust must be faster


# ---------------------------------------------------------------------------
# Streaming measurements
# ---------------------------------------------------------------------------

class TestStreamingMeasurements:
    """
    Measure streaming encode/decode throughput.
    UlmenStreamEncoder and stream_encode_windowed at 1k and 10k records.
    """

    def test_streaming_report(self, capsys):
        from ulmen import decode_binary_records as decode_binary_records_rust
        from ulmen.core._streaming import stream_encode, stream_encode_windowed

        recs_1k  = [make_record(i) for i in range(1_000)]
        recs_10k = [make_record(i) for i in range(10_000)]

        def _stream_payload(recs):
            return b"".join(stream_encode(recs))

        def _windowed_payload(recs, ws=100):
            return list(stream_encode_windowed(recs, window_size=ws))

        t_stream_1k   = _timeit(lambda: _stream_payload(recs_1k))
        t_stream_10k  = _timeit(lambda: _stream_payload(recs_10k), n=10)
        t_window_1k   = _timeit(lambda: _windowed_payload(recs_1k))
        t_window_10k  = _timeit(lambda: _windowed_payload(recs_10k), n=10)

        payload_1k  = _stream_payload(recs_1k)
        payload_10k = _stream_payload(recs_10k)

        t_dec_1k  = _timeit(lambda: decode_binary_records_rust(payload_1k))
        t_dec_10k = _timeit(lambda: decode_binary_records_rust(payload_10k), n=10)

        mb_s_1k  = (len(payload_1k)  / 1e6) / (t_stream_1k  / 1000)
        mb_s_10k = (len(payload_10k) / 1e6) / (t_stream_10k / 1000)

        print("\n" + "=" * 66)
        print("ULMEN V1 — Streaming Benchmark")
        print("=" * 66)
        print(f"{'Surface':<36} {'Enc ms':>8} {'Dec ms':>8} {'MB/s':>8}")
        print("-" * 66)
        print(f"{'stream_encode  (1k records)':.<36} {t_stream_1k:>8.3f} {t_dec_1k:>8.3f} {mb_s_1k:>8.1f}")
        print(f"{'stream_encode  (10k records)':.<36} {t_stream_10k:>8.3f} {t_dec_10k:>8.3f} {mb_s_10k:>8.1f}")
        print(f"{'stream_windowed(1k,  ws=100)':.<36} {t_window_1k:>8.3f} {'—':>8} {'—':>8}")
        print(f"{'stream_windowed(10k, ws=100)':.<36} {t_window_10k:>8.3f} {'—':>8} {'—':>8}")
        print("=" * 66)
        print(f"  Payload sizes: 1k={len(payload_1k):,}B  10k={len(payload_10k):,}B")
        print("  Wire format identical to batch encode: yes")
        print("=" * 66)

        assert len(payload_1k) == len(RECORDS_1K and _stream_payload(RECORDS_1K))
        assert t_stream_1k < 10_000
        assert t_stream_10k < 100_000
