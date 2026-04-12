"""
LUMEN V1 — Official benchmark suite.

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

from lumen import (
    RUST_AVAILABLE,
    LumenDictRust,
    decode_binary_records,
    decode_lumen_llm,
    encode_lumen_llm,
)
from lumen.core import LumenDict, LumenDictFull
from tests.conftest import make_record

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
    All assertions verify LUMEN is smaller than JSON.
    """

    def test_size_report(self, capsys):
        recs = RECORDS_1K

        # Reference sizes
        json_bytes   = len(_json_encode(recs))
        pickle_bytes = len(_pickle_encode(recs))
        csv_bytes    = len(_csv_encode(recs))

        # LUMEN sizes
        ld = LumenDict(recs)
        ldf = LumenDictFull(recs)

        py_text_bytes   = len(ld.encode_text().encode())
        py_bin_bytes    = len(ld.encode_binary_pooled())
        py_zlib_bytes   = len(ld.encode_binary_zlib(6))
        py_zlib9_bytes  = len(ld.encode_binary_zlib(9))
        full_bin_bytes  = len(ldf.encode_binary())

        lumia_bytes     = len(encode_lumen_llm(recs).encode())

        if RUST_AVAILABLE:
            rs = LumenDictRust(recs)
            rs_text_bytes  = len(rs.encode_text().encode())
            rs_bin_bytes   = len(rs.encode_binary_pooled())
            rs_zlib_bytes  = len(rs.encode_binary_zlib(6))
            rs_lumia_bytes = len(rs.encode_lumen_llm().encode())
        else:
            rs_text_bytes = rs_bin_bytes = rs_zlib_bytes = rs_lumia_bytes = 0

        def pct(x):
            return f"{100 * x / json_bytes:.1f}%"

        print("\n" + "=" * 60)
        print("LUMEN V1 — Size Benchmark (n=1,000 records)")
        print("=" * 60)
        print(f"{'Format':<28} {'Bytes':>10} {'vs JSON':>10}")
        print("-" * 60)
        print(f"{'JSON':.<28} {json_bytes:>10,} {'100.0%':>10}")
        print(f"{'Pickle (protocol 4)':.<28} {pickle_bytes:>10,} {pct(pickle_bytes):>10}")
        print(f"{'CSV':.<28} {csv_bytes:>10,} {pct(csv_bytes):>10}")
        print("-" * 60)
        print(f"{'LUMEN text (Python)':.<28} {py_text_bytes:>10,} {pct(py_text_bytes):>10}")
        print(f"{'LUMEN binary (Python)':.<28} {py_bin_bytes:>10,} {pct(py_bin_bytes):>10}")
        print(f"{'LUMEN binary full pool':.<28} {full_bin_bytes:>10,} {pct(full_bin_bytes):>10}")
        print(f"{'LUMEN zlib-6 (Python)':.<28} {py_zlib_bytes:>10,} {pct(py_zlib_bytes):>10}")
        print(f"{'LUMEN zlib-9 (Python)':.<28} {py_zlib9_bytes:>10,} {pct(py_zlib9_bytes):>10}")
        print(f"{'LUMIA text (Python)':.<28} {lumia_bytes:>10,} {pct(lumia_bytes):>10}")
        if RUST_AVAILABLE:
            print("-" * 60)
            print(f"{'LUMEN text (Rust)':.<28} {rs_text_bytes:>10,} {pct(rs_text_bytes):>10}")
            print(f"{'LUMEN binary (Rust)':.<28} {rs_bin_bytes:>10,} {pct(rs_bin_bytes):>10}")
            print(f"{'LUMEN zlib-6 (Rust)':.<28} {rs_zlib_bytes:>10,} {pct(rs_zlib_bytes):>10}")
            print(f"{'LUMIA text (Rust)':.<28} {rs_lumia_bytes:>10,} {pct(rs_lumia_bytes):>10}")
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
            'lumia':        lumia_bytes,
            'rs_text':      rs_text_bytes,
            'rs_bin':       rs_bin_bytes,
            'rs_zlib6':     rs_zlib_bytes,
            'rs_lumia':     rs_lumia_bytes,
        }

        # Assertions
        assert py_text_bytes  < json_bytes
        assert py_bin_bytes   < json_bytes
        assert py_zlib_bytes  < json_bytes
        assert lumia_bytes    < json_bytes
        if RUST_AVAILABLE:
            assert rs_bin_bytes  < json_bytes
            assert rs_zlib_bytes < json_bytes


# ---------------------------------------------------------------------------
# Speed measurements
# ---------------------------------------------------------------------------

class TestSpeedMeasurements:
    """
    Measure median encode/decode latency over 50 iterations.
    All LUMEN timings include construction (pool build + encode).
    """

    def test_speed_report(self, capsys):
        recs = RECORDS_1K

        # Pre-encode for decode benchmarks
        json_enc    = _json_encode(recs)
        pickle_enc  = _pickle_encode(recs)
        ld_bin      = LumenDict(recs).encode_binary_pooled()
        lumia_enc   = encode_lumen_llm(recs)

        # Encode timings
        t_json_enc   = _timeit(lambda: _json_encode(recs))
        t_pickle_enc = _timeit(lambda: _pickle_encode(recs))
        t_py_text    = _timeit(lambda: LumenDict(recs).encode_text())
        t_py_bin     = _timeit(lambda: LumenDict(recs).encode_binary_pooled())
        t_py_zlib    = _timeit(lambda: LumenDict(recs).encode_binary_zlib(6))
        t_lumia_enc  = _timeit(lambda: encode_lumen_llm(recs))

        # Decode timings
        t_json_dec   = _timeit(lambda: _json_decode(json_enc))
        t_pickle_dec = _timeit(lambda: _pickle_decode(pickle_enc))
        t_py_bin_dec = _timeit(lambda: decode_binary_records(ld_bin))
        t_lumia_dec  = _timeit(lambda: decode_lumen_llm(lumia_enc))

        if RUST_AVAILABLE:
            rs_bin     = LumenDictRust(recs).encode_binary_pooled()
            rs_lumia   = LumenDictRust(recs).encode_lumen_llm()
            t_rs_text  = _timeit(lambda: LumenDictRust(recs).encode_text())
            t_rs_bin   = _timeit(lambda: LumenDictRust(recs).encode_binary_pooled())
            t_rs_zlib  = _timeit(lambda: LumenDictRust(recs).encode_binary_zlib(6))
            t_rs_lumia = _timeit(lambda: LumenDictRust(recs).encode_lumen_llm())
            t_rs_bin_dec   = _timeit(lambda: decode_binary_records(rs_bin))
            t_rs_lumia_dec = _timeit(lambda: decode_lumen_llm(rs_lumia))
        else:
            t_rs_text = t_rs_bin = t_rs_zlib = t_rs_lumia = 0.0
            t_rs_bin_dec = t_rs_lumia_dec = 0.0

        print("\n" + "=" * 60)
        print("LUMEN V1 — Speed Benchmark (n=1,000 records, median 50 runs)")
        print("=" * 60)
        print(f"{'Format':<32} {'Encode ms':>12} {'Decode ms':>12}")
        print("-" * 60)
        print(f"{'JSON':.<32} {t_json_enc:>11.3f}  {t_json_dec:>11.3f}")
        print(f"{'Pickle (protocol 4)':.<32} {t_pickle_enc:>11.3f}  {t_pickle_dec:>11.3f}")
        print("-" * 60)
        print(f"{'LUMEN text (Python)':.<32} {t_py_text:>11.3f}  {'—':>11}")
        print(f"{'LUMEN binary (Python)':.<32} {t_py_bin:>11.3f}  {t_py_bin_dec:>11.3f}")
        print(f"{'LUMEN zlib-6 (Python)':.<32} {t_py_zlib:>11.3f}  {'—':>11}")
        print(f"{'LUMIA (Python)':.<32} {t_lumia_enc:>11.3f}  {t_lumia_dec:>11.3f}")
        if RUST_AVAILABLE:
            print("-" * 60)
            print(f"{'LUMEN text (Rust)':.<32} {t_rs_text:>11.3f}  {'—':>11}")
            print(f"{'LUMEN binary (Rust)':.<32} {t_rs_bin:>11.3f}  {t_rs_bin_dec:>11.3f}")
            print(f"{'LUMEN zlib-6 (Rust)':.<32} {t_rs_zlib:>11.3f}  {'—':>11}")
            print(f"{'LUMIA (Rust)':.<32} {t_rs_lumia:>11.3f}  {t_rs_lumia_dec:>11.3f}")
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
            't_py_zlib': t_py_zlib, 't_lumia_enc': t_lumia_enc,
            't_lumia_dec': t_lumia_dec, 't_py_bin_dec': t_py_bin_dec,
            't_rs_text': t_rs_text, 't_rs_bin': t_rs_bin,
            't_rs_zlib': t_rs_zlib, 't_rs_lumia': t_rs_lumia,
            't_rs_bin_dec': t_rs_bin_dec, 't_rs_lumia_dec': t_rs_lumia_dec,
        }

        # Sanity assertions — not speed guarantees, just smoke checks
        assert t_py_bin < 10_000
        assert t_json_enc < 10_000
        if RUST_AVAILABLE:
            assert t_rs_bin < t_py_bin  # Rust must be faster
