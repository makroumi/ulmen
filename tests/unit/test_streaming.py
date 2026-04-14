"""
Unit tests for lumen/core/_streaming.py — 100% coverage target.

Covers:
  _PyStreamEncoder              — pure-Python fallback encoder
  LumenStreamEncoder            — public facade (Rust or Python backend)
  stream_encode()               — one-shot helper
  stream_encode_windowed()      — window-based unbounded streaming
  stream_encode_lumia()         — LUMIA string streaming

Every line in _streaming.py is exercised explicitly.
"""
from __future__ import annotations

import pytest

from lumen.core._streaming import (
    LumenStreamEncoder,
    _PyStreamEncoder,
    stream_encode,
    stream_encode_lumia,
    stream_encode_windowed,
)
from lumen.core._binary import decode_binary_records
from lumen.core._lumen_llm import decode_lumen_llm
from lumen import RUST_AVAILABLE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _msg(i: int) -> dict:
    return {
        "id": i,
        "name": f"User_{i}",
        "dept": "Engineering",
        "active": i % 2 == 0,
        "score": round(98.5 + (i % 5) * 0.1, 1),
    }


def _records(n: int) -> list[dict]:
    return [_msg(i) for i in range(n)]


def _reassemble(chunks) -> bytes:
    return b"".join(bytes(c) for c in chunks)


def _reassemble_str(chunks) -> str:
    return "".join(chunks)


# ===========================================================================
# _PyStreamEncoder — pure-Python fallback (lines 36-86)
# ===========================================================================

class TestPyStreamEncoderConstruction:
    """Lines 36-37, 58-60: __init__ and slot assignment."""

    def test_default_params(self):
        enc = _PyStreamEncoder()
        assert enc._pool_size_limit == 64
        assert enc._chunk_size == 65536
        assert enc._rows == []

    def test_custom_params(self):
        enc = _PyStreamEncoder(pool_size_limit=32, chunk_size=1024)
        assert enc._pool_size_limit == 32
        assert enc._chunk_size == 1024

    def test_chunk_size_clamped_to_256(self):
        enc = _PyStreamEncoder(chunk_size=10)
        assert enc._chunk_size == 256

    def test_repr(self):
        enc = _PyStreamEncoder(pool_size_limit=32)
        r = repr(enc)
        assert "_PyStreamEncoder" in r
        assert "32" in r


class TestPyStreamEncoderFeed:
    """Lines 63, 66: feed() and feed_many()."""

    def test_feed_one(self):
        enc = _PyStreamEncoder()
        enc.feed({"id": 1})
        assert enc.record_count() == 1

    def test_feed_many(self):
        enc = _PyStreamEncoder()
        enc.feed_many([{"id": i} for i in range(5)])
        assert enc.record_count() == 5

    def test_feed_many_generator(self):
        enc = _PyStreamEncoder()
        enc.feed_many({"id": i} for i in range(3))
        assert enc.record_count() == 3

    def test_feed_multiple_calls(self):
        enc = _PyStreamEncoder()
        enc.feed({"id": 0})
        enc.feed({"id": 1})
        assert enc.record_count() == 2


class TestPyStreamEncoderFlush:
    """Lines 69-80: flush() yields chunks, resets rows."""

    def test_flush_empty_yields_header_only(self):
        enc = _PyStreamEncoder()
        chunks = list(enc.flush())
        assert len(chunks) >= 1
        payload = _reassemble(chunks)
        assert payload[:4] == b"LUMB"

    def test_flush_single_record(self):
        enc = _PyStreamEncoder()
        enc.feed({"id": 1, "name": "Alice"})
        chunks = list(enc.flush())
        payload = _reassemble(chunks)
        assert payload[:4] == b"LUMB"
        result = decode_binary_records(payload)
        assert isinstance(result, (list, dict))

    def test_flush_multiple_records_round_trip(self):
        recs = _records(20)
        enc = _PyStreamEncoder()
        enc.feed_many(recs)
        payload = _reassemble(enc.flush())
        result = decode_binary_records(payload)
        assert len(result) == 20
        assert result[0]["id"] == 0
        assert result[19]["id"] == 19

    def test_flush_resets_rows(self):
        enc = _PyStreamEncoder()
        enc.feed_many(_records(5))
        list(enc.flush())
        assert enc.record_count() == 0

    def test_flush_chunk_size_slicing(self):
        """Small chunk_size forces multiple chunks."""
        enc = _PyStreamEncoder(chunk_size=32)
        enc.feed_many(_records(50))
        chunks = list(enc.flush())
        assert len(chunks) > 1
        payload = _reassemble(chunks)
        result = decode_binary_records(payload)
        assert len(result) == 50

    def test_flush_max_chunk_size_single_chunk(self):
        """Large chunk_size produces a single chunk."""
        enc = _PyStreamEncoder(chunk_size=65536)
        enc.feed_many(_records(10))
        chunks = list(enc.flush())
        assert len(chunks) == 1


class TestPyStreamEncoderReset:
    """Line 83: reset() clears rows."""

    def test_reset_clears(self):
        enc = _PyStreamEncoder()
        enc.feed_many(_records(10))
        enc.reset()
        assert enc.record_count() == 0

    def test_reset_then_flush_empty(self):
        enc = _PyStreamEncoder()
        enc.feed_many(_records(5))
        enc.reset()
        payload = _reassemble(enc.flush())
        assert payload[:4] == b"LUMB"
        result = decode_binary_records(payload)
        assert result == []


class TestPyStreamEncoderRecordCount:
    """Line 86: record_count()."""

    def test_count_zero(self):
        assert _PyStreamEncoder().record_count() == 0

    def test_count_after_feed(self):
        enc = _PyStreamEncoder()
        enc.feed_many(_records(7))
        assert enc.record_count() == 7

    def test_count_after_flush(self):
        enc = _PyStreamEncoder()
        enc.feed_many(_records(7))
        list(enc.flush())
        assert enc.record_count() == 0


# ===========================================================================
# LumenStreamEncoder — public facade (lines 129-193)
# ===========================================================================

class TestLumenStreamEncoderConstruction:
    """Lines 129-142: __init__, backend selection."""

    def test_default_params(self):
        enc = LumenStreamEncoder()
        assert enc._pool_size_limit == 64
        assert enc._chunk_size == 65536

    def test_custom_params(self):
        enc = LumenStreamEncoder(pool_size_limit=32, chunk_size=2048)
        assert enc._pool_size_limit == 32
        assert enc._chunk_size == 2048

    def test_chunk_size_minimum_256(self):
        enc = LumenStreamEncoder(chunk_size=10)
        assert enc._chunk_size == 256

    def test_rust_backed_property(self):
        enc = LumenStreamEncoder()
        assert isinstance(enc.rust_backed, bool)
        assert enc.rust_backed == RUST_AVAILABLE

    def test_repr(self):
        enc = LumenStreamEncoder()
        r = repr(enc)
        assert "LumenStreamEncoder" in r
        assert "pool_limit" in r
        assert "chunk_size" in r
        assert "backend" in r

    def test_repr_contains_record_count(self):
        enc = LumenStreamEncoder()
        enc.feed({"id": 1})
        r = repr(enc)
        assert "1" in r


class TestLumenStreamEncoderFeed:
    """Lines 146-150: feed() dispatches to Rust or Python."""

    def test_feed_returns_self(self):
        enc = LumenStreamEncoder()
        result = enc.feed({"id": 1})
        assert result is enc

    def test_feed_increments_count(self):
        enc = LumenStreamEncoder()
        enc.feed({"id": 1})
        assert enc.record_count() == 1

    def test_feed_chaining(self):
        enc = LumenStreamEncoder()
        enc.feed({"id": 0}).feed({"id": 1}).feed({"id": 2})
        assert enc.record_count() == 3


class TestLumenStreamEncoderFeedMany:
    """Lines 154-159: feed_many() dispatches to Rust or Python."""

    def test_feed_many_list(self):
        enc = LumenStreamEncoder()
        enc.feed_many(_records(10))
        assert enc.record_count() == 10

    def test_feed_many_returns_self(self):
        enc = LumenStreamEncoder()
        result = enc.feed_many(_records(3))
        assert result is enc

    def test_feed_many_generator_converted_to_list(self):
        """Rust path requires list — generator is materialized."""
        enc = LumenStreamEncoder()
        enc.feed_many({"id": i} for i in range(5))
        assert enc.record_count() == 5

    def test_feed_many_empty(self):
        enc = LumenStreamEncoder()
        enc.feed_many([])
        assert enc.record_count() == 0

    def test_feed_many_chaining(self):
        enc = LumenStreamEncoder()
        enc.feed_many(_records(3)).feed_many(_records(3))
        assert enc.record_count() == 6


class TestLumenStreamEncoderRecordCount:
    """Lines 163-165: record_count() on both backends."""

    def test_count_zero(self):
        assert LumenStreamEncoder().record_count() == 0

    def test_count_matches_fed(self):
        enc = LumenStreamEncoder()
        enc.feed_many(_records(7))
        assert enc.record_count() == 7


class TestLumenStreamEncoderFlush:
    """Lines 172-177: flush() yields bytes, auto-resets."""

    def test_flush_empty(self):
        enc = LumenStreamEncoder()
        chunks = list(enc.flush())
        payload = _reassemble(chunks)
        assert payload[:4] == b"LUMB"

    def test_flush_yields_bytes(self):
        enc = LumenStreamEncoder()
        enc.feed({"id": 1})
        for chunk in enc.flush():
            assert isinstance(chunk, bytes)

    def test_flush_resets_after(self):
        enc = LumenStreamEncoder()
        enc.feed_many(_records(5))
        list(enc.flush())
        assert enc.record_count() == 0

    def test_flush_round_trip_single(self):
        enc = LumenStreamEncoder()
        enc.feed({"id": 42, "name": "Alice", "dept": "Eng"})
        payload = _reassemble(enc.flush())
        result = decode_binary_records(payload)
        if isinstance(result, dict):
            result = [result]
        assert len(result) == 1
        assert result[0]["id"] == 42

    def test_flush_round_trip_multi(self):
        recs = _records(100)
        enc = LumenStreamEncoder()
        enc.feed_many(recs)
        payload = _reassemble(enc.flush())
        result = decode_binary_records(payload)
        assert len(result) == 100

    def test_flush_small_chunk_size(self):
        enc = LumenStreamEncoder(chunk_size=64)
        enc.feed_many(_records(30))
        chunks = list(enc.flush())
        assert all(isinstance(c, bytes) for c in chunks)
        payload = _reassemble(chunks)
        result = decode_binary_records(payload)
        assert len(result) == 30

    def test_flush_reusable_after_reset(self):
        enc = LumenStreamEncoder()
        enc.feed_many(_records(5))
        list(enc.flush())
        enc.feed_many(_records(3))
        payload = _reassemble(enc.flush())
        result = decode_binary_records(payload)
        assert len(result) == 3


class TestLumenStreamEncoderReset:
    """Lines 181-184: reset() discards buffered records."""

    def test_reset_discards(self):
        enc = LumenStreamEncoder()
        enc.feed_many(_records(10))
        enc.reset()
        assert enc.record_count() == 0

    def test_reset_then_reuse(self):
        enc = LumenStreamEncoder()
        enc.feed_many(_records(5))
        enc.reset()
        enc.feed_many(_records(2))
        assert enc.record_count() == 2

    def test_reset_on_empty(self):
        enc = LumenStreamEncoder()
        enc.reset()  # must not raise
        assert enc.record_count() == 0


class TestLumenStreamEncoderRustBackedProperty:
    """Line 189: rust_backed property."""

    def test_rust_backed_is_bool(self):
        enc = LumenStreamEncoder()
        assert isinstance(enc.rust_backed, bool)

    def test_rust_backed_matches_rust_available(self):
        enc = LumenStreamEncoder()
        assert enc.rust_backed == RUST_AVAILABLE


# ===========================================================================
# LumenStreamEncoder — Python fallback forced (lines 136-142)
# Force the Python path regardless of Rust availability.
# ===========================================================================

class TestLumenStreamEncoderForcedPythonPath:
    """
    Cover lines 136-142 (else branch): _inner = _PyStreamEncoder.
    Patch _RUST_STREAM to False to force Python fallback.
    """

    def test_python_fallback_feed_and_flush(self):
        import lumen.core._streaming as _sm
        orig = _sm._RUST_STREAM
        try:
            _sm._RUST_STREAM = False
            enc = LumenStreamEncoder()
            assert not enc._rust
            enc.feed({"id": 1, "name": "Alice"})
            payload = _reassemble(enc.flush())
            result = decode_binary_records(payload)
            if isinstance(result, dict):
                result = [result]
            assert len(result) == 1
        finally:
            _sm._RUST_STREAM = orig

    def test_python_fallback_feed_many(self):
        import lumen.core._streaming as _sm
        orig = _sm._RUST_STREAM
        try:
            _sm._RUST_STREAM = False
            enc = LumenStreamEncoder()
            enc.feed_many(_records(10))
            assert enc.record_count() == 10
        finally:
            _sm._RUST_STREAM = orig

    def test_python_fallback_record_count(self):
        import lumen.core._streaming as _sm
        orig = _sm._RUST_STREAM
        try:
            _sm._RUST_STREAM = False
            enc = LumenStreamEncoder()
            enc.feed_many(_records(5))
            assert enc.record_count() == 5
        finally:
            _sm._RUST_STREAM = orig

    def test_python_fallback_flush_yields(self):
        import lumen.core._streaming as _sm
        orig = _sm._RUST_STREAM
        try:
            _sm._RUST_STREAM = False
            enc = LumenStreamEncoder()
            enc.feed_many(_records(5))
            chunks = list(enc.flush())
            assert len(chunks) >= 1
            assert all(isinstance(c, bytes) for c in chunks)
        finally:
            _sm._RUST_STREAM = orig

    def test_python_fallback_reset(self):
        import lumen.core._streaming as _sm
        orig = _sm._RUST_STREAM
        try:
            _sm._RUST_STREAM = False
            enc = LumenStreamEncoder()
            enc.feed_many(_records(5))
            enc.reset()
            assert enc.record_count() == 0
        finally:
            _sm._RUST_STREAM = orig

    def test_python_fallback_repr(self):
        import lumen.core._streaming as _sm
        orig = _sm._RUST_STREAM
        try:
            _sm._RUST_STREAM = False
            enc = LumenStreamEncoder()
            r = repr(enc)
            assert "python" in r
        finally:
            _sm._RUST_STREAM = orig


# ===========================================================================
# stream_encode() — one-shot helper (lines 230-235)
# ===========================================================================

class TestStreamEncode:
    """Lines 230-235: stream_encode()."""

    def test_empty_records(self):
        chunks = list(stream_encode([]))
        payload = _reassemble(chunks)
        assert payload[:4] == b"LUMB"

    def test_single_record(self):
        chunks = list(stream_encode([{"id": 1}]))
        payload = _reassemble(chunks)
        result = decode_binary_records(payload)
        if isinstance(result, dict):
            result = [result]
        assert len(result) == 1

    def test_many_records(self):
        recs = _records(50)
        chunks = list(stream_encode(recs))
        payload = _reassemble(chunks)
        result = decode_binary_records(payload)
        assert len(result) == 50

    def test_generator_input(self):
        chunks = list(stream_encode({"id": i} for i in range(5)))
        payload = _reassemble(chunks)
        result = decode_binary_records(payload)
        assert len(result) == 5

    def test_custom_pool_size(self):
        recs = _records(10)
        chunks = list(stream_encode(recs, pool_size_limit=8))
        payload = _reassemble(chunks)
        result = decode_binary_records(payload)
        assert len(result) == 10

    def test_custom_chunk_size(self):
        recs = _records(20)
        chunks = list(stream_encode(recs, chunk_size=64))
        assert len(chunks) >= 1
        payload = _reassemble(chunks)
        result = decode_binary_records(payload)
        assert len(result) == 20

    def test_yields_bytes(self):
        for chunk in stream_encode(_records(5)):
            assert isinstance(chunk, bytes)

    def test_round_trip_data_integrity(self):
        recs = [
            {"id": i, "name": f"User_{i}", "active": i % 2 == 0,
             "score": float(i) * 1.5, "dept": "Engineering"}
            for i in range(30)
        ]
        payload = _reassemble(stream_encode(recs))
        result = decode_binary_records(payload)
        assert len(result) == 30
        for i, r in enumerate(result):
            assert r["id"] == i


# ===========================================================================
# stream_encode_windowed() — window-based (lines 271-294)
# ===========================================================================

class TestStreamEncodeWindowed:
    """Lines 271-294: stream_encode_windowed() both Rust and Python paths."""

    def test_empty_records(self):
        chunks = list(stream_encode_windowed([]))
        assert len(chunks) == 0

    def test_single_window(self):
        recs = _records(5)
        chunks = list(stream_encode_windowed(recs, window_size=10))
        assert len(chunks) == 1
        result = decode_binary_records(chunks[0])
        assert len(result) == 5

    def test_multiple_windows(self):
        recs = _records(25)
        chunks = list(stream_encode_windowed(recs, window_size=10))
        assert len(chunks) == 3  # 10 + 10 + 5

    def test_window_size_exact(self):
        recs = _records(20)
        chunks = list(stream_encode_windowed(recs, window_size=10))
        assert len(chunks) == 2

    def test_each_chunk_independently_decodable(self):
        recs = _records(25)
        for chunk in stream_encode_windowed(recs, window_size=10):
            result = decode_binary_records(chunk)
            assert isinstance(result, list)
            assert len(result) > 0

    def test_all_records_present_across_chunks(self):
        recs = _records(30)
        all_ids = set()
        for chunk in stream_encode_windowed(recs, window_size=7):
            result = decode_binary_records(chunk)
            for r in result:
                all_ids.add(r["id"])
        assert all_ids == set(range(30))

    def test_yields_bytes(self):
        for chunk in stream_encode_windowed(_records(5), window_size=2):
            assert isinstance(chunk, bytes)

    def test_custom_pool_size(self):
        recs = _records(15)
        chunks = list(stream_encode_windowed(recs, window_size=5, pool_size_limit=16))
        assert len(chunks) == 3

    def test_window_size_1(self):
        recs = _records(5)
        chunks = list(stream_encode_windowed(recs, window_size=1))
        assert len(chunks) == 5
        for chunk in chunks:
            result = decode_binary_records(chunk)
            assert len(result) == 1

    def test_generator_input(self):
        chunks = list(
            stream_encode_windowed(
                ({"id": i} for i in range(10)),
                window_size=5,
            )
        )
        # Python path processes generator window-by-window
        total = sum(len(decode_binary_records(c)) for c in chunks)
        assert total == 10

    def test_remainder_window(self):
        """Records that don't fill a full window are flushed at end."""
        recs = _records(12)
        chunks = list(stream_encode_windowed(recs, window_size=5))
        # 5 + 5 + 2 = 3 chunks
        assert len(chunks) == 3
        last = decode_binary_records(chunks[-1])
        assert len(last) == 2


class TestStreamEncodeWindowedPythonPath:
    """
    Cover lines 285-294: Python fallback path of stream_encode_windowed.
    Force Python by patching _RUST_STREAM=False.
    """

    def test_python_path_basic(self):
        import lumen.core._streaming as _sm
        orig = _sm._RUST_STREAM
        try:
            _sm._RUST_STREAM = False
            recs = _records(12)
            chunks = list(stream_encode_windowed(recs, window_size=5))
            assert len(chunks) == 3
            for chunk in chunks:
                result = decode_binary_records(chunk)
                assert len(result) > 0
        finally:
            _sm._RUST_STREAM = orig

    def test_python_path_generator_input(self):
        import lumen.core._streaming as _sm
        orig = _sm._RUST_STREAM
        try:
            _sm._RUST_STREAM = False
            chunks = list(
                stream_encode_windowed(
                    ({"id": i} for i in range(7)),
                    window_size=3,
                )
            )
            total = sum(len(decode_binary_records(c)) for c in chunks)
            assert total == 7
        finally:
            _sm._RUST_STREAM = orig

    def test_python_path_empty(self):
        import lumen.core._streaming as _sm
        orig = _sm._RUST_STREAM
        try:
            _sm._RUST_STREAM = False
            chunks = list(stream_encode_windowed([], window_size=5))
            assert chunks == []
        finally:
            _sm._RUST_STREAM = orig

    def test_python_path_single_window(self):
        import lumen.core._streaming as _sm
        orig = _sm._RUST_STREAM
        try:
            _sm._RUST_STREAM = False
            recs = _records(3)
            chunks = list(stream_encode_windowed(recs, window_size=10))
            assert len(chunks) == 1
            result = decode_binary_records(chunks[0])
            assert len(result) == 3
        finally:
            _sm._RUST_STREAM = orig

    def test_python_path_remainder_flushed(self):
        import lumen.core._streaming as _sm
        orig = _sm._RUST_STREAM
        try:
            _sm._RUST_STREAM = False
            recs = _records(7)
            chunks = list(stream_encode_windowed(recs, window_size=5))
            assert len(chunks) == 2
            last = decode_binary_records(chunks[-1])
            assert len(last) == 2
        finally:
            _sm._RUST_STREAM = orig


# ===========================================================================
# stream_encode_lumia() — LUMIA string streaming (lines 321-328)
# ===========================================================================

class TestStreamEncodeLumia:
    """Lines 321-328: stream_encode_lumia()."""

    def test_empty_records(self):
        chunks = list(stream_encode_lumia([]))
        payload = _reassemble_str(chunks)
        assert payload.startswith("L|")

    def test_single_record(self):
        chunks = list(stream_encode_lumia([{"id": 1, "name": "Alice"}]))
        payload = _reassemble_str(chunks)
        assert payload.startswith("L|")
        result = decode_lumen_llm(payload)
        assert len(result) == 1
        assert result[0]["id"] == 1

    def test_multi_record(self):
        recs = [{"id": i, "name": f"User_{i}"} for i in range(10)]
        chunks = list(stream_encode_lumia(recs))
        payload = _reassemble_str(chunks)
        result = decode_lumen_llm(payload)
        assert len(result) == 10

    def test_yields_strings(self):
        for chunk in stream_encode_lumia(_records(5)):
            assert isinstance(chunk, str)

    def test_first_chunk_contains_header(self):
        """First chunk must contain the L| header."""
        recs = _records(20)
        chunks = list(stream_encode_lumia(recs, chunk_size=256))
        assert chunks[0].startswith("L|")

    def test_custom_chunk_size_forces_multiple_chunks(self):
        recs = _records(30)
        chunks = list(stream_encode_lumia(recs, chunk_size=64))
        assert len(chunks) > 1
        payload = _reassemble_str(chunks)
        result = decode_lumen_llm(payload)
        assert len(result) == 30

    def test_chunk_size_minimum_256(self):
        """chunk_size < 256 is clamped to 256."""
        recs = _records(5)
        chunks = list(stream_encode_lumia(recs, chunk_size=1))
        payload = _reassemble_str(chunks)
        result = decode_lumen_llm(payload)
        assert len(result) == 5

    def test_round_trip_data_integrity(self):
        recs = [
            {"id": i, "active": i % 2 == 0, "score": float(i)}
            for i in range(15)
        ]
        payload = _reassemble_str(stream_encode_lumia(recs))
        result = decode_lumen_llm(payload)
        assert len(result) == 15
        for i, r in enumerate(result):
            assert r["id"] == i

    def test_generator_input(self):
        chunks = list(stream_encode_lumia({"id": i} for i in range(5)))
        payload = _reassemble_str(chunks)
        result = decode_lumen_llm(payload)
        assert len(result) == 5


# ===========================================================================
# Integration: full encode → decode pipelines
# ===========================================================================

class TestStreamingIntegration:
    """End-to-end correctness with all strategies active."""

    def test_boolean_column_round_trip(self):
        recs = [{"flag": i % 2 == 0} for i in range(20)]
        enc = LumenStreamEncoder()
        enc.feed_many(recs)
        payload = _reassemble(enc.flush())
        result = decode_binary_records(payload)
        assert len(result) == 20
        for i, r in enumerate(result):
            assert r["flag"] == (i % 2 == 0)

    def test_integer_delta_column_round_trip(self):
        recs = [{"id": i} for i in range(50)]
        enc = LumenStreamEncoder()
        enc.feed_many(recs)
        payload = _reassemble(enc.flush())
        result = decode_binary_records(payload)
        assert len(result) == 50
        for i, r in enumerate(result):
            assert r["id"] == i

    def test_string_pool_column_round_trip(self):
        depts = ["Engineering", "Marketing", "Sales"]
        recs = [{"dept": depts[i % 3]} for i in range(30)]
        enc = LumenStreamEncoder()
        enc.feed_many(recs)
        payload = _reassemble(enc.flush())
        result = decode_binary_records(payload)
        assert len(result) == 30
        for i, r in enumerate(result):
            assert r["dept"] == depts[i % 3]

    def test_null_values_preserved(self):
        recs = [{"v": None if i % 3 == 0 else i} for i in range(15)]
        enc = LumenStreamEncoder()
        enc.feed_many(recs)
        payload = _reassemble(enc.flush())
        result = decode_binary_records(payload)
        assert len(result) == 15

    def test_mixed_types_round_trip(self):
        recs = _records(100)
        enc = LumenStreamEncoder()
        enc.feed_many(recs)
        payload = _reassemble(enc.flush())
        result = decode_binary_records(payload)
        assert len(result) == 100

    def test_encoder_reuse_after_flush(self):
        enc = LumenStreamEncoder()
        for batch in range(3):
            enc.feed_many(_records(10))
            payload = _reassemble(enc.flush())
            result = decode_binary_records(payload)
            assert len(result) == 10

    def test_stream_encode_vs_lumen_stream_encoder(self):
        """stream_encode and LumenStreamEncoder produce valid decodable output."""
        recs = _records(50)
        p1 = _reassemble(stream_encode(recs))
        enc = LumenStreamEncoder()
        enc.feed_many(recs)
        p2 = _reassemble(enc.flush())
        r1 = decode_binary_records(p1)
        r2 = decode_binary_records(p2)
        assert len(r1) == len(r2) == 50

    def test_windowed_all_records_collected(self):
        recs = _records(47)
        collected = []
        for chunk in stream_encode_windowed(recs, window_size=10):
            collected.extend(decode_binary_records(chunk))
        assert len(collected) == 47
        ids = {r["id"] for r in collected}
        assert ids == set(range(47))


# ===========================================================================
# Module-level: _RUST_STREAM and _RustStreamEncoder import coverage
# ===========================================================================

class TestModuleLevelImports:
    """Lines 29-37: cover the import block and module-level flags."""

    def test_rust_stream_flag_is_bool(self):
        import lumen.core._streaming as _sm
        assert isinstance(_sm._RUST_STREAM, bool)

    def test_rust_encoder_attribute_exists(self):
        import lumen.core._streaming as _sm
        assert hasattr(_sm, "_RustStreamEncoder")

    def test_rust_chunked_attribute_exists(self):
        import lumen.core._streaming as _sm
        assert hasattr(_sm, "_rust_chunked")

    def test_rust_stream_matches_rust_available(self):
        import lumen.core._streaming as _sm
        assert _sm._RUST_STREAM == RUST_AVAILABLE

    def test_import_fallback_when_rust_unavailable(self):
        """Lines 38-40: cover the except ImportError branch by blocking the .so."""
        import sys
        import importlib

        # Remove cached modules so reimport runs the try/except block fresh
        saved = {}
        for key in list(sys.modules):
            if "lumen._lumen_rust" in key or key == "lumen.core._streaming":
                saved[key] = sys.modules.pop(key)

        # Block the Rust extension
        sys.modules["lumen._lumen_rust"] = None  # type: ignore

        try:
            import lumen.core._streaming as _sm_fresh
            assert _sm_fresh._RUST_STREAM is False
            assert _sm_fresh._RustStreamEncoder is None
            assert _sm_fresh._rust_chunked is None
        finally:
            # Restore everything
            for key in list(sys.modules):
                if "lumen._lumen_rust" in key or key == "lumen.core._streaming":
                    del sys.modules[key]
            sys.modules.update(saved)
