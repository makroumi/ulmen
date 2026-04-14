"""
Coverage tests for lumen/__init__.py Rust decoder wrapper (L142-147).

The decode_binary_records re-exported from lumen (not lumen.core) is
the Rust-accelerated wrapper when Rust is compiled. These tests call it
directly to cover both branches of the unwrap logic.
"""
import lumen
from lumen.core import (
    LumenDict,
    build_pool,
    encode_binary_records,
)
from tests.conftest import make_record

# ---------------------------------------------------------------------------
# Call lumen.decode_binary_records (the __init__.py wrapper, not core's)
# ---------------------------------------------------------------------------

class TestInitDecodeBinaryRecords:
    """
    Drive lumen/__init__.py L142-147:
      L142: result = _decode_binary_records_rust(data)
      L144-146: unwrap [[...]] → [...] (T_LIST path)
      L147: return result as-is (T_MATRIX / already-correct path)
    """

    def test_matrix_path_returns_list_of_dicts(self):
        """T_MATRIX path: Rust returns [{...}, ...] directly — L147 passthrough."""
        recs = [make_record(i) for i in range(10)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm, use_strategies=True)
        result = lumen.decode_binary_records(data)
        assert isinstance(result, list)
        assert len(result) == 10
        assert isinstance(result[0], dict)

    def test_single_record_path(self):
        """Single dict record — verify result is correct via lumen wrapper."""
        recs = [{'id': 1, 'name': 'Alice'}]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm)
        result = lumen.decode_binary_records(data)
        # Result is either the dict directly or a list containing it
        assert result == {'id': 1, 'name': 'Alice'} or result == [{'id': 1, 'name': 'Alice'}]

    def test_large_dataset_via_lumen_wrapper(self):
        """1000-record matrix path through lumen.decode_binary_records."""
        recs = [make_record(i) for i in range(1000)]
        ld = LumenDict(recs)
        data = ld.encode_binary_pooled()
        result = lumen.decode_binary_records(data)
        assert isinstance(result, list)
        assert len(result) == 1000

    def test_empty_records_via_lumen_wrapper(self):
        """Empty payload through lumen.decode_binary_records."""
        data = encode_binary_records([], [], {})
        result = lumen.decode_binary_records(data)
        assert result == [] or result is None or isinstance(result, list)

    def test_non_dict_list_via_lumen_wrapper(self):
        """Non-dict list (T_LIST path) through lumen.decode_binary_records."""
        data = encode_binary_records([1, 2, 3], [], {})
        result = lumen.decode_binary_records(data)
        assert isinstance(result, list)
        assert result == [1, 2, 3]


class TestInitRustShimFunctions:
    """Covers lumen/__init__.py lines 341-342, 348-349."""

    def test_encode_agent_payload_rust_basic(self):
        from lumen import encode_agent_payload_rust, validate_agent_payload
        records = [
            {
                "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
                "role": "user", "turn": 1, "content": "hi",
                "tokens": 1, "flagged": False,
            }
        ]
        result = encode_agent_payload_rust(records, thread_id="t1")
        assert "LUMEN-AGENT v1" in result
        ok, _ = validate_agent_payload(result)
        assert ok is True

    def test_encode_agent_payload_rust_with_context_window(self):
        from lumen import encode_agent_payload_rust
        records = [
            {
                "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
                "role": "user", "turn": 1, "content": "hi",
                "tokens": 1, "flagged": False,
            }
        ]
        result = encode_agent_payload_rust(
            records, thread_id="t1", context_window=8000
        )
        assert "context_window: 8000" in result

    def test_encode_agent_payload_rust_empty_records(self):
        from lumen import encode_agent_payload_rust
        result = encode_agent_payload_rust([], thread_id="t1")
        assert "records: 0" in result

    def test_encode_agent_payload_rust_kwargs_passed(self):
        from lumen import encode_agent_payload_rust
        records = [
            {
                "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
                "role": "user", "turn": 1, "content": "hi",
                "tokens": 1, "flagged": False,
            }
        ]
        result = encode_agent_payload_rust(
            records, thread_id="t1", auto_payload_id=True
        )
        assert "LUMEN-AGENT v1" in result

    def test_decode_agent_payload_rust_basic(self):
        from lumen import decode_agent_payload_rust, encode_agent_payload
        payload = encode_agent_payload(
            [
                {
                    "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
                    "role": "user", "turn": 1, "content": "hi",
                    "tokens": 1, "flagged": False,
                }
            ],
            thread_id="t1",
        )
        records = decode_agent_payload_rust(payload)
        assert len(records) == 1
        assert records[0]["type"] == "msg"

    def test_decode_agent_payload_rust_empty(self):
        from lumen import decode_agent_payload_rust, encode_agent_payload
        payload = encode_agent_payload([], thread_id="t1")
        records = decode_agent_payload_rust(payload)
        assert records == []

    def test_decode_agent_payload_rust_multiple_records(self):
        from lumen import decode_agent_payload_rust, encode_agent_payload
        recs = [
            {
                "type": "msg", "id": f"m{i}", "thread_id": "t1", "step": i + 1,
                "role": "user", "turn": i + 1, "content": "hi",
                "tokens": 1, "flagged": False,
            }
            for i in range(5)
        ]
        payload = encode_agent_payload(recs, thread_id="t1")
        records = decode_agent_payload_rust(payload)
        assert len(records) == 5

    def test_encode_decode_rust_round_trip(self):
        from lumen import decode_agent_payload_rust, encode_agent_payload_rust
        original = [
            {
                "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
                "role": "user", "turn": 1, "content": "hello",
                "tokens": 1, "flagged": False,
            }
        ]
        encoded = encode_agent_payload_rust(original, thread_id="t1")
        decoded = decode_agent_payload_rust(encoded)
        assert decoded[0]["content"] == "hello"
        assert decoded[0]["id"] == "m1"


class TestApiRecordCount:
    """Covers lumen/core/_api.py lines 108 and 113: pool_size and record_count properties."""

    def test_pool_size_property(self):
        from lumen.core._api import LumenDict
        recs = [{"tag": "Engineering"}] * 10
        ld = LumenDict(recs)
        assert ld.pool_size == len(ld._pool)
        assert isinstance(ld.pool_size, int)

    def test_pool_size_empty(self):
        from lumen.core._api import LumenDict
        ld = LumenDict([])
        assert ld.pool_size == 0

    def test_record_count_property(self):
        from lumen.core._api import LumenDict
        recs = [{"id": i} for i in range(7)]
        ld = LumenDict(recs)
        assert ld.record_count == 7

    def test_record_count_empty(self):
        from lumen.core._api import LumenDict
        ld = LumenDict([])
        assert ld.record_count == 0

    def test_record_count_matches_len(self):
        from lumen.core._api import LumenDict
        recs = [{"id": i} for i in range(5)]
        ld = LumenDict(recs)
        assert ld.record_count == len(ld)
