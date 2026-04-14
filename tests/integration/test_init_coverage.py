"""
Coverage tests for ulmen/__init__.py Rust decoder wrapper (L142-147).

The decode_binary_records re-exported from ulmen (not ulmen.core) is
the Rust-accelerated wrapper when Rust is compiled. These tests call it
directly to cover both branches of the unwrap logic.
"""
import ulmen
from tests.conftest import make_record
from ulmen.core import (
    UlmenDict,
    build_pool,
    encode_binary_records,
)

# ---------------------------------------------------------------------------
# Call ulmen.decode_binary_records (the __init__.py wrapper, not core's)
# ---------------------------------------------------------------------------

class TestInitDecodeBinaryRecords:
    """
    Drive ulmen/__init__.py L142-147:
      L142: result = _decode_binary_records_rust(data)
      L144-146: unwrap [[...]] → [...] (T_LIST path)
      L147: return result as-is (T_MATRIX / already-correct path)
    """

    def test_matrix_path_returns_list_of_dicts(self):
        """T_MATRIX path: Rust returns [{...}, ...] directly — L147 passthrough."""
        recs = [make_record(i) for i in range(10)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm, use_strategies=True)
        result = ulmen.decode_binary_records(data)
        assert isinstance(result, list)
        assert len(result) == 10
        assert isinstance(result[0], dict)

    def test_single_record_path(self):
        """Single dict record — verify result is correct via ulmen wrapper."""
        recs = [{'id': 1, 'name': 'Alice'}]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm)
        result = ulmen.decode_binary_records(data)
        # Result is either the dict directly or a list containing it
        assert result == {'id': 1, 'name': 'Alice'} or result == [{'id': 1, 'name': 'Alice'}]

    def test_large_dataset_via_ulmen_wrapper(self):
        """1000-record matrix path through ulmen.decode_binary_records."""
        recs = [make_record(i) for i in range(1000)]
        ld = UlmenDict(recs)
        data = ld.encode_binary_pooled()
        result = ulmen.decode_binary_records(data)
        assert isinstance(result, list)
        assert len(result) == 1000

    def test_empty_records_via_ulmen_wrapper(self):
        """Empty payload through ulmen.decode_binary_records."""
        data = encode_binary_records([], [], {})
        result = ulmen.decode_binary_records(data)
        assert result == [] or result is None or isinstance(result, list)

    def test_non_dict_list_via_ulmen_wrapper(self):
        """Non-dict list (T_LIST path) through ulmen.decode_binary_records."""
        data = encode_binary_records([1, 2, 3], [], {})
        result = ulmen.decode_binary_records(data)
        assert isinstance(result, list)
        assert result == [1, 2, 3]


class TestInitRustShimFunctions:
    """Covers ulmen/__init__.py lines 341-342, 348-349."""

    def test_encode_agent_payload_rust_basic(self):
        from ulmen import encode_agent_payload_rust, validate_agent_payload
        records = [
            {
                "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
                "role": "user", "turn": 1, "content": "hi",
                "tokens": 1, "flagged": False,
            }
        ]
        result = encode_agent_payload_rust(records, thread_id="t1")
        assert "ULMEN-AGENT v1" in result
        ok, _ = validate_agent_payload(result)
        assert ok is True

    def test_encode_agent_payload_rust_with_context_window(self):
        from ulmen import encode_agent_payload_rust
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
        from ulmen import encode_agent_payload_rust
        result = encode_agent_payload_rust([], thread_id="t1")
        assert "records: 0" in result

    def test_encode_agent_payload_rust_kwargs_passed(self):
        from ulmen import encode_agent_payload_rust
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
        assert "ULMEN-AGENT v1" in result

    def test_decode_agent_payload_rust_basic(self):
        from ulmen import decode_agent_payload_rust, encode_agent_payload
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
        from ulmen import decode_agent_payload_rust, encode_agent_payload
        payload = encode_agent_payload([], thread_id="t1")
        records = decode_agent_payload_rust(payload)
        assert records == []

    def test_decode_agent_payload_rust_multiple_records(self):
        from ulmen import decode_agent_payload_rust, encode_agent_payload
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
        from ulmen import decode_agent_payload_rust, encode_agent_payload_rust
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
    """Covers ulmen/core/_api.py lines 108 and 113: pool_size and record_count properties."""

    def test_pool_size_property(self):
        from ulmen.core._api import UlmenDict
        recs = [{"tag": "Engineering"}] * 10
        ld = UlmenDict(recs)
        assert ld.pool_size == len(ld._pool)
        assert isinstance(ld.pool_size, int)

    def test_pool_size_empty(self):
        from ulmen.core._api import UlmenDict
        ld = UlmenDict([])
        assert ld.pool_size == 0

    def test_record_count_property(self):
        from ulmen.core._api import UlmenDict
        recs = [{"id": i} for i in range(7)]
        ld = UlmenDict(recs)
        assert ld.record_count == 7

    def test_record_count_empty(self):
        from ulmen.core._api import UlmenDict
        ld = UlmenDict([])
        assert ld.record_count == 0

    def test_record_count_matches_len(self):
        from ulmen.core._api import UlmenDict
        recs = [{"id": i} for i in range(5)]
        ld = UlmenDict(recs)
        assert ld.record_count == len(ld)
