"""
Coverage tests for lumen/__init__.py Rust decoder wrapper (L142-147).

The decode_binary_records re-exported from lumen (not lumen.core) is
the Rust-accelerated wrapper when Rust is compiled. These tests call it
directly to cover both branches of the unwrap logic.
"""
import pytest
import lumen
from lumen.core import (
    LumenDict, LumenDictFull,
    encode_binary_records, build_pool,
    MAGIC,
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
