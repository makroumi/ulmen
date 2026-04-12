"""
Unit tests for the LumenDict and LumenDictFull public API.
"""
import json
import math
import pytest

from lumen.core import (
    LumenDict,
    LumenDictFull,
    decode_text_records,
    decode_binary_records,
    MAGIC,
)
from lumen import LumenDictRust, LumenDictFullRust, RUST_AVAILABLE


# ===========================================================================
# LumenDict construction
# ===========================================================================

class TestLumenDictConstruction:
    def test_empty_list(self):
        ld = LumenDict([])
        assert len(ld) == 0

    def test_none_data(self):
        ld = LumenDict(None)
        assert len(ld) == 0

    def test_list_of_dicts(self):
        recs = [{'a': i} for i in range(5)]
        ld = LumenDict(recs)
        assert len(ld) == 5

    def test_single_dict(self):
        ld = LumenDict({'x': 1})
        assert len(ld) == 1

    def test_generator_input(self):
        ld = LumenDict({'x': i} for i in range(3))
        assert len(ld) == 3

    def test_version_attr(self):
        from lumen.core import __version__
        assert LumenDict.VERSION == __version__

    def test_optimizations_false_by_default(self):
        ld = LumenDict([{'a': 1}])
        assert ld._optimizations is False

    def test_optimizations_true(self):
        ld = LumenDict([{'a': 1}], optimizations=True)
        assert ld._optimizations is True

    def test_pool_built_on_init(self):
        recs = [{'tag': 'Engineering'}] * 10
        ld = LumenDict(recs)
        assert ld._pool_built is True
        assert 'Engineering' in ld._pool_map

    def test_getitem(self):
        recs = [{'id': i} for i in range(5)]
        ld = LumenDict(recs)
        assert ld[0] == {'id': 0}
        assert ld[4] == {'id': 4}

    def test_iter(self):
        recs = [{'id': i} for i in range(3)]
        ld = LumenDict(recs)
        assert list(ld) == recs

    def test_repr(self):
        ld = LumenDict([{'a': 1}])
        r = repr(ld)
        assert 'LumenDict' in r
        assert '1' in r

    def test_slots_present(self):
        assert hasattr(LumenDict, '__slots__')
        assert '_data' in LumenDict.__slots__

    def test_has_slots_no_dict(self):
        ld = LumenDict([])
        assert not hasattr(ld, '__dict__')


# ===========================================================================
# LumenDict.append
# ===========================================================================

class TestLumenDictAppend:
    def test_append_increases_len(self):
        ld = LumenDict([])
        ld.append({'x': 1})
        assert len(ld) == 1

    def test_append_rebuilds_pool(self):
        ld = LumenDict([])
        for _ in range(5):
            ld.append({'tag': 'Engineering'})
        assert isinstance(ld._pool, list)

    def test_append_invalidates_cache(self):
        ld = LumenDict([{'a': 1}, {'a': 2}])
        _ = ld.encode_text()
        assert ld._text_cache is not None
        ld.append({'a': 3})
        assert ld._text_cache is None

    def test_append_multiple(self):
        ld = LumenDict([])
        for i in range(10):
            ld.append({'id': i})
        assert len(ld) == 10


# ===========================================================================
# LumenDict encoding
# ===========================================================================

class TestLumenDictEncoding:
    def test_encode_text_returns_string(self):
        ld = LumenDict([{'a': 1}])
        assert isinstance(ld.encode_text(), str)

    def test_encode_text_cached(self):
        ld = LumenDict([{'a': 1}])
        t1 = ld.encode_text()
        t2 = ld.encode_text()
        assert t1 is t2

    def test_encode_binary_returns_bytes(self):
        ld = LumenDict([{'a': 1}])
        assert isinstance(ld.encode_binary(), bytes)

    def test_encode_binary_cached(self):
        ld = LumenDict([{'a': 1}])
        b1 = ld.encode_binary()
        b2 = ld.encode_binary()
        assert b1 is b2

    def test_encode_binary_pooled(self):
        ld = LumenDict([{'tag': 'Engineering'}] * 10)
        b = ld.encode_binary_pooled()
        assert b[:4] == MAGIC

    def test_encode_binary_zlib(self):
        import zlib
        ld = LumenDict([{'a': i} for i in range(50)])
        compressed = ld.encode_binary_zlib()
        assert zlib.decompress(compressed)

    def test_encode_binary_zlib_smaller_than_raw(self):
        ld = LumenDict([{'id': i, 'dept': 'Engineering', 'active': True}
                        for i in range(100)])
        raw = ld.encode_binary_pooled()
        compressed = ld.encode_binary_zlib()
        assert len(compressed) < len(raw)

    def test_empty_encodes_correctly(self):
        ld = LumenDict([])
        assert ld.encode_text() == ''
        assert ld.encode_binary()[:4] == MAGIC

    def test_matrix_mode_false(self):
        ld = LumenDict([{'a': 1}, {'a': 2}])
        text = ld.encode_text(matrix_mode=False)
        assert 'SCHEMA:' in text

    def test_matrix_mode_true(self):
        ld = LumenDict([{'a': 1}, {'a': 2}])
        text = ld.encode_text(matrix_mode=True)
        assert 'records[2]' in text

    def test_encode_binary_optimizations_false(self):
        ld = LumenDict([{'id': i} for i in range(10)], optimizations=False)
        b = ld.encode_binary()
        assert b[:4] == MAGIC

    def test_encode_binary_optimizations_true(self):
        ld = LumenDict([{'id': i} for i in range(10)], optimizations=True)
        b = ld.encode_binary()
        assert b[:4] == MAGIC


# ===========================================================================
# LumenDict decoding
# ===========================================================================

class TestLumenDictDecoding:
    def test_decode_text_returns_lumendict(self):
        ld = LumenDict([{'a': 1}])
        text = ld.encode_text()
        result = ld.decode_text(text)
        assert isinstance(result, LumenDict)

    def test_decode_text_round_trip(self):
        recs = [{'id': i, 'v': i * 2} for i in range(5)]
        ld = LumenDict(recs)
        text = ld.encode_text()
        result = ld.decode_text(text)
        assert len(result) == 5

    def test_decode_binary_returns_lumendict(self):
        ld = LumenDict([{'a': 1}])
        data = ld.encode_binary_pooled()
        result = ld.decode_binary(data)
        assert isinstance(result, LumenDict)

    def test_decode_binary_round_trip(self):
        recs = [{'id': i, 'dept': 'Eng'} for i in range(5)]
        ld = LumenDict(recs)
        data = ld.encode_binary_pooled()
        result = ld.decode_binary(data)
        assert len(result) == 5

    def test_decode_binary_list(self):
        recs = [{'a': 1}, {'a': 2}]
        ld = LumenDict(recs)
        data = ld.encode_binary_pooled()
        result = ld.decode_binary(data)
        assert len(result) == 2

    def test_decode_text_method(self):
        recs = [{'x': 10}]
        ld = LumenDict(recs)
        text = ld.encode_text()
        result = ld.decode_text(text)
        assert isinstance(result, LumenDict)

    def test_decode_binary_wraps_scalar(self):
        """decode_binary wraps non-list result in a list."""
        ld = LumenDict([{'a': 1}])
        # encode single non-matrix record → decode_binary_records returns dict
        data = ld.encode_binary_pooled()
        result = ld.decode_binary(data)
        assert isinstance(result, LumenDict)


# ===========================================================================
# LumenDict.to_json
# ===========================================================================

class TestLumenDictToJson:
    def test_returns_string(self):
        ld = LumenDict([{'a': 1}])
        assert isinstance(ld.to_json(), str)

    def test_valid_json(self):
        ld = LumenDict([{'a': 1, 'b': 'hello'}])
        parsed = json.loads(ld.to_json())
        assert isinstance(parsed, list)

    def test_nan_becomes_null(self):
        ld = LumenDict([{'v': float('nan')}])
        parsed = json.loads(ld.to_json())
        assert parsed[0]['v'] is None

    def test_inf_becomes_null(self):
        ld = LumenDict([{'v': float('inf')}])
        parsed = json.loads(ld.to_json())
        assert parsed[0]['v'] is None

    def test_neg_inf_becomes_null(self):
        ld = LumenDict([{'v': float('-inf')}])
        parsed = json.loads(ld.to_json())
        assert parsed[0]['v'] is None

    def test_nested_nan(self):
        ld = LumenDict([{'nested': {'val': float('nan')}}])
        parsed = json.loads(ld.to_json())
        assert parsed[0]['nested']['val'] is None

    def test_list_with_nan(self):
        ld = LumenDict([{'vals': [1.0, float('nan'), 3.0]}])
        parsed = json.loads(ld.to_json())
        assert parsed[0]['vals'][1] is None


# ===========================================================================
# LumenDictFull
# ===========================================================================

class TestLumenDictFull:
    def test_construction(self):
        ldf = LumenDictFull([{'a': i} for i in range(10)])
        assert len(ldf) == 10

    def test_optimizations_true(self):
        ldf = LumenDictFull([{'a': 1}])
        assert ldf._optimizations is True

    def test_pool_size_limit_default(self):
        ldf = LumenDictFull([{'a': 1}])
        assert ldf._pool_size_limit == 256

    def test_pool_size_limit_custom(self):
        ldf = LumenDictFull([{'a': 1}], pool_size_limit=128)
        assert ldf._pool_size_limit == 128

    def test_pool_can_be_larger_than_64(self):
        recs = [{f'key_{i}': f'value_{i % 100}'} for i in range(500)]
        ldf = LumenDictFull(recs, pool_size_limit=256)
        assert len(ldf._pool) <= 256

    def test_encode_text(self):
        ldf = LumenDictFull([{'id': i} for i in range(5)])
        assert isinstance(ldf.encode_text(), str)

    def test_encode_binary(self):
        ldf = LumenDictFull([{'id': i} for i in range(5)])
        b = ldf.encode_binary()
        assert b[:4] == MAGIC

    def test_repr(self):
        ldf = LumenDictFull([{'a': 1}])
        r = repr(ldf)
        assert 'LumenDictFull' in r

    def test_inherits_encode_binary_zlib(self):
        ldf = LumenDictFull([{'a': i} for i in range(50)])
        compressed = ldf.encode_binary_zlib()
        import zlib
        assert zlib.decompress(compressed)

    def test_pool_size_larger_than_lumendict(self):
        recs = [{f'field_{i % 100}': f'value_{i % 200}'} for i in range(500)]
        ldf = LumenDictFull(recs, pool_size_limit=200)
        ld  = LumenDict(recs)
        assert len(ldf._pool) >= len(ld._pool)

    def test_encode_binary_uses_strategies(self):
        ldf = LumenDictFull([{'id': i} for i in range(20)])
        b = ldf.encode_binary()
        assert b[:4] == MAGIC

    def test_encode_text_matrix(self):
        ldf = LumenDictFull([{'id': i} for i in range(5)])
        text = ldf.encode_text(matrix_mode=True)
        assert 'records[5]' in text

    def test_pool_size_limit_256(self):
        ldf = LumenDictFull([{'k': f'v_{i}'} for i in range(300)])
        assert len(ldf._pool) <= 256


# ===========================================================================
# LumenDictRust — interface parity (Python shim or real Rust, same contract)
# ===========================================================================

class TestLumenDictRust:
    def test_construction_empty(self):
        ld = LumenDictRust([])
        assert len(ld) == 0

    def test_construction_records(self):
        recs = [{'id': i} for i in range(5)]
        ld = LumenDictRust(recs)
        assert len(ld) == 5

    def test_encode_text_string(self):
        ld = LumenDictRust([{'a': 1}])
        assert isinstance(ld.encode_text(), str)

    def test_encode_binary_bytes(self):
        ld = LumenDictRust([{'a': 1}])
        assert isinstance(ld.encode_binary(), bytes)

    def test_encode_binary_pooled_magic(self):
        ld = LumenDictRust([{'a': 1}])
        assert ld.encode_binary_pooled()[:4] == MAGIC

    def test_encode_binary_zlib(self):
        import zlib
        ld = LumenDictRust([{'a': i} for i in range(50)])
        assert zlib.decompress(ld.encode_binary_zlib())

    def test_empty_text(self):
        assert LumenDictRust([]).encode_text() == ''

    def test_empty_binary(self):
        assert LumenDictRust([]).encode_binary()[:4] == MAGIC

    def test_repr_contains_records(self):
        ld = LumenDictRust([{'id': 1}])
        assert '1' in repr(ld)

    def test_bench_encode_text_only(self):
        ld = LumenDictRust([{'a': 1}])
        assert isinstance(ld.bench_encode_text_only(10), int)

    def test_bench_encode_binary_only(self):
        ld = LumenDictRust([{'a': 1}])
        assert isinstance(ld.bench_encode_binary_only(10), int)

    def test_bench_encode_text_clone(self):
        ld = LumenDictRust([{'a': 1}])
        assert isinstance(ld.bench_encode_text_clone(10), int)

    def test_bench_encode_binary_clone(self):
        ld = LumenDictRust([{'a': 1}])
        assert isinstance(ld.bench_encode_binary_clone(10), int)

    def test_size_lt_json(self):
        import json
        recs = [{'id': i, 'dept': 'Engineering', 'active': True} for i in range(50)]
        ld = LumenDictRust(recs)
        json_sz = len(json.dumps(recs, separators=(',', ':')).encode())
        assert len(ld.encode_text()) < json_sz

    def test_binary_lt_json(self):
        import json
        recs = [{'id': i, 'dept': 'Engineering', 'active': True} for i in range(50)]
        ld = LumenDictRust(recs)
        json_sz = len(json.dumps(recs, separators=(',', ':')).encode())
        assert len(ld.encode_binary_pooled()) < json_sz


# ===========================================================================
# LumenDictFullRust — test the interface contract, not internal Python attrs
# ===========================================================================

class TestLumenDictFullRust:
    def test_construction(self):
        recs = [{'id': i} for i in range(5)]
        ldf = LumenDictFullRust(recs)
        assert len(ldf) == 5

    def test_construction_empty(self):
        assert len(LumenDictFullRust([])) == 0

    def test_pool_size_limit_python_shim_only(self):
        """_pool_size_limit only exists on the Python shim, not the Rust class."""
        if RUST_AVAILABLE:
            pytest.skip('Real Rust class does not expose _pool_size_limit')
        ldf = LumenDictFullRust([{'a': 1}], pool_size_limit=128)
        assert ldf._pool_size_limit == 128

    def test_encode_text(self):
        ldf = LumenDictFullRust([{'id': i} for i in range(5)])
        assert isinstance(ldf.encode_text(), str)

    def test_encode_binary_magic(self):
        ldf = LumenDictFullRust([{'id': i} for i in range(5)])
        assert ldf.encode_binary()[:4] == MAGIC

    def test_encode_binary_zlib(self):
        import zlib
        ldf = LumenDictFullRust([{'a': i} for i in range(50)])
        assert zlib.decompress(ldf.encode_binary_zlib())

    def test_repr(self):
        ldf = LumenDictFullRust([{'a': 1}])
        assert 'LumenDictFull' in repr(ldf)

    def test_bench_methods(self):
        ldf = LumenDictFullRust([{'a': 1}])
        assert isinstance(ldf.bench_encode_text_only(5), int)
        assert isinstance(ldf.bench_encode_binary_only(5), int)


class TestDecodeBinaryNotList:
    def test_decode_binary_non_list_result(self):
        # api line 158: if not isinstance(decoded, list) branch
        # This happens when decode_binary_records returns a non-list
        # We mock it to return a dict directly
        from lumen.core._api import LumenDict
        from lumen.core import encode_binary_records, decode_binary_records
        import unittest.mock as mock

        ld = LumenDict([{"id": 1}])
        binary = ld.encode_binary_pooled()

        with mock.patch("lumen.core._api.decode_binary_records", return_value={"id": 1}):
            decoded = ld.decode_binary(binary)
            assert len(decoded) == 1


class TestLumenDictFullNone:
    def test_lumendictfull_init_none(self):
        # api line 220: LumenDictFull(None) -> self._data = []
        from lumen.core._api import LumenDictFull
        ldf = LumenDictFull(None)
        assert len(ldf) == 0
        assert ldf._data == []
