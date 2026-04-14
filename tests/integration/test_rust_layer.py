"""
Integration tests for the Rust extension layer.
All tests run regardless of whether Rust is compiled — the Python shim
satisfies the same interface contract.
"""
import pytest

from tests.conftest import RustShim, make_record
from ulmen import RUST_AVAILABLE, UlmenDictFullRust, UlmenDictRust
from ulmen.core import MAGIC, UlmenDict, UlmenDictFull, decode_binary_records

# ===========================================================================
# Interface contract (both Rust and shim must satisfy)
# ===========================================================================

class TestRustInterface:
    @pytest.fixture(params=['rust', 'shim'])
    def impl(self, request):
        recs = [make_record(i) for i in range(10)]
        if request.param == 'rust':
            return UlmenDictRust(recs)
        return RustShim(recs)

    def test_encode_binary_pooled_raw_magic(self, impl):
        assert impl.encode_binary_pooled_raw()[:4] == MAGIC

    def test_encode_binary_zlib_valid(self, impl):
        import zlib
        compressed = impl.encode_binary_zlib()
        assert zlib.decompress(compressed)

    def test_bench_text_returns_int(self, impl):
        assert isinstance(impl.bench_encode_text_only(10), int)

    def test_bench_binary_returns_int(self, impl):
        assert isinstance(impl.bench_encode_binary_only(10), int)

    def test_bench_text_clone_returns_int(self, impl):
        assert isinstance(impl.bench_encode_text_clone(10), int)

    def test_bench_binary_clone_returns_int(self, impl):
        assert isinstance(impl.bench_encode_binary_clone(10), int)

    def test_repr_has_record_count(self, impl):
        assert '10' in repr(impl)


# ===========================================================================
# UlmenDictRust specific
# ===========================================================================

class TestUlmenDictRust:
    def test_available_flag_is_bool(self):
        assert isinstance(RUST_AVAILABLE, bool)

    def test_init_empty(self):
        ld = UlmenDictRust([])
        assert len(ld) == 0

    def test_len(self):
        ld = UlmenDictRust([make_record(i) for i in range(7)])
        assert len(ld) == 7

    def test_encode_text_empty(self):
        assert UlmenDictRust([]).encode_text() == ''

    def test_encode_binary_magic(self):
        ld = UlmenDictRust([{'x': 1}])
        assert ld.encode_binary()[:4] == MAGIC

    def test_encode_binary_zlib_smaller(self):
        recs = [make_record(i) for i in range(100)]
        ld = UlmenDictRust(recs)
        raw = ld.encode_binary_pooled()
        compressed = ld.encode_binary_zlib()
        assert len(compressed) < len(raw)

    def test_text_size_close_to_python(self):
        recs = [make_record(i) for i in range(50)]
        py_sz = len(UlmenDict(recs).encode_text())
        rs_sz = len(UlmenDictRust(recs).encode_text())
        assert abs(py_sz - rs_sz) / max(py_sz, rs_sz) < 0.05

    def test_binary_size_close_to_python(self):
        recs = [make_record(i) for i in range(50)]
        py_sz = len(UlmenDict(recs).encode_binary_pooled())
        rs_sz = len(UlmenDictRust(recs).encode_binary_pooled())
        assert abs(py_sz - rs_sz) / max(py_sz, rs_sz) < 0.05

    def test_decode_matches_python(self):
        recs = [make_record(i) for i in range(100)]
        py_result = decode_binary_records(UlmenDict(recs).encode_binary_pooled())
        rs_result = decode_binary_records(UlmenDictRust(recs).encode_binary_pooled())
        assert len(py_result) == len(rs_result)

    def test_repr(self):
        ld = UlmenDictRust([{'id': 1}])
        assert '1' in repr(ld)

    def test_text_non_empty_1k(self):
        recs = [make_record(i) for i in range(1000)]
        assert len(UlmenDictRust(recs).encode_text()) > 1000

    def test_binary_magic_1k(self):
        recs = [make_record(i) for i in range(1000)]
        assert UlmenDictRust(recs).encode_binary_pooled()[:4] == MAGIC

    def test_zlib_smaller_than_raw_1k(self):
        recs = [make_record(i) for i in range(1000)]
        ld = UlmenDictRust(recs)
        assert len(ld.encode_binary_zlib()) < len(ld.encode_binary_pooled())


# ===========================================================================
# UlmenDictFullRust specific
# ===========================================================================

class TestUlmenDictFullRust:
    def test_init(self):
        recs = [{'k': f'value_{i % 30}'} for i in range(200)]
        ldf = UlmenDictFullRust(recs)
        assert len(ldf) == 200

    def test_repr(self):
        ldf = UlmenDictFullRust([{'id': 1}])
        assert 'UlmenDictFull' in repr(ldf)

    def test_encode_binary_magic(self):
        ldf = UlmenDictFullRust([{'id': i} for i in range(10)])
        assert ldf.encode_binary()[:4] == MAGIC

    def test_zlib_smaller_than_raw(self):
        recs = [make_record(i) for i in range(100)]
        ldf = UlmenDictFullRust(recs)
        assert len(ldf.encode_binary_zlib()) < len(ldf.encode_binary_pooled())

    def test_text_size_within_5pct_of_python(self):
        recs = [make_record(i) for i in range(50)]
        py_sz = len(UlmenDictFull(recs).encode_text())
        rs_sz = len(UlmenDictFullRust(recs).encode_text())
        assert abs(py_sz - rs_sz) / max(py_sz, rs_sz) < 0.05
