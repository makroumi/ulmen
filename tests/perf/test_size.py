"""
Performance/size guarantee tests.
Lumen text/binary must be smaller than JSON, CSV, TOML, and pickle
on realistic 1 000-record datasets.
"""
import pytest

from lumen import LumenDictRust
from lumen.core import LumenDict, LumenDictFull
from tests.conftest import (
    csv_size,
    json_size,
    make_record,
    pickle_size,
    toml_size,
)


@pytest.fixture(scope='module')
def recs_1k():
    return [make_record(i) for i in range(1000)]


@pytest.fixture(scope='module')
def recs_small():
    return [make_record(i) for i in range(10)]


# ===========================================================================
# vs JSON
# ===========================================================================

class TestSizeVsJson:
    def test_py_text_lt_json_1k(self, recs_1k):
        assert len(LumenDict(recs_1k).encode_text()) < json_size(recs_1k)

    def test_rust_text_lt_json_1k(self, recs_1k):
        assert len(LumenDictRust(recs_1k).encode_text()) < json_size(recs_1k)

    def test_py_binary_lt_json_1k(self, recs_1k):
        assert len(LumenDict(recs_1k).encode_binary_pooled()) < json_size(recs_1k)

    def test_rust_binary_lt_json_1k(self, recs_1k):
        assert len(LumenDictRust(recs_1k).encode_binary_pooled()) < json_size(recs_1k)

    def test_py_text_lt_json_small(self, recs_small):
        assert len(LumenDict(recs_small).encode_text()) < json_size(recs_small)

    def test_rust_text_lt_json_small(self, recs_small):
        assert len(LumenDictRust(recs_small).encode_text()) < json_size(recs_small)

    def test_zlib_lt_json_1k(self, recs_1k):
        assert len(LumenDict(recs_1k).encode_binary_zlib()) < json_size(recs_1k)


# ===========================================================================
# vs CSV
# ===========================================================================

class TestSizeVsCsv:
    def test_py_text_lt_csv(self, recs_1k):
        assert len(LumenDict(recs_1k).encode_text()) < csv_size(recs_1k)

    def test_rust_text_lt_csv(self, recs_1k):
        assert len(LumenDictRust(recs_1k).encode_text()) < csv_size(recs_1k)

    def test_py_binary_lt_csv(self, recs_1k):
        assert len(LumenDict(recs_1k).encode_binary_pooled()) < csv_size(recs_1k)

    def test_rust_binary_lt_csv(self, recs_1k):
        assert len(LumenDictRust(recs_1k).encode_binary_pooled()) < csv_size(recs_1k)


# ===========================================================================
# vs TOML
# ===========================================================================

class TestSizeVsToml:
    def test_py_text_lt_toml(self, recs_1k):
        assert len(LumenDict(recs_1k).encode_text()) < toml_size(recs_1k)

    def test_rust_text_lt_toml(self, recs_1k):
        assert len(LumenDictRust(recs_1k).encode_text()) < toml_size(recs_1k)

    def test_py_binary_lt_toml(self, recs_1k):
        assert len(LumenDict(recs_1k).encode_binary_pooled()) < toml_size(recs_1k)


# ===========================================================================
# vs Pickle
# ===========================================================================

class TestSizeVsPickle:
    def test_py_binary_lt_pickle(self, recs_1k):
        assert len(LumenDict(recs_1k).encode_binary_pooled()) < pickle_size(recs_1k)

    def test_rust_binary_lt_pickle(self, recs_1k):
        assert len(LumenDictRust(recs_1k).encode_binary_pooled()) < pickle_size(recs_1k)

    def test_zlib_lt_pickle(self, recs_1k):
        assert len(LumenDict(recs_1k).encode_binary_zlib()) < pickle_size(recs_1k)


# ===========================================================================
# Internal ordering: zlib < binary < text (roughly)
# ===========================================================================

class TestInternalSizeOrdering:
    def test_zlib_lt_binary(self, recs_1k):
        ld = LumenDict(recs_1k)
        assert len(ld.encode_binary_zlib()) < len(ld.encode_binary_pooled())

    def test_rust_zlib_lt_binary(self, recs_1k):
        ld = LumenDictRust(recs_1k)
        assert len(ld.encode_binary_zlib()) < len(ld.encode_binary_pooled())

    def test_full_pool_binary_le_default(self, recs_1k):
        full = LumenDictFull(recs_1k).encode_binary()
        default = LumenDict(recs_1k).encode_binary_pooled()
        # Full may be slightly larger due to bigger pool header — within 10%
        assert len(full) <= len(default) * 1.10

    def test_text_size_reasonable_vs_binary(self, recs_1k):
        ld = LumenDict(recs_1k)
        text_sz = len(ld.encode_text().encode())
        bin_sz  = len(ld.encode_binary_pooled())
        # Text should be within 3× binary
        assert text_sz < bin_sz * 3
