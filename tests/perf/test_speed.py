"""
Speed smoke tests: verify encode operations complete within generous time
budgets (CI-safe). Not a micro-benchmark — just ensures nothing is
catastrophically slow.
"""
import time
import pytest

from lumen.core import LumenDict, LumenDictFull
from lumen import LumenDictRust, RUST_AVAILABLE
from tests.conftest import make_record


def _timeit_ms(fn, n: int = 1) -> float:
    """Return average ms per call over n iterations."""
    t0 = time.perf_counter()
    for _ in range(n):
        fn()
    return (time.perf_counter() - t0) * 1000 / n


@pytest.fixture(scope='module')
def recs_1k():
    return [make_record(i) for i in range(1000)]


# ===========================================================================
# Smoke tests (generous 10-second budget each)
# ===========================================================================

class TestSpeedSmoke:
    def test_python_text_encode_completes(self, recs_1k):
        ld = LumenDict(recs_1k)
        ms = _timeit_ms(ld.encode_text, n=3)
        assert ms < 10_000

    def test_python_binary_encode_completes(self, recs_1k):
        ld = LumenDict(recs_1k)
        ms = _timeit_ms(ld.encode_binary_pooled, n=3)
        assert ms < 10_000

    def test_rust_text_encode_completes(self, recs_1k):
        ld = LumenDictRust(recs_1k)
        ms = _timeit_ms(ld.encode_text, n=3)
        assert ms < 10_000

    def test_rust_binary_encode_completes(self, recs_1k):
        ld = LumenDictRust(recs_1k)
        ms = _timeit_ms(ld.encode_binary_pooled, n=3)
        assert ms < 10_000

    def test_rust_speedup_when_available(self, recs_1k):
        """If Rust is compiled, it should be faster than Python."""
        if not RUST_AVAILABLE:
            pytest.skip('Rust extension not compiled')
        py_ms = _timeit_ms(lambda: LumenDict(recs_1k).encode_text(), n=5)
        rs_ms = _timeit_ms(lambda: LumenDictRust(recs_1k).encode_text(), n=5)
        assert rs_ms < py_ms   # Rust must be faster
