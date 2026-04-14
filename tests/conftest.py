"""
Shared fixtures and test helpers for the Ulmen test suite.
"""
import json
import pickle

import pytest

from ulmen import (
    UlmenDictFullRust,
    UlmenDictRust,
)
from ulmen.core import (
    UlmenDict,
    UlmenDictFull,
)

# ---------------------------------------------------------------------------
# Helper classes (used as test data in unit/integration tests)
# ---------------------------------------------------------------------------

class CustomStr:
    """Non-standard type with __str__ for fallback encoding tests."""
    def __init__(self, val: str):
        self.val = val
    def __str__(self):
        return self.val


class WeirdStr:
    """Alternate non-standard type for encoding fallback tests."""
    def __init__(self, val: str):
        self.val = val
    def __str__(self):
        return self.val


class RustShim:
    """
    Minimal shim that mimics the UlmenDictRust interface using pure Python.
    Used to test Rust-layer contracts without requiring the Rust extension.
    """
    def __init__(self, data=None):
        self._inner = UlmenDict(data or [])

    def encode_binary_pooled_raw(self) -> bytes:
        return self._inner.encode_binary_pooled()

    def encode_binary_zlib(self, level: int = 6) -> bytes:
        return self._inner.encode_binary_zlib(level)

    def bench_encode_text_only(self, n: int = 100) -> int:
        return n

    def bench_encode_binary_only(self, n: int = 100) -> int:
        return n

    def bench_encode_text_clone(self, n: int = 100) -> int:
        return n

    def bench_encode_binary_clone(self, n: int = 100) -> int:
        return n

    def __repr__(self):
        return f"RustShim(records={len(self._inner)})"


# ---------------------------------------------------------------------------
# Record factory
# ---------------------------------------------------------------------------

def make_record(i: int) -> dict:
    """Canonical benchmark record shape used across all test modules."""
    cities  = ['New York', 'London', 'Tokyo', 'Paris', 'Berlin']
    depts   = ['Engineering', 'Marketing', 'Sales', 'HR', 'Finance']
    tags    = ['alpha', 'beta', 'gamma', 'delta']
    status  = ['active', 'inactive', 'pending']
    return {
        'id':     i,
        'name':   f'User_{i}',
        'age':    25 + (i % 40),
        'salary': 50000 + (i % 50) * 1000,
        'score':  round(85.0 + (i % 10) * 0.5, 1),
        'active': i % 3 != 0,
        'city':   cities[i % len(cities)],
        'dept':   depts[i % len(depts)],
        'tag':    tags[i % len(tags)],
        'status': status[i % len(status)],
    }


# ---------------------------------------------------------------------------
# Comparison helpers
# ---------------------------------------------------------------------------

def records_eq(a: list, b: list) -> bool:
    """
    Deep-equality check for lists of records that handles NaN correctly.
    """
    import math
    if len(a) != len(b):
        return False
    for ra, rb in zip(a, b):
        if type(ra) != type(rb):
            return False
        if isinstance(ra, dict):
            if set(ra.keys()) != set(rb.keys()):
                return False
            for k in ra:
                va, vb = ra[k], rb[k]
                if isinstance(va, float) and isinstance(vb, float):
                    if math.isnan(va) and math.isnan(vb):
                        continue
                if va != vb:
                    return False
        elif ra != rb:
            return False
    return True


def json_size(records: list) -> int:
    return len(json.dumps(records, separators=(',', ':')).encode())


def pickle_size(records: list) -> int:
    return len(pickle.dumps(records, protocol=4))


def csv_size(records: list) -> int:
    import csv
    import io
    if not records or not isinstance(records[0], dict):
        return 0
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=records[0].keys())
    w.writeheader()
    w.writerows(records)
    return len(buf.getvalue().encode())


def toml_size(records: list) -> int:
    lines = []
    for i, r in enumerate(records):
        lines.append('[[record]]')
        for k, v in r.items():
            if isinstance(v, str):
                lines.append(f'{k} = "{v}"')
            elif isinstance(v, bool):
                lines.append(f'{k} = {"true" if v else "false"}')
            else:
                lines.append(f'{k} = {v}')
    return len('\n'.join(lines).encode())


# ---------------------------------------------------------------------------
# Pytest fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def benchmark_records():
    """1 000-record realistic dataset used for size/speed assertions."""
    return [make_record(i) for i in range(1000)]


@pytest.fixture
def sample_records():
    """Small 10-record dataset for quick functional tests."""
    return [make_record(i) for i in range(10)]


@pytest.fixture
def py_ld(benchmark_records):
    return UlmenDict(benchmark_records)


@pytest.fixture
def py_ldf(benchmark_records):
    return UlmenDictFull(benchmark_records)


@pytest.fixture
def rs_ld(benchmark_records):
    return UlmenDictRust(benchmark_records)


@pytest.fixture
def rs_ldf(benchmark_records):
    return UlmenDictFullRust(benchmark_records)


@pytest.fixture
def benchmark_1k(benchmark_records):
    return benchmark_records


@pytest.fixture
def sample_5():
    return [make_record(i) for i in range(5)]


# ---------------------------------------------------------------------------
# Encoding shortcuts (used inline in tests, not fixtures)
# ---------------------------------------------------------------------------

def py_text(records):
    return UlmenDict(records).encode_text()

def rust_text(records):
    return UlmenDictRust(records).encode_text()

def py_bin(records):
    return UlmenDict(records).encode_binary_pooled()

def rust_bin(records):
    return UlmenDictRust(records).encode_binary_pooled()
