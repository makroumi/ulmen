# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
# ULMEN is licensed under the Business Source License 1.1.
"""Tests for UlmenStore."""

import pytest
from langgraph.store.memory import InMemoryStore

from ulmen.ext.langgraph import UlmenStore


@pytest.fixture
def store():
    return UlmenStore(InMemoryStore())


def test_put_get_roundtrip(store):
    ns    = ("user", "alice")
    value = {"theme": "dark", "lang": "en", "score": 42}
    store.put(ns, "prefs", value, ttl=None)
    item = store.get(ns, "prefs")
    assert item is not None
    assert item.value["theme"] == "dark"
    assert item.value["score"] == 42


def test_get_missing_returns_none(store):
    assert store.get(("user", "nobody"), "prefs") is None


def test_delete(store):
    ns = ("user", "bob")
    store.put(ns, "k", {"v": 1}, ttl=None)
    store.delete(ns, "k")
    assert store.get(ns, "k") is None


def test_list_namespaces():
    s = UlmenStore(InMemoryStore())
    s.put(("ns", "a"), "k1", {"x": 1}, ttl=None)
    s.put(("ns", "b"), "k2", {"x": 2}, ttl=None)
    ns = s.list_namespaces(prefix=("ns",))
    assert len(ns) >= 2


def test_repr():
    store = UlmenStore(InMemoryStore())
    assert "UlmenStore" in repr(store)
    assert "InMemoryStore" in repr(store)


def test_foreign_value_passthrough():
    """
    Values not encoded by UlmenStore must pass through unchanged.
    We write directly to the inner store to simulate a foreign value.
    """
    inner = InMemoryStore()
    store = UlmenStore(inner)
    inner.put(("ns",), "k", {"plain": "value"}, ttl=None)
    item = store.get(("ns",), "k")
    assert item is not None
    assert item.value.get("plain") == "value"


@pytest.mark.asyncio
async def test_async_put_get(store):
    ns    = ("async", "user")
    value = {"data": "hello", "count": 7}
    await store.aput(ns, "key", value, ttl=None)
    item = await store.aget(ns, "key")
    assert item is not None
    assert item.value["count"] == 7


@pytest.mark.asyncio
async def test_async_delete(store):
    ns = ("async", "del")
    await store.aput(ns, "k", {"v": 99}, ttl=None)
    await store.adelete(ns, "k")
    item = await store.aget(ns, "k")
    assert item is None


# ---------------------------------------------------------------------------
# _store.py internal helpers coverage
# ---------------------------------------------------------------------------

def test_decode_value_non_dict():
    from ulmen.ext.langgraph._store import _decode_value
    assert _decode_value("string") == "string"  # type: ignore[arg-type]
    assert _decode_value(42)       == 42         # type: ignore[arg-type]
    assert _decode_value(None)     is None       # type: ignore[arg-type]


def test_decode_value_no_ulmen_key():
    from ulmen.ext.langgraph._store import _decode_value
    d = {"plain": "value"}
    assert _decode_value(d) == d


def test_decode_value_non_bytes_blob():
    from ulmen.ext.langgraph._store import _decode_value
    d = {"__ulmen__": "not bytes"}
    assert _decode_value(d) == d


def test_decode_value_wrong_marker():
    from ulmen.ext.langgraph._store import _decode_value
    d = {"__ulmen__": b"BADX" + b"\x00" * 10}
    assert _decode_value(d) == d


def test_decode_item_none():
    from ulmen.ext.langgraph._store import _decode_item
    assert _decode_item(None) is None


def test_decode_item_no_value_attr():
    from ulmen.ext.langgraph._store import _decode_item
    class NoValue:
        pass
    obj = NoValue()
    assert _decode_item(obj) is obj


def test_search_returns_decoded():
    s = UlmenStore(InMemoryStore())
    s.put(("ns",), "k1", {"score": 10}, ttl=None)
    s.put(("ns",), "k2", {"score": 20}, ttl=None)
    results = s.search(("ns",))
    assert len(results) >= 1
    for item in results:
        assert "score" in item.value





def test_batch_delegates():
    s      = UlmenStore(InMemoryStore())
    result = s.batch([])
    assert isinstance(result, list)


@pytest.mark.asyncio
async def test_asearch_returns_decoded():
    s = UlmenStore(InMemoryStore())
    await s.aput(("ns",), "k1", {"val": "hello"}, ttl=None)
    results = await s.asearch(("ns",))
    assert len(results) >= 1
    assert "val" in results[0].value


@pytest.mark.asyncio
async def test_alist_namespaces():
    s = UlmenStore(InMemoryStore())
    await s.aput(("x", "y"), "k", {"z": 1}, ttl=None)
    ns = await s.alist_namespaces(prefix=("x",))
    assert len(ns) >= 1


@pytest.mark.asyncio
async def test_abatch_delegates():
    s      = UlmenStore(InMemoryStore())
    result = await s.abatch([])
    assert isinstance(result, list)


def test_store_inner_property():
    inner = InMemoryStore()
    store = UlmenStore(inner)
    assert store.inner is inner
