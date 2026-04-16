# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
# ULMEN is licensed under the Business Source License 1.1.
"""Tests for UlmenCheckpointer and UlmenSerde."""

import uuid

import pytest
from langgraph.checkpoint.memory import MemorySaver

from ulmen.ext.langgraph import UlmenCheckpointer
from ulmen.ext.langgraph._checkpointer import _ULMZ, UlmenSerde

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_checkpoint(channel_values: dict) -> dict:
    """
    Build a valid Checkpoint dict.

    channel_versions must list every key in channel_values with a version
    string. new_versions passed to put() must match so MemorySaver stores
    each blob under (thread_id, checkpoint_ns, key, version).
    """
    versions = {k: f"v{i+1}" for i, k in enumerate(channel_values)}
    return {
        "v":                1,
        "id":               str(uuid.uuid4()),
        "ts":               "2024-01-01T00:00:00Z",
        "channel_values":   channel_values,
        "channel_versions": versions,
        "versions_seen":    {},
        "updated_channels": list(channel_values.keys()),
    }


def make_config(thread_id: str = "t1") -> dict:
    return {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}


def make_meta() -> dict:
    return {"source": "input", "step": 1, "writes": {}, "parents": {}}


def put_checkpoint(saver, config, channel_values: dict):
    """Helper: build checkpoint + new_versions and call put."""
    cp           = make_checkpoint(channel_values)
    new_versions = cp["channel_versions"]   # same keys + versions
    return saver.put(config, cp, make_meta(), new_versions)


def make_inner_serde():
    from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
    return JsonPlusSerializer()


# ---------------------------------------------------------------------------
# UlmenSerde unit tests
# ---------------------------------------------------------------------------

def test_ulmen_serde_adds_magic_prefix():
    serde         = UlmenSerde(make_inner_serde(), zlib_level=6)
    type_str, raw = serde.dumps_typed({"x": 1, "y": "hello"})
    assert raw[:4] == _ULMZ


def test_ulmen_serde_roundtrip_dict():
    serde = UlmenSerde(make_inner_serde(), zlib_level=6)
    obj   = {"active": True, "score": 42, "name": "alice"}
    back  = serde.loads_typed(serde.dumps_typed(obj))
    assert back == obj


def test_ulmen_serde_roundtrip_list():
    serde = UlmenSerde(make_inner_serde(), zlib_level=6)
    obj   = [1, 2, 3, "hello", True, None]
    back  = serde.loads_typed(serde.dumps_typed(obj))
    assert back == obj


def test_ulmen_serde_roundtrip_none():
    serde = UlmenSerde(make_inner_serde(), zlib_level=6)
    back  = serde.loads_typed(serde.dumps_typed(None))
    assert back is None


def test_ulmen_serde_roundtrip_bytes():
    serde = UlmenSerde(make_inner_serde(), zlib_level=6)
    obj   = b"\x00\x01\x02\xff"
    back  = serde.loads_typed(serde.dumps_typed(obj))
    assert back == obj


def test_ulmen_serde_roundtrip_bool_false():
    serde = UlmenSerde(make_inner_serde(), zlib_level=6)
    back  = serde.loads_typed(serde.dumps_typed(False))
    assert back is False


def test_ulmen_serde_roundtrip_int():
    serde = UlmenSerde(make_inner_serde(), zlib_level=6)
    back  = serde.loads_typed(serde.dumps_typed(42))
    assert back == 42


def test_ulmen_serde_roundtrip_string():
    serde = UlmenSerde(make_inner_serde(), zlib_level=6)
    back  = serde.loads_typed(serde.dumps_typed("hello world"))
    assert back == "hello world"


def test_ulmen_serde_foreign_passthrough():
    """Values serialized by inner serde only (no ULMZ) must round-trip."""
    inner = make_inner_serde()
    serde = UlmenSerde(inner, zlib_level=6)
    typed = inner.dumps_typed({"plain": "value"})
    back  = serde.loads_typed(typed)
    assert back == {"plain": "value"}


def test_ulmen_serde_repr():
    serde = UlmenSerde(make_inner_serde())
    assert "UlmenSerde" in repr(serde)


def test_ulmen_serde_compressed_smaller_than_raw():
    inner = make_inner_serde()
    serde = UlmenSerde(inner, zlib_level=6)
    obj   = {"key": "value " * 100, "repeat": list(range(50))}
    _, raw_bytes        = inner.dumps_typed(obj)
    _, compressed_bytes = serde.dumps_typed(obj)
    assert len(compressed_bytes) - 4 < len(raw_bytes)


def test_ulmen_serde_zlib_levels():
    """All valid zlib levels must produce decodable output."""
    inner = make_inner_serde()
    obj   = {"data": "x" * 200}
    for level in range(10):
        serde = UlmenSerde(inner, zlib_level=level)
        back  = serde.loads_typed(serde.dumps_typed(obj))
        assert back == obj


# ---------------------------------------------------------------------------
# UlmenCheckpointer unit tests
# ---------------------------------------------------------------------------

def test_wraps_memory_saver():
    inner = MemorySaver()
    saver = UlmenCheckpointer(inner)
    assert saver.inner is inner


def test_repr():
    saver = UlmenCheckpointer(MemorySaver())
    assert "UlmenCheckpointer" in repr(saver)
    assert "MemorySaver"       in repr(saver)


def test_inner_serde_patched():
    """After wrapping, the inner saver's serde must be UlmenSerde."""
    inner = MemorySaver()
    UlmenCheckpointer(inner)
    assert isinstance(inner.serde, UlmenSerde)


def test_zlib_level_stored():
    saver = UlmenCheckpointer(MemorySaver(), zlib_level=9)
    assert saver.zlib_level == 9


# ---------------------------------------------------------------------------
# Full put / get_tuple roundtrip — correct channel_versions + new_versions
# ---------------------------------------------------------------------------

def test_put_get_tuple_roundtrip_bool():
    inner      = MemorySaver()
    saver      = UlmenCheckpointer(inner)
    config     = make_config("thread-bool")
    new_config = put_checkpoint(saver, config, {"active": True})
    tup        = saver.get_tuple(new_config)

    assert tup is not None
    assert tup.checkpoint["channel_values"].get("active") is True


def test_put_get_tuple_roundtrip_bool_false():
    inner      = MemorySaver()
    saver      = UlmenCheckpointer(inner)
    config     = make_config("thread-bool-false")
    new_config = put_checkpoint(saver, config, {"active": False})
    tup        = saver.get_tuple(new_config)

    assert tup is not None
    assert tup.checkpoint["channel_values"].get("active") is False


def test_put_get_tuple_roundtrip_int():
    inner      = MemorySaver()
    saver      = UlmenCheckpointer(inner)
    config     = make_config("thread-int")
    new_config = put_checkpoint(saver, config, {"score": 99})
    tup        = saver.get_tuple(new_config)

    assert tup is not None
    assert tup.checkpoint["channel_values"].get("score") == 99


def test_put_get_tuple_roundtrip_string():
    inner      = MemorySaver()
    saver      = UlmenCheckpointer(inner)
    config     = make_config("thread-str")
    new_config = put_checkpoint(saver, config, {"tag": "async-test", "session": "s1"})
    tup        = saver.get_tuple(new_config)

    assert tup is not None
    cv = tup.checkpoint["channel_values"]
    assert cv.get("tag")     == "async-test"
    assert cv.get("session") == "s1"


def test_put_get_tuple_roundtrip_multiple_channels():
    inner      = MemorySaver()
    saver      = UlmenCheckpointer(inner)
    config     = make_config("thread-multi")
    new_config = put_checkpoint(saver, config, {
        "active": True, "score": 42, "name": "alice", "step": 7,
    })
    tup = saver.get_tuple(new_config)

    assert tup is not None
    cv = tup.checkpoint["channel_values"]
    assert cv.get("active") is True
    assert cv.get("score")  == 42
    assert cv.get("name")   == "alice"
    assert cv.get("step")   == 7


def test_list_yields_correct_values():
    inner  = MemorySaver()
    saver  = UlmenCheckpointer(inner)
    config = make_config("thread-list")
    put_checkpoint(saver, config, {"x": 42})

    tuples = list(saver.list(config))
    assert len(tuples) >= 1
    assert tuples[0].checkpoint["channel_values"].get("x") == 42


def test_stored_bytes_are_compressed():
    """Inner saver blobs must have ULMZ prefix."""
    inner  = MemorySaver()
    saver  = UlmenCheckpointer(inner, zlib_level=6)
    config = make_config("thread-bytes-check")
    put_checkpoint(saver, config, {"data": "hello world"})

    blobs = list(inner.blobs.values())
    assert len(blobs) >= 1
    for type_str, raw in blobs:
        if isinstance(raw, (bytes, bytearray)) and len(raw) >= 4:
            assert raw[:4] == _ULMZ


def test_multiple_puts_list_all():
    inner  = MemorySaver()
    saver  = UlmenCheckpointer(inner)
    config = make_config("thread-multi-put")
    put_checkpoint(saver, config, {"step": 1})
    put_checkpoint(saver, config, {"step": 2})

    tuples = list(saver.list(config))
    assert len(tuples) >= 2


# ---------------------------------------------------------------------------
# Async roundtrip
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_aput_aget_tuple_bool():
    inner      = MemorySaver()
    saver      = UlmenCheckpointer(inner)
    config     = make_config("thread-async-bool")
    cp         = make_checkpoint({"active": False, "tag": "async-test"})
    new_config = await saver.aput(config, cp, make_meta(), cp["channel_versions"])
    tup        = await saver.aget_tuple(new_config)

    assert tup is not None
    cv = tup.checkpoint["channel_values"]
    assert cv.get("active") is False
    assert cv.get("tag")    == "async-test"


@pytest.mark.asyncio
async def test_async_list_yields_correct_values():
    inner  = MemorySaver()
    saver  = UlmenCheckpointer(inner)
    config = make_config("thread-async-list")
    cp     = make_checkpoint({"y": 77})
    await saver.aput(config, cp, make_meta(), cp["channel_versions"])

    tuples = []
    async for tup in saver.alist(config):
        tuples.append(tup)

    assert len(tuples) >= 1
    assert tuples[0].checkpoint["channel_values"].get("y") == 77


@pytest.mark.asyncio
async def test_async_put_get_multiple_channels():
    inner      = MemorySaver()
    saver      = UlmenCheckpointer(inner)
    config     = make_config("thread-async-multi")
    cp         = make_checkpoint({"a": 1, "b": "hello", "c": True})
    new_config = await saver.aput(config, cp, make_meta(), cp["channel_versions"])
    tup        = await saver.aget_tuple(new_config)

    assert tup is not None
    cv = tup.checkpoint["channel_values"]
    assert cv.get("a") == 1
    assert cv.get("b") == "hello"
    assert cv.get("c") is True


# ---------------------------------------------------------------------------
# Delegate methods coverage (delete / prune / copy_thread)
# ---------------------------------------------------------------------------

def test_delete_thread_delegates():
    saver = UlmenCheckpointer(MemorySaver())
    saver.delete_thread("nonexistent")


def test_delete_for_runs_delegates():
    saver = UlmenCheckpointer(MemorySaver())
    with pytest.raises(NotImplementedError):
        saver.delete_for_runs([])


def test_prune_delegates():
    saver = UlmenCheckpointer(MemorySaver())
    with pytest.raises(NotImplementedError):
        saver.prune([], strategy="keep_latest")


def test_copy_thread_delegates():
    saver = UlmenCheckpointer(MemorySaver())
    with pytest.raises(NotImplementedError):
        saver.copy_thread("src", "dst")


def test_get_next_version_delegates():
    saver  = UlmenCheckpointer(MemorySaver())
    result = saver.get_next_version(None, None)
    assert result is not None


def test_serde_no_serde_attr():
    """When inner saver has no serde, UlmenCheckpointer uses serde=None."""
    inner = MemorySaver()
    # Patch getattr to return None for serde
    original_serde = inner.serde
    inner.serde    = None  # type: ignore[assignment]
    saver          = UlmenCheckpointer(inner)
    assert saver is not None
    # Restore
    inner.serde = original_serde


@pytest.mark.asyncio
async def test_adelete_thread_delegates():
    saver = UlmenCheckpointer(MemorySaver())
    await saver.adelete_thread("nonexistent")


@pytest.mark.asyncio
async def test_adelete_for_runs_delegates():
    saver = UlmenCheckpointer(MemorySaver())
    with pytest.raises(NotImplementedError):
        await saver.adelete_for_runs([])


@pytest.mark.asyncio
async def test_aprune_delegates():
    saver = UlmenCheckpointer(MemorySaver())
    with pytest.raises(NotImplementedError):
        await saver.aprune([], strategy="keep_latest")


@pytest.mark.asyncio
async def test_acopy_thread_delegates():
    saver = UlmenCheckpointer(MemorySaver())
    with pytest.raises(NotImplementedError):
        await saver.acopy_thread("src", "dst")


# ---------------------------------------------------------------------------
# Direct coverage for get(), aget(), put_writes(), aput_writes()
# ---------------------------------------------------------------------------

def test_get_returns_checkpoint():
    inner      = MemorySaver()
    saver      = UlmenCheckpointer(inner)
    config     = make_config("thread-get")
    cp         = make_checkpoint({"val": 55})
    new_config = saver.put(config, cp, make_meta(), cp["channel_versions"])
    result     = saver.get(new_config)
    assert result is not None
    assert result["channel_values"].get("val") == 55


def test_get_returns_none_missing():
    saver  = UlmenCheckpointer(MemorySaver())
    config = make_config("thread-missing")
    result = saver.get(config)
    assert result is None


def test_put_writes_delegates():
    inner  = MemorySaver()
    saver  = UlmenCheckpointer(inner)
    config = make_config("thread-pw")
    cp     = make_checkpoint({"x": 1})
    nc     = saver.put(config, cp, make_meta(), cp["channel_versions"])
    # put_writes with empty writes must not raise
    saver.put_writes(nc, [], task_id="task-1")


@pytest.mark.asyncio
async def test_aget_returns_checkpoint():
    inner      = MemorySaver()
    saver      = UlmenCheckpointer(inner)
    config     = make_config("thread-aget")
    cp         = make_checkpoint({"val": 77})
    new_config = await saver.aput(config, cp, make_meta(), cp["channel_versions"])
    result     = await saver.aget(new_config)
    assert result is not None
    assert result["channel_values"].get("val") == 77


@pytest.mark.asyncio
async def test_aget_returns_none_missing():
    saver  = UlmenCheckpointer(MemorySaver())
    config = make_config("thread-aget-missing")
    result = await saver.aget(config)
    assert result is None


@pytest.mark.asyncio
async def test_aput_writes_delegates():
    inner  = MemorySaver()
    saver  = UlmenCheckpointer(inner)
    config = make_config("thread-apw")
    cp     = make_checkpoint({"x": 2})
    nc     = await saver.aput(config, cp, make_meta(), cp["channel_versions"])
    await saver.aput_writes(nc, [], task_id="task-2")


def test_ulmen_serde_zlib_level_zero():
    """Level 0 (no compression) must still add ULMZ prefix."""
    serde         = UlmenSerde(make_inner_serde(), zlib_level=0)
    type_str, raw = serde.dumps_typed({"k": "v"})
    assert raw[:4] == _ULMZ
    back = serde.loads_typed((type_str, raw))
    assert back == {"k": "v"}
