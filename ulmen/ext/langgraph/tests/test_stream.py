# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
# ULMEN is licensed under the Business Source License 1.1.
"""Tests for UlmenStreamSink and UlmenAsyncStreamSink."""

import pytest

from ulmen.ext.langgraph import UlmenAsyncStreamSink, UlmenStreamSink, decode_stream_chunk
from ulmen.ext.langgraph.tests.conftest import make_state


def make_events(n: int = 3):
    """Simulate graph.stream() update-mode events."""
    return [
        {"planner":   make_state(n_turns=1)},
        {"searcher":  make_state(n_turns=2)},
        {"responder": make_state(n_turns=1)},
    ][:n]


def test_sync_sink_yields_bytes():
    events = make_events(3)
    chunks = list(UlmenStreamSink(iter(events)))
    assert len(chunks) == 3
    for chunk in chunks:
        assert isinstance(chunk, bytes)
        assert len(chunk) > 0


def test_sync_sink_chunks_decodable():
    events = make_events(3)
    for chunk in UlmenStreamSink(iter(events)):
        records = decode_stream_chunk(chunk)
        assert isinstance(records, list)
        assert len(records) > 0


def test_sync_sink_chunks_smaller_than_json():
    import json
    events     = make_events(3)
    json_total = sum(len(json.dumps(e, default=str).encode()) for e in events)
    ulmen_total = sum(len(c) for c in UlmenStreamSink(iter(events)))
    assert ulmen_total < json_total


def test_chunks_emitted_counter():
    events = make_events(3)
    sink   = UlmenStreamSink(iter(events))
    list(sink)
    assert sink.chunks_emitted == 3


def test_repr_sync():
    sink = UlmenStreamSink(iter([]))
    assert "UlmenStreamSink" in repr(sink)


@pytest.mark.asyncio
async def test_async_sink_yields_bytes():
    events = make_events(3)

    async def _gen():
        for e in events:
            yield e

    chunks = []
    async for chunk in UlmenAsyncStreamSink(_gen()):
        chunks.append(chunk)

    assert len(chunks) == 3
    for chunk in chunks:
        assert isinstance(chunk, bytes)


@pytest.mark.asyncio
async def test_async_sink_decodable():
    events = make_events(2)

    async def _gen():
        for e in events:
            yield e

    async for chunk in UlmenAsyncStreamSink(_gen()):
        records = decode_stream_chunk(chunk)
        assert len(records) > 0


def test_full_event_envelope():
    """Test the 'event' key envelope shape (stream_mode=debug)."""
    event = {
        "event":  "on_chain_stream",
        "name":   "planner",
        "run_id": "run-001",
        "data":   {"chunk": make_state(n_turns=1)},
    }
    chunks = list(UlmenStreamSink(iter([event])))
    assert len(chunks) == 1
    records = decode_stream_chunk(chunks[0])
    assert any(r.get("key") == "event_meta" for r in records if r.get("type") == "mem")


# ---------------------------------------------------------------------------
# _stream.py coverage gaps
# ---------------------------------------------------------------------------

def test_empty_event_dict_encodes():
    from ulmen.ext.langgraph import decode_stream_chunk
    from ulmen.ext.langgraph._stream import _encode_event
    chunk   = _encode_event({}, zlib_level=6)
    records = decode_stream_chunk(chunk)
    assert isinstance(records, list)
    assert len(records) >= 1


def test_event_with_non_dict_node_value():
    from ulmen.ext.langgraph._stream import _encode_event
    chunk = _encode_event({"node_name": "not a dict"}, zlib_level=6)
    assert isinstance(chunk, bytes)


@pytest.mark.asyncio
async def test_async_sink_chunks_emitted_count():
    events = [make_events(1)[0], make_events(1)[0]]

    async def _gen():
        for e in events:
            yield e

    sink   = UlmenAsyncStreamSink(_gen())
    async for _ in sink:
        pass
    assert sink.chunks_emitted == 2


def test_async_sink_repr():
    async def _empty():
        # async generator that yields nothing
        if False:  # pragma: no cover
            yield

    sink = UlmenAsyncStreamSink(_empty())
    assert "UlmenAsyncStreamSink" in repr(sink)


@pytest.mark.asyncio
async def test_async_sink_chunks_smaller_than_json():
    """Async ULMEN chunks must be smaller than raw JSON events."""
    import json
    events = make_events(3)

    async def _gen():
        for e in events:
            yield e

    json_total  = sum(len(json.dumps(e, default=str).encode()) for e in events)
    ulmen_total = 0
    async for chunk in UlmenAsyncStreamSink(_gen()):
        ulmen_total += len(chunk)

    assert ulmen_total < json_total
