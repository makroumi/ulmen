# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
# ULMEN is licensed under the Business Source License 1.1.
"""
UlmenStreamSink / UlmenAsyncStreamSink

Wraps graph.stream() / graph.astream() and re-encodes each emitted
event chunk to ULMEN binary (zlib) before yielding it.

Usage
-----
    # Sync
    from ulmen.ext.langgraph import UlmenStreamSink

    for chunk in UlmenStreamSink(graph.stream(input, config)):
        redis_client.publish("events", chunk)   # bytes, not JSON

    # Async
    from ulmen.ext.langgraph import UlmenAsyncStreamSink

    async for chunk in UlmenAsyncStreamSink(graph.astream(input, config)):
        await redis_client.publish("events", chunk)

    # Decode on the consumer side
    from ulmen.ext.langgraph import decode_stream_chunk

    raw   = redis_client.subscribe("events")
    event = decode_stream_chunk(raw)   # original event dict

Wire format
-----------
Each chunk is independently decodable ULMEN binary (zlib).
The event dict is encoded as a list of ULMEN records:
    - one 'mem' record carrying event metadata (event name, run_id, node)
    - one or more records from the node's output state
"""

from __future__ import annotations

import json
import zlib
from typing import Any, AsyncIterator, Iterator

from ulmen import UlmenDict, decode_binary_records
from ulmen.ext.langgraph._constants import DEFAULT_ZLIB_LEVEL, HANDOFF_CONFIDENCE, HANDOFF_TTL
from ulmen.ext.langgraph._serializer import langgraph_state_to_ulmen_records

# ---------------------------------------------------------------------------
# Event → ULMEN records
# ---------------------------------------------------------------------------

def _event_to_records(event: dict) -> list[dict]:
    """
    Convert a single graph.stream() event dict to ULMEN records.

    The event dict has the shape:
        {"event": str, "name": str, "run_id": str, "data": {"chunk": state}}
    or for node output events:
        {"node_name": state_dict}
    """
    records: list[dict] = []

    # LangGraph emits two event shapes depending on stream_mode:
    #   stream_mode="updates": {node_name: {state updates}}
    #   stream_mode="values":  full state dict
    # We handle both.

    if "event" in event:
        # Full event envelope (stream_mode="debug" or custom)
        meta_record = {
            "type":       "mem",
            "id":         f"evt-{event.get('run_id', 'unknown')}",
            "thread_id":  "stream",
            "step":       1,
            "key":        "event_meta",
            "value":      json.dumps({
                "event":  event.get("event"),
                "name":   event.get("name"),
                "run_id": event.get("run_id"),
            }, separators=(",", ":")),
            "confidence": HANDOFF_CONFIDENCE,
            "ttl":        HANDOFF_TTL,
        }
        records.append(meta_record)

        chunk = event.get("data", {}).get("chunk", {})
        if isinstance(chunk, dict) and chunk:
            records.extend(langgraph_state_to_ulmen_records(chunk))

    else:
        # updates shape: {node_name: state_dict, ...}
        for node_name, state_update in event.items():
            if not isinstance(state_update, dict):
                continue
            meta_record = {
                "type":       "mem",
                "id":         f"node-{node_name}",
                "thread_id":  "stream",
                "step":       1,
                "key":        "node",
                "value":      node_name,
                "confidence": HANDOFF_CONFIDENCE,
                "ttl":        HANDOFF_TTL,
            }
            records.append(meta_record)
            records.extend(langgraph_state_to_ulmen_records(state_update))

    return records


def _encode_event(event: dict, zlib_level: int) -> bytes:
    """Encode a stream event to ULMEN binary (zlib)."""
    records = _event_to_records(event)
    if not records:
        # Empty event — encode a single marker record
        records = [{
            "type":       "mem",
            "id":         "empty-event",
            "thread_id":  "stream",
            "step":       1,
            "key":        "empty",
            "value":      "true",
            "confidence": 1.0,
            "ttl":        -1,
        }]
    ud = UlmenDict(records, optimizations=True)
    return ud.encode_binary_zlib(level=zlib_level)


def _decode_event(data: bytes) -> list[dict]:
    """Decode a ULMEN binary chunk back to a list of records."""
    raw     = zlib.decompress(data)
    records = decode_binary_records(raw)
    return records if isinstance(records, list) else [records]


# ---------------------------------------------------------------------------
# Public decode helper
# ---------------------------------------------------------------------------

def decode_stream_chunk(data: bytes) -> list[dict]:
    """
    Decode a ULMEN binary stream chunk back to a list of records.

    Parameters
    ----------
    data : bytes produced by UlmenStreamSink or UlmenAsyncStreamSink

    Returns
    -------
    List of record dicts. The first record is always the event metadata
    mem record; subsequent records are the node output records.
    """
    return _decode_event(data)


# ---------------------------------------------------------------------------
# Sync sink
# ---------------------------------------------------------------------------

class UlmenStreamSink:
    """
    Sync wrapper for graph.stream() that re-encodes each event to
    ULMEN binary (zlib).

    Parameters
    ----------
    stream      : Iterator from graph.stream()
    zlib_level  : Compression level 0-9, default 6

    Yields
    ------
    bytes — independently decodable ULMEN binary chunk per event

    Example
    -------
        for chunk in UlmenStreamSink(graph.stream(input, config)):
            socket.sendall(chunk)

        # Consumer
        records = decode_stream_chunk(chunk)
    """

    def __init__(
        self,
        stream: Iterator[Any],
        zlib_level: int = DEFAULT_ZLIB_LEVEL,
    ) -> None:
        self._stream     = stream
        self._zlib_level = zlib_level
        self._count      = 0

    def __iter__(self) -> Iterator[bytes]:
        for event in self._stream:
            self._count += 1
            yield _encode_event(event, self._zlib_level)

    @property
    def chunks_emitted(self) -> int:
        """Number of chunks yielded so far."""
        return self._count

    def __repr__(self) -> str:
        return f"UlmenStreamSink(zlib_level={self._zlib_level})"


# ---------------------------------------------------------------------------
# Async sink
# ---------------------------------------------------------------------------

class UlmenAsyncStreamSink:
    """
    Async wrapper for graph.astream() that re-encodes each event to
    ULMEN binary (zlib).

    Parameters
    ----------
    stream      : AsyncIterator from graph.astream()
    zlib_level  : Compression level 0-9, default 6

    Yields
    ------
    bytes — independently decodable ULMEN binary chunk per event

    Example
    -------
        async for chunk in UlmenAsyncStreamSink(graph.astream(input, config)):
            await redis.publish("events", chunk)
    """

    def __init__(
        self,
        stream: AsyncIterator[Any],
        zlib_level: int = DEFAULT_ZLIB_LEVEL,
    ) -> None:
        self._stream     = stream
        self._zlib_level = zlib_level
        self._count      = 0

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[bytes]:
        async for event in self._stream:
            self._count += 1
            yield _encode_event(event, self._zlib_level)

    @property
    def chunks_emitted(self) -> int:
        return self._count

    def __repr__(self) -> str:
        return f"UlmenAsyncStreamSink(zlib_level={self._zlib_level})"
