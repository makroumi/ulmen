# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
# ULMEN is licensed under the Business Source License 1.1.
"""
Subgraph handoff helpers and ULMEN-aware Send.

LangGraph multi-agent systems pass state at two boundaries:
    1. Parent → child subgraph  (state dict serialized at the edge)
    2. Send()  API              (arbitrary payload sent to a node)

encode_handoff / decode_handoff
--------------------------------
Encode a state dict to ULMEN binary for transmission across a subgraph
boundary. Decode it back on the other side. Both functions are lossless.

    # Sending agent
    blob = encode_handoff(state)
    # blob is bytes — store in Redis, pass via HTTP, write to queue

    # Receiving agent
    state = decode_handoff(blob)

ulmen_send
----------
Drop-in for langgraph.types.Send that automatically encodes the arg
payload to ULMEN binary. The receiving node calls decode_handoff on
the arg to get the original state back.

    # Instead of:
    Send("child_agent", state)

    # Use:
    ulmen_send("child_agent", state)

    # In the child node:
    def child_node(state):
        real_state = decode_handoff(state["__ulmen_handoff__"])
        ...
"""

from __future__ import annotations

import zlib

from ulmen import UlmenDict, decode_binary_records
from ulmen.ext.langgraph._compat import Send
from ulmen.ext.langgraph._constants import DEFAULT_ZLIB_LEVEL
from ulmen.ext.langgraph._serializer import (
    langgraph_state_to_ulmen_records,
    ulmen_records_to_langgraph_state,
)

_HANDOFF_MARKER = b"ULMH"   # 4-byte magic for handoff blobs


# ---------------------------------------------------------------------------
# encode / decode
# ---------------------------------------------------------------------------

def encode_handoff(
    state: dict,
    zlib_level: int = DEFAULT_ZLIB_LEVEL,
) -> bytes:
    """
    Encode a LangGraph state dict to ULMEN binary for subgraph handoff.

    Parameters
    ----------
    state       : LangGraph state dict
    zlib_level  : zlib compression level 0-9, default 6

    Returns
    -------
    bytes — prefixed with ULMH magic for safe detection on decode
    """
    records = langgraph_state_to_ulmen_records(state)
    ud      = UlmenDict(records, optimizations=True)
    blob    = ud.encode_binary_zlib(level=zlib_level)
    return _HANDOFF_MARKER + blob


def decode_handoff(data: bytes) -> dict:
    """
    Decode a ULMEN handoff blob back to a LangGraph state dict.

    Parameters
    ----------
    data : bytes produced by encode_handoff()

    Returns
    -------
    LangGraph state dict

    Raises
    ------
    ValueError if data does not start with the ULMH magic prefix.
    """
    if not isinstance(data, (bytes, bytearray)):
        raise TypeError(f"decode_handoff expects bytes, got {type(data).__name__}")
    if data[:4] != _HANDOFF_MARKER:
        raise ValueError(
            "decode_handoff: data does not have ULMH prefix. "
            "Was this encoded with encode_handoff()?"
        )
    raw     = zlib.decompress(data[4:])
    records = decode_binary_records(raw)
    if not isinstance(records, list):  # pragma: no cover
        records = [records]
    return ulmen_records_to_langgraph_state(records)


# ---------------------------------------------------------------------------
# ulmen_send
# ---------------------------------------------------------------------------

def ulmen_send(node: str, state: dict, zlib_level: int = DEFAULT_ZLIB_LEVEL) -> Send:
    """
    ULMEN-aware drop-in for langgraph.types.Send.

    Encodes the state payload to ULMEN binary before constructing
    the Send object. The receiving node must call decode_handoff()
    on state["__ulmen_handoff__"] to recover the original state.

    Parameters
    ----------
    node        : target node name
    state       : state dict to send
    zlib_level  : compression level 0-9, default 6

    Returns
    -------
    langgraph.types.Send with arg={"__ulmen_handoff__": <bytes>}

    Example
    -------
        # Sender node
        def router(state):
            return [ulmen_send("worker", state)]

        # Worker node
        def worker(state):
            real = decode_handoff(state["__ulmen_handoff__"])
            ...
    """
    blob = encode_handoff(state, zlib_level=zlib_level)
    return Send(node, {"__ulmen_handoff__": blob})


# ---------------------------------------------------------------------------
# Size reporting helper (used in benchmarks)
# ---------------------------------------------------------------------------

def handoff_size_report(state: dict, zlib_level: int = DEFAULT_ZLIB_LEVEL) -> dict:
    """
    Return a size comparison dict for a state handoff.

    Keys
    ----
    json_bytes      : len of json.dumps(state)
    ulmen_bytes     : len of encode_handoff(state)
    saving_pct      : percentage smaller than JSON
    """
    import json

    def _serialise(s: dict) -> bytes:
        safe = dict(s)
        msgs = safe.get("messages", [])
        from ulmen.ext.langgraph._serializer import _msg_to_dict
        safe["messages"] = [_msg_to_dict(m) for m in msgs]
        return json.dumps(safe, separators=(",", ":"), default=str).encode()

    json_bytes  = len(_serialise(state))
    ulmen_bytes = len(encode_handoff(state, zlib_level=zlib_level))
    saving_pct  = round((1 - ulmen_bytes / json_bytes) * 100, 1) if json_bytes else 0.0

    return {
        "json_bytes":  json_bytes,
        "ulmen_bytes": ulmen_bytes,
        "saving_pct":  saving_pct,
    }
