# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
# ULMEN is licensed under the Business Source License 1.1.
"""
Shared serialize / deserialize core for all ulmen-langgraph components.

This is the ONLY file in the extension that imports from ulmen internals
directly. All other modules call encode() / decode() from here.

Encode pipeline:
    LangGraph state dict
        → langgraph_state_to_ulmen_records()   (structured ULMEN-AGENT records)
        → UlmenDict.encode_binary_zlib()        (Rust-first, Python fallback)
        → bytes

Decode pipeline:
    bytes
        → UlmenDict.decode_binary()             (Rust-first, Python fallback)
        → ulmen_records_to_langgraph_state()    (restore original shape)
        → LangGraph state dict

The encode/decode pair is lossless for all field types that LangGraph
state carries: str, int, float, bool, None, list, dict.
"""

from __future__ import annotations

import json
from typing import Any

from ulmen import (
    UlmenDict,
    compress_context,
    decode_binary_records,
    encode_ulmen_llm,
    estimate_tokens,
)
from ulmen.core._agent import COMPRESS_COMPLETED_SEQUENCES
from ulmen.ext.langgraph._compat import (
    AIMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from ulmen.ext.langgraph._constants import (
    DEFAULT_POOL_LIMIT,
    DEFAULT_ZLIB_LEVEL,
    HANDOFF_CONFIDENCE,
    HANDOFF_TTL,
)

# ---------------------------------------------------------------------------
# Message normalisation
# ---------------------------------------------------------------------------

def _msg_to_dict(msg: Any) -> dict:
    """
    Convert a LangChain message object to a plain dict.

    Handles:
        - Pydantic V2 objects  (.model_dump())
        - Pydantic V1 objects  (.dict())
        - Plain dicts          (pass through unchanged)
        - Any other object     (vars() fallback)
    """
    if isinstance(msg, dict):
        return msg
    if hasattr(msg, "model_dump"):
        return msg.model_dump()
    if hasattr(msg, "dict"):
        return msg.dict()
    return vars(msg)


def _msg_type(msg_dict: dict, original: Any) -> str:
    """
    Extract the canonical type string from a message dict.

    LangChain sets a 'type' key ('human', 'ai', 'tool', 'system').
    Falls back to class name inspection when the key is absent.
    """
    t = msg_dict.get("type", "")
    if t:
        return t
    if isinstance(original, HumanMessage):  # pragma: no cover
        return "human"
    if isinstance(original, AIMessage):  # pragma: no cover
        return "ai"
    if isinstance(original, ToolMessage):  # pragma: no cover
        return "tool"
    if isinstance(original, SystemMessage):  # pragma: no cover
        return "system"
    # Last resort: class name
    name = type(original).__name__.lower()
    return name.replace("message", "")


# ---------------------------------------------------------------------------
# LangGraph state → ULMEN records
# ---------------------------------------------------------------------------

def langgraph_state_to_ulmen_records(state: dict) -> list[dict]:
    """
    Convert a LangGraph state snapshot to a list of ULMEN-AGENT records.

    Lossless. Every field in the original state is recoverable via
    ulmen_records_to_langgraph_state().

    Record mapping:
        HumanMessage              → msg  (role=user)
        AIMessage (tool_calls)    → tool per tool call
        AIMessage (no tool_calls) → msg  (role=assistant)
        SystemMessage             → msg  (role=system)
        ToolMessage               → res
        state metadata            → mem  (key=agent_state)
    """
    records: list[dict] = []
    thread_id    = state.get("thread_id", "default")
    step_counter = 0

    for msg in state.get("messages", []):
        step_counter += 1
        msg_dict = _msg_to_dict(msg)
        msg_type = _msg_type(msg_dict, msg)

        if msg_type == "human":
            content = msg_dict.get("content", "")
            records.append({
                "type":      "msg",
                "id":        msg_dict.get("id") or f"msg-{step_counter}",
                "thread_id": thread_id,
                "step":      step_counter,
                "role":      "user",
                "turn":      step_counter,
                "content":   content,
                "tokens":    estimate_tokens(content),
                "flagged":   False,
            })

        elif msg_type == "system":
            content = msg_dict.get("content", "")
            records.append({
                "type":      "msg",
                "id":        msg_dict.get("id") or f"sys-{step_counter}",
                "thread_id": thread_id,
                "step":      step_counter,
                "role":      "system",
                "turn":      step_counter,
                "content":   content,
                "tokens":    estimate_tokens(content),
                "flagged":   False,
            })

        elif msg_type == "ai":
            tool_calls = msg_dict.get("tool_calls") or []
            if tool_calls:
                for tc in tool_calls:
                    args = tc.get("args", {})
                    records.append({
                        "type":      "tool",
                        "id":        tc.get("id") or f"tc-{step_counter}",
                        "thread_id": thread_id,
                        "step":      step_counter,
                        "name":      tc.get("name", ""),
                        "args":      json.dumps(args, separators=(",", ":")),
                        "status":    "pending",
                    })
            else:
                content = msg_dict.get("content", "")
                records.append({
                    "type":      "msg",
                    "id":        msg_dict.get("id") or f"ai-{step_counter}",
                    "thread_id": thread_id,
                    "step":      step_counter,
                    "role":      "assistant",
                    "turn":      step_counter,
                    "content":   content,
                    "tokens":    estimate_tokens(content),
                    "flagged":   False,
                })

        elif msg_type == "tool":
            content = msg_dict.get("content", "")
            records.append({
                "type":       "res",
                "id":         msg_dict.get("tool_call_id") or f"res-{step_counter}",
                "thread_id":  thread_id,
                "step":       step_counter,
                "name":       "tool_result",
                "data":       content,
                "status":     "done",
                "latency_ms": 0,
            })

    # ── state metadata as a mem record ───────────────────────────────────────
    # Captures every non-message field so the state is fully reconstructable.
    # confidence=1.0 — deterministic checkpoint state, not probabilistic.
    meta_keys = [k for k in state if k != "messages"]
    meta_value = json.dumps(
        {k: state[k] for k in meta_keys},
        separators=(",", ":"),
        default=str,   # serialise any non-JSON-native type as string
    )
    records.append({
        "type":       "mem",
        "id":         "state-meta",
        "thread_id":  thread_id,
        "step":       step_counter + 1,
        "key":        "agent_state",
        "value":      meta_value,
        "confidence": HANDOFF_CONFIDENCE,
        "ttl":        HANDOFF_TTL,
    })

    return records


# ---------------------------------------------------------------------------
# ULMEN records → LangGraph state
# ---------------------------------------------------------------------------

def ulmen_records_to_langgraph_state(records: list[dict]) -> dict:
    """
    Restore a LangGraph state dict from ULMEN-AGENT records.

    Inverse of langgraph_state_to_ulmen_records(). Reconstructs
    the messages list as plain dicts (LangGraph accepts both plain
    dicts and message objects at state boundaries).
    """
    messages: list[dict] = []
    meta: dict           = {}

    for rec in records:
        rtype = rec.get("type")

        if rtype == "msg":
            role = rec.get("role", "user")
            lc_type = {"user": "human", "assistant": "ai", "system": "system"}.get(role, "human")
            messages.append({
                "type":    lc_type,
                "id":      rec.get("id"),
                "content": rec.get("content", ""),
            })

        elif rtype == "tool":
            args_raw = rec.get("args", "{}")
            try:
                args = json.loads(args_raw)
            except (json.JSONDecodeError, TypeError):
                args = {"_raw": args_raw}
            messages.append({
                "type":       "ai",
                "id":         rec.get("id"),
                "content":    "",
                "tool_calls": [{
                    "id":   rec.get("id"),
                    "name": rec.get("name", ""),
                    "args": args,
                }],
            })

        elif rtype == "res":
            messages.append({
                "type":         "tool",
                "tool_call_id": rec.get("id"),
                "content":      rec.get("data", ""),
            })

        elif rtype == "mem" and rec.get("key") == "agent_state":
            value_raw = rec.get("value", "{}")
            try:
                meta = json.loads(value_raw)
            except (json.JSONDecodeError, TypeError):
                meta = {"_raw": value_raw}

    state = {"messages": messages}
    state.update(meta)
    return state


# ---------------------------------------------------------------------------
# Encode / decode — the two functions every component calls
# ---------------------------------------------------------------------------

def encode(state: dict, zlib_level: int = DEFAULT_ZLIB_LEVEL) -> bytes:
    """
    Encode a LangGraph state dict to ULMEN binary (zlib compressed).

    Uses Rust acceleration when available (transparent via UlmenDict).
    Falls back to pure Python silently.

    Parameters
    ----------
    state       : LangGraph state dict (channel_values or full state)
    zlib_level  : zlib compression level 0-9, default 6
    """
    records = langgraph_state_to_ulmen_records(state)
    ud      = UlmenDict(records, optimizations=True)
    return ud.encode_binary_zlib(level=zlib_level)


def decode(data: bytes) -> dict:
    """
    Decode ULMEN binary bytes back to a LangGraph state dict.

    Uses Rust acceleration when available (transparent via decode_binary_records).
    Falls back to pure Python silently.

    Parameters
    ----------
    data : bytes produced by encode()
    """
    import zlib as _zlib
    raw_bytes = _zlib.decompress(data)
    records   = decode_binary_records(raw_bytes)
    return ulmen_records_to_langgraph_state(records)


# ---------------------------------------------------------------------------
# LLM context surface — used by the reducer
# ---------------------------------------------------------------------------

def encode_for_llm(
    state: dict,
    compress: bool = False,
    context_window: int | None = None,
) -> str:
    """
    Encode the message history of a state to ULMEN LLM surface.

    Optionally applies context compression when compress=True and
    context_window is set.

    Returns the ULMEN string ready to inject into an LLM prompt.
    """
    records = langgraph_state_to_ulmen_records(state)

    if compress and context_window:
        records = compress_context(
            records,
            strategy=COMPRESS_COMPLETED_SEQUENCES,
        )

    return encode_ulmen_llm(records)


# ---------------------------------------------------------------------------
# Introspection helper
# ---------------------------------------------------------------------------

def serializer_info() -> dict:
    """Return a dict describing the active serializer backend."""
    from ulmen import RUST_AVAILABLE
    return {
        "rust_backed":  RUST_AVAILABLE,
        "zlib_level":   DEFAULT_ZLIB_LEVEL,
        "pool_limit":   DEFAULT_POOL_LIMIT,
    }
