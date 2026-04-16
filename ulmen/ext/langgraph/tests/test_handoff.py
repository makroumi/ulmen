# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
# ULMEN is licensed under the Business Source License 1.1.
"""Tests for encode_handoff / decode_handoff / ulmen_send."""

import pytest
from langgraph.types import Send

from ulmen.ext.langgraph import decode_handoff, encode_handoff, handoff_size_report, ulmen_send
from ulmen.ext.langgraph.tests.conftest import make_state


def test_encode_returns_bytes(state_2t):
    blob = encode_handoff(state_2t)
    assert isinstance(blob, bytes)
    assert blob[:4] == b"ULMH"


def test_decode_roundtrip_message_count(state_2t):
    blob   = encode_handoff(state_2t)
    result = decode_handoff(blob)
    assert len(result["messages"]) == len(state_2t["messages"])


def test_decode_roundtrip_metadata(state_2t):
    blob   = encode_handoff(state_2t)
    result = decode_handoff(blob)
    assert result.get("thread_id") == state_2t["thread_id"]
    assert result.get("agent_id")  == state_2t["agent_id"]


def test_decode_wrong_magic_raises():
    with pytest.raises(ValueError, match="ULMH"):
        decode_handoff(b"BADX" + b"\x00" * 20)


def test_decode_wrong_type_raises():
    with pytest.raises(TypeError):
        decode_handoff("not bytes")  # type: ignore


def test_ulmen_send_returns_send(state_2t):
    result = ulmen_send("child", state_2t)
    assert isinstance(result, Send)
    assert result.node == "child"
    assert "__ulmen_handoff__" in result.arg


def test_ulmen_send_decodable(state_2t):
    result = ulmen_send("child", state_2t)
    blob   = result.arg["__ulmen_handoff__"]
    state  = decode_handoff(blob)
    assert len(state["messages"]) == len(state_2t["messages"])


def test_size_report_keys(state_4t):
    report = handoff_size_report(state_4t)
    assert "json_bytes"  in report
    assert "ulmen_bytes" in report
    assert "saving_pct"  in report


def test_size_report_ulmen_smaller(state_4t):
    report = handoff_size_report(state_4t)
    assert report["ulmen_bytes"] < report["json_bytes"]
    assert report["saving_pct"] > 0


@pytest.mark.parametrize("n_turns", [1, 2, 4, 8])
def test_roundtrip_all_turn_counts(n_turns):
    state  = make_state(n_turns=n_turns)
    blob   = encode_handoff(state)
    result = decode_handoff(blob)
    assert len(result["messages"]) == len(state["messages"])


# ---------------------------------------------------------------------------
# _handoff.py coverage gaps
# ---------------------------------------------------------------------------

def test_handoff_size_report_no_messages():
    state  = {"thread_id": "t1", "active": True, "step": 1}
    report = handoff_size_report(state)
    assert report["json_bytes"]  > 0
    assert report["ulmen_bytes"] > 0
    assert isinstance(report["saving_pct"], float)


def test_ulmen_send_custom_zlib_level(state_2t):
    """ulmen_send must work with non-default zlib levels."""
    result = ulmen_send("child", state_2t, zlib_level=9)
    assert isinstance(result, Send)
    blob  = result.arg["__ulmen_handoff__"]
    state = decode_handoff(blob)
    assert len(state["messages"]) == len(state_2t["messages"])
