# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
# ULMEN is licensed under the Business Source License 1.1.
"""Tests for _serializer.py — the shared encode/decode core."""

import pytest

from ulmen.ext.langgraph._serializer import (
    decode,
    encode,
    encode_for_llm,
    langgraph_state_to_ulmen_records,
    serializer_info,
    ulmen_records_to_langgraph_state,
)
from ulmen.ext.langgraph.tests.conftest import make_state


def test_encode_returns_bytes(state_2t):
    result = encode(state_2t)
    assert isinstance(result, bytes)
    assert len(result) > 0


def test_decode_returns_dict(state_2t):
    blob   = encode(state_2t)
    result = decode(blob)
    assert isinstance(result, dict)


def test_encode_decode_message_count(state_2t):
    blob    = encode(state_2t)
    result  = decode(blob)
    # 2 turns × 4 messages each
    assert len(result.get("messages", [])) == len(state_2t["messages"])


def test_encode_decode_metadata(state_2t):
    blob   = encode(state_2t)
    result = decode(blob)
    assert result.get("thread_id") == state_2t["thread_id"]
    assert result.get("agent_id")  == state_2t["agent_id"]
    assert result.get("active")    == state_2t["active"]


def test_records_include_mem_record(state_2t):
    records = langgraph_state_to_ulmen_records(state_2t)
    mem     = [r for r in records if r["type"] == "mem"]
    assert len(mem) == 1
    assert mem[0]["confidence"] == 1.0
    assert mem[0]["key"] == "agent_state"


def test_records_steps_non_decreasing(state_2t):
    records = langgraph_state_to_ulmen_records(state_2t)
    steps   = [r["step"] for r in records]
    assert steps == sorted(steps)


def test_ulmen_records_roundtrip(state_4t):
    records  = langgraph_state_to_ulmen_records(state_4t)
    restored = ulmen_records_to_langgraph_state(records)
    assert len(restored["messages"]) == len(state_4t["messages"])


def test_encode_for_llm_returns_str(state_2t):
    result = encode_for_llm(state_2t)
    assert isinstance(result, str)
    assert len(result) > 0


def test_encode_for_llm_smaller_than_json(state_4t):
    import json

    from ulmen.ext.langgraph._serializer import _msg_to_dict

    safe = dict(state_4t)
    safe["messages"] = [_msg_to_dict(m) for m in state_4t["messages"]]
    json_size  = len(json.dumps(safe))
    ulmen_size = len(encode_for_llm(state_4t))
    assert ulmen_size < json_size


def test_serializer_info():
    info = serializer_info()
    assert "rust_backed" in info
    assert "zlib_level"  in info
    assert isinstance(info["rust_backed"], bool)


@pytest.mark.parametrize("n_turns", [1, 2, 4, 8])
def test_roundtrip_all_turn_counts(n_turns):
    state  = make_state(n_turns=n_turns)
    blob   = encode(state)
    result = decode(blob)
    assert len(result.get("messages", [])) == len(state["messages"])


def test_empty_messages_state():
    state  = {"messages": [], "thread_id": "t1", "active": False}
    blob   = encode(state)
    result = decode(blob)
    assert result.get("messages", []) == []


def test_plain_dict_messages():
    """Serializer must handle pre-converted plain dict messages."""
    from ulmen.ext.langgraph._serializer import _msg_to_dict
    state = make_state(n_turns=2)
    state["messages"] = [_msg_to_dict(m) for m in state["messages"]]
    blob   = encode(state)
    result = decode(blob)
    assert len(result["messages"]) == len(state["messages"])


# ---------------------------------------------------------------------------
# _serializer.py coverage gaps
# ---------------------------------------------------------------------------

def test_system_message_object():
    from langchain_core.messages import SystemMessage

    from ulmen.ext.langgraph._serializer import langgraph_state_to_ulmen_records
    state   = {"messages": [SystemMessage(content="Be concise.", id="s1")],
                "thread_id": "t-sys"}
    records = langgraph_state_to_ulmen_records(state)
    sys_rec = next(r for r in records if r.get("role") == "system")
    assert sys_rec["content"] == "Be concise."


def test_system_message_dict():
    from ulmen.ext.langgraph._serializer import langgraph_state_to_ulmen_records
    state   = {"messages": [{"type": "system", "id": "s1", "content": "sys"}],
                "thread_id": "t-sys-d"}
    records = langgraph_state_to_ulmen_records(state)
    sys_rec = next(r for r in records if r.get("role") == "system")
    assert sys_rec["content"] == "sys"


def test_multimodal_content_list():
    from langchain_core.messages import AIMessage

    from ulmen.ext.langgraph._serializer import langgraph_state_to_ulmen_records
    state = {
        "messages": [
            AIMessage(content=[{"type": "text", "text": "result"}], id="ai-m")
        ],
        "thread_id": "t-multi",
    }
    records = langgraph_state_to_ulmen_records(state)
    assert len(records) >= 1


def test_encode_for_llm_compress_branch():
    from ulmen.ext.langgraph import encode_for_llm
    state  = make_state(n_turns=4)
    result = encode_for_llm(state, compress=True, context_window=50)
    assert isinstance(result, str)


def test_encode_for_llm_no_compress():
    from ulmen.ext.langgraph import encode_for_llm
    state  = make_state(n_turns=2)
    result = encode_for_llm(state, compress=False)
    assert isinstance(result, str)


def test_ulmen_records_to_state_tool_record():
    from ulmen.ext.langgraph._serializer import ulmen_records_to_langgraph_state
    records = [{
        "type": "tool", "id": "tc1", "thread_id": "t1", "step": 1,
        "name": "search", "args": '{"q":"test"}', "status": "pending",
    }]
    state = ulmen_records_to_langgraph_state(records)
    assert state["messages"][0]["tool_calls"][0]["name"] == "search"


def test_ulmen_records_invalid_args_json():
    from ulmen.ext.langgraph._serializer import ulmen_records_to_langgraph_state
    records = [{
        "type": "tool", "id": "tc1", "thread_id": "t1", "step": 1,
        "name": "search", "args": "NOT_JSON", "status": "pending",
    }]
    state = ulmen_records_to_langgraph_state(records)
    assert state["messages"][0]["tool_calls"][0]["args"] == {"_raw": "NOT_JSON"}


def test_ulmen_records_invalid_meta_json():
    from ulmen.ext.langgraph._serializer import ulmen_records_to_langgraph_state
    records = [{
        "type": "mem", "id": "m1", "thread_id": "t1", "step": 1,
        "key": "agent_state", "value": "NOT_JSON",
        "confidence": 1.0, "ttl": -1,
    }]
    state = ulmen_records_to_langgraph_state(records)
    assert isinstance(state, dict)


def test_msg_to_dict_vars_fallback():
    from ulmen.ext.langgraph._serializer import _msg_to_dict
    class FakeMsg:
        def __init__(self):
            self.content = "hello"
            self.type    = "human"
    result = _msg_to_dict(FakeMsg())
    assert result["content"] == "hello"


def test_msg_to_dict_pydantic_v1_dict_method():
    """Object with .dict() but no .model_dump() hits the Pydantic V1 branch."""
    from ulmen.ext.langgraph._serializer import _msg_to_dict
    class PydanticV1Msg:
        def dict(self):
            return {"type": "human", "content": "v1 message"}
    result = _msg_to_dict(PydanticV1Msg())
    assert result["content"] == "v1 message"


def test_msg_type_lc_objects():
    from langchain_core.messages import (
        AIMessage,
        HumanMessage,
        SystemMessage,
        ToolMessage,
    )

    from ulmen.ext.langgraph._serializer import _msg_to_dict, _msg_type
    for msg, expected in [
        (HumanMessage(content="hi",  id="h"), "human"),
        (AIMessage(content="there",  id="a"), "ai"),
        (SystemMessage(content="s",  id="s"), "system"),
        (ToolMessage(content="r", tool_call_id="tc", id="t"), "tool"),
    ]:
        assert _msg_type(_msg_to_dict(msg), msg) == expected


def test_msg_type_class_name_fallback():
    from ulmen.ext.langgraph._serializer import _msg_type
    class HumanMessageFake:
        pass
    result = _msg_type({}, HumanMessageFake())
    assert isinstance(result, str)


def test_serializer_info_all_keys():
    from ulmen.ext.langgraph import serializer_info
    info = serializer_info()
    assert "rust_backed" in info
    assert "zlib_level"  in info
    assert "pool_limit"  in info


# ---------------------------------------------------------------------------
# __init__.py UlmenExtInfo instance coverage
# ---------------------------------------------------------------------------

def test_ulmen_ext_info_instance():
    from ulmen.ext.langgraph import UlmenExtInfo
    info = UlmenExtInfo()
    assert isinstance(repr(info), str)
    assert isinstance(info.rust_backed(), bool)
    si = info.serializer_info()
    assert "rust_backed" in si
    assert isinstance(info.version,           str)
    assert isinstance(info.langgraph_version, str)
    assert isinstance(info.langchain_version, str)


# ---------------------------------------------------------------------------
# conftest.py fixture coverage (state_8t, state_single_turn)
# ---------------------------------------------------------------------------

def test_state_8t_fixture(state_8t):
    """Exercises conftest state_8t fixture (line 53)."""
    assert len(state_8t["messages"]) == 32


def test_state_single_turn_fixture():
    """Exercises conftest make_state_single_turn (line 58)."""
    from ulmen.ext.langgraph.tests.conftest import make_state_single_turn
    state = make_state_single_turn()
    assert len(state["messages"]) == 4


def test_encode_custom_zlib_level(state_2t):
    """encode must work with non-default zlib levels."""
    from ulmen.ext.langgraph._serializer import decode, encode
    for level in [0, 1, 9]:
        blob   = encode(state_2t, zlib_level=level)
        result = decode(blob)
        assert len(result["messages"]) == len(state_2t["messages"])


def test_encode_state_no_thread_id():
    """State without thread_id must use default and not crash."""
    from ulmen.ext.langgraph._serializer import decode, encode
    state  = {"messages": [], "active": True}
    blob   = encode(state)
    result = decode(blob)
    assert result.get("active") is True
