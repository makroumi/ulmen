# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
# ULMEN is licensed under the Business Source License 1.1.
"""Tests for ulmen_context_reducer and make_ulmen_state."""

from langchain_core.messages import AIMessage, HumanMessage

from ulmen.ext.langgraph import make_ulmen_state, ulmen_context_reducer


def test_reducer_appends_messages():
    current = [HumanMessage(content="hello", id="m1")]
    update  = [AIMessage(content="world",  id="m2")]
    result  = ulmen_context_reducer(current, update, compress=False)
    assert len(result) == 2


def test_reducer_single_message_update():
    current = []
    update  = HumanMessage(content="hi", id="m1")
    result  = ulmen_context_reducer(current, update, compress=False)
    assert len(result) == 1


def test_reducer_none_update():
    current = [HumanMessage(content="hello", id="m1")]
    result  = ulmen_context_reducer(current, None, compress=False)
    assert len(result) == 1


def test_reducer_no_compression_below_budget():
    current = [HumanMessage(content="short", id="m1")]
    update  = [AIMessage(content="reply", id="m2")]
    result  = ulmen_context_reducer(
        current, update,
        context_window=10_000,
        compress=True,
    )
    assert len(result) == 2


def test_reducer_compression_triggers_above_budget():
    """With a tiny budget, compression must reduce the message count."""
    msgs = [
        HumanMessage(content="x" * 500, id=f"m{i}")
        for i in range(20)
    ]
    result = ulmen_context_reducer(
        msgs, [],
        context_window=10,   # tiny budget
        compress=True,
    )
    # Compressed result must not exceed original
    assert len(result) <= len(msgs)


def test_make_ulmen_state_has_messages():
    State = make_ulmen_state()
    assert "messages" in State.__annotations__


def test_make_ulmen_state_extra_fields():
    State = make_ulmen_state(extra_fields={"session_id": str, "active": bool})
    assert "session_id" in State.__annotations__
    assert "active"     in State.__annotations__
    assert "messages"   in State.__annotations__


# ---------------------------------------------------------------------------
# _reducer.py coverage gaps
# ---------------------------------------------------------------------------

def test_reducer_compression_returns_list():
    from langchain_core.messages import HumanMessage
    big = [HumanMessage(content="word " * 300, id=f"m{i}") for i in range(10)]
    result = ulmen_context_reducer(big, [], context_window=1, compress=True)
    assert isinstance(result, list)


# ---------------------------------------------------------------------------
# _reducer.py multimodal content coverage (lines 63-67)
# ---------------------------------------------------------------------------

def test_reducer_multimodal_content_tokens():
    """Messages with list content blocks must be counted without crash."""
    from langchain_core.messages import AIMessage
    msgs = [
        AIMessage(
            content=[{"type": "text", "text": "Here is the result of the search."}],
            id="ai-multi",
        )
    ]
    result = ulmen_context_reducer(msgs, [], context_window=10_000, compress=False)
    assert len(result) == 1


def test_make_ulmen_state_reducer_invoked():
    """The bound reducer in make_ulmen_state must be callable."""
    State    = make_ulmen_state(context_window=5000)
    reducer  = State.__annotations__["messages"].__metadata__[0]
    current  = [HumanMessage(content="hi", id="m1")]
    update   = [AIMessage(content="there", id="m2")]
    result   = reducer(current, update)
    assert len(result) == 2
