# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
# ULMEN is licensed under the Business Source License 1.1.
"""
Shared fixtures for ulmen-langgraph tests.
"""

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage


def make_state(n_turns: int = 2, thread_id: str = "t1") -> dict:
    """Build a realistic LangGraph state dict with n_turns of conversation."""
    messages = []
    for i in range(1, n_turns + 1):
        tc_id = f"call_{i:04d}"
        messages.append(HumanMessage(content=f"Turn {i} question", id=f"hm-{i}"))
        messages.append(AIMessage(
            content="",
            tool_calls=[{"id": tc_id, "name": "search", "args": {"q": f"topic {i}"}}],
            id=f"ai-tc-{i}",
        ))
        messages.append(ToolMessage(
            content=f"Result for topic {i}",
            tool_call_id=tc_id,
            id=f"tm-{i}",
        ))
        messages.append(AIMessage(
            content=f"Summary for turn {i}.",
            id=f"ai-fin-{i}",
        ))
    return {
        "messages":   messages,
        "thread_id":  thread_id,
        "agent_id":   "test-agent",
        "session_id": "sess-test",
        "active":     True,
        "step":       n_turns * 4,
    }


@pytest.fixture
def state_2t():
    return make_state(n_turns=2)


@pytest.fixture
def state_4t():
    return make_state(n_turns=4)


@pytest.fixture
def state_8t():
    return make_state(n_turns=8)


def make_state_single_turn():
    """Convenience fixture exercising every message type including final AI."""
    return make_state(n_turns=1)
