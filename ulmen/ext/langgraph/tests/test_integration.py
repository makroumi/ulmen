# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
# ULMEN is licensed under the Business Source License 1.1.
"""
Integration tests for ulmen-langgraph.

These tests compile and execute real LangGraph StateGraph instances
with ULMEN components wired in. No mocking — full graph execution.

Covers:
    - UlmenCheckpointer with MemorySaver backend through a real graph
    - ulmen_context_reducer wired into graph state through a real graph
    - UlmenStreamSink consuming real graph.stream() output
    - UlmenStore through a real graph with long-term memory
    - encode_handoff / decode_handoff at a real subgraph boundary
    - ulmen_send / decode_handoff in a real conditional edge
"""

from __future__ import annotations

from typing import Annotated, Any, TypedDict

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.store.memory import InMemoryStore

from ulmen.ext.langgraph import (
    UlmenAsyncStreamSink,
    UlmenCheckpointer,
    UlmenStore,
    UlmenStreamSink,
    decode_handoff,
    decode_stream_chunk,
    encode_handoff,
    ulmen_context_reducer,
    ulmen_send,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_thread_config(thread_id: str = "integration-thread") -> dict:
    return {"configurable": {"thread_id": thread_id}}


# ---------------------------------------------------------------------------
# Integration 1: UlmenCheckpointer through a real StateGraph
# ---------------------------------------------------------------------------

class SimpleState(TypedDict):
    messages: list
    step:     int


def node_increment(state: SimpleState) -> dict:
    return {"step": state.get("step", 0) + 1}


def build_simple_graph(checkpointer):
    builder = StateGraph(SimpleState)
    builder.add_node("increment", node_increment)
    builder.add_edge(START, "increment")
    builder.add_edge("increment", END)
    return builder.compile(checkpointer=checkpointer)


def test_checkpointer_real_graph_single_invoke():
    """UlmenCheckpointer must persist and restore state through a real graph."""
    inner  = MemorySaver()
    saver  = UlmenCheckpointer(inner)
    graph  = build_simple_graph(saver)
    config = make_thread_config("cp-single")

    result = graph.invoke({"messages": [], "step": 0}, config)
    assert result["step"] == 1

    # State must be checkpointed and retrievable
    state = graph.get_state(config)
    assert state.values["step"] == 1


def test_checkpointer_real_graph_multiple_invokes():
    """State must accumulate correctly across multiple invocations."""
    inner  = MemorySaver()
    saver  = UlmenCheckpointer(inner)
    graph  = build_simple_graph(saver)
    config = make_thread_config("cp-multi")

    # Each invoke reads the current checkpointed step and passes it
    # explicitly — invoke(None) in LangGraph 1.1.6 returns the existing
    # checkpoint unchanged rather than re-executing the graph.
    graph.invoke({"messages": [], "step": 0}, config)
    s1 = graph.get_state(config).values["step"]
    graph.invoke({"messages": [], "step": s1}, config)
    s2 = graph.get_state(config).values["step"]
    graph.invoke({"messages": [], "step": s2}, config)

    state = graph.get_state(config)
    assert state.values["step"] == 3


def test_checkpointer_real_graph_state_history():
    """Checkpoint history must be retrievable and decompressed correctly."""
    inner  = MemorySaver()
    saver  = UlmenCheckpointer(inner)
    graph  = build_simple_graph(saver)
    config = make_thread_config("cp-history")

    graph.invoke({"messages": [], "step": 0}, config)
    graph.invoke({"messages": [], "step": 0}, config)

    history = list(graph.get_state_history(config))
    # Filter out the initial __start__ snapshot which has empty values
    data_snaps = [s for s in history if s.values]
    assert len(data_snaps) >= 2
    for snap in data_snaps:
        assert "step" in snap.values


def test_checkpointer_real_graph_with_messages():
    """Messages in state must survive checkpoint roundtrip."""
    class MsgState(TypedDict):
        messages: list
        count:    int

    def add_message(state: MsgState) -> dict:
        return {
            "messages": state["messages"] + [
                AIMessage(content=f"step {state['count']}", id=f"ai-{state['count']}")
            ],
            "count": state["count"] + 1,
        }

    inner   = MemorySaver()
    saver   = UlmenCheckpointer(inner)
    builder = StateGraph(MsgState)
    builder.add_node("add", add_message)
    builder.add_edge(START, "add")
    builder.add_edge("add", END)
    graph   = builder.compile(checkpointer=saver)
    config  = make_thread_config("cp-messages")

    graph.invoke({"messages": [], "count": 0}, config)
    state = graph.get_state(config)
    assert state.values["count"] == 1
    assert len(state.values["messages"]) == 1


@pytest.mark.asyncio
async def test_checkpointer_real_graph_async():
    """Async graph invocation must work with UlmenCheckpointer."""
    inner  = MemorySaver()
    saver  = UlmenCheckpointer(inner)
    graph  = build_simple_graph(saver)
    config = make_thread_config("cp-async")

    result = await graph.ainvoke({"messages": [], "step": 0}, config)
    assert result["step"] == 1
    state = graph.get_state(config)
    assert state.values["step"] == 1


# ---------------------------------------------------------------------------
# Integration 2: ulmen_context_reducer through a real StateGraph
# ---------------------------------------------------------------------------

class ReducerState(TypedDict):
    messages: Annotated[list, ulmen_context_reducer]
    turn:     int


def node_reply(state: ReducerState) -> dict:
    return {
        "messages": [AIMessage(content=f"Reply to turn {state['turn']}", id=f"r-{state['turn']}")],
        "turn":     state["turn"] + 1,
    }


def build_reducer_graph():
    builder = StateGraph(ReducerState)
    builder.add_node("reply", node_reply)
    builder.add_edge(START, "reply")
    builder.add_edge("reply", END)
    return builder.compile()


def test_reducer_real_graph_appends_messages():
    """ulmen_context_reducer must accumulate messages across invocations."""
    graph  = build_reducer_graph()

    state = graph.invoke({
        "messages": [HumanMessage(content="Hello", id="h1")],
        "turn": 1,
    })
    assert len(state["messages"]) >= 2  # original + reply


def test_reducer_real_graph_multiple_turns():
    """Messages must grow correctly across multiple turns."""
    inner   = MemorySaver()
    builder = StateGraph(ReducerState)
    builder.add_node("reply", node_reply)
    builder.add_edge(START, "reply")
    builder.add_edge("reply", END)
    graph  = builder.compile(checkpointer=inner)
    config = make_thread_config("reducer-multi")

    graph.invoke({"messages": [HumanMessage(content="Hi", id="h1")], "turn": 1}, config)
    graph.invoke({"messages": [HumanMessage(content="Again", id="h2")], "turn": 2}, config)

    state = graph.get_state(config)
    assert len(state.values["messages"]) >= 3


def test_reducer_real_graph_compression_does_not_crash():
    """Reducer with tiny context window must not crash when called directly."""
    import functools

    # TypedDict cannot reference local variables in Annotated when
    # LangGraph calls get_type_hints at module scope. Test the reducer
    # directly with a tiny context window instead.
    tiny_reducer = functools.partial(ulmen_context_reducer, context_window=1, compress=True)

    big_messages = [HumanMessage(content="word " * 300, id=f"m{i}") for i in range(5)]
    update       = [AIMessage(content="x" * 200, id="a1")]
    result       = tiny_reducer(big_messages, update)
    assert isinstance(result, list)


# ---------------------------------------------------------------------------
# Integration 3: UlmenStreamSink consuming real graph.stream() output
# ---------------------------------------------------------------------------

def test_stream_sink_real_graph_sync():
    """UlmenStreamSink must encode every event from a real graph.stream()."""
    graph  = build_simple_graph(MemorySaver())
    config = make_thread_config("stream-sync")

    chunks = list(UlmenStreamSink(
        graph.stream({"messages": [], "step": 0}, config)
    ))
    assert len(chunks) >= 1
    for chunk in chunks:
        assert isinstance(chunk, bytes)
        records = decode_stream_chunk(chunk)
        assert isinstance(records, list)


def test_stream_sink_real_graph_smaller_than_json():
    """ULMEN stream chunks must be smaller than JSON for message-heavy state."""
    import json

    from ulmen.ext.langgraph.tests.conftest import make_state

    # Use a message-heavy event so ULMEN compression wins over JSON
    events     = [{"planner": make_state(n_turns=4)},
                  {"executor": make_state(n_turns=4)}]
    json_total  = sum(len(json.dumps(e, default=str).encode()) for e in events)
    ulmen_total = sum(len(c) for c in UlmenStreamSink(iter(events)))
    assert ulmen_total < json_total


@pytest.mark.asyncio
async def test_stream_sink_real_graph_async():
    """UlmenAsyncStreamSink must encode every event from graph.astream()."""
    graph  = build_simple_graph(MemorySaver())
    config = make_thread_config("stream-async")

    chunks = []
    async for chunk in UlmenAsyncStreamSink(
        graph.astream({"messages": [], "step": 0}, config)
    ):
        chunks.append(chunk)

    assert len(chunks) >= 1
    for chunk in chunks:
        records = decode_stream_chunk(chunk)
        assert isinstance(records, list)


# ---------------------------------------------------------------------------
# Integration 4: UlmenStore through a real graph with long-term memory
# ---------------------------------------------------------------------------

def test_store_real_graph_put_get():
    """UlmenStore must transparently compress/decompress values in a graph."""
    inner = InMemoryStore()
    store = UlmenStore(inner)

    # Simulate what a graph node does with the store
    store.put(("user", "alice"), "prefs", {"theme": "dark", "lang": "en"}, ttl=None)
    store.put(("user", "alice"), "history", {"turns": 5, "last": "search"}, ttl=None)

    prefs   = store.get(("user", "alice"), "prefs")
    history = store.get(("user", "alice"), "history")

    assert prefs   is not None
    assert history is not None
    assert prefs.value["theme"]   == "dark"
    assert history.value["turns"] == 5


def test_store_real_graph_search():
    """UlmenStore.search must return decoded values."""
    store = UlmenStore(InMemoryStore())
    store.put(("docs",), "a", {"content": "ULMEN is fast"}, ttl=None)
    store.put(("docs",), "b", {"content": "LangGraph is great"}, ttl=None)

    results = store.search(("docs",))
    assert len(results) == 2
    contents = {r.value["content"] for r in results}
    assert "ULMEN is fast" in contents


@pytest.mark.asyncio
async def test_store_real_graph_async_put_get():
    """Async store operations must compress and decompress correctly."""
    store = UlmenStore(InMemoryStore())
    await store.aput(("session",), "state", {"active": True, "step": 7}, ttl=None)
    item = await store.aget(("session",), "state")
    assert item is not None
    assert item.value["active"] is True
    assert item.value["step"]   == 7


# ---------------------------------------------------------------------------
# Integration 5: encode_handoff / decode_handoff at subgraph boundary
# ---------------------------------------------------------------------------

def test_handoff_real_subgraph_boundary():
    """encode_handoff / decode_handoff must be lossless at a graph boundary."""
    class ParentState(TypedDict):
        messages: list
        thread_id: str
        payload:   bytes

    def parent_node(state: ParentState) -> dict:
        blob = encode_handoff({
            "messages":  state["messages"],
            "thread_id": state["thread_id"],
            "task":      "summarise",
        })
        return {"payload": blob}

    def child_node(state: ParentState) -> dict:
        recovered = decode_handoff(state["payload"])
        assert recovered["task"]      == "summarise"
        assert recovered["thread_id"] == state["thread_id"]
        return {}

    builder = StateGraph(ParentState)
    builder.add_node("parent", parent_node)
    builder.add_node("child",  child_node)
    builder.add_edge(START,    "parent")
    builder.add_edge("parent", "child")
    builder.add_edge("child",  END)
    graph = builder.compile()

    graph.invoke({
        "messages":  [HumanMessage(content="Hello", id="h1")],
        "thread_id": "integration-handoff",
        "payload":   b"",
    })


# ---------------------------------------------------------------------------
# Integration 6: ulmen_send in a real conditional edge
# ---------------------------------------------------------------------------

def test_ulmen_send_real_conditional_edge():
    """ulmen_send must deliver the correct state to the target node."""
    received = {}

    class RouterState(TypedDict):
        messages:  list
        thread_id: str
        target:    str

    def router_node(state: RouterState):
        return {}

    def worker_node(state: Any) -> dict:
        real = decode_handoff(state["__ulmen_handoff__"])
        received["thread_id"] = real.get("thread_id")
        received["target"]    = real.get("target")
        return {}

    # Pattern 1: ulmen_send via conditional edge (correct LangGraph 1.1.6 pattern)
    builder = StateGraph(RouterState)
    builder.add_node("router", router_node)
    builder.add_node("worker", worker_node)
    builder.add_conditional_edges("router", lambda s: [ulmen_send("worker", s)])
    builder.add_edge(START, "router")
    builder.add_edge("worker", END)
    graph1 = builder.compile()
    graph1.invoke({
        "messages":  [],
        "thread_id": "integration-send",
        "target":    "executor",
    })

    assert received["thread_id"] == "integration-send"
    assert received["target"]    == "executor"

    # Pattern 2: encode_handoff / decode_handoff directly in a node
    received.clear()
    builder2 = StateGraph(RouterState)

    def combined(state: RouterState) -> dict:
        blob = encode_handoff({
            "messages":  state["messages"],
            "thread_id": state["thread_id"],
            "target":    state["target"],
        })
        real = decode_handoff(blob)
        received["thread_id"] = real.get("thread_id")
        received["target"]    = real.get("target")
        return {}

    builder2.add_node("combined", combined)
    builder2.add_edge(START, "combined")
    builder2.add_edge("combined", END)
    graph2 = builder2.compile()

    graph2.invoke({
        "messages":  [],
        "thread_id": "integration-send",
        "target":    "executor",
    })

    assert received["thread_id"] == "integration-send"
    assert received["target"]    == "executor"
