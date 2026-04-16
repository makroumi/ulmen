# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
# ULMEN is licensed under the Business Source License 1.1.
"""
Smoke test for ulmen-langgraph.

Verifies that every public symbol is importable, every component
instantiates, and the full encode→store→retrieve→decode pipeline
works end-to-end in under one second.

Run this after any install to confirm the extension is healthy:

    pytest ulmen/ext/langgraph/tests/test_smoke.py -v
"""

from __future__ import annotations

import time
from typing import TypedDict

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.store.memory import InMemoryStore

# ---------------------------------------------------------------------------
# 1. All public symbols importable
# ---------------------------------------------------------------------------

def test_all_public_symbols_importable():
    from ulmen.ext.langgraph import (  # noqa: F401
        UlmenAsyncStreamSink,
        UlmenCheckpointer,
        UlmenExtInfo,
        UlmenStore,
        UlmenStreamSink,
        __version__,
        decode,
        decode_handoff,
        decode_stream_chunk,
        encode,
        encode_for_llm,
        encode_handoff,
        handoff_size_report,
        langgraph_state_to_ulmen_records,
        make_ulmen_state,
        serializer_info,
        ulmen_context_reducer,
        ulmen_records_to_langgraph_state,
        ulmen_send,
    )
    assert True


# ---------------------------------------------------------------------------
# 2. All components instantiate
# ---------------------------------------------------------------------------

def test_components_instantiate():
    from ulmen.ext.langgraph import (
        UlmenCheckpointer,
        UlmenExtInfo,
        UlmenStore,
        UlmenStreamSink,
        make_ulmen_state,
    )

    assert UlmenCheckpointer(MemorySaver()) is not None
    assert UlmenStore(InMemoryStore())      is not None
    assert UlmenStreamSink(iter([]))        is not None
    assert make_ulmen_state()               is not None
    assert UlmenExtInfo()                   is not None


# ---------------------------------------------------------------------------
# 3. UlmenExtInfo reports correct metadata
# ---------------------------------------------------------------------------

def test_ext_info_metadata():
    from ulmen.ext.langgraph import UlmenExtInfo
    info = UlmenExtInfo()
    assert info.version           == "0.1.0"
    assert len(info.langgraph_version)  > 0
    assert len(info.langchain_version)  > 0
    assert isinstance(info.rust_backed(), bool)
    si = info.serializer_info()
    assert "rust_backed" in si
    assert "zlib_level"  in si
    assert "pool_limit"  in si


# ---------------------------------------------------------------------------
# 4. Serializer encode/decode pipeline
# ---------------------------------------------------------------------------

def test_serializer_pipeline():
    from ulmen.ext.langgraph import decode, encode

    state = {
        "messages": [
            HumanMessage(content="Hello ULMEN", id="h1"),
            AIMessage(content="Hello LangGraph", id="a1"),
        ],
        "thread_id":  "smoke-t1",
        "agent_id":   "smoke-agent",
        "session_id": "smoke-session",
        "active":     True,
        "step":       1,
    }
    blob   = encode(state)
    result = decode(blob)

    assert isinstance(blob, bytes)
    assert result.get("thread_id")  == "smoke-t1"
    assert result.get("agent_id")   == "smoke-agent"
    assert result.get("active")     is True
    assert len(result["messages"])  == len(state["messages"])


# ---------------------------------------------------------------------------
# 5. UlmenCheckpointer full pipeline
# ---------------------------------------------------------------------------

def test_checkpointer_pipeline():
    from ulmen.ext.langgraph import UlmenCheckpointer

    class SmokeState(TypedDict):
        messages: list
        value:    str

    def node(state: SmokeState) -> dict:
        return {"value": "processed", "messages": state["messages"]}

    inner   = MemorySaver()
    saver   = UlmenCheckpointer(inner)
    builder = StateGraph(SmokeState)
    builder.add_node("node", node)
    builder.add_edge(START, "node")
    builder.add_edge("node", END)
    graph   = builder.compile(checkpointer=saver)
    config  = {"configurable": {"thread_id": "smoke-cp"}}

    result = graph.invoke({"messages": [], "value": "initial"}, config)
    assert result["value"] == "processed"

    state = graph.get_state(config)
    assert state.values["value"] == "processed"


# ---------------------------------------------------------------------------
# 6. ulmen_context_reducer pipeline
# ---------------------------------------------------------------------------

def test_reducer_pipeline():
    """ulmen_context_reducer must merge and optionally compress message lists."""
    from ulmen.ext.langgraph import ulmen_context_reducer

    # TypedDict inside a function cannot reference local names in Annotated
    # when LangGraph calls get_type_hints. Test the reducer directly.
    current = [HumanMessage(content="hi", id="h1")]
    update  = [AIMessage(content="reply", id="r1")]
    result  = ulmen_context_reducer(current, update, compress=False)

    assert len(result) == 2
    assert result[0].content == "hi"
    assert result[1].content == "reply"


# ---------------------------------------------------------------------------
# 7. UlmenStreamSink pipeline
# ---------------------------------------------------------------------------

def test_stream_sink_pipeline():
    from ulmen.ext.langgraph import UlmenStreamSink, decode_stream_chunk

    class SmokeState(TypedDict):
        messages: list
        step:     int

    def node(state: SmokeState) -> dict:
        return {"step": state["step"] + 1, "messages": state["messages"]}

    builder = StateGraph(SmokeState)
    builder.add_node("node", node)
    builder.add_edge(START, "node")
    builder.add_edge("node", END)
    graph = builder.compile()

    chunks = list(UlmenStreamSink(
        graph.stream({"messages": [], "step": 0})
    ))
    assert len(chunks) >= 1
    for chunk in chunks:
        assert isinstance(chunk, bytes)
        records = decode_stream_chunk(chunk)
        assert isinstance(records, list)


# ---------------------------------------------------------------------------
# 8. UlmenStore pipeline
# ---------------------------------------------------------------------------

def test_store_pipeline():
    from ulmen.ext.langgraph import UlmenStore

    store = UlmenStore(InMemoryStore())
    store.put(("smoke",), "key", {"value": 42, "label": "test"}, ttl=None)
    item = store.get(("smoke",), "key")

    assert item is not None
    assert item.value["value"] == 42
    assert item.value["label"] == "test"


# ---------------------------------------------------------------------------
# 9. Handoff pipeline
# ---------------------------------------------------------------------------

def test_handoff_pipeline():
    from ulmen.ext.langgraph import decode_handoff, encode_handoff, ulmen_send

    state = {
        "messages":  [HumanMessage(content="task", id="h1")],
        "thread_id": "smoke-handoff",
        "active":    True,
    }

    # encode_handoff / decode_handoff
    blob      = encode_handoff(state)
    recovered = decode_handoff(blob)
    assert recovered["thread_id"] == "smoke-handoff"
    assert recovered["active"]    is True
    assert len(recovered["messages"]) == 1

    # ulmen_send
    send_obj = ulmen_send("target_node", state)
    assert send_obj.node == "target_node"
    inner    = decode_handoff(send_obj.arg["__ulmen_handoff__"])
    assert inner["thread_id"] == "smoke-handoff"


# ---------------------------------------------------------------------------
# 10. End-to-end pipeline speed
# ---------------------------------------------------------------------------

def test_full_pipeline_completes_in_time():
    """
    Full encode → checkpoint → stream → store → handoff pipeline
    must complete in under 2 seconds on any reasonable machine.
    """
    from ulmen.ext.langgraph import (
        UlmenCheckpointer,
        UlmenStore,
        UlmenStreamSink,
        decode_handoff,
        encode_handoff,
    )

    class E2EState(TypedDict):
        messages: list
        step:     int

    def node(state: E2EState) -> dict:
        return {"step": state["step"] + 1, "messages": state["messages"]}

    inner   = MemorySaver()
    saver   = UlmenCheckpointer(inner)
    store   = UlmenStore(InMemoryStore())
    builder = StateGraph(E2EState)
    builder.add_node("node", node)
    builder.add_edge(START, "node")
    builder.add_edge("node", END)
    graph  = builder.compile(checkpointer=saver)
    config = {"configurable": {"thread_id": "e2e-speed"}}

    start = time.perf_counter()

    # Checkpoint
    graph.invoke({"messages": [], "step": 0}, config)

    # Stream
    list(UlmenStreamSink(
        graph.stream({"messages": [], "step": 0}, config)
    ))

    # Store
    store.put(("e2e",), "k", {"x": 1}, ttl=None)
    store.get(("e2e",), "k")

    # Handoff
    blob = encode_handoff({"messages": [], "thread_id": "e2e", "step": 1})
    decode_handoff(blob)

    elapsed = time.perf_counter() - start
    assert elapsed < 2.0, f"Pipeline took {elapsed:.2f}s — too slow"


# ---------------------------------------------------------------------------
# 11. RUST_AVAILABLE reported correctly
# ---------------------------------------------------------------------------

def test_rust_available_reported():
    from ulmen import RUST_AVAILABLE
    from ulmen.ext.langgraph import UlmenExtInfo
    assert UlmenExtInfo.rust_backed() == RUST_AVAILABLE


# ---------------------------------------------------------------------------
# 12. Async smoke
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_checkpointer_smoke():
    from ulmen.ext.langgraph import UlmenCheckpointer

    class SmokeState(TypedDict):
        messages: list
        value:    str

    def node(state: SmokeState) -> dict:
        return {"value": "async-ok", "messages": state["messages"]}

    inner   = MemorySaver()
    saver   = UlmenCheckpointer(inner)
    builder = StateGraph(SmokeState)
    builder.add_node("node", node)
    builder.add_edge(START, "node")
    builder.add_edge("node", END)
    graph  = builder.compile(checkpointer=saver)
    config = {"configurable": {"thread_id": "smoke-async"}}

    result = await graph.ainvoke({"messages": [], "value": ""}, config)
    assert result["value"] == "async-ok"


@pytest.mark.asyncio
async def test_async_store_smoke():
    from ulmen.ext.langgraph import UlmenStore

    store = UlmenStore(InMemoryStore())
    await store.aput(("smoke",), "k", {"async": True}, ttl=None)
    item = await store.aget(("smoke",), "k")
    assert item is not None
    assert item.value["async"] is True


@pytest.mark.asyncio
async def test_async_stream_sink_smoke():
    from ulmen.ext.langgraph import UlmenAsyncStreamSink, decode_stream_chunk

    class SmokeState(TypedDict):
        messages: list
        step:     int

    def node(state: SmokeState) -> dict:
        return {"step": 1, "messages": state["messages"]}

    builder = StateGraph(SmokeState)
    builder.add_node("node", node)
    builder.add_edge(START, "node")
    builder.add_edge("node", END)
    graph = builder.compile()

    chunks = []
    async for chunk in UlmenAsyncStreamSink(
        graph.astream({"messages": [], "step": 0})
    ):
        chunks.append(chunk)

    assert len(chunks) >= 1
    records = decode_stream_chunk(chunks[0])
    assert isinstance(records, list)
