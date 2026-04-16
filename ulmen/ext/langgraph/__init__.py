# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
# ULMEN is licensed under the Business Source License 1.1.
"""
ulmen-langgraph — ULMEN extension for LangGraph.

Install
-------
    pip install ulmen-langgraph

Quick start
-----------
    from langgraph.checkpoint.memory import MemorySaver
    from ulmen.ext.langgraph import (
        UlmenCheckpointer,
        ulmen_context_reducer,
        UlmenStreamSink,
        UlmenStore,
        encode_handoff,
        decode_handoff,
        ulmen_send,
        make_ulmen_state,
    )

    saver = UlmenCheckpointer(MemorySaver())
    graph = builder.compile(checkpointer=saver)
"""

from ulmen.ext.langgraph._checkpointer import UlmenCheckpointer
from ulmen.ext.langgraph._compat import LANGCHAIN_CORE_VERSION, LANGGRAPH_VERSION
from ulmen.ext.langgraph._constants import EXT_VERSION
from ulmen.ext.langgraph._handoff import (
    decode_handoff,
    encode_handoff,
    handoff_size_report,
    ulmen_send,
)
from ulmen.ext.langgraph._reducer import make_ulmen_state, ulmen_context_reducer
from ulmen.ext.langgraph._serializer import (
    decode,
    encode,
    encode_for_llm,
    langgraph_state_to_ulmen_records,
    serializer_info,
    ulmen_records_to_langgraph_state,
)
from ulmen.ext.langgraph._store import UlmenStore
from ulmen.ext.langgraph._stream import (
    UlmenAsyncStreamSink,
    UlmenStreamSink,
    decode_stream_chunk,
)


class UlmenExtInfo:
    """Introspection helper for the ulmen-langgraph extension."""

    version            = EXT_VERSION
    langgraph_version  = LANGGRAPH_VERSION
    langchain_version  = LANGCHAIN_CORE_VERSION

    @staticmethod
    def rust_backed() -> bool:
        from ulmen import RUST_AVAILABLE
        return RUST_AVAILABLE

    @staticmethod
    def serializer_info() -> dict:
        return serializer_info()

    def __repr__(self) -> str:
        return (
            f"UlmenExtInfo("
            f"version={self.version}, "
            f"langgraph={self.langgraph_version}, "
            f"rust={self.rust_backed()})"
        )


__version__ = EXT_VERSION

__all__ = [
    # Checkpointer
    "UlmenCheckpointer",
    # Reducer
    "ulmen_context_reducer",
    "make_ulmen_state",
    # Stream
    "UlmenStreamSink",
    "UlmenAsyncStreamSink",
    "decode_stream_chunk",
    # Store
    "UlmenStore",
    # Handoff
    "encode_handoff",
    "decode_handoff",
    "ulmen_send",
    "handoff_size_report",
    # Serializer (low-level)
    "encode",
    "decode",
    "encode_for_llm",
    "langgraph_state_to_ulmen_records",
    "ulmen_records_to_langgraph_state",
    "serializer_info",
    # Introspection
    "UlmenExtInfo",
    "__version__",
]
