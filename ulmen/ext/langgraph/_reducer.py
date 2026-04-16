# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
# ULMEN is licensed under the Business Source License 1.1.
"""
ulmen_context_reducer — LangGraph state reducer for ULMEN-compressed
message history.

Usage
-----
    from typing import Annotated, TypedDict
    from ulmen.ext.langgraph import ulmen_context_reducer

    class AgentState(TypedDict):
        messages: Annotated[list, ulmen_context_reducer]

    # Or use the helper that builds the TypedDict for you:
    from ulmen.ext.langgraph import make_ulmen_state
    AgentState = make_ulmen_state()

How it works
------------
LangGraph calls the reducer function whenever a node returns an update
to the messages channel. The reducer receives (current_messages, update)
and returns the new message list.

ulmen_context_reducer does three things on top of the default list-append:

    1. Merges the update into the current list (same as operator.add)
    2. Estimates the total token count of the merged list
    3. When the token count exceeds context_window_threshold, applies
       ULMEN context compression (COMPRESS_COMPLETED_SEQUENCES strategy)
       to keep the list within budget before returning it

The returned list is always plain Python dicts — LangGraph accepts both
dicts and LangChain message objects interchangeably at node boundaries.

make_ulmen_state()
------------------
Returns a TypedDict class with the messages field pre-wired to
ulmen_context_reducer. Additional fields can be added by the caller.
"""

from __future__ import annotations

from typing import Any

from ulmen import compress_context, estimate_tokens
from ulmen.core._agent import COMPRESS_COMPLETED_SEQUENCES
from ulmen.ext.langgraph._constants import DEFAULT_CONTEXT_WINDOW
from ulmen.ext.langgraph._serializer import _msg_to_dict

# ---------------------------------------------------------------------------
# Token estimation for a message list
# ---------------------------------------------------------------------------

def _estimate_messages_tokens(messages: list) -> int:
    """Estimate total tokens across a list of messages."""
    total = 0
    for msg in messages:
        d       = _msg_to_dict(msg)
        content = d.get("content", "")
        if isinstance(content, str):
            total += estimate_tokens(content)
        elif isinstance(content, list):  # pragma: no cover
            # Multi-modal content blocks
            for block in content:
                if isinstance(block, dict):
                    total += estimate_tokens(block.get("text", ""))
    return total


# ---------------------------------------------------------------------------
# Reducer
# ---------------------------------------------------------------------------

def ulmen_context_reducer(
    current: list,
    update: Any,
    *,
    context_window: int = DEFAULT_CONTEXT_WINDOW,
    compress: bool = True,
) -> list:
    """
    LangGraph reducer for the messages channel with ULMEN compression.

    Parameters
    ----------
    current        : current message list held in graph state
    update         : new messages from the node (list or single message)
    context_window : token budget; compression triggers above this
    compress       : set False to disable compression (append-only mode)

    Returns
    -------
    Merged message list, compressed if token budget exceeded.
    """
    # Normalise update to a list
    if update is None:
        update_list = []
    elif isinstance(update, list):
        update_list = update
    else:
        update_list = [update]

    # Merge
    merged = list(current) + update_list

    if not compress:
        return merged

    # Check token budget
    total_tokens = _estimate_messages_tokens(merged)
    if total_tokens <= context_window:
        return merged

    # Convert to ULMEN-AGENT records for compression
    from ulmen.ext.langgraph._serializer import langgraph_state_to_ulmen_records

    # Build a minimal state dict so the serializer can process messages
    pseudo_state = {
        "messages":  merged,
        "thread_id": "reducer",
    }
    records = langgraph_state_to_ulmen_records(pseudo_state)

    # Apply ULMEN compression
    compressed_records = compress_context(
        records,
        strategy=COMPRESS_COMPLETED_SEQUENCES,
    )

    # Reconstruct message list from compressed records
    from ulmen.ext.langgraph._serializer import ulmen_records_to_langgraph_state
    restored = ulmen_records_to_langgraph_state(compressed_records)
    return restored.get("messages", merged)


# ---------------------------------------------------------------------------
# make_ulmen_state helper
# ---------------------------------------------------------------------------

def make_ulmen_state(
    extra_fields: dict[str, Any] | None = None,
    context_window: int = DEFAULT_CONTEXT_WINDOW,
) -> type:
    """
    Build a LangGraph-compatible TypedDict with messages wired to
    ulmen_context_reducer.

    Parameters
    ----------
    extra_fields    : additional {field_name: type_annotation} to include
    context_window  : token budget passed to the reducer

    Returns
    -------
    A TypedDict class ready to use as StateGraph state.

    Example
    -------
        AgentState = make_ulmen_state(
            extra_fields={"session_id": str, "active": bool},
            context_window=8000,
        )
        builder = StateGraph(AgentState)
    """
    import functools
    from typing import Annotated

    # Bind context_window into the reducer
    bound_reducer = functools.partial(
        ulmen_context_reducer,
        context_window=context_window,
    )
    # functools.partial is not recognised by some TypedDict validators;
    # wrap it so it looks like a plain callable.
    bound_reducer.__name__ = "ulmen_context_reducer"  # type: ignore[attr-defined]

    fields: dict[str, Any] = {
        "messages": Annotated[list, bound_reducer],
    }
    if extra_fields:
        fields.update(extra_fields)

    # Dynamically build the TypedDict
    UlmenState = type("UlmenState", (dict,), {"__annotations__": fields})
    return UlmenState
