# ulmen-langgraph

ULMEN extension for LangGraph. Drop-in ULMEN-compressed replacements
for LangGraph's checkpointer, store, stream, and subgraph handoff surfaces.

## Install

```bash
pip install ulmen-langgraph
```

## What it does

| Surface | Today (LangGraph) | With ulmen-langgraph |
|---------|-------------------|----------------------|
| Checkpoint storage | json.dumps per step | ULMEN binary + zlib |
| LLM context | Raw JSON message list | ULMEN LLM surface (~44% fewer tokens) |
| graph.stream() egress | JSON blob per event | ULMEN binary per chunk |
| Subgraph handoff | JSON state at boundary | ULMEN binary blob |
| Long-term store | Raw dict values | ULMEN binary + zlib |

## Quick Start

```python
from langgraph.checkpoint.memory import MemorySaver
from langgraph.store.memory import InMemoryStore
from ulmen.ext.langgraph import (
    UlmenCheckpointer,
    ulmen_context_reducer,
    UlmenStreamSink,
    UlmenStore,
    encode_handoff,
    decode_handoff,
    ulmen_send,
    make_ulmen_state,
    UlmenExtInfo,
)

# Introspection
print(UlmenExtInfo())

# 1. Checkpointer: wrap any existing saver, zero graph changes
saver = UlmenCheckpointer(MemorySaver())
graph = builder.compile(checkpointer=saver)

# 2. Context reducer: compress message history automatically
from typing import Annotated, TypedDict
class State(TypedDict):
    messages: Annotated[list, ulmen_context_reducer]

# Or use the helper
AgentState = make_ulmen_state(
    extra_fields={"session_id": str},
    context_window=8000,
)

# 3. Stream sink: binary egress instead of JSON
for chunk in UlmenStreamSink(graph.stream(input, config)):
    redis.publish("events", chunk)   # bytes, not JSON

# Async
async for chunk in UlmenAsyncStreamSink(graph.astream(input, config)):
    await redis.publish("events", chunk)

# 4. Store: compress long-term memory values
store = UlmenStore(InMemoryStore())
store.put(("user", "alice"), "prefs", {"theme": "dark"})
item = store.get(("user", "alice"), "prefs")

# 5. Subgraph handoff
blob  = encode_handoff(state)          # bytes
state = decode_handoff(blob)           # back to dict

# ULMEN-aware Send
return [ulmen_send("child_agent", state)]
```

## Works with any backend

```python
# MemorySaver
UlmenCheckpointer(MemorySaver())

# SqliteSaver
from langgraph.checkpoint.sqlite import SqliteSaver
UlmenCheckpointer(SqliteSaver.from_conn_string("state.db"))

# PostgresSaver
from langgraph.checkpoint.postgres import PostgresSaver
UlmenCheckpointer(PostgresSaver.from_conn_string(DB_URI))
```
License
BSL 1.1: free for entities under $10M annual revenue.
See [LICENSE] for full terms.