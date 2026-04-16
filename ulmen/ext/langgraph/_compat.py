# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
# ULMEN is licensed under the Business Source License 1.1.
"""
LangGraph compatibility layer.

All langgraph and langchain-core imports are centralised here.
Every other module in the extension imports LangGraph types from
this module only — version shims and import error messages live
in one place.

Raises ImportError with a clear install instruction if langgraph
or langchain-core are not installed.
"""

from __future__ import annotations

import importlib.metadata

from ulmen.ext.langgraph._constants import (
    EXT_NAME,
    MIN_LANGCHAIN_CORE_VERSION,
    MIN_LANGGRAPH_VERSION,
)

# ── version resolution via importlib.metadata ────────────────────────────────
# langgraph does not expose __version__ on the module object.
# importlib.metadata is the only reliable source for installed version.

def _get_version(package: str) -> str:
    try:
        return importlib.metadata.version(package)
    except importlib.metadata.PackageNotFoundError:  # pragma: no cover
        return "0.0.0"


def _parse_version(ver_str: str) -> tuple[int, ...]:
    """
    Parse 'X.Y.Z', 'X.Y.Z.postN', 'X.Y.ZaN' etc. into (X, Y, Z).
    Splits on '.' and takes only the leading digit sequence of each part.
    """
    parts = []
    for segment in ver_str.split(".")[:3]:
        digits = ""
        for ch in segment:
            if ch.isdigit():
                digits += ch
            else:  # pragma: no cover
                break           # stop at first non-digit (a, b, rc, post...)
        parts.append(int(digits) if digits else 0)
    while len(parts) < 3:  # pragma: no cover
        parts.append(0)
    return tuple(parts)


# ── langgraph ────────────────────────────────────────────────────────────────
try:
    import langgraph as _lg  # noqa: F401
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        f"{EXT_NAME} requires langgraph>=1.0.0. "
        f"Install it with: pip install ulmen-langgraph"
    ) from exc

# ── langchain-core ───────────────────────────────────────────────────────────
try:
    import langchain_core as _lcc  # noqa: F401
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        f"{EXT_NAME} requires langchain-core>=0.1.0. "
        f"Install it with: pip install ulmen-langgraph"
    ) from exc

# ── version checks ───────────────────────────────────────────────────────────

_lg_ver_str  = _get_version("langgraph")
_lcc_ver_str = _get_version("langchain-core")

_lg_ver  = _parse_version(_lg_ver_str)
_lcc_ver = _parse_version(_lcc_ver_str)

if _lg_ver < MIN_LANGGRAPH_VERSION:  # pragma: no cover
    raise ImportError(
        f"{EXT_NAME} requires langgraph>="
        f"{'.'.join(str(x) for x in MIN_LANGGRAPH_VERSION)}, "
        f"found {_lg_ver_str}. "
        f"Upgrade with: pip install --upgrade langgraph"
    )

if _lcc_ver < MIN_LANGCHAIN_CORE_VERSION:  # pragma: no cover
    raise ImportError(
        f"{EXT_NAME} requires langchain-core>="
        f"{'.'.join(str(x) for x in MIN_LANGCHAIN_CORE_VERSION)}, "
        f"found {_lcc_ver_str}. "
        f"Upgrade with: pip install --upgrade langchain-core"
    )

# ── public re-exports — every other module imports from here only ─────────────

# LangChain message types used by the serializer
from langchain_core.messages import (  # noqa: E402
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

# RunnableConfig lives in langchain-core
from langchain_core.runnables import RunnableConfig  # noqa: E402
from langgraph.checkpoint.base import (  # noqa: E402
    BaseCheckpointSaver,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)
from langgraph.store.base import BaseStore  # noqa: E402
from langgraph.types import Send  # noqa: E402

# ChannelVersions type alias (dict[str, str | int])
try:
    from langgraph.checkpoint.base import ChannelVersions  # noqa: E402
except ImportError:  # pragma: no cover
    ChannelVersions = dict  # type: ignore[misc,assignment]

# PendingWrite type alias
try:
    from langgraph.checkpoint.base import PendingWrite  # noqa: E402
except ImportError:  # pragma: no cover
    PendingWrite = tuple  # type: ignore[misc,assignment]

# Exposed version strings for UlmenExtInfo
LANGGRAPH_VERSION      = _lg_ver_str
LANGCHAIN_CORE_VERSION = _lcc_ver_str

__all__ = [
    # LangGraph checkpoint
    "BaseCheckpointSaver",
    "Checkpoint",
    "CheckpointMetadata",
    "CheckpointTuple",
    "ChannelVersions",
    "PendingWrite",
    "RunnableConfig",
    # LangGraph store
    "BaseStore",
    # LangGraph types
    "Send",
    # LangChain messages
    "BaseMessage",
    "HumanMessage",
    "AIMessage",
    "SystemMessage",
    "ToolMessage",
    # Version strings
    "LANGGRAPH_VERSION",
    "LANGCHAIN_CORE_VERSION",
]
