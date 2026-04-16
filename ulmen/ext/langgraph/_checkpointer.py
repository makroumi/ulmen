# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
# ULMEN is licensed under the Business Source License 1.1.
"""
UlmenCheckpointer — drop-in ULMEN-compressed wrapper for any
LangGraph BaseCheckpointSaver.

Architecture (corrected)
------------------------
MemorySaver (and all other LangGraph checkpointers) do NOT store
channel_values as a single blob. They pop channel_values out of the
checkpoint and store each channel value individually via serde:

    serde.dumps_typed(values[k])  →  (type_str, bytes)  stored per channel
    serde.loads_typed((type, b))  →  original value      on read

The correct interception point is the serde layer, not channel_values.

UlmenCheckpointer wraps the inner saver's serde with UlmenSerde, which:
    - On dumps_typed: serializes via the original serde, then zlib-compresses
      the resulting bytes and prefixes with our 4-byte magic ULMZ.
    - On loads_typed: detects our magic prefix, decompresses, then
      deserializes via the original serde. Foreign values (no magic) pass
      through the original serde unchanged.

This approach works for every LangGraph backend (Memory, Sqlite, Postgres,
custom) because they all use serde for channel value serialization.
"""

from __future__ import annotations

import zlib as _zlib
from typing import Any, AsyncIterator, Iterator, Sequence

from ulmen.ext.langgraph._compat import (
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    RunnableConfig,
)
from ulmen.ext.langgraph._constants import DEFAULT_ZLIB_LEVEL

# 4-byte magic prefix on every ULMEN-compressed serde value
_ULMZ = b"ULMZ"


# ---------------------------------------------------------------------------
# UlmenSerde — wraps the inner serde at the dumps_typed / loads_typed level
# ---------------------------------------------------------------------------

class UlmenSerde:
    """
    Serde wrapper that zlib-compresses each channel value after the
    inner serde serializes it, and decompresses transparently on read.

    Compatible with langgraph.checkpoint.serde.base.SerializerProtocol.
    """

    def __init__(self, inner_serde: Any, zlib_level: int = DEFAULT_ZLIB_LEVEL) -> None:
        self._inner      = inner_serde
        self._zlib_level = zlib_level

    def dumps_typed(self, obj: Any) -> tuple[str, bytes]:
        """Serialize via inner serde then zlib-compress the bytes."""
        type_str, raw = self._inner.dumps_typed(obj)
        compressed    = _ULMZ + _zlib.compress(raw, self._zlib_level)
        return type_str, compressed

    def loads_typed(self, data: tuple[str, bytes]) -> Any:
        """Detect our magic prefix, decompress, then deserialize via inner serde."""
        type_str, raw = data
        if isinstance(raw, (bytes, bytearray)) and raw[:4] == _ULMZ:
            raw = _zlib.decompress(raw[4:])
        return self._inner.loads_typed((type_str, raw))

    # Forward any other attributes the inner serde exposes
    def __getattr__(self, name: str) -> Any:  # pragma: no cover
        return getattr(self._inner, name)

    def __repr__(self) -> str:
        return (
            f"UlmenSerde("
            f"inner={type(self._inner).__name__}, "
            f"zlib_level={self._zlib_level})"
        )


# ---------------------------------------------------------------------------
# Tuple decompression helpers (for get_tuple / list)
# ---------------------------------------------------------------------------
# These are no-ops now — decompression happens inside UlmenSerde.loads_typed
# which the inner saver calls automatically. We keep these as passthroughs
# so the interface is consistent and testable.

def _decompress_tuple(tup: CheckpointTuple) -> CheckpointTuple:
    """Pass through — decompression handled by UlmenSerde inside inner saver."""
    return tup  # pragma: no cover


# ---------------------------------------------------------------------------
# UlmenCheckpointer
# ---------------------------------------------------------------------------

class UlmenCheckpointer(BaseCheckpointSaver):
    """
    ULMEN-compressed wrapper for any LangGraph BaseCheckpointSaver.

    Intercepts at the serde layer so compression is transparent to
    every backend (MemorySaver, SqliteSaver, PostgresSaver, custom).

    Parameters
    ----------
    saver       : Any BaseCheckpointSaver instance.
    zlib_level  : zlib compression level 0-9. Default 6.
    """

    def __init__(
        self,
        saver: BaseCheckpointSaver,
        zlib_level: int = DEFAULT_ZLIB_LEVEL,
    ) -> None:
        # Wrap the inner saver's serde with UlmenSerde
        inner_serde  = getattr(saver, "serde", None)
        ulmen_serde  = UlmenSerde(inner_serde, zlib_level) if inner_serde else None
        super().__init__(serde=ulmen_serde)

        # Patch the inner saver's serde so it uses compression on all
        # its internal blob reads/writes
        if inner_serde is not None and ulmen_serde is not None:
            saver.serde = ulmen_serde  # type: ignore[assignment]

        self._inner      = saver
        self._zlib_level = zlib_level

    # ------------------------------------------------------------------
    # Sync interface — fully delegate to inner saver
    # (inner saver now uses UlmenSerde for all channel value IO)
    # ------------------------------------------------------------------

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return self._inner.get_tuple(config)

    def get(self, config: RunnableConfig) -> Checkpoint | None:
        return self._inner.get(config)

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        return self._inner.list(config, filter=filter, before=before, limit=limit)

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return self._inner.put(config, checkpoint, metadata, new_versions)

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        self._inner.put_writes(config, writes, task_id, task_path)

    def get_next_version(self, current: Any, channel: Any) -> Any:
        return self._inner.get_next_version(current, channel)

    # ------------------------------------------------------------------
    # Async interface
    # ------------------------------------------------------------------

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return await self._inner.aget_tuple(config)

    async def aget(self, config: RunnableConfig) -> Checkpoint | None:
        return await self._inner.aget(config)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        async for tup in self._inner.alist(
            config, filter=filter, before=before, limit=limit
        ):
            yield tup

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return await self._inner.aput(config, checkpoint, metadata, new_versions)

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        await self._inner.aput_writes(config, writes, task_id, task_path)

    # ------------------------------------------------------------------
    # Delete / prune — delegate directly
    # ------------------------------------------------------------------

    def delete_thread(self, thread_id: str) -> None:
        self._inner.delete_thread(thread_id)

    def delete_for_runs(self, run_ids: Sequence[str]) -> None:
        self._inner.delete_for_runs(run_ids)

    def prune(
        self, thread_ids: Sequence[str], *, strategy: str = "keep_latest"
    ) -> None:
        self._inner.prune(thread_ids, strategy=strategy)

    def copy_thread(
        self, source_thread_id: str, target_thread_id: str
    ) -> None:
        self._inner.copy_thread(source_thread_id, target_thread_id)

    async def adelete_thread(self, thread_id: str) -> None:
        await self._inner.adelete_thread(thread_id)

    async def adelete_for_runs(self, run_ids: Sequence[str]) -> None:
        await self._inner.adelete_for_runs(run_ids)

    async def aprune(
        self, thread_ids: Sequence[str], *, strategy: str = "keep_latest"
    ) -> None:
        await self._inner.aprune(thread_ids, strategy=strategy)

    async def acopy_thread(
        self, source_thread_id: str, target_thread_id: str
    ) -> None:
        await self._inner.acopy_thread(source_thread_id, target_thread_id)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def inner(self) -> BaseCheckpointSaver:
        return self._inner

    @property
    def zlib_level(self) -> int:
        return self._zlib_level

    def __repr__(self) -> str:
        return (
            f"UlmenCheckpointer("
            f"inner={type(self._inner).__name__}, "
            f"zlib_level={self._zlib_level})"
        )
