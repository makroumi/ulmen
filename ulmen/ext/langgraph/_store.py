# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
# ULMEN is licensed under the Business Source License 1.1.
"""
UlmenStore — ULMEN-compressed wrapper for any LangGraph BaseStore.

Usage
-----
    from langgraph.store.memory import InMemoryStore
    from ulmen.ext.langgraph import UlmenStore

    inner = InMemoryStore()
    store = UlmenStore(inner)

    await store.aput(("user", "alice"), "prefs", {"theme": "dark"})
    item = await store.aget(("user", "alice"), "prefs")
    print(item.value)   # {"theme": "dark"}

How it works
------------
On write: value dict is encoded to ULMEN binary (zlib) and stored as
{"__ulmen__": STORE_ULMEN_MARKER + <compressed bytes>}.

On read: the marker is detected and the value is decoded transparently.
Foreign values (not encoded by UlmenStore) pass through unchanged.
"""

from __future__ import annotations

import zlib
from typing import Any, Iterable, Literal

from ulmen import UlmenDict, decode_binary_records
from ulmen.ext.langgraph._compat import BaseStore
from ulmen.ext.langgraph._constants import DEFAULT_ZLIB_LEVEL, STORE_ULMEN_MARKER

# Import NotProvided for correct type signature matching BaseStore
try:
    from langgraph.store.base import NOT_PROVIDED, NotProvided
except ImportError:  # pragma: no cover
    # Fallback sentinel if LangGraph version differs
    class NotProvided:  # type: ignore[no-redef]
        pass
    NOT_PROVIDED = NotProvided()

_STORE_KEY = "__ulmen__"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _encode_value(value: dict, zlib_level: int) -> dict:
    """Encode a value dict to a single-key ULMEN blob dict."""
    records = [{"__type": "store_value", **value}]
    ud      = UlmenDict(records, optimizations=True)
    blob    = STORE_ULMEN_MARKER + ud.encode_binary_zlib(level=zlib_level)
    return {_STORE_KEY: blob}


def _decode_value(stored: dict) -> dict:
    """
    Decode a ULMEN blob dict back to the original value dict.
    Returns stored unchanged if it was not encoded by UlmenStore.
    """
    if not isinstance(stored, dict):
        return stored
    blob = stored.get(_STORE_KEY)
    if not isinstance(blob, (bytes, bytearray)):
        return stored
    if blob[:4] != STORE_ULMEN_MARKER:
        return stored  # pragma: no cover
    raw     = zlib.decompress(blob[4:])
    records = decode_binary_records(raw)
    if not records:  # pragma: no cover
        return stored
    rec = records[0] if isinstance(records, list) else records
    return {k: v for k, v in rec.items() if k != "__type"}


def _decode_item(item: Any) -> Any:
    """Decode the value field of a store Item if it is ULMEN-encoded."""
    if item is None:
        return None
    if hasattr(item, "value"):
        decoded = _decode_value(item.value)
        if decoded is not item.value:
            try:
                return item.__class__(**{**vars(item), "value": decoded})
            except Exception:
                object.__setattr__(item, "value", decoded)
    return item


# ---------------------------------------------------------------------------
# UlmenStore
# ---------------------------------------------------------------------------

class UlmenStore(BaseStore):
    """
    ULMEN-compressed wrapper for any LangGraph BaseStore.

    Parameters
    ----------
    store       : Any BaseStore instance (InMemoryStore or custom).
    zlib_level  : zlib compression level 0-9. Default 6.
    """

    def __init__(
        self,
        store: BaseStore,
        zlib_level: int = DEFAULT_ZLIB_LEVEL,
    ) -> None:
        self._inner      = store
        self._zlib_level = zlib_level

    # ------------------------------------------------------------------
    # Sync interface
    # ------------------------------------------------------------------

    def put(
        self,
        namespace: tuple[str, ...],
        key: str,
        value: dict[str, Any],
        index: Literal[False] | list[str] | None = None,
        *,
        ttl: float | NotProvided | None = NOT_PROVIDED,
    ) -> None:
        encoded = _encode_value(value, self._zlib_level)
        self._inner.put(namespace, key, encoded, index, ttl=ttl)

    def get(
        self,
        namespace: tuple[str, ...],
        key: str,
        *,
        refresh_ttl: bool | None = None,
    ) -> Any | None:
        item = self._inner.get(namespace, key, refresh_ttl=refresh_ttl)
        return _decode_item(item)

    def delete(self, namespace: tuple[str, ...], key: str) -> None:
        self._inner.delete(namespace, key)

    def search(
        self,
        namespace_prefix: tuple[str, ...],
        /,
        *,
        query: str | None = None,
        filter: dict[str, Any] | None = None,
        limit: int = 10,
        offset: int = 0,
        refresh_ttl: bool | None = None,
    ) -> list[Any]:
        items = self._inner.search(
            namespace_prefix,
            query=query,
            filter=filter,
            limit=limit,
            offset=offset,
            refresh_ttl=refresh_ttl,
        )
        return [_decode_item(item) for item in items]

    def list_namespaces(
        self,
        *,
        prefix: Any = None,
        suffix: Any = None,
        max_depth: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[tuple[str, ...]]:
        return self._inner.list_namespaces(
            prefix=prefix,
            suffix=suffix,
            max_depth=max_depth,
            limit=limit,
            offset=offset,
        )

    def batch(self, ops: Iterable[Any]) -> list[Any]:
        return self._inner.batch(ops)

    # ------------------------------------------------------------------
    # Async interface
    # ------------------------------------------------------------------

    async def aput(
        self,
        namespace: tuple[str, ...],
        key: str,
        value: dict[str, Any],
        index: Literal[False] | list[str] | None = None,
        *,
        ttl: float | NotProvided | None = NOT_PROVIDED,
    ) -> None:
        encoded = _encode_value(value, self._zlib_level)
        await self._inner.aput(namespace, key, encoded, index, ttl=ttl)

    async def aget(
        self,
        namespace: tuple[str, ...],
        key: str,
        *,
        refresh_ttl: bool | None = None,
    ) -> Any | None:
        item = await self._inner.aget(namespace, key, refresh_ttl=refresh_ttl)
        return _decode_item(item)

    async def adelete(self, namespace: tuple[str, ...], key: str) -> None:
        await self._inner.adelete(namespace, key)

    async def asearch(
        self,
        namespace_prefix: tuple[str, ...],
        /,
        *,
        query: str | None = None,
        filter: dict[str, Any] | None = None,
        limit: int = 10,
        offset: int = 0,
        refresh_ttl: bool | None = None,
    ) -> list[Any]:
        items = await self._inner.asearch(
            namespace_prefix,
            query=query,
            filter=filter,
            limit=limit,
            offset=offset,
            refresh_ttl=refresh_ttl,
        )
        return [_decode_item(item) for item in items]

    async def alist_namespaces(
        self,
        *,
        prefix: Any = None,
        suffix: Any = None,
        max_depth: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[tuple[str, ...]]:
        return await self._inner.alist_namespaces(
            prefix=prefix,
            suffix=suffix,
            max_depth=max_depth,
            limit=limit,
            offset=offset,
        )

    async def abatch(self, ops: Iterable[Any]) -> list[Any]:
        return await self._inner.abatch(ops)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def inner(self) -> BaseStore:
        return self._inner  # pragma: no cover

    def __repr__(self) -> str:
        return (
            f"UlmenStore("
            f"inner={type(self._inner).__name__}, "
            f"zlib_level={self._zlib_level})"
        )
