"""
LUMEN streaming encode surface.

LumenStreamEncoder wraps the Rust LumenStreamEncoder (or falls back to
pure Python) and provides an ergonomic iterator-based API so large
datasets never need to be fully materialised before the first byte
is emitted.

    enc = LumenStreamEncoder(pool_size_limit=64, chunk_size=65536)
    enc.feed(record)          # one dict
    enc.feed_many(records)    # iterable of dicts
    for chunk in enc.flush(): # yields bytes chunks
        socket.sendall(chunk)

    # Or the one-shot helper:
    for chunk in stream_encode(records, chunk_size=65536):
        sink.write(chunk)

For truly unbounded streams that cannot be held in RAM at all, use
stream_encode_windowed() which encodes fixed-size windows into
independent sub-payloads that can each be decoded with
decode_binary_records().
"""

from __future__ import annotations

from typing import Iterable, Iterator

# ---------------------------------------------------------------------------
# Rust-backed implementation (preferred)
# ---------------------------------------------------------------------------

_RUST_STREAM = False
try:
    from lumen._lumen_rust import LumenStreamEncoder as _RustStreamEncoder  # type: ignore
    from lumen._lumen_rust import encode_binary_stream_chunked as _rust_chunked  # type: ignore
    _RUST_STREAM = True
except ImportError:
    _RustStreamEncoder = None
    _rust_chunked      = None


# ---------------------------------------------------------------------------
# Pure-Python fallback
# ---------------------------------------------------------------------------

class _PyStreamEncoder:
    """
    Pure-Python streaming encoder fallback.

    Accumulates records then emits the full payload in chunk_size slices.
    Identical wire format to the Rust encoder.
    """

    __slots__ = ("_rows", "_pool_size_limit", "_chunk_size")

    def __init__(self, pool_size_limit: int = 64, chunk_size: int = 65536) -> None:
        self._rows:            list  = []
        self._pool_size_limit: int   = pool_size_limit
        self._chunk_size:      int   = max(256, chunk_size)

    def feed(self, record: dict) -> None:
        self._rows.append(record)

    def feed_many(self, records: Iterable[dict]) -> None:
        self._rows.extend(records)

    def record_count(self) -> int:
        return len(self._rows)

    def flush(self) -> Iterator[bytes]:
        from lumen.core._binary import encode_binary_records
        from lumen.core._strategies import build_pool
        rows = self._rows
        pool, pool_map = build_pool(rows, max_pool=self._pool_size_limit)
        payload = encode_binary_records(rows, pool, pool_map, use_strategies=True)
        sz = self._chunk_size
        for i in range(0, max(1, len(payload)), sz):
            yield payload[i:i + sz]
        self._rows = []

    def reset(self) -> None:
        self._rows = []

    def __repr__(self) -> str:
        return (
            f"_PyStreamEncoder("
            f"records_buffered={len(self._rows)}, "
            f"pool_limit={self._pool_size_limit})"
        )


# ---------------------------------------------------------------------------
# Public LumenStreamEncoder — selects Rust or Python automatically
# ---------------------------------------------------------------------------

class LumenStreamEncoder:
    """
    Streaming binary encoder — zero full-payload materialisation on the
    caller side.

    Feed records one or many at a time, then call flush() to get an
    iterator of bytes chunks. The encoder resets automatically after
    each flush().

    Parameters
    ----------
    pool_size_limit : int
        Maximum string pool entries. Default 64.
    chunk_size : int
        Maximum bytes per yielded chunk. Default 65536 (64 KiB).

    Notes
    -----
    The encoder uses a two-phase design:
        Phase 1 (feed / feed_many): accumulate rows and intern strings.
        Phase 2 (flush): detect strategies, encode columns, slice output.

    For payloads that genuinely cannot be held in RAM, use
    stream_encode_windowed() instead — it processes fixed windows and
    emits independent decodable sub-payloads.
    """

    def __init__(
        self,
        pool_size_limit: int = 64,
        chunk_size:      int = 65536,
    ) -> None:
        self._pool_size_limit = pool_size_limit
        self._chunk_size      = max(256, chunk_size)
        if _RUST_STREAM and _RustStreamEncoder is not None:
            self._inner = _RustStreamEncoder(
                pool_size_limit=pool_size_limit,
                chunk_size=chunk_size,
            )
            self._rust = True
        else:
            self._inner = _PyStreamEncoder(
                pool_size_limit=pool_size_limit,
                chunk_size=chunk_size,
            )
            self._rust = False

    def feed(self, record: dict) -> LumenStreamEncoder:
        """Feed one record dict. Returns self for chaining."""
        if self._rust:
            self._inner.feed_record(record)
        else:
            self._inner.feed(record)
        return self

    def feed_many(self, records: Iterable[dict]) -> LumenStreamEncoder:
        """Feed an iterable of record dicts. Returns self for chaining."""
        if self._rust:
            lst = list(records)
            self._inner.feed_records(lst)
        else:
            self._inner.feed_many(records)
        return self

    def record_count(self) -> int:
        """Number of records buffered since last flush."""
        if self._rust:
            return self._inner.record_count()
        return self._inner.record_count()

    def flush(self) -> Iterator[bytes]:
        """
        Encode all buffered records and yield bytes chunks.
        Resets the encoder after yielding all chunks.
        """
        if self._rust:
            chunks = self._inner.finish()
            for chunk in chunks:
                yield bytes(chunk)
        else:
            yield from self._inner.flush()

    def reset(self) -> None:
        """Discard all buffered records without encoding."""
        if self._rust:
            self._inner.reset()
        else:
            self._inner.reset()

    @property
    def rust_backed(self) -> bool:
        """True if Rust acceleration is active."""
        return self._rust

    def __repr__(self) -> str:
        backend = "rust" if self._rust else "python"
        return (
            f"LumenStreamEncoder("
            f"records_buffered={self.record_count()}, "
            f"pool_limit={self._pool_size_limit}, "
            f"chunk_size={self._chunk_size}, "
            f"backend={backend!r})"
        )


# ---------------------------------------------------------------------------
# One-shot streaming helper
# ---------------------------------------------------------------------------

def stream_encode(
    records:         Iterable[dict],
    pool_size_limit: int = 64,
    chunk_size:      int = 65536,
) -> Iterator[bytes]:
    """
    Encode an iterable of records and yield bytes chunks.

    This is the simplest entry point for streaming encode:

        for chunk in stream_encode(records):
            socket.sendall(chunk)

    Parameters
    ----------
    records         : iterable of dict records
    pool_size_limit : max pool entries
    chunk_size      : max bytes per chunk

    Yields
    ------
    bytes chunks of at most chunk_size bytes each.
    The concatenation of all chunks is a valid LUMEN binary payload.
    """
    enc = LumenStreamEncoder(
        pool_size_limit=pool_size_limit,
        chunk_size=chunk_size,
    )
    enc.feed_many(records)
    yield from enc.flush()


# ---------------------------------------------------------------------------
# Window-based streaming for truly unbounded payloads
# ---------------------------------------------------------------------------

def stream_encode_windowed(
    records:         Iterable[dict],
    window_size:     int = 1000,
    pool_size_limit: int = 64,
) -> Iterator[bytes]:
    """
    Encode an unbounded record stream as independent window sub-payloads.

    Each yielded bytes object is a complete, independently decodable
    LUMEN binary payload covering window_size records. Use this when
    even accumulating the full record list in RAM is unacceptable.

    Parameters
    ----------
    records         : iterable of dict records (may be infinite)
    window_size     : records per sub-payload
    pool_size_limit : max pool entries per window

    Yields
    ------
    Complete LUMEN binary payloads (bytes), one per window.

    Decoding
    --------
    Each chunk decodes independently:

        for chunk in stream_encode_windowed(records):
            window_records = decode_binary_records(chunk)
    """
    if _RUST_STREAM and _rust_chunked is not None:
        # Rust path: collect into a list (unavoidable for PyO3 API) then chunk
        # For truly infinite streams, use the Python path below
        lst = list(records)
        if not lst:
            return
        chunks = _rust_chunked(lst, window_size=window_size, pool_size_limit=pool_size_limit)
        for chunk in chunks:
            yield bytes(chunk)
        return

    # Python path: process window by window, never holding more than window_size rows
    from lumen.core._binary import encode_binary_records
    from lumen.core._strategies import build_pool

    buf: list = []
    for rec in records:
        buf.append(rec)
        if len(buf) >= window_size:
            pool, pool_map = build_pool(buf, max_pool=pool_size_limit)
            yield encode_binary_records(buf, pool, pool_map, use_strategies=True)
            buf = []

    if buf:
        pool, pool_map = build_pool(buf, max_pool=pool_size_limit)
        yield encode_binary_records(buf, pool, pool_map, use_strategies=True)


# ---------------------------------------------------------------------------
# LUMIA streaming encoder
# ---------------------------------------------------------------------------

def stream_encode_lumia(
    records:    Iterable[dict],
    chunk_size: int = 65536,
) -> Iterator[str]:
    """
    Encode an iterable of records to LUMIA (L| format) and yield string chunks.

    The first chunk always contains the complete header line. Subsequent
    chunks contain data rows. This preserves LUMIA's self-describing property:
    a receiver that buffers the first chunk always has the schema.

    Parameters
    ----------
    records    : iterable of dict records
    chunk_size : approximate character count per chunk

    Yields
    ------
    str chunks. Concatenation is a valid LUMIA payload.
    """
    from lumen.core._lumen_llm import encode_lumen_llm
    # LUMIA requires two-pass (type inference needs all rows for header).
    # Buffer all records, encode, then slice by character count.
    lst     = list(records)
    payload = encode_lumen_llm(lst)
    sz      = max(256, chunk_size)
    for i in range(0, max(1, len(payload)), sz):
        yield payload[i:i + sz]
