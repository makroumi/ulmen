"""
LUMEN V1 -- Lightweight Universal Minimal Encoding Notation
Copyright (c) El Mehdi Makroumi. All rights reserved.
Proprietary and confidential.

Three surfaces over a single data model:
    Binary   -- smallest on wire, columnar, pool + strategies
    Text     -- human-readable, diff-friendly
    LUMIA    -- LLM-native typed CSV (L| prefix)

Agentic protocol:
    LUMEN-AGENT -- structured typed pipe-delimited agent communication

Usage
-----
    from lumen import LumenDict, LumenDictRust
    from lumen import encode_lumen_llm, decode_lumen_llm
    from lumen import encode_agent_payload, decode_agent_payload
    from lumen import compress_context, decode_agent_stream

    records = [{"id": i, "name": "Alice", "score": 98.5} for i in range(1000)]

    ld     = LumenDict(records)
    text   = ld.encode_text()
    lumia  = ld.encode_lumen_llm()
    binary = ld.encode_binary_pooled()
    zlib_  = ld.encode_binary_zlib()
"""

# ---------------------------------------------------------------------------
# msgpack compatibility shim — zero external dependencies
# Makes `import msgpack` available after `import lumen` for benchmarks.
# ---------------------------------------------------------------------------
import sys as _sys

if 'msgpack' not in _sys.modules:
    from lumen.core import _msgpack_compat as _mp_shim
    _sys.modules['msgpack'] = _mp_shim

import zlib as _zlib

# ---------------------------------------------------------------------------
# Python reference implementation -- always available
# ---------------------------------------------------------------------------
from lumen.core import (
    LUMEN_LLM_MAGIC,
    MAGIC,
    S_BITS,
    S_DELTA,
    S_POOL,
    S_RAW,
    S_RLE,
    T_BITS,
    T_BOOL,
    T_DELTA_RAW,
    T_FLOAT,
    T_INT,
    T_LIST,
    T_MAP,
    T_MATRIX,
    T_NULL,
    T_POOL_DEF,
    T_POOL_REF,
    T_RLE,
    T_STR,
    T_STR_TINY,
    VERSION,
    LumenDict,
    LumenDictFull,
    __edition__,
    __version__,
    _encode_obj_iterative_text,
    _encode_value_text,
    _format_float,
    _parse_value,
    _text_escape,
    _text_unescape,
    build_pool,
    compute_bits_savings,
    compute_delta_savings,
    compute_rle_savings,
    decode_binary_records,
    decode_lumen_llm,
    decode_text_records,
    decode_varint,
    decode_zigzag,
    deep_eq,
    deep_size,
    detect_column_strategy,
    encode_binary_records,
    encode_lumen_llm,
    encode_text_records,
    encode_varint,
    encode_zigzag,
    estimate_tokens,
    fnv1a,
    fnv1a_str,
    pack_bits,
    pack_bool,
    pack_delta_raw,
    pack_float,
    pack_int,
    pack_null,
    pack_pool_ref,
    pack_rle,
    pack_string,
    unpack_bits,
    unpack_delta_raw,
)

# ---------------------------------------------------------------------------
# LUMEN-AGENT -- always available
# ---------------------------------------------------------------------------
from lumen.core._agent import (
    AGENT_MAGIC,
    AGENT_VERSION,
    COMPRESS_COMPLETED_SEQUENCES,
    COMPRESS_KEEP_TYPES,
    COMPRESS_SLIDING_WINDOW,
    FIELD_COUNTS,
    META_FIELDS,
    PRIORITY_COMPRESSIBLE,
    PRIORITY_KEEP_IF_ROOM,
    PRIORITY_MUST_KEEP,
    RECORD_TYPES,
    AgentHeader,
    ContextBudgetExceededError,
    ValidationError,
    build_summary_chain,
    chunk_payload,
    compress_context,
    convert_agent_to_lumia,
    convert_lumia_to_agent,
    decode_agent_payload,
    decode_agent_payload_full,
    decode_agent_record,
    decode_agent_stream,
    dedup_mem,
    encode_agent_payload,
    encode_agent_record,
    estimate_context_usage,
    extract_subgraph,
    extract_subgraph_payload,
    generate_system_prompt,
    get_latest_mem,
    make_validation_error,
    merge_chunks,
    validate_agent_payload,
    validate_schema_compliance,
    migrate_schema,
    DEFAULT_SCHEMA_VERSION,
    SCHEMA_VERSIONS,
)


# ---------------------------------------------------------------------------
# Public API surface
# ---------------------------------------------------------------------------

__all__ = [
    # Core classes
    "LumenDict", "LumenDictFull", "LumenDictRust", "LumenDictFullRust",
    "RUST_AVAILABLE",
    # Primitives
    "encode_varint", "decode_varint",
    "encode_zigzag", "decode_zigzag",
    "pack_string", "pack_int", "pack_float", "pack_bool", "pack_null",
    "pack_pool_ref",
    "pack_bits", "pack_delta_raw", "pack_rle",
    "unpack_bits", "unpack_delta_raw",
    # Strategies
    "build_pool", "detect_column_strategy",
    "compute_delta_savings", "compute_rle_savings", "compute_bits_savings",
    # Text codec
    "encode_text_records", "decode_text_records",
    # Binary codec
    "encode_binary_records", "decode_binary_records",
    # LUMIA
    "encode_lumen_llm", "decode_lumen_llm", "LUMEN_LLM_MAGIC",
    # LUMEN-AGENT
    "encode_agent_payload", "decode_agent_payload", "decode_agent_payload_full",
    "decode_agent_record", "encode_agent_record",
    "decode_agent_stream",
    "validate_agent_payload",
    "compress_context",
    "estimate_context_usage",
    "extract_subgraph", "extract_subgraph_payload",
    "make_validation_error",
    "AGENT_MAGIC", "AGENT_VERSION", "RECORD_TYPES", "FIELD_COUNTS",
    "DEFAULT_SCHEMA_VERSION", "SCHEMA_VERSIONS",
    "validate_schema_compliance", "migrate_schema",
    "META_FIELDS",
    "PRIORITY_MUST_KEEP", "PRIORITY_KEEP_IF_ROOM", "PRIORITY_COMPRESSIBLE",
    "COMPRESS_COMPLETED_SEQUENCES", "COMPRESS_KEEP_TYPES", "COMPRESS_SLIDING_WINDOW",
    "AgentHeader",
    "ContextBudgetExceededError",
    "ValidationError",
    "build_summary_chain",
    "chunk_payload",
    "convert_agent_to_lumia",
    "convert_lumia_to_agent",
    "decode_agent_record",
    "decode_agent_stream",
    "dedup_mem",
    "encode_agent_record",
    "generate_system_prompt",
    "get_latest_mem",
    "merge_chunks",
    # Utilities
    "fnv1a", "fnv1a_str", "estimate_tokens",
    "deep_size", "deep_eq",
    # Constants
    "MAGIC", "VERSION", "__version__", "__edition__",
    # Text helpers
    "_encode_value_text", "_encode_obj_iterative_text",
    "_text_escape", "_text_unescape",
    "_format_float", "_parse_value",
    # Wire-format tags
    "T_STR_TINY", "T_STR", "T_INT", "T_FLOAT", "T_BOOL", "T_NULL",
    "T_LIST", "T_MAP", "T_POOL_DEF", "T_POOL_REF", "T_MATRIX",
    "T_DELTA_RAW", "T_BITS", "T_RLE",
    "S_RAW", "S_DELTA", "S_RLE", "S_BITS", "S_POOL",
    # New capabilities
    "count_tokens_exact", "count_tokens_exact_records",
    "AgentRouter", "validate_routing_consistency",
    "ThreadRegistry", "merge_threads",
    "ReplayLog",
    "parse_llm_output",
    # Streaming
    "LumenStreamEncoder",
    "stream_encode",
    "stream_encode_lumia",
    "stream_encode_windowed",
]

# ---------------------------------------------------------------------------
# New capability modules — zero dependencies
# ---------------------------------------------------------------------------
from lumen.core._repair import (
    parse_llm_output,
)
from lumen.core._replay import (
    ReplayLog,
)
from lumen.core._routing import (
    AgentRouter,
    validate_routing_consistency,
)
from lumen.core._threading import (
    ThreadRegistry,
    merge_threads,
)
from lumen.core._tokens import (
    count_tokens_exact,
    count_tokens_exact_records,
)

# ---------------------------------------------------------------------------
# Rust acceleration -- optional, falls back to Python silently
# ---------------------------------------------------------------------------

RUST_AVAILABLE = False

try:
    from lumen._lumen_rust import LumenDictFullRust, LumenDictRust  # type: ignore
    from lumen._lumen_rust import decode_binary_records_rust as _dbr_rust  # type: ignore
    from lumen._lumen_rust import decode_lumen_llm_rust as _dll_rust  # type: ignore
    from lumen._lumen_rust import encode_lumen_llm_rust as _ell_rust  # type: ignore

    def decode_binary_records(data: bytes) -> list:  # type: ignore[no-redef]
        """Decode LUMEN binary using Rust-accelerated decoder."""
        return _dbr_rust(data)

    def encode_lumen_llm(records: list) -> str:  # type: ignore[no-redef]
        """Encode records to LUMIA format using Rust acceleration."""
        return _ell_rust(records)

    def decode_lumen_llm(text: str) -> list:  # type: ignore[no-redef]
        """Decode LUMIA format using Rust acceleration."""
        return _dll_rust(text)

    RUST_AVAILABLE = True

except ImportError:  # pragma: no cover

    class LumenDictRust(LumenDict):  # type: ignore[no-redef]
        """Python shim for LumenDictRust when Rust extension is not compiled."""

        def encode_binary_pooled_raw(self) -> bytes:
            return encode_binary_records(
                self._data, self._pool, self._pool_map,
                use_strategies=False,
            )

        def encode_binary_zlib(self, level: int = 6) -> bytes:
            return _zlib.compress(self.encode_binary_pooled(), level)

        def encode_lumen_llm(self) -> str:
            from lumen.core._lumen_llm import encode_lumen_llm as _enc
            return _enc(self._data)

        def bench_encode_text_only(self, iters: int) -> int:
            return len(self.encode_text()) * iters

        def bench_encode_binary_only(self, iters: int) -> int:
            return len(self.encode_binary_pooled()) * iters

        def bench_encode_text_clone(self, iters: int) -> int:
            return len(self.encode_text()) * iters

        def bench_encode_binary_clone(self, iters: int) -> int:
            return len(self.encode_binary_pooled()) * iters

        def __repr__(self) -> str:
            return (
                f"LumenDictRust[PythonShim]("
                f"records={len(self._data)}, pool={len(self._pool)})"
            )

    class LumenDictFullRust(LumenDictFull):  # type: ignore[no-redef]
        """Python shim for LumenDictFullRust when Rust extension is not compiled."""

        def encode_binary_pooled_raw(self) -> bytes:
            return encode_binary_records(
                self._data, self._pool, self._pool_map,
                use_strategies=False,
            )

        def encode_binary_zlib(self, level: int = 6) -> bytes:
            return _zlib.compress(self.encode_binary_pooled(), level)

        def encode_lumen_llm(self) -> str:
            from lumen.core._lumen_llm import encode_lumen_llm as _enc
            return _enc(self._data)

        def bench_encode_text_only(self, iters: int) -> int:
            return len(self.encode_text()) * iters

        def bench_encode_binary_only(self, iters: int) -> int:
            return len(self.encode_binary_pooled()) * iters

        def bench_encode_text_clone(self, iters: int) -> int:
            return len(self.encode_text()) * iters

        def bench_encode_binary_clone(self, iters: int) -> int:
            return len(self.encode_binary_pooled()) * iters

        def __repr__(self) -> str:
            return (
                f"LumenDictFullRust[PythonShim]("
                f"records={len(self._data)}, pool={len(self._pool)})"
            )

# ---------------------------------------------------------------------------
# LUMEN-AGENT Rust acceleration shims (GAP #5)
# Python implementations exposed under Rust-named symbols for API compat.
# ---------------------------------------------------------------------------

def encode_agent_payload_rust(
    records,
    thread_id=None,
    context_window=None,
    meta_fields=(),
    **kwargs,
) -> str:
    """LUMEN-AGENT encode with Rust-compatible API (Python implementation)."""
    from lumen.core._agent import encode_agent_payload as _enc
    return _enc(records, thread_id=thread_id,
                context_window=context_window, meta_fields=meta_fields, **kwargs)


def decode_agent_payload_rust(text: str):
    """LUMEN-AGENT decode with Rust-compatible API (Python implementation)."""
    from lumen.core._agent import decode_agent_payload as _dec
    return _dec(text)


# ---------------------------------------------------------------------------
# Streaming encode surface
# ---------------------------------------------------------------------------
from lumen.core._streaming import (
    LumenStreamEncoder,
    stream_encode,
    stream_encode_lumia,
    stream_encode_windowed,
)
