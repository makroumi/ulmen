# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
#
# This file is part of ULMEN.
# ULMEN is licensed under the Business Source License 1.1.
# See the LICENSE file in the project root for full license information.

"""
ULMEN V1 -- Ultra Lightweight Minimal Encoding Notation
Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
ULMEN is licensed under the Business Source License 1.1.

Three surfaces over a single data model:
    Binary   -- smallest on wire, columnar, pool + strategies
    Text     -- human-readable, diff-friendly
    ULMEN    -- LLM-native typed CSV (L| prefix)

Agentic protocol:
    ULMEN-AGENT -- structured typed pipe-delimited agent communication

Usage
-----
    from ulmen import UlmenDict, UlmenDictRust
    from ulmen import encode_ulmen_llm, decode_ulmen_llm
    from ulmen import encode_agent_payload, decode_agent_payload
    from ulmen import compress_context, decode_agent_stream

    records = [{"id": i, "name": "Alice", "score": 98.5} for i in range(1000)]

    ld     = UlmenDict(records)
    text   = ld.encode_text()
    ulmen  = ld.encode_ulmen_llm()
    binary = ld.encode_binary_pooled()
    zlib_  = ld.encode_binary_zlib()
"""

# ---------------------------------------------------------------------------
# msgpack compatibility shim — zero external dependencies
# Makes `import msgpack` available after `import ulmen` for benchmarks.
# ---------------------------------------------------------------------------
import sys as _sys

if 'msgpack' not in _sys.modules:
    from ulmen.core import _msgpack_compat as _mp_shim
    _sys.modules['msgpack'] = _mp_shim

import zlib as _zlib

# ---------------------------------------------------------------------------
# Python reference implementation -- always available
# ---------------------------------------------------------------------------
from ulmen.core import (
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
    ULMEN_LLM_MAGIC,
    VERSION,
    UlmenDict,
    UlmenDictFull,
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
    decode_text_records,
    decode_ulmen_llm,
    decode_varint,
    decode_zigzag,
    deep_eq,
    deep_size,
    detect_column_strategy,
    encode_binary_records,
    encode_text_records,
    encode_ulmen_llm,
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
# ULMEN-AGENT -- always available
# ---------------------------------------------------------------------------
from ulmen.core._agent import (
    AGENT_MAGIC,
    AGENT_VERSION,
    COMPRESS_COMPLETED_SEQUENCES,
    COMPRESS_KEEP_TYPES,
    COMPRESS_SLIDING_WINDOW,
    DEFAULT_SCHEMA_VERSION,
    FIELD_COUNTS,
    META_FIELDS,
    PRIORITY_COMPRESSIBLE,
    PRIORITY_KEEP_IF_ROOM,
    PRIORITY_MUST_KEEP,
    RECORD_TYPES,
    SCHEMA_VERSIONS,
    AgentHeader,
    ContextBudgetExceededError,
    ValidationError,
    build_summary_chain,
    chunk_payload,
    compress_context,
    convert_agent_to_ulmen,
    convert_ulmen_to_agent,
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
    migrate_schema,
    validate_agent_payload,
    validate_schema_compliance,
)

# ---------------------------------------------------------------------------
# Public API surface
# ---------------------------------------------------------------------------

__all__ = [
    # Core classes
    "UlmenDict", "UlmenDictFull", "UlmenDictRust", "UlmenDictFullRust",
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
    # ULMEN
    "encode_ulmen_llm", "decode_ulmen_llm", "ULMEN_LLM_MAGIC",
    # ULMEN-AGENT
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
    "convert_agent_to_ulmen",
    "convert_ulmen_to_agent",
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
    # JSON bridge
    "from_json", "to_json", "compare_sizes",
    # Streaming
    "UlmenStreamEncoder",
    "stream_encode",
    "stream_encode_ulmen",
    "stream_encode_windowed",
]

# ---------------------------------------------------------------------------
# New capability modules — zero dependencies
# ---------------------------------------------------------------------------
from ulmen.core._repair import (
    parse_llm_output,
)
from ulmen.core._replay import (
    ReplayLog,
)
from ulmen.core._routing import (
    AgentRouter,
    validate_routing_consistency,
)
from ulmen.core._threading import (
    ThreadRegistry,
    merge_threads,
)
from ulmen.core._tokens import (
    count_tokens_exact,
    count_tokens_exact_records,
)

# ---------------------------------------------------------------------------
# Rust acceleration -- optional, falls back to Python silently
# ---------------------------------------------------------------------------

RUST_AVAILABLE = False

try:
    from ulmen._ulmen_rust import UlmenDictFullRust, UlmenDictRust  # type: ignore
    from ulmen._ulmen_rust import decode_binary_records_rust as _dbr_rust  # type: ignore
    from ulmen._ulmen_rust import decode_ulmen_llm_rust as _dll_rust  # type: ignore
    from ulmen._ulmen_rust import encode_ulmen_llm_rust as _ell_rust  # type: ignore
    from ulmen._ulmen_rust import (  # type: ignore
        encode_agent_record_rust   as _ear_rust,
        decode_agent_record_rust   as _dar_rust,
        encode_agent_payload_rust  as _eap_rust,
        decode_agent_payload_rust  as _dap_rust,
        validate_agent_payload_rust as _vap_rust,
    )

    def decode_binary_records(data: bytes) -> list:  # type: ignore[no-redef]
        """Decode ULMEN binary using Rust-accelerated decoder."""
        return _dbr_rust(data)

    def encode_ulmen_llm(records: list) -> str:  # type: ignore[no-redef]
        """Encode records to ULMEN format using Rust acceleration."""
        return _ell_rust(records)

    def decode_ulmen_llm(text: str) -> list:  # type: ignore[no-redef]
        """Decode ULMEN format using Rust acceleration."""
        return _dll_rust(text)

    def encode_agent_record(rec: dict, meta_fields: tuple = ()) -> str:  # type: ignore[no-redef]
        """Encode a single agent record to a pipe-delimited row (Rust)."""
        return _ear_rust(rec, list(meta_fields))

    def decode_agent_record(line: str, meta_fields: tuple = ()) -> dict:  # type: ignore[no-redef]
        """Decode a pipe-delimited row to an agent record dict (Rust)."""
        return _dar_rust(line, list(meta_fields))

    def encode_agent_payload(  # type: ignore[no-redef]
        records,
        thread_id=None,
        context_window=None,
        meta_fields=(),
        auto_context=True,
        enforce_budget=False,
        payload_id=None,
        parent_payload_id=None,
        agent_id=None,
        session_id=None,
        schema_version=None,
        auto_payload_id=False,
    ) -> str:
        """Encode agent records to a ULMEN-AGENT v1 payload (Rust)."""
        return _eap_rust(
            records,
            thread_id=thread_id,
            context_window=context_window,
            meta_fields=list(meta_fields) if meta_fields else [],
            auto_context=auto_context,
            enforce_budget=enforce_budget,
            payload_id=payload_id,
            parent_payload_id=parent_payload_id,
            agent_id=agent_id,
            session_id=session_id,
            schema_version=schema_version,
            auto_payload_id=auto_payload_id,
        )

    def decode_agent_payload(text: str) -> list:  # type: ignore[no-redef]
        """Decode a ULMEN-AGENT v1 payload to a list of record dicts (Rust)."""
        return list(_dap_rust(text))

    def validate_agent_payload(  # type: ignore[no-redef]
        text: str,
        structured: bool = False,
    ) -> tuple:
        """Validate a ULMEN-AGENT v1 payload (Rust)."""
        return _vap_rust(text, structured)

    # M1c-M1g: stream decoder, compression, chunking, repair, token counting
    from ulmen._ulmen_rust import (  # type: ignore
        decode_agent_stream_rust    as _das_rust,
        compress_context_rust       as _cc_rust,
        chunk_payload_rust          as _cp_rust,
        merge_chunks_rust           as _mc_rust,
        parse_llm_output_rust       as _plo_rust,
        count_tokens_exact_rust     as _cte_rust,
        count_tokens_exact_records_rust as _cter_rust,
        from_json                   as _from_json_rust,
        to_json                     as _to_json_rust,
        compare_sizes               as _compare_sizes_rust,
    )

    def decode_agent_stream(lines):  # type: ignore[no-redef]
        """Stream-decode a ULMEN-AGENT v1 payload (Rust)."""
        return iter(_das_rust(list(lines)))

    def compress_context(  # type: ignore[no-redef]
        records, strategy="completed_sequences", keep_priority=2,
        target_reduction=0.5, keep_types=None, window_size=None,
        preserve_cot=False,
    ):
        """Compress agent records to reduce context window usage (Rust)."""
        return list(_cc_rust(
            records, strategy=strategy, keep_priority=keep_priority,
            target_reduction=target_reduction, keep_types=keep_types,
            window_size=window_size, preserve_cot=preserve_cot,
        ))

    def chunk_payload(  # type: ignore[no-redef]
        records, token_budget, thread_id=None, meta_fields=(),
        overlap=0, parent_payload_id=None, session_id=None,
    ):
        """Split records into chunked payloads (Rust)."""
        return list(_cp_rust(
            records, token_budget, thread_id=thread_id,
            meta_fields=list(meta_fields) if meta_fields else [],
            overlap=overlap, parent_payload_id=parent_payload_id,
            session_id=session_id,
        ))

    def merge_chunks(payloads):  # type: ignore[no-redef]
        """Merge chunked payloads back into records (Rust)."""
        return list(_mc_rust(payloads))

    def parse_llm_output(raw_text, thread_id=None, strict=False):  # type: ignore[no-redef]
        """Parse and repair raw LLM output (Rust)."""
        return _plo_rust(raw_text, thread_id=thread_id, strict=strict)

    def count_tokens_exact(text):  # type: ignore[no-redef]
        """Count tokens using cl100k_base BPE approximation (Rust)."""
        return _cte_rust(text)

    def count_tokens_exact_records(text, per_record_overhead=3):  # type: ignore[no-redef]
        """Count tokens with per-record overhead (Rust)."""
        return _cter_rust(text, per_record_overhead=per_record_overhead)

    def from_json(json_str, thread_id=None, context_window=None):  # type: ignore[no-redef]
        """Convert JSON array of records to ULMEN-AGENT payload (Rust)."""
        return _from_json_rust(json_str, thread_id=thread_id, context_window=context_window)

    def to_json(payload, pretty=False):  # type: ignore[no-redef]
        """Convert ULMEN-AGENT payload to JSON string (Rust)."""
        return _to_json_rust(payload, pretty=pretty)

    def compare_sizes(json_str):  # type: ignore[no-redef]
        """Compare JSON vs ULMEN-AGENT sizes. Returns (json_bytes, ulmen_bytes, saving_pct)."""
        return _compare_sizes_rust(json_str)

    RUST_AVAILABLE = True

except ImportError:  # pragma: no cover

    class UlmenDictRust(UlmenDict):  # type: ignore[no-redef]
        """Python shim for UlmenDictRust when Rust extension is not compiled."""

        def encode_binary_pooled_raw(self) -> bytes:
            return encode_binary_records(
                self._data, self._pool, self._pool_map,
                use_strategies=False,
            )

        def encode_binary_zlib(self, level: int = 6) -> bytes:
            return _zlib.compress(self.encode_binary_pooled(), level)

        def encode_ulmen_llm(self) -> str:
            from ulmen.core._ulmen_llm import encode_ulmen_llm as _enc
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
                f"UlmenDictRust[PythonShim]("
                f"records={len(self._data)}, pool={len(self._pool)})"
            )

    class UlmenDictFullRust(UlmenDictFull):  # type: ignore[no-redef]
        """Python shim for UlmenDictFullRust when Rust extension is not compiled."""

        def encode_binary_pooled_raw(self) -> bytes:
            return encode_binary_records(
                self._data, self._pool, self._pool_map,
                use_strategies=False,
            )

        def encode_binary_zlib(self, level: int = 6) -> bytes:
            return _zlib.compress(self.encode_binary_pooled(), level)

        def encode_ulmen_llm(self) -> str:
            from ulmen.core._ulmen_llm import encode_ulmen_llm as _enc
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
                f"UlmenDictFullRust[PythonShim]("
                f"records={len(self._data)}, pool={len(self._pool)})"
            )

    # Fallback: from_json / to_json / compare_sizes when Rust not available
    def from_json(json_str, thread_id=None, context_window=None):
        """Convert JSON array of records to ULMEN-AGENT payload (Python fallback)."""
        import json as _json
        from ulmen.core._agent import encode_agent_payload as _eap
        records = _json.loads(json_str)
        return _eap(records, thread_id=thread_id, context_window=context_window, auto_context=True)

    def to_json(payload, pretty=False):
        """Convert ULMEN-AGENT payload to JSON string (Python fallback)."""
        import json as _json
        from ulmen.core._agent import decode_agent_payload as _dap
        records = _dap(payload)
        if pretty:
            return _json.dumps(records, indent=2, ensure_ascii=False)
        return _json.dumps(records, ensure_ascii=False)

    def compare_sizes(json_str):
        """Compare JSON vs ULMEN-AGENT sizes (Python fallback)."""
        ulmen_payload = from_json(json_str)
        json_bytes = len(json_str)
        ulmen_bytes = len(ulmen_payload)
        saving = (1.0 - ulmen_bytes / json_bytes) * 100.0 if json_bytes > 0 else 0.0
        return (json_bytes, ulmen_bytes, saving)


# ---------------------------------------------------------------------------
# Backward-compatible Rust-named API aliases
#
# These names are kept for compatibility with existing integrations and tests.
# They delegate to the active package-level implementation, which is Rust when
# available and Python otherwise.
# ---------------------------------------------------------------------------

def encode_agent_record_rust(rec, meta_fields=()):
    return encode_agent_record(rec, meta_fields=meta_fields)

def decode_agent_record_rust(line: str, meta_fields=()):
    return decode_agent_record(line, meta_fields=meta_fields)

def encode_agent_payload_rust(
    records,
    thread_id=None,
    context_window=None,
    meta_fields=(),
    auto_context=True,
    enforce_budget=False,
    payload_id=None,
    parent_payload_id=None,
    agent_id=None,
    session_id=None,
    schema_version=None,
    auto_payload_id=False,
):
    return encode_agent_payload(
        records,
        thread_id=thread_id,
        context_window=context_window,
        meta_fields=meta_fields,
        auto_context=auto_context,
        enforce_budget=enforce_budget,
        payload_id=payload_id,
        parent_payload_id=parent_payload_id,
        agent_id=agent_id,
        session_id=session_id,
        schema_version=schema_version,
        auto_payload_id=auto_payload_id,
    )

def decode_agent_payload_rust(text: str):
    return decode_agent_payload(text)

def validate_agent_payload_rust(text: str, structured: bool = False):
    return validate_agent_payload(text, structured=structured)

# ---------------------------------------------------------------------------
# Streaming encode surface
# ---------------------------------------------------------------------------
from ulmen.core._streaming import (
    UlmenStreamEncoder,
    stream_encode,
    stream_encode_ulmen,
    stream_encode_windowed,
)
