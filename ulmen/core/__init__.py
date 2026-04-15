# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
#
# This file is part of ULMEN.
# ULMEN is licensed under the Business Source License 1.1.
# See the LICENSE file in the project root for full license information.

"""
ulmen.core -- ULMEN v3.3.1 codec package.

Re-exports all public symbols from the submodules so that the rest of
the codebase can import from either ulmen.core or ulmen.core.<submodule>.

Submodule responsibilities:
    _constants   wire-format tags, strategy bytes, MAGIC, VERSION
    _utils       fnv1a, estimate_tokens, deep_size, deep_eq
    _primitives  varint, zigzag, pack_*, unpack_*
    _strategies  detect_column_strategy, compute_*, build_pool
    _text        text encoder, text decoder, text helpers
    _binary      binary encoder, binary decoder
    _api         UlmenDict, UlmenDictFull
"""

from ulmen.core._api import (
    UlmenDict,
    UlmenDictFull,
)
from ulmen.core._binary import (
    decode_binary_records,
    encode_binary_records,
)
from ulmen.core._constants import (
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
    T_STRATEGY,
    VERSION,
)
from ulmen.core._primitives import (
    decode_varint,
    decode_zigzag,
    encode_varint,
    encode_zigzag,
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
from ulmen.core._strategies import (
    build_pool,
    compute_bits_savings,
    compute_delta_savings,
    compute_rle_savings,
    detect_column_strategy,
)
from ulmen.core._text import (
    _encode_obj_iterative_text,
    _encode_value_text,
    _format_float,
    _parse_value,
    _text_escape,
    _text_unescape,
    decode_text_records,
    encode_text_records,
)
from ulmen.core._utils import (
    __edition__,
    __version__,
    deep_eq,
    deep_size,
    estimate_tokens,
    fnv1a,
    fnv1a_str,
)

__all__ = [
    # version
    "__version__",
    "__edition__",
    # utilities
    "fnv1a",
    "fnv1a_str",
    "estimate_tokens",
    "deep_size",
    "deep_eq",
    # constants
    "MAGIC",
    "VERSION",
    "T_STR_TINY", "T_STR", "T_INT", "T_FLOAT", "T_BOOL", "T_NULL",
    "T_LIST", "T_MAP", "T_POOL_DEF", "T_POOL_REF", "T_MATRIX",
    "T_DELTA_RAW", "T_STRATEGY", "T_BITS", "T_RLE",
    "S_RAW", "S_DELTA", "S_RLE", "S_BITS", "S_POOL",
    # primitives
    "encode_varint", "decode_varint",
    "encode_zigzag", "decode_zigzag",
    "pack_string", "pack_int", "pack_float", "pack_bool", "pack_null",
    "pack_pool_ref",
    "pack_bits", "unpack_bits",
    "pack_delta_raw", "unpack_delta_raw",
    "pack_rle",
    # strategies
    "detect_column_strategy",
    "compute_delta_savings",
    "compute_rle_savings",
    "compute_bits_savings",
    "build_pool",
    # text codec
    "_text_escape", "_text_unescape",
    "_format_float", "_parse_value",
    "_encode_value_text", "_encode_obj_iterative_text",
    "encode_text_records", "decode_text_records",
    # binary codec
    "encode_binary_records", "decode_binary_records",
    # API
    "UlmenDict",
    "UlmenDictFull",
]

from ulmen.core._ulmen_llm import (
    ULMEN_LLM_MAGIC,
    decode_ulmen_llm,
    encode_ulmen_llm,
)

__all__ += [
    "encode_ulmen_llm",
    "decode_ulmen_llm",
    "ULMEN_LLM_MAGIC",
]

# Re-export ULMEN so ulmen.core imports also work
from ulmen.core._agent import (
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
    validate_agent_payload,
)
from ulmen.core._ulmen_llm import ULMEN_LLM_MAGIC, decode_ulmen_llm, encode_ulmen_llm  # noqa: F811

__all__ += [
    "AGENT_MAGIC",
    "AGENT_VERSION",
    "COMPRESS_COMPLETED_SEQUENCES",
    "COMPRESS_KEEP_TYPES",
    "COMPRESS_SLIDING_WINDOW",
    "FIELD_COUNTS",
    "META_FIELDS",
    "PRIORITY_COMPRESSIBLE",
    "PRIORITY_KEEP_IF_ROOM",
    "PRIORITY_MUST_KEEP",
    "RECORD_TYPES",
    "AgentHeader",
    "ContextBudgetExceededError",
    "ValidationError",
    "build_summary_chain",
    "chunk_payload",
    "compress_context",
    "convert_agent_to_ulmen",
    "convert_ulmen_to_agent",
    "decode_agent_payload",
    "decode_agent_payload_full",
    "decode_agent_record",
    "decode_agent_stream",
    "dedup_mem",
    "encode_agent_payload",
    "encode_agent_record",
    "estimate_context_usage",
    "extract_subgraph",
    "extract_subgraph_payload",
    "generate_system_prompt",
    "get_latest_mem",
    "make_validation_error",
    "merge_chunks",
    "validate_agent_payload",
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

__all__ += [
    "count_tokens_exact",
    "count_tokens_exact_records",
    "AgentRouter",
    "validate_routing_consistency",
    "ThreadRegistry",
    "merge_threads",
    "ReplayLog",
    "parse_llm_output",
]


# ---------------------------------------------------------------------------
# Streaming encode surface
# ---------------------------------------------------------------------------
from ulmen.core._streaming import (
    UlmenStreamEncoder,
    stream_encode,
    stream_encode_ulmen,
    stream_encode_windowed,
)

__all__ += [
    "UlmenStreamEncoder",
    "stream_encode",
    "stream_encode_windowed",
    "stream_encode_ulmen",
]
