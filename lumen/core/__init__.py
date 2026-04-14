"""
lumen.core -- LUMEN v3.3.1 codec package.

Re-exports all public symbols from the submodules so that the rest of
the codebase can import from either lumen.core or lumen.core.<submodule>.

Submodule responsibilities:
    _constants   wire-format tags, strategy bytes, MAGIC, VERSION
    _utils       fnv1a, estimate_tokens, deep_size, deep_eq
    _primitives  varint, zigzag, pack_*, unpack_*
    _strategies  detect_column_strategy, compute_*, build_pool
    _text        text encoder, text decoder, text helpers
    _binary      binary encoder, binary decoder
    _api         LumenDict, LumenDictFull
"""

from lumen.core._api import (
    LumenDict,
    LumenDictFull,
)
from lumen.core._binary import (
    decode_binary_records,
    encode_binary_records,
)
from lumen.core._constants import (
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
from lumen.core._primitives import (
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
from lumen.core._strategies import (
    build_pool,
    compute_bits_savings,
    compute_delta_savings,
    compute_rle_savings,
    detect_column_strategy,
)
from lumen.core._text import (
    _encode_obj_iterative_text,
    _encode_value_text,
    _format_float,
    _parse_value,
    _text_escape,
    _text_unescape,
    decode_text_records,
    encode_text_records,
)
from lumen.core._utils import (
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
    "LumenDict",
    "LumenDictFull",
]

from lumen.core._lumen_llm import (
    LUMEN_LLM_MAGIC,
    decode_lumen_llm,
    encode_lumen_llm,
)

__all__ += [
    "encode_lumen_llm",
    "decode_lumen_llm",
    "LUMEN_LLM_MAGIC",
]

# Re-export LUMIA so lumen.core imports also work
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
)
from lumen.core._lumen_llm import LUMEN_LLM_MAGIC, decode_lumen_llm, encode_lumen_llm  # noqa: F811

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
    "convert_agent_to_lumia",
    "convert_lumia_to_agent",
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
from lumen.core._streaming import (
    LumenStreamEncoder,
    stream_encode,
    stream_encode_lumia,
    stream_encode_windowed,
)

__all__ += [
    "LumenStreamEncoder",
    "stream_encode",
    "stream_encode_windowed",
    "stream_encode_lumia",
]
