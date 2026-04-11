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

from lumen.core._utils import (
    __version__,
    __edition__,
    fnv1a,
    fnv1a_str,
    estimate_tokens,
    deep_size,
    deep_eq,
)

from lumen.core._constants import (
    MAGIC,
    VERSION,
    T_STR_TINY,
    T_STR,
    T_INT,
    T_FLOAT,
    T_BOOL,
    T_NULL,
    T_LIST,
    T_MAP,
    T_POOL_DEF,
    T_POOL_REF,
    T_MATRIX,
    T_DELTA_RAW,
    T_STRATEGY,
    T_BITS,
    T_RLE,
    S_RAW,
    S_DELTA,
    S_RLE,
    S_BITS,
    S_POOL,
)

from lumen.core._primitives import (
    encode_varint,
    decode_varint,
    encode_zigzag,
    decode_zigzag,
    pack_string,
    pack_int,
    pack_float,
    pack_bool,
    pack_null,
    pack_pool_ref,
    pack_bits,
    unpack_bits,
    pack_delta_raw,
    unpack_delta_raw,
    pack_rle,
)

from lumen.core._strategies import (
    detect_column_strategy,
    compute_delta_savings,
    compute_rle_savings,
    compute_bits_savings,
    build_pool,
)

from lumen.core._text import (
    _text_escape,
    _text_unescape,
    _format_float,
    _parse_value,
    _encode_value_text,
    _encode_obj_iterative_text,
    encode_text_records,
    decode_text_records,
)

from lumen.core._binary import (
    encode_binary_records,
    decode_binary_records,
)

from lumen.core._api import (
    LumenDict,
    LumenDictFull,
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
    encode_lumen_llm,
    decode_lumen_llm,
    LUMEN_LLM_MAGIC,
)

__all__ += [
    "encode_lumen_llm",
    "decode_lumen_llm",
    "LUMEN_LLM_MAGIC",
]

# Re-export LUMIA so lumen.core imports also work
from lumen.core._lumen_llm import encode_lumen_llm, decode_lumen_llm, LUMEN_LLM_MAGIC  # noqa: F811

from lumen.core._agent import (
    encode_agent_payload,
    decode_agent_payload,
    validate_agent_payload,
    extract_subgraph,
    extract_subgraph_payload,
    make_validation_error,
    AGENT_MAGIC,
    AGENT_VERSION,
    RECORD_TYPES,
)

__all__ += [
    "encode_agent_payload",
    "decode_agent_payload",
    "validate_agent_payload",
    "extract_subgraph",
    "extract_subgraph_payload",
    "make_validation_error",
    "AGENT_MAGIC",
    "AGENT_VERSION",
    "RECORD_TYPES",
]
