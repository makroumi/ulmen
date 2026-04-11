"""
Backward-compatibility shim for lumen.core.

The implementation has been split into the lumen/core/ package.
This module re-exports everything so that existing imports of the form

    from lumen.core import LumenDict, encode_varint, ...

continue to work without modification.
"""

from lumen.core import *                        # noqa: F401, F403
from lumen.core import (                        # noqa: F401
    __version__,
    __edition__,
    MAGIC,
    VERSION,
    T_STR_TINY, T_STR, T_INT, T_FLOAT, T_BOOL, T_NULL,
    T_LIST, T_MAP, T_POOL_DEF, T_POOL_REF, T_MATRIX,
    T_DELTA_RAW, T_STRATEGY, T_BITS, T_RLE,
    S_RAW, S_DELTA, S_RLE, S_BITS, S_POOL,
    fnv1a, fnv1a_str,
    estimate_tokens,
    deep_size, deep_eq,
    encode_varint, decode_varint,
    encode_zigzag, decode_zigzag,
    pack_string, pack_int, pack_float, pack_bool, pack_null, pack_pool_ref,
    pack_bits, unpack_bits,
    pack_delta_raw, unpack_delta_raw,
    pack_rle,
    detect_column_strategy,
    compute_delta_savings, compute_rle_savings, compute_bits_savings,
    build_pool,
    _text_escape, _text_unescape,
    _format_float, _parse_value,
    _encode_value_text, _encode_obj_iterative_text,
    encode_text_records, decode_text_records,
    encode_binary_records, decode_binary_records,
    LumenDict,
    LumenDictFull,
)
