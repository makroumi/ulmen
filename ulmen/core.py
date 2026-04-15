# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
#
# This file is part of ULMEN.
# ULMEN is licensed under the Business Source License 1.1.
# See the LICENSE file in the project root for full license information.

"""
Backward-compatibility shim for ulmen.core.

The implementation has been split into the ulmen/core/ package.
This module re-exports everything so that existing imports of the form

    from ulmen.core import UlmenDict, encode_varint, ...

continue to work without modification.
"""

from ulmen.core import *  # noqa: F401, F403
from ulmen.core import (  # noqa: F401
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
    decode_varint,
    decode_zigzag,
    deep_eq,
    deep_size,
    detect_column_strategy,
    encode_binary_records,
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
