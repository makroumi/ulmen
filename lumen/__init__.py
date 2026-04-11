"""
LUMEN v1.0.0 -- Lumex Ultra Absolute #1
The number one serialization format across size, tokens, speed, and memory.
LUMIA: the LLM-native text surface for zero-hallucination agent communication.
LUMEN-AGENT: the structured agentic communication protocol.

Pure Python reference + optional Rust acceleration. Zero runtime dependencies.

Usage
-----
    from lumen import LumenDict, LumenDictFull
    from lumen import LumenDictRust, LumenDictFullRust
    from lumen import encode_lumen_llm, decode_lumen_llm
    from lumen import encode_agent_payload, decode_agent_payload

    records = [{"id": i, "name": "Alice", "score": 98.5} for i in range(1000)]

    ld     = LumenDict(records)
    text   = ld.encode_text()
    lumia  = ld.encode_lumen_llm()
    binary = ld.encode_binary_pooled()
    zlib_  = ld.encode_binary_zlib()
"""

import zlib as _zlib

# ---------------------------------------------------------------------------
# Python reference implementation -- always available
# ---------------------------------------------------------------------------

from lumen.core import (
    LumenDict,
    LumenDictFull,
    _encode_value_text,
    _encode_obj_iterative_text,
    _text_escape,
    _text_unescape,
    _format_float,
    _parse_value,
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
    T_BITS,
    T_RLE,
    S_RAW,
    S_DELTA,
    S_RLE,
    S_BITS,
    S_POOL,
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
    pack_delta_raw,
    pack_rle,
    unpack_bits,
    unpack_delta_raw,
    build_pool,
    detect_column_strategy,
    encode_text_records,
    encode_binary_records,
    decode_text_records,
    decode_binary_records,
    encode_lumen_llm,
    decode_lumen_llm,
    LUMEN_LLM_MAGIC,
    fnv1a,
    fnv1a_str,
    estimate_tokens,
    deep_size,
    deep_eq,
    compute_delta_savings,
    compute_rle_savings,
    compute_bits_savings,
    MAGIC,
    VERSION,
    __version__,
    __edition__,
)

# ---------------------------------------------------------------------------
# LUMEN-AGENT -- pure Python, always available, no Rust dependency
# ---------------------------------------------------------------------------

from lumen.core._agent import (
    encode_agent_payload,
    decode_agent_payload,
    validate_agent_payload,
    extract_subgraph,
    extract_subgraph_payload,
    make_validation_error,
    encode_agent_record,
    decode_agent_record,
    AGENT_MAGIC,
    AGENT_VERSION,
    RECORD_TYPES,
    FIELD_COUNTS,
)

# ---------------------------------------------------------------------------
# Public API surface
# ---------------------------------------------------------------------------

__all__ = [
    # Core classes
    "LumenDict",
    "LumenDictFull",
    "LumenDictRust",
    "LumenDictFullRust",
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
    # LUMIA -- LLM-native format
    "encode_lumen_llm", "decode_lumen_llm", "LUMEN_LLM_MAGIC",
    # LUMEN-AGENT -- agentic protocol
    "encode_agent_payload", "decode_agent_payload",
    "validate_agent_payload",
    "extract_subgraph", "extract_subgraph_payload",
    "make_validation_error",
    "encode_agent_record", "decode_agent_record",
    "AGENT_MAGIC", "AGENT_VERSION", "RECORD_TYPES", "FIELD_COUNTS",
    # Utilities
    "fnv1a", "fnv1a_str", "estimate_tokens",
    "deep_size", "deep_eq",
    # Constants
    "MAGIC", "VERSION", "__version__", "__edition__",
    # Text helpers (internal but exported for testing)
    "_encode_value_text", "_encode_obj_iterative_text",
    "_text_escape", "_text_unescape",
    "_format_float", "_parse_value",
    # Wire-format tags
    "T_STR_TINY", "T_STR", "T_INT", "T_FLOAT", "T_BOOL", "T_NULL",
    "T_LIST", "T_MAP", "T_POOL_DEF", "T_POOL_REF", "T_MATRIX",
    "T_DELTA_RAW", "T_BITS", "T_RLE",
    "S_RAW", "S_DELTA", "S_RLE", "S_BITS", "S_POOL",
]

# ---------------------------------------------------------------------------
# Rust acceleration -- optional, falls back to Python silently
# ---------------------------------------------------------------------------

RUST_AVAILABLE = False

try:
    from lumen._lumen_rust import LumenDictRust, LumenDictFullRust          # type: ignore
    from lumen._lumen_rust import decode_binary_records_rust as _dbr_rust   # type: ignore
    from lumen._lumen_rust import encode_lumen_llm_rust as _ell_rust        # type: ignore
    from lumen._lumen_rust import decode_lumen_llm_rust as _dll_rust        # type: ignore

    def decode_binary_records(data: bytes) -> list:  # type: ignore[no-redef]
        """Decode LUMEN binary data using the Rust-accelerated decoder."""
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
        """
        Python shim for LumenDictRust.
        Active only when the Rust extension is not compiled.
        Build with: maturin develop --release
        """

        def encode_binary_pooled_raw(self) -> bytes:
            return encode_binary_records(
                self._data, self._pool, self._pool_map,
                use_strategies=False,
            )

        def encode_binary_zlib(self) -> bytes:  # type: ignore[override]
            return _zlib.compress(self.encode_binary_pooled(), 6)

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
        """
        Python shim for LumenDictFullRust.
        Active only when the Rust extension is not compiled.
        Build with: maturin develop --release
        """

        def encode_binary_pooled_raw(self) -> bytes:
            return encode_binary_records(
                self._data, self._pool, self._pool_map,
                use_strategies=False,
            )

        def encode_binary_zlib(self) -> bytes:  # type: ignore[override]
            return _zlib.compress(self.encode_binary_pooled(), 6)

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
