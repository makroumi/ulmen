"""
Public API classes: LumenDict and LumenDictFull.

These classes are thin stateful wrappers around the stateless codec
functions. They add:
    Automatic pool management (build_pool called on init and after append)
    Encode result caching (invalidated on mutation)
    Convenience methods (decode_text, decode_binary, to_json, encode_lumen_llm)

LumenDict      default pool size (64), strategies optional
LumenDictFull  extended pool (up to 256), strategies always on
"""

import json
import math
import zlib
from typing import Any

from lumen.core._binary import decode_binary_records, encode_binary_records
from lumen.core._lumen_llm import decode_lumen_llm, encode_lumen_llm
from lumen.core._strategies import build_pool
from lumen.core._text import decode_text_records, encode_text_records
from lumen.core._utils import __version__


class LumenDict:
    """
    LUMEN record container -- Python reference implementation.

    Manages a list of records together with an automatically maintained
    string pool. Encode results (text and binary) are cached and invalidated
    whenever the record set changes via append().

    Parameters
    ----------
    data : list | dict | iterable | None
        Records to initialise with. A single dict is wrapped in a list.
        Any iterable is consumed into a list. None produces an empty container.
    optimizations : bool
        When True, encode_binary() applies column strategies (delta, rle,
        bits, pool). When False, all columns are stored raw. Default: False.
    """

    __slots__ = (
        '_data', '_pool', '_pool_map', '_optimizations',
        '_pool_built', '_version',
        '_text_cache', '_binary_raw_cache', '_binary_strat_cache',
        '_lumen_llm_cache',
    )
    VERSION = __version__

    def __init__(self, data=None, optimizations: bool = False) -> None:
        self._version            = __version__
        self._optimizations      = optimizations
        self._text_cache         = None
        self._binary_raw_cache   = None
        self._binary_strat_cache = None
        self._lumen_llm_cache        = None

        if data is None:
            self._data = []
        elif isinstance(data, list):
            self._data = data
        elif isinstance(data, dict):
            self._data = [data]
        else:
            self._data = list(data)

        self._pool, self._pool_map = build_pool(self._data, max_pool=64)
        self._pool_built = True

    def _invalidate(self) -> None:
        self._text_cache         = None
        self._binary_raw_cache   = None
        self._binary_strat_cache = None
        self._lumen_llm_cache        = None

    def append(self, record: Any) -> None:
        """Append a record. Rebuilds the pool and invalidates encode caches."""
        self._data.append(record)
        self._invalidate()
        self._pool, self._pool_map = build_pool(self._data, max_pool=64)

    def __len__(self) -> int:
        return len(self._data)

    def __getitem__(self, idx):
        return self._data[idx]

    def __iter__(self):
        return iter(self._data)

    def encode_text(self, matrix_mode: bool = True) -> str:
        """Encode to LUMEN text format. Result is cached until next mutation."""
        if self._text_cache is None:
            self._text_cache = encode_text_records(
                self._data, self._pool, self._pool_map,
                matrix_mode=matrix_mode,
            )
        return self._text_cache

    def encode_binary(self) -> bytes:
        """
        Encode to LUMEN binary. Result is cached until next mutation.
        Strategies are applied only when optimizations=True.
        """
        if self._optimizations:
            if self._binary_strat_cache is None:
                self._binary_strat_cache = encode_binary_records(
                    self._data, self._pool, self._pool_map,
                    use_strategies=True,
                )
            return self._binary_strat_cache
        if self._binary_raw_cache is None:
            self._binary_raw_cache = encode_binary_records(
                self._data, self._pool, self._pool_map,
                use_strategies=False,
            )
        return self._binary_raw_cache

    def encode_binary_pooled(self) -> bytes:
        """Encode to LUMEN binary with all column strategies enabled."""
        if self._binary_strat_cache is None:
            self._binary_strat_cache = encode_binary_records(
                self._data, self._pool, self._pool_map,
                use_strategies=True,
            )
        return self._binary_strat_cache

    def encode_binary_zlib(self, level: int = 6) -> bytes:
        """Encode with all strategies then compress with zlib."""
        return zlib.compress(self.encode_binary_pooled(), level)

    def encode_lumen_llm(self) -> str:
        """
        Encode to LUMIA format -- the LLM-native text surface.

        LUMIA is a header-prefixed CSV format where every row is self-describing.
        No pool references, no index counting, no grammar to memorize.
        An LLM can read and generate LUMIA without any special training.
        Result is cached until next mutation.
        """
        if self._lumen_llm_cache is None:
            self._lumen_llm_cache = encode_lumen_llm(self._data)
        return self._lumen_llm_cache

    def decode_text(self, text: str) -> 'LumenDict':
        """Decode a LUMEN text payload into a new LumenDict."""
        return LumenDict(
            decode_text_records(text),
            optimizations=self._optimizations,
        )

    def decode_binary(self, data: bytes) -> 'LumenDict':
        """Decode a LUMEN binary payload into a new LumenDict."""
        decoded = decode_binary_records(data)
        if not isinstance(decoded, list):
            decoded = [decoded]
        return LumenDict(decoded, optimizations=self._optimizations)

    def decode_lumen_llm(self, text: str) -> 'LumenDict':
        """Decode a LUMIA payload into a new LumenDict."""
        return LumenDict(
            decode_lumen_llm(text),
            optimizations=self._optimizations,
        )

    def to_json(self) -> str:
        """
        Serialize records to standard JSON.

        NaN and signed infinities are replaced with null because the JSON
        specification does not allow float specials.
        """
        def _fix(obj: Any) -> Any:
            if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
                return None
            if isinstance(obj, dict):  return {k: _fix(v) for k, v in obj.items()}
            if isinstance(obj, list):  return [_fix(v) for v in obj]
            return obj

        return json.dumps([_fix(r) for r in self._data], separators=(',', ':'))

    def __repr__(self) -> str:
        return (
            f"LumenDict("
            f"records={len(self._data)}, "
            f"pool={len(self._pool)}, "
            f"optimizations={self._optimizations})"
        )


class LumenDictFull(LumenDict):
    """
    LumenDict with an extended string pool and strategies always enabled.

    Suitable for large repetitive datasets where maximising compression
    is more important than encode speed. The pool can hold up to
    pool_size_limit strings vs the fixed 64 in LumenDict.

    Parameters
    ----------
    data : list | dict | iterable | None
    pool_size_limit : int
        Maximum number of strings in the pool. Default: 256.
    """

    __slots__ = ('_pool_size_limit',)

    def __init__(self, data=None, pool_size_limit: int = 256) -> None:
        self._version            = __version__
        self._optimizations      = True
        self._text_cache         = None
        self._binary_raw_cache   = None
        self._binary_strat_cache = None
        self._lumen_llm_cache        = None
        self._pool_size_limit    = pool_size_limit

        if data is None:
            self._data = []
        elif isinstance(data, list):
            self._data = data
        elif isinstance(data, dict):
            self._data = [data]
        else:
            self._data = list(data)

        self._pool, self._pool_map = build_pool(self._data, max_pool=pool_size_limit)
        self._pool_built = True

    def append(self, record: Any) -> None:
        """Append a record. Rebuilds the pool and invalidates encode caches."""
        self._data.append(record)
        self._invalidate()
        self._pool, self._pool_map = build_pool(
            self._data, max_pool=self._pool_size_limit,
        )

    def encode_binary(self) -> bytes:
        """Encode with all strategies and the extended pool."""
        return self.encode_binary_pooled()

    def encode_text(self, matrix_mode: bool = True) -> str:
        """Encode to text format using the extended pool."""
        if self._text_cache is None:
            self._text_cache = encode_text_records(
                self._data, self._pool, self._pool_map,
                matrix_mode=True,
            )
        return self._text_cache

    def __repr__(self) -> str:
        return (
            f"LumenDictFull("
            f"records={len(self._data)}, "
            f"pool={len(self._pool)}, "
            f"pool_limit={self._pool_size_limit})"
        )


def encode_lumen_llm_direct(records: list) -> str:
    """
    Module-level shortcut: encode records to LUMEN LLM-native format
    without constructing a LumenDict. Fastest path for one-shot encoding.
    """
    from lumen.core._lumen_llm import encode_lumen_llm
    return encode_lumen_llm(records)


def decode_lumen_llm_direct(text: str) -> list:
    """
    Module-level shortcut: decode LUMEN LLM-native text without
    constructing a LumenDict. Fastest path for one-shot decoding.
    """
    from lumen.core._lumen_llm import decode_lumen_llm
    return decode_lumen_llm(text)
