"""
Wire-format constants for the ULMEN binary and text codecs.

Type tags, strategy bytes, and format identifiers are defined here and
imported by every other submodule. Nothing in this file has side-effects.
"""

# ---------------------------------------------------------------------------
# Format identifiers
# ---------------------------------------------------------------------------

MAGIC   = b'LUMB'        # 4-byte magic number written at the start of every binary payload
VERSION = bytes([3, 3])  # major.minor wire-format version bytes

# ---------------------------------------------------------------------------
# Type tags -- single byte written before every encoded value
# ---------------------------------------------------------------------------

T_STR_TINY  = 0x01   # UTF-8 string, 0-3 bytes  (tag, len_byte, bytes)
T_STR       = 0x02   # UTF-8 string, >= 4 bytes  (tag, varint_len, bytes)
T_INT       = 0x03   # signed integer             (tag, zigzag_varint)
T_FLOAT     = 0x04   # IEEE-754 double            (tag, 8 bytes big-endian)
T_BOOL      = 0x05   # boolean                    (tag, 0x00 or 0x01)
T_NULL      = 0x06   # null / None                (tag only)
T_LIST      = 0x07   # heterogeneous list         (tag, varint_n, n x value)
T_MAP       = 0x08   # key-value map              (tag, varint_n, n x key x value)
T_POOL_DEF  = 0x09   # string pool definition     (tag, varint_n, n x string)
T_POOL_REF  = 0x0A   # pool index reference       (tag, varint_index)
T_MATRIX    = 0x0B   # columnar record set        (tag, rows, cols, headers, data)
T_DELTA_RAW = 0x0C   # delta-encoded int column   (tag, varint_n, zigzag x n)
T_STRATEGY  = 0x0D   # reserved strategy marker
T_BITS      = 0x0E   # packed boolean column      (tag, varint_n, ceil(n/8) bytes)
T_RLE       = 0x0F   # run-length encoded column  (tag, varint_runs, (value, count) x runs)

# ---------------------------------------------------------------------------
# Column strategy bytes -- written in the T_MATRIX column header
# ---------------------------------------------------------------------------

S_RAW   = 0x00   # no compression, values stored individually
S_DELTA = 0x01   # delta encoding for integer sequences
S_RLE   = 0x02   # run-length encoding
S_BITS  = 0x03   # bitpacked booleans
S_POOL  = 0x04   # string pool references

# Mapping from strategy name to strategy byte, used by the binary encoder
STRATEGY_BYTE = {
    'raw':   S_RAW,
    'delta': S_DELTA,
    'rle':   S_RLE,
    'bits':  S_BITS,
    'pool':  S_POOL,
}
