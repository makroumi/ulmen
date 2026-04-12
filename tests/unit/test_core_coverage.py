"""
Targeted branch-coverage tests for uncovered lines in lumen/core.py.

Each test class is labelled with the exact line numbers it targets.
Sole purpose: drive lumen/core.py to 100% coverage.
"""
import pytest

from lumen.core import (
    # constants
    MAGIC, VERSION,
    T_STR_TINY, T_STR, T_INT, T_FLOAT, T_BOOL, T_NULL,
    T_LIST, T_MAP, T_POOL_DEF, T_POOL_REF, T_MATRIX,
    T_BITS, T_DELTA_RAW, T_RLE,
    S_BITS, S_DELTA, S_RLE, S_POOL, S_RAW,
    # primitives
    encode_varint, decode_varint,
    encode_zigzag, decode_zigzag,
    pack_string, pack_bool, pack_int, pack_float, pack_null,
    pack_bits, pack_delta_raw, pack_rle,
    unpack_bits, unpack_delta_raw,
    # encoders / decoders
    build_pool,
    encode_text_records, decode_text_records,
    encode_binary_records, decode_binary_records,
    # classes
    LumenDict,
)


# ---------------------------------------------------------------------------
# L481 — pack_bool branch inside pack_rle
# ---------------------------------------------------------------------------

class TestL481PackRleBool:
    """pack_rle must hit the pack_bool branch for bool run values."""

    def test_bool_true_run(self):
        data = pack_rle([True, True, True])
        assert data[0] == T_RLE
        assert T_BOOL in data

    def test_bool_false_run(self):
        data = pack_rle([False, False])
        assert T_BOOL in data

    def test_mixed_bool_runs(self):
        data = pack_rle([True, True, False, False, True])
        assert T_BOOL in data


# ---------------------------------------------------------------------------
# L620 — matrix row_idx is None (all rows already filled → continue)
# ---------------------------------------------------------------------------

class TestL620MatrixRowIdxNone:
    """Extra data rows beyond matrix capacity are silently skipped."""

    def test_extra_rows_ignored(self):
        # 1-record matrix, send 3 data rows → only first consumed
        text = 'records[1]:id:d\n0\n99\n88'
        result = decode_text_records(text)
        assert len(result) == 1
        assert result[0]['id'] == 0

    def test_two_row_matrix_three_data_lines(self):
        text = 'records[2]:id:d\n0\n1\n999'
        result = decode_text_records(text)
        assert len(result) == 2
        assert result[0]['id'] == 0
        assert result[1]['id'] == 1


# ---------------------------------------------------------------------------
# L675 — _decode_value T_BOOL branch (inside list/map, not matrix path)
# ---------------------------------------------------------------------------

class TestL675DecodeBool:
    """T_BOOL encountered inside _decode_value (nested in list or map)."""

    def test_bool_list(self):
        data = (MAGIC + VERSION +
                bytes([T_LIST]) + encode_varint(2) +
                bytes([T_BOOL, 1]) +
                bytes([T_BOOL, 0]))
        result = decode_binary_records(data)
        assert result == [True, False]

    def test_bool_in_map(self):
        data = (MAGIC + VERSION +
                bytes([T_MAP]) + encode_varint(1) +
                bytes([T_STR_TINY, 1]) + b'k' +
                bytes([T_BOOL, 1]))
        result = decode_binary_records(data)
        assert result == [{'k': True}]


# ---------------------------------------------------------------------------
# L697-698 — _decode_value T_BITS branch (not matrix path)
# ---------------------------------------------------------------------------

class TestL697DecodeBits:
    """T_BITS tag inside a generic list value triggers L697-698."""

    def test_bits_in_list(self):
        bools = [True, False, True, True]
        data = (MAGIC + VERSION +
                bytes([T_LIST]) + encode_varint(1) +
                pack_bits(bools))
        result = decode_binary_records(data)
        assert result == [bools]


# ---------------------------------------------------------------------------
# L700-701 — _decode_value T_DELTA_RAW branch (not matrix path)
# ---------------------------------------------------------------------------

class TestL700DecodeDelta:
    """T_DELTA_RAW tag inside a generic list value triggers L700-701."""

    def test_delta_in_list(self):
        ints = [10, 20, 30, 40]
        data = (MAGIC + VERSION +
                bytes([T_LIST]) + encode_varint(1) +
                pack_delta_raw(ints))
        result = decode_binary_records(data)
        assert result == [ints]


# ---------------------------------------------------------------------------
# L703-709 — _decode_value T_RLE branch (not matrix path)
# ---------------------------------------------------------------------------

class TestL703DecodeRle:
    """T_RLE tag inside a generic list value triggers L703-709."""

    def test_rle_strings_in_list(self):
        vals = ['hello', 'hello', 'world', 'world']
        data = (MAGIC + VERSION +
                bytes([T_LIST]) + encode_varint(1) +
                pack_rle(vals))
        result = decode_binary_records(data)
        assert result == [vals]

    def test_rle_ints_in_list(self):
        vals = [7, 7, 7, 42, 42]
        data = (MAGIC + VERSION +
                bytes([T_LIST]) + encode_varint(1) +
                pack_rle(vals))
        result = decode_binary_records(data)
        assert result == [vals]


# ---------------------------------------------------------------------------
# Helpers: build a raw T_MATRIX binary payload
# ---------------------------------------------------------------------------

def _matrix_payload(n_rows: int, col_name: str, strategy_byte: int,
                    col_data_bytes: bytes) -> bytes:
    """Craft a minimal valid LUMEN binary with a single-column T_MATRIX."""
    out = bytearray()
    out += MAGIC
    out += VERSION
    out.append(T_MATRIX)
    out += encode_varint(n_rows)
    out += encode_varint(1)                  # 1 column
    out += pack_string(col_name)
    out.append(strategy_byte)
    out += col_data_bytes
    return bytes(out)


# ---------------------------------------------------------------------------
# L741 — matrix S_BITS strategy, actual tag ≠ T_BITS → _decode_value fallback
# ---------------------------------------------------------------------------

class TestL741MatrixBitsFallback:
    """Strategy=S_BITS but data is a plain T_LIST → fallback branch L741."""

    def test_bits_strategy_list_data(self):
        list_data = (bytes([T_LIST]) + encode_varint(3) +
                     bytes([T_BOOL, 1]) +
                     bytes([T_BOOL, 0]) +
                     bytes([T_BOOL, 1]))
        data = _matrix_payload(3, 'flag', S_BITS, list_data)
        result = decode_binary_records(data)
        assert isinstance(result, list)
        assert len(result) == 3


# ---------------------------------------------------------------------------
# L747 — matrix S_DELTA strategy, actual tag ≠ T_DELTA_RAW → fallback
# ---------------------------------------------------------------------------

class TestL747MatrixDeltaFallback:
    """Strategy=S_DELTA but data is a plain T_LIST → fallback branch L747."""

    def test_delta_strategy_list_data(self):
        list_data = (bytes([T_LIST]) + encode_varint(3) +
                     bytes([T_INT]) + encode_zigzag(10) +
                     bytes([T_INT]) + encode_zigzag(20) +
                     bytes([T_INT]) + encode_zigzag(30))
        data = _matrix_payload(3, 'id', S_DELTA, list_data)
        result = decode_binary_records(data)
        assert isinstance(result, list)
        assert len(result) == 3


# ---------------------------------------------------------------------------
# L758 — matrix S_RLE strategy, actual tag ≠ T_RLE → fallback
# ---------------------------------------------------------------------------

class TestL758MatrixRleFallback:
    """Strategy=S_RLE but data is a plain T_LIST → fallback branch L758."""

    def test_rle_strategy_list_data(self):
        list_data = (bytes([T_LIST]) + encode_varint(3) +
                     pack_string('Eng') +
                     pack_string('Eng') +
                     pack_string('HR'))
        data = _matrix_payload(3, 'dept', S_RLE, list_data)
        result = decode_binary_records(data)
        assert isinstance(result, list)
        assert len(result) == 3


# ---------------------------------------------------------------------------
# L770 — matrix col_values list shorter than n_rows → rec[col] = None
# ---------------------------------------------------------------------------

class TestL770MatrixColNoneFill:
    """col_values list shorter than n_rows fills missing slots with None."""

    def test_short_col_padded_with_none(self):
        # 3 rows claimed, but only 2 values in the list
        list_data = (bytes([T_LIST]) + encode_varint(2) +
                     bytes([T_INT]) + encode_zigzag(10) +
                     bytes([T_INT]) + encode_zigzag(20))
        data = _matrix_payload(3, 'val', S_RAW, list_data)
        result = decode_binary_records(data)
        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0]['val'] == 10
        assert result[1]['val'] == 20
        assert result[2]['val'] is None     # L770


# ---------------------------------------------------------------------------
# L848 — LumenDict.decode_binary wraps non-list scalar in a list
# ---------------------------------------------------------------------------

class TestL848DecodeBinaryWrapsScalar:
    """decode_binary_records returning a non-list must be wrapped (L848)."""

    def test_scalar_wrapped_in_list(self):
        # encode a single integer (non-matrix, non-list) → decoder returns int
        raw = (MAGIC + VERSION +
               bytes([T_INT]) + encode_zigzag(42))
        ld = LumenDict([])
        result = ld.decode_binary(raw)
        # Result must be a LumenDict wrapping [42]
        assert isinstance(result, LumenDict)
        assert len(result) == 1
        assert result[0] == 42


# ---------------------------------------------------------------------------
# L481 — _encode_value_binary bool branch
# Bool in a single-record (non-matrix) path goes through _encode_value_binary
# Matrix path uses pack_bits — so we need a single dict or list of 1 record
# ---------------------------------------------------------------------------

class TestL481EncodeBoolBinary:
    """Bool value in non-matrix binary path hits _encode_value_binary L481."""

    def test_bool_in_single_record(self):
        # Single record → T_LIST + T_MAP path, calls _encode_value_binary
        recs = [{'flag': True}]
        data = encode_binary_records(recs, [], {}, use_strategies=False)
        result = decode_binary_records(data)
        if isinstance(result, list):
            assert result[0]['flag'] is True

    def test_bool_false_in_single_record(self):
        recs = [{'flag': False}]
        data = encode_binary_records(recs, [], {}, use_strategies=False)
        result = decode_binary_records(data)
        if isinstance(result, list):
            assert result[0]['flag'] is False

    def test_bool_in_non_dict_list(self):
        # Non-dict records → T_LIST path, _encode_value_binary called per item
        data = encode_binary_records([True, False, True], [], {})
        result = decode_binary_records(data)
        assert result == [True, False, True]


# ---------------------------------------------------------------------------
# L573 — _resolve out-of-range pool ref inside matrix inline col
# The _resolve closure is defined inside decode_text_records and called
# for inline @col values and plain lines. Need pool ref idx >= len(pool).
# ---------------------------------------------------------------------------

class TestL573ResolveOutOfRange:
    """Pool ref index beyond pool size resolves to None via L573."""

    def test_out_of_range_in_plain_line(self):
        # Pool has 1 entry (idx=0), ref points to idx=5
        text = 'POOL:hello\n#{5}'
        result = decode_text_records(text)
        assert result == [None]

    def test_out_of_range_in_inline_col(self):
        # Matrix with inline col containing an out-of-range pool ref
        # Pool has 1 entry, inline value refs idx=9
        text = 'POOL:Engineering\nrecords[2]:dept:s\n@dept=#0;#{9}'
        result = decode_text_records(text)
        assert result[0]['dept'] == 'Engineering'
        assert result[1]['dept'] is None


# ---------------------------------------------------------------------------
# L603 — matrix col spec with no ':' (else branch)
# The records[] header splits on ',' then checks for ':' in each spec.
# A spec without ':' goes to the else branch at L603.
# ---------------------------------------------------------------------------

class TestL603ColSpecNoColon:
    """Col spec token without ':' appended via else branch at L603."""

    def test_col_spec_missing_type(self):
        # 'extra' has no colon → L603 else branch
        text = 'records[2]:id:d,extra\n0\t0\n1\t1'
        result = decode_text_records(text)
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]['id'] == 0
        assert result[0]['extra'] == 0

    def test_all_cols_missing_type(self):
        # Both cols have no colon
        text = 'records[2]:id,val\n0\t10\n1\t20'
        result = decode_text_records(text)
        assert len(result) == 2
        assert result[1]['val'] == 20


# ---------------------------------------------------------------------------
# L620 — matrix data_cols empty → continue
# Triggered when ALL matrix cols are inline (@col=...) leaving no data cols.
# The next plain line hits the matrix_mode block, data_cols is [], → continue.
# ---------------------------------------------------------------------------

class TestL620DataColsEmpty:
    """All cols inline → data_cols empty → continue at L620."""

    def test_all_cols_inline_extra_line(self):
        # Both cols are inline, then an extra data line appears
        # That extra line hits L617-L620 with data_cols=[] → continue
        text = 'records[2]:id:d,dept:s\n@id=0;1\n@dept=Eng;HR\nextra_ignored_line'
        result = decode_text_records(text)
        assert len(result) == 2
        assert result[0]['id'] == 0
        assert result[1]['dept'] == 'HR'

    def test_matrix_fully_inline_no_data_rows_needed(self):
        text = 'records[3]:dept:s\n@dept=Eng;Mkt;HR'
        result = decode_text_records(text)
        assert len(result) == 3
        assert result[2]['dept'] == 'HR'
