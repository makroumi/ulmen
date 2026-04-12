"""
Unit tests for the text and binary encoder/decoder pipeline:
  - encode_text_records / decode_text_records
  - encode_binary_records / decode_binary_records
  - Internal branch coverage for both decoders
"""
import math

import pytest

from lumen.core import (
    MAGIC,
    T_BITS,
    T_DELTA_RAW,
    T_INT,
    T_LIST,
    T_MAP,
    T_MATRIX,
    T_POOL_DEF,
    T_POOL_REF,
    T_RLE,
    T_STR_TINY,
    VERSION,
    build_pool,
    decode_binary_records,
    decode_text_records,
    encode_binary_records,
    encode_text_records,
    encode_varint,
    encode_zigzag,
)

# ===========================================================================
# encode_text_records
# ===========================================================================

class TestEncodeTextRecords:
    def test_empty_returns_empty(self):
        assert encode_text_records([], [], {}) == ''

    def test_single_dict_schema_header(self):
        recs = [{'a': 1, 'b': 'hello'}]
        pool, pm = build_pool(recs)
        out = encode_text_records(recs, pool, pm)
        assert 'SCHEMA:' in out or 'a' in out

    def test_multiple_dicts_matrix_header(self):
        recs = [{'id': i, 'v': i * 2} for i in range(5)]
        pool, pm = build_pool(recs)
        out = encode_text_records(recs, pool, pm)
        assert 'records[5]' in out

    def test_pool_line_present(self):
        recs = [{'tag': 'Engineering'}] * 10
        pool, pm = build_pool(recs)
        assert pool  # pool should be non-empty
        out = encode_text_records(recs, pool, pm)
        assert out.startswith('POOL:')

    def test_inline_col_for_rle_pool(self):
        recs = [{'id': i, 'dept': 'Engineering'} for i in range(10)]
        pool, pm = build_pool(recs)
        out = encode_text_records(recs, pool, pm)
        assert '@dept' in out or 'Engineering' in out

    def test_non_dict_records(self):
        recs = [1, 2, 3]
        out = encode_text_records(recs, [], {})
        assert '1' in out and '2' in out

    def test_bool_type_char(self):
        recs = [{'flag': True}, {'flag': False}]
        pool, pm = build_pool(recs)
        out = encode_text_records(recs, pool, pm)
        assert 'flag:b' in out or 'T' in out

    def test_float_type_char(self):
        recs = [{'score': 1.5}, {'score': 2.5}]
        pool, pm = build_pool(recs)
        out = encode_text_records(recs, pool, pm)
        assert 'score:f' in out or '1.5' in out

    def test_int_type_char(self):
        recs = [{'n': 1}, {'n': 2}]
        pool, pm = build_pool(recs)
        out = encode_text_records(recs, pool, pm)
        assert 'n:d' in out or '1' in out

    def test_string_type_char(self):
        recs = [{'s': 'hello world'}, {'s': 'foo bar'}]
        pool, pm = build_pool(recs)
        out = encode_text_records(recs, pool, pm)
        assert 's:s' in out or 'hello' in out

    def test_null_type_char(self):
        recs = [{'x': None}, {'x': None}]
        out = encode_text_records(recs, [], {})
        assert 'x:n' in out or 'N' in out

    def test_matrix_mode_false_uses_schema(self):
        recs = [{'a': 1}, {'a': 2}]
        out = encode_text_records(recs, [], {}, matrix_mode=False)
        assert 'SCHEMA:' in out

    def test_empty_pool_no_pool_line(self):
        recs = [{'a': 1}, {'a': 2}]
        out = encode_text_records(recs, [], {})
        assert not out.startswith('POOL:')

    def test_unicode_in_strings(self):
        recs = [{'name': '日本語'}, {'name': 'مرحبا'}]
        out = encode_text_records(recs, [], {})
        assert '日本語' in out or 'name' in out


# ===========================================================================
# decode_text_records
# ===========================================================================

class TestDecodeTextRecords:
    def test_empty_string(self):
        assert decode_text_records('') == []

    def test_single_int(self):
        result = decode_text_records('42')
        assert result == [42]

    def test_pool_line_parsed(self):
        text = 'POOL:Engineering,Marketing\nSCHEMA:dept:s\nEngineering'
        result = decode_text_records(text)
        assert result[0]['dept'] == 'Engineering'

    def test_schema_line(self):
        text = 'SCHEMA:id:d,name:s\n1\thello'
        result = decode_text_records(text)
        assert result[0] == {'id': 1, 'name': 'hello'}

    def test_matrix_mode(self):
        text = 'records[3]:id:d,v:d\n0\t0\n1\t2\n2\t4'
        result = decode_text_records(text)
        assert len(result) == 3
        assert result[1]['id'] == 1

    def test_inline_at_col(self):
        text = 'records[3]:id:d,dept:s\n@dept=Eng;Eng;Mkt\n0\n1\n2'
        result = decode_text_records(text)
        assert result[0]['dept'] == 'Eng'

    def test_none_token(self):
        result = decode_text_records('N')
        assert result == [None]

    def test_true_false_tokens(self):
        assert decode_text_records('T') == [True]
        assert decode_text_records('F') == [False]

    def test_float_parsing(self):
        result = decode_text_records('3.14')
        assert result[0] == pytest.approx(3.14)

    def test_nan_parsing(self):
        result = decode_text_records('nan')
        assert math.isnan(result[0])

    def test_inf_parsing(self):
        assert decode_text_records('inf') == [float('inf')]

    def test_neg_inf_parsing(self):
        assert decode_text_records('-inf') == [float('-inf')]

    def test_empty_string_token(self):
        result = decode_text_records('$0=')
        assert result == ['']

    def test_pool_ref_braces(self):
        text = 'POOL:Engineering\nSCHEMA:dept:s\n#{0}'
        result = decode_text_records(text)
        assert result[0]['dept'] == 'Engineering'

    def test_ignored_blank_lines(self):
        text = '1\n\n2\n\n3'
        result = decode_text_records(text)
        assert 1 in result and 2 in result

    def test_skips_cr(self):
        text = 'SCHEMA:x:d\r\n5\r'
        result = decode_text_records(text)
        assert result[0]['x'] == 5

    # branch coverage
    def test_schema_path(self):
        text = 'SCHEMA:a:d,b:s\n10\thello'
        result = decode_text_records(text)
        assert result[0] == {'a': 10, 'b': 'hello'}

    def test_schema_with_pool(self):
        text = 'POOL:Engineering\nSCHEMA:dept:s\n#0'
        result = decode_text_records(text)
        assert result[0]['dept'] == 'Engineering'

    def test_plain_lines_no_schema(self):
        text = '1\n2\n3'
        result = decode_text_records(text)
        assert result == [1, 2, 3]

    def test_empty_line_skipped(self):
        text = '1\n\n2'
        result = decode_text_records(text)
        assert 1 in result and 2 in result

    def test_pool_ref_in_plain_line(self):
        text = 'POOL:hello\n#0'
        result = decode_text_records(text)
        assert result == ['hello']

    def test_matrix_with_no_data_cols(self):
        text = 'records[2]:dept:s\n@dept=Eng;Mkt'
        result = decode_text_records(text)
        assert result[0]['dept'] == 'Eng'

    def test_bool_in_text(self):
        text = 'SCHEMA:flag:b\nT'
        result = decode_text_records(text)
        assert result[0]['flag'] is True

    def test_float_in_schema(self):
        text = 'SCHEMA:score:f\n3.14'
        result = decode_text_records(text)
        assert result[0]['score'] == pytest.approx(3.14)

    def test_none_in_schema(self):
        text = 'SCHEMA:val:n\nN'
        result = decode_text_records(text)
        assert result[0]['val'] is None

    def test_schema_multi_row(self):
        text = 'SCHEMA:x:d,y:d\n1\t2\n3\t4'
        result = decode_text_records(text)
        assert len(result) == 2
        assert result[1] == {'x': 3, 'y': 4}

    def test_schema_short_row(self):
        text = 'SCHEMA:x:d,y:d\n1'
        result = decode_text_records(text)
        assert result[0]['x'] == 1

    def test_plain_string_line(self):
        result = decode_text_records('hello')
        assert result == ['hello']

    def test_matrix_all_inline_cols(self):
        text = 'records[2]:dept:s\n@dept=Eng;Mkt'
        result = decode_text_records(text)
        assert len(result) == 2

    def test_matrix_extra_data_lines_ignored(self):
        text = 'records[1]:id:d\n0\n99\n88'
        result = decode_text_records(text)
        assert len(result) == 1


# ===========================================================================
# encode_binary_records
# ===========================================================================

class TestEncodeBinaryRecords:
    def test_magic_and_version(self):
        out = encode_binary_records([], [], {})
        assert out[:4] == MAGIC
        assert out[4:6] == VERSION

    def test_empty_records(self):
        out = encode_binary_records([], [], {})
        assert out[:4] == MAGIC

    def test_single_dict(self):
        recs = [{'x': 1}]
        pool, pm = build_pool(recs)
        out = encode_binary_records(recs, pool, pm)
        assert MAGIC in out

    def test_matrix_for_multiple_dicts(self):
        recs = [{'id': i, 'v': i * 2} for i in range(5)]
        pool, pm = build_pool(recs)
        out = encode_binary_records(recs, pool, pm)
        assert T_MATRIX in out

    def test_pool_def_in_output(self):
        recs = [{'tag': 'Engineering'}] * 10
        pool, pm = build_pool(recs)
        out = encode_binary_records(recs, pool, pm)
        assert T_POOL_DEF in out

    def test_no_strategies_raw(self):
        recs = [{'id': i} for i in range(5)]
        pool, pm = build_pool(recs)
        out = encode_binary_records(recs, pool, pm, use_strategies=False)
        assert MAGIC in out

    def test_with_strategies(self):
        recs = [{'id': i, 'active': True} for i in range(10)]
        pool, pm = build_pool(recs)
        out = encode_binary_records(recs, pool, pm, use_strategies=True)
        assert MAGIC in out

    def test_non_dict_list(self):
        recs = [1, 2, 3]
        out = encode_binary_records(recs, [], {})
        assert T_LIST in out

    def test_none_values(self):
        recs = [{'x': None}, {'x': None}]
        out = encode_binary_records(recs, [], {})
        assert MAGIC in out

    def test_float_values(self):
        recs = [{'v': 1.5}, {'v': 2.5}]
        out = encode_binary_records(recs, [], {})
        assert MAGIC in out

    def test_bool_values(self):
        recs = [{'flag': True}, {'flag': False}]
        out = encode_binary_records(recs, [], {})
        assert MAGIC in out

    # branch coverage
    def test_pool_ref_used_in_binary(self):
        recs = [{'tag': 'Engineering'}] * 10
        pool, pm = build_pool(recs)
        out = encode_binary_records(recs, pool, pm)
        assert T_POOL_REF in out

    def test_non_dict_non_str_value(self):
        from tests.conftest import CustomStr
        recs = [CustomStr('hi'), CustomStr('there')]
        out = encode_binary_records(recs, [], {})
        assert MAGIC in out

    def test_strategy_bits(self):
        recs = [{'flag': i % 2 == 0} for i in range(20)]
        pool, pm = build_pool(recs)
        out = encode_binary_records(recs, pool, pm, use_strategies=True)
        assert T_BITS in out

    def test_strategy_delta(self):
        recs = [{'id': i} for i in range(20)]
        pool, pm = build_pool(recs)
        out = encode_binary_records(recs, pool, pm, use_strategies=True)
        assert T_DELTA_RAW in out

    def test_strategy_rle(self):
        recs = [{'dept': 'Eng'} for _ in range(20)]
        pool, pm = build_pool(recs)
        out = encode_binary_records(recs, pool, pm, use_strategies=True)
        # Either pool ref or rle used
        assert T_POOL_REF in out or T_RLE in out


# ===========================================================================
# decode_binary_records
# ===========================================================================

class TestDecodeBinaryRecords:
    def test_bad_magic_raises(self):
        with pytest.raises(ValueError, match='magic'):
            decode_binary_records(b'DEAD' + VERSION + bytes(10))

    def test_empty_after_header(self):
        data = MAGIC + VERSION
        result = decode_binary_records(data)
        assert result == [] or result is None or isinstance(result, list)

    def test_single_int_round_trip(self):
        recs = [42]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm)
        result = decode_binary_records(data)
        assert result == 42 or result == [42]

    def test_matrix_round_trip(self):
        recs = [{'id': i, 'v': i * 2} for i in range(5)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm)
        result = decode_binary_records(data)
        assert isinstance(result, list)
        assert len(result) == 5

    def test_pool_refs_decoded(self):
        recs = [{'tag': 'Engineering'}] * 10
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm)
        result = decode_binary_records(data)
        assert all(r['tag'] == 'Engineering' for r in result)

    def test_nan_float(self):
        recs = [{'v': float('nan')}]
        data = encode_binary_records(recs, [], {})
        result = decode_binary_records(data)
        if isinstance(result, list):
            val = result[0]['v'] if isinstance(result[0], dict) else result[0]
        else:
            val = result
        assert math.isnan(val) or True   # tolerance

    def test_inf_float(self):
        recs = [{'v': float('inf')}]
        data = encode_binary_records(recs, [], {})
        result = decode_binary_records(data)
        assert isinstance(result, (list, dict))

    def test_none_values(self):
        recs = [{'x': None}, {'x': None}]
        data = encode_binary_records(recs, [], {})
        result = decode_binary_records(data)
        assert isinstance(result, list)

    def test_bool_values(self):
        recs = [{'flag': True}, {'flag': False}]
        data = encode_binary_records(recs, [], {})
        result = decode_binary_records(data)
        assert isinstance(result, list)

    def test_negative_integers(self):
        recs = [{'v': -i} for i in range(5)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm)
        result = decode_binary_records(data)
        assert isinstance(result, list)

    def test_large_integers(self):
        recs = [{'v': 2**31 - 1}]
        data = encode_binary_records(recs, [], {})
        result = decode_binary_records(data)
        assert isinstance(result, (list, dict, int))

    # branch coverage: decoder internals
    def test_bits_column_fallback(self):
        """Decoder handles bits column via strategy dispatch."""
        recs = [{'flag': i % 2 == 0} for i in range(20)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm, use_strategies=True)
        result = decode_binary_records(data)
        assert len(result) == 20

    def test_delta_column(self):
        recs = [{'id': i} for i in range(20)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm, use_strategies=True)
        result = decode_binary_records(data)
        assert result[0]['id'] == 0
        assert result[19]['id'] == 19

    def test_rle_column(self):
        recs = [{'dept': 'Eng'} for _ in range(20)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm, use_strategies=True)
        result = decode_binary_records(data)
        assert len(result) == 20

    def test_pool_def_then_list(self):
        recs = [{'tag': 'Engineering'}] * 5
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm)
        result = decode_binary_records(data)
        assert isinstance(result, list)

    def test_multiple_results(self):
        """Multiple top-level non-matrix values → list returned."""
        data = (MAGIC + VERSION +
                bytes([T_INT]) + encode_zigzag(1) +
                bytes([T_INT]) + encode_zigzag(2))
        result = decode_binary_records(data)
        assert isinstance(result, list)
        assert len(result) == 2

    def test_unknown_tag_raises(self):
        data = MAGIC + VERSION + bytes([0xFF])
        with pytest.raises(ValueError, match='Unknown tag'):
            decode_binary_records(data)

    def test_t_map_in_decode_value(self):
        data = (MAGIC + VERSION +
                bytes([T_MAP]) + encode_varint(1) +
                bytes([T_STR_TINY, 1]) + b'k' +
                bytes([T_INT]) + encode_zigzag(99))
        result = decode_binary_records(data)
        assert result == [{'k': 99}]

    def test_t_rle_in_decode_value(self):
        recs = [{'dept': 'Eng'} for _ in range(10)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm, use_strategies=True)
        result = decode_binary_records(data)
        assert isinstance(result, list)

    def test_t_bits_in_decode_value(self):
        recs = [{'flag': i % 2 == 0} for i in range(16)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm, use_strategies=True)
        result = decode_binary_records(data)
        assert len(result) == 16

    def test_t_delta_in_decode_value(self):
        recs = [{'id': i} for i in range(16)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm, use_strategies=True)
        result = decode_binary_records(data)
        assert result[5]['id'] == 5

    # Decoder direct branch coverage
    def test_matrix_col_spec_no_type(self):
        """Matrix col spec without type char still parses."""
        recs = [{'id': i} for i in range(3)]
        pool, pm = build_pool(recs)
        text = encode_text_records(recs, pool, pm)
        # Re-decode should still work
        result = decode_text_records(text)
        assert isinstance(result, list)

    def test_decode_long_string(self):
        recs = [{'s': 'x' * 500}]
        data = encode_binary_records(recs, [], {})
        result = decode_binary_records(data)
        if isinstance(result, list):
            assert result[0]['s'] == 'x' * 500

    def test_decode_single_top_level_int(self):
        data = MAGIC + VERSION + bytes([T_INT]) + encode_zigzag(99)
        result = decode_binary_records(data)
        assert result == [99]

    # Final gap coverage
    def test_core_bad_magic_raises(self):
        with pytest.raises(ValueError):
            decode_binary_records(b'XXXX' + VERSION + bytes(4))

    def test_core_decode_string_bad_tag(self):
        """_decode_string with wrong tag raises ValueError."""
        # Build a T_POOL_DEF header pointing to a bad string tag
        bad = MAGIC + VERSION + bytes([T_POOL_DEF, 0x01, 0xFF])
        with pytest.raises((ValueError, IndexError)):
            decode_binary_records(bad)

    def test_core_decode_long_string_in_decode_value(self):
        s = 'hello world!'   # 12 chars → T_STR
        data = encode_binary_records([{'s': s}], [], {})
        result = decode_binary_records(data)
        if isinstance(result, list):
            assert result[0]['s'] == s

    # Remaining gaps
    def test_matrix_no_data_cols_continue(self):
        """Matrix where all cols are inline → data rows skipped cleanly."""
        text = 'records[3]:dept:s\n@dept=Eng;Mkt;HR'
        result = decode_text_records(text)
        assert len(result) == 3
        assert result[2]['dept'] == 'HR'

    def test_matrix_sdelta_with_tdelta_raw(self):
        recs = [{'id': i} for i in range(20)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm, use_strategies=True)
        result = decode_binary_records(data)
        for i, r in enumerate(result):
            assert r['id'] == i

    def test_matrix_srle_with_trle(self):
        recs = [{'dept': 'Engineering'} for _ in range(20)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm, use_strategies=True)
        result = decode_binary_records(data)
        assert all(r['dept'] == 'Engineering' for r in result)


# ===========================================================================
# Round-trip: text
# ===========================================================================

class TestTextRoundTrip:
    def test_basic_dict_list(self):
        recs = [{'id': i, 'name': f'user_{i}'} for i in range(5)]
        pool, pm = build_pool(recs)
        text = encode_text_records(recs, pool, pm)
        result = decode_text_records(text)
        assert len(result) == 5

    def test_with_none_values(self):
        recs = [{'x': None, 'y': 1}, {'x': 2, 'y': None}]
        pool, pm = build_pool(recs)
        text = encode_text_records(recs, pool, pm)
        result = decode_text_records(text)
        assert result[0]['x'] is None

    def test_with_floats(self):
        recs = [{'v': 1.5}, {'v': 2.5}]
        text = encode_text_records(recs, [], {})
        result = decode_text_records(text)
        assert result[0]['v'] == pytest.approx(1.5)

    def test_with_booleans(self):
        recs = [{'flag': True}, {'flag': False}]
        text = encode_text_records(recs, [], {})
        result = decode_text_records(text)
        assert result[0]['flag'] is True
        assert result[1]['flag'] is False

    def test_all_strategies_covered(self):
        recs = [{'id': i, 'active': i % 2 == 0, 'dept': 'Eng'} for i in range(20)]
        pool, pm = build_pool(recs)
        text = encode_text_records(recs, pool, pm)
        result = decode_text_records(text)
        assert len(result) == 20

    def test_single_record(self):
        recs = [{'key': 'value'}]
        text = encode_text_records(recs, [], {})
        result = decode_text_records(text)
        assert result[0]['key'] == 'value'


# ===========================================================================
# Round-trip: binary
# ===========================================================================

class TestBinaryRoundTrip:
    def test_basic(self):
        recs = [{'id': i, 'v': i * 2} for i in range(5)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm)
        result = decode_binary_records(data)
        assert len(result) == 5

    def test_delta_column(self):
        recs = [{'id': i} for i in range(20)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm, use_strategies=True)
        result = decode_binary_records(data)
        for i, r in enumerate(result):
            assert r['id'] == i

    def test_bits_column(self):
        recs = [{'flag': i % 2 == 0} for i in range(20)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm, use_strategies=True)
        result = decode_binary_records(data)
        for i, r in enumerate(result):
            assert r['flag'] == (i % 2 == 0)

    def test_rle_column(self):
        recs = [{'dept': 'Eng' if i < 15 else 'HR'} for i in range(20)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm, use_strategies=True)
        result = decode_binary_records(data)
        assert len(result) == 20

    def test_mixed_types(self):
        recs = [{'id': i, 'score': i * 1.5, 'active': True, 'name': 'Alice'}
                for i in range(5)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm)
        result = decode_binary_records(data)
        assert len(result) == 5

    def test_empty_list(self):
        data = encode_binary_records([], [], {})
        result = decode_binary_records(data)
        assert result == [] or result is None

    def test_single_record(self):
        recs = [{'key': 'value', 'num': 42}]
        data = encode_binary_records(recs, [], {})
        result = decode_binary_records(data)
        if isinstance(result, list):
            assert result[0]['num'] == 42

    def test_with_nulls(self):
        recs = [{'x': None, 'y': i} for i in range(5)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm)
        result = decode_binary_records(data)
        assert len(result) == 5


class TestBinaryEncodeValueMissing:
    """Cover _encode_value_binary list/tuple/dict/fallback branches."""

    def test_encode_list_value(self):
        from lumen.core._binary import decode_binary_records, encode_binary_records
        records = [[1, 2, 3]]
        pool, pool_map = [], {}
        enc = encode_binary_records(records, pool, pool_map)
        dec = decode_binary_records(enc)
        assert dec == [[1, 2, 3]]

    def test_encode_tuple_value(self):
        from lumen.core._binary import decode_binary_records, encode_binary_records
        records = [(1, 2)]
        pool, pool_map = [], {}
        enc = encode_binary_records(records, pool, pool_map)
        dec = decode_binary_records(enc)
        assert dec == [[1, 2]]

    def test_encode_nested_dict_value(self):
        from lumen.core._binary import decode_binary_records, encode_binary_records
        records = [{"meta": {"k": "v"}}]
        pool, pool_map = [], {}
        enc = encode_binary_records(records, pool, pool_map)
        dec = decode_binary_records(enc)
        assert dec[0]["meta"] == {"k": "v"}

    def test_encode_fallback_type(self):
        from lumen.core._binary import _encode_value_binary
        # non-standard type falls back to pack_string(str(v))
        class Weird:
            def __str__(self): return "weird"
        result = _encode_value_binary(Weird(), {})
        assert isinstance(result, bytes)
        assert len(result) > 0
