"""
Unit tests for low-level primitives in lumen.core:
  - FNV-1a hash
  - estimate_tokens / deep_size / deep_eq
  - Varint / Zigzag encoding
  - Pack functions: string, int, float, bool, null, pool_ref, bits, delta, rle
  - Text helpers: escape, unescape, format_float, parse_value, encode_value_text
  - encode_obj_iterative_text (recursive object → text)
"""
import math
import struct

import pytest

from lumen.core import (
    T_BITS,
    T_BOOL,
    T_DELTA_RAW,
    T_FLOAT,
    T_INT,
    T_NULL,
    T_POOL_REF,
    T_RLE,
    T_STR,
    T_STR_TINY,
    _encode_obj_iterative_text,
    _encode_value_text,
    _format_float,
    _parse_value,
    _text_escape,
    _text_unescape,
    decode_varint,
    decode_zigzag,
    deep_eq,
    deep_size,
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

# ===========================================================================
# Constants
# ===========================================================================

class TestConstants:
    def test_magic(self):
        from lumen.core import MAGIC
        assert MAGIC == b'LUMB'

    def test_version_bytes(self):
        from lumen.core import VERSION
        assert bytes([3, 3]) == VERSION

    def test_version_string(self):
        from lumen.core import __version__
        assert __version__ == '1.0.0'

    def test_edition_contains_lumex(self):
        from lumen.core import __edition__
        assert 'Lumex' in __edition__

    def test_all_type_tags_distinct(self):
        tags = [
            T_STR_TINY, T_STR, T_INT, T_FLOAT, T_BOOL, T_NULL,
            T_POOL_REF, T_BITS, T_DELTA_RAW, T_RLE,
        ]
        from lumen.core import T_LIST, T_MAP, T_MATRIX, T_POOL_DEF, T_STRATEGY
        tags += [T_LIST, T_MAP, T_POOL_DEF, T_MATRIX, T_STRATEGY]
        assert len(tags) == len(set(tags))

    def test_strategy_bytes_distinct(self):
        from lumen.core import S_BITS, S_DELTA, S_POOL, S_RAW, S_RLE
        vals = [S_RAW, S_DELTA, S_RLE, S_BITS, S_POOL]
        assert len(vals) == len(set(vals))

    def test_type_tag_values(self):
        assert T_STR_TINY == 0x01
        assert T_STR      == 0x02
        assert T_INT      == 0x03
        assert T_FLOAT    == 0x04
        assert T_BOOL     == 0x05
        assert T_NULL     == 0x06

    def test_strategy_bytes_values(self):
        from lumen.core import S_BITS, S_DELTA, S_POOL, S_RAW, S_RLE
        assert S_RAW   == 0x00
        assert S_DELTA == 0x01
        assert S_RLE   == 0x02
        assert S_BITS  == 0x03
        assert S_POOL  == 0x04


# ===========================================================================
# FNV-1a
# ===========================================================================

class TestFnv1a:
    def test_empty_is_offset_basis(self):
        assert fnv1a(b'') == 0x811C9DC5

    def test_deterministic(self):
        assert fnv1a(b'hello') == fnv1a(b'hello')

    def test_distinct_inputs(self):
        assert fnv1a(b'hello') != fnv1a(b'world')

    def test_32bit_result(self):
        assert 0 <= fnv1a(b'test') <= 0xFFFFFFFF

    def test_single_byte_zero(self):
        assert isinstance(fnv1a(bytes([0])), int)

    def test_all_bytes(self):
        result = fnv1a(bytes(range(256)))
        assert isinstance(result, int)

    def test_fnv1a_str_matches_bytes(self):
        assert fnv1a_str('hello') == fnv1a(b'hello')

    def test_fnv1a_str_unicode(self):
        s = '日本語'
        assert fnv1a_str(s) == fnv1a(s.encode('utf-8'))

    def test_fnv1a_str_empty(self):
        assert fnv1a_str('') == fnv1a(b'')

    def test_known_value_hello(self):
        # FNV-1a 32-bit of b'hello' is a known constant
        assert fnv1a(b'hello') == 0x4F9F2CAB


# ===========================================================================
# estimate_tokens
# ===========================================================================

class TestEstimateTokens:
    def test_empty(self):
        assert estimate_tokens('') == 0

    def test_one_char(self):
        assert estimate_tokens('a') == 1

    def test_four_chars(self):
        assert estimate_tokens('abcd') == 1

    def test_five_chars(self):
        assert estimate_tokens('abcde') == 2

    def test_400_chars(self):
        assert estimate_tokens('a' * 400) == 100

    def test_minimum_nonzero(self):
        assert estimate_tokens('x') >= 1

    def test_1000_chars(self):
        assert estimate_tokens('a' * 1000) == 250


# ===========================================================================
# deep_size
# ===========================================================================

class TestDeepSize:
    def test_int_positive(self):
        assert deep_size(42) > 0

    def test_larger_list(self):
        assert deep_size([1, 2, 3]) > deep_size(1)

    def test_dict_positive(self):
        assert deep_size({'a': 1}) > 0

    def test_nested_dict(self):
        assert deep_size({'a': {'b': {'c': 1}}}) > deep_size({'a': 1})

    def test_set(self):
        assert deep_size({1, 2, 3}) > 0

    def test_frozenset(self):
        assert deep_size(frozenset([1, 2, 3])) > 0

    def test_tuple(self):
        assert deep_size((1, 2, 3)) > 0

    def test_cycle_safe(self):
        d = {}
        d['self'] = d
        size = deep_size(d)
        assert size > 0  # no infinite loop

    def test_none(self):
        assert deep_size(None) > 0

    def test_string(self):
        assert deep_size('hello') > 0

    # branch coverage
    def test_frozenset_branch(self):
        fs = frozenset(['a', 'b', 'c'])
        assert deep_size(fs) > 0

    def test_circular_ref(self):
        lst = [1, 2, 3]
        lst.append(lst)
        assert deep_size(lst) > 0


# ===========================================================================
# deep_eq
# ===========================================================================

class TestDeepEq:
    def test_int_equal(self):        assert deep_eq(1, 1)
    def test_int_unequal(self):      assert not deep_eq(1, 2)
    def test_none_equal(self):       assert deep_eq(None, None)
    def test_bool_tt(self):          assert deep_eq(True, True)
    def test_bool_ff(self):          assert deep_eq(False, False)
    def test_bool_tf(self):          assert not deep_eq(True, False)
    def test_str_equal(self):        assert deep_eq('hi', 'hi')
    def test_str_unequal(self):      assert not deep_eq('hi', 'bye')
    def test_nan_nan(self):          assert deep_eq(float('nan'), float('nan'))
    def test_nan_float(self):        assert not deep_eq(float('nan'), 1.0)
    def test_pos_inf(self):          assert deep_eq(float('inf'), float('inf'))
    def test_neg_inf(self):          assert deep_eq(float('-inf'), float('-inf'))
    def test_inf_sign_mismatch(self):assert not deep_eq(float('inf'), float('-inf'))
    def test_zero_float(self):       assert deep_eq(0.0, 0.0)
    def test_int_float_same(self):   assert deep_eq(1, 1.0)
    def test_int_float_diff(self):   assert not deep_eq(1, 2.0)
    def test_str_int_mismatch(self): assert not deep_eq('1', 1)
    def test_none_int_mismatch(self):assert not deep_eq(None, 0)

    def test_dict_equal(self):
        assert deep_eq({'a': 1, 'b': 2}, {'b': 2, 'a': 1})

    def test_dict_key_mismatch(self):
        assert not deep_eq({'a': 1}, {'b': 1})

    def test_dict_value_mismatch(self):
        assert not deep_eq({'a': 1}, {'a': 2})

    def test_dict_nan_value(self):
        assert deep_eq({'v': float('nan')}, {'v': float('nan')})

    def test_dict_size_mismatch(self):
        assert not deep_eq({'a': 1}, {'a': 1, 'b': 2})

    def test_list_equal(self):
        assert deep_eq([1, 2, 3], [1, 2, 3])

    def test_list_length_diff(self):
        assert not deep_eq([1, 2], [1, 2, 3])

    def test_list_elem_diff(self):
        assert not deep_eq([1, 2], [1, 3])

    def test_tuple_equal(self):
        assert deep_eq((1, 2), (1, 2))

    def test_list_nan(self):
        assert deep_eq([float('nan')], [float('nan')])

    def test_type_mismatch_no_numeric(self):
        assert not deep_eq('1', [1])

    def test_nested(self):
        a = {'x': [1, {'y': float('nan')}]}
        b = {'x': [1, {'y': float('nan')}]}
        assert deep_eq(a, b)


# ===========================================================================
# Varint
# ===========================================================================

class TestVarint:
    CASES = [0, 1, 63, 127, 128, 255, 256, 16383, 16384, 2**21, 2**28, 2**35]

    def test_round_trip_all(self):
        for v in self.CASES:
            enc = encode_varint(v)
            dec, pos = decode_varint(enc, 0)
            assert dec == v
            assert pos == len(enc)

    def test_zero_single_byte(self):
        assert encode_varint(0) == bytes([0])

    def test_127_single_byte(self):
        assert len(encode_varint(127)) == 1

    def test_128_two_bytes(self):
        assert len(encode_varint(128)) == 2

    def test_negative_raises(self):
        with pytest.raises(ValueError):
            encode_varint(-1)

    def test_decode_with_offset(self):
        buf = bytes([0xFF, 0x00])      # junk + zero varint
        v, pos = decode_varint(buf, 1)
        assert v == 0
        assert pos == 2

    def test_multibyte_continuations(self):
        for v in [128, 16384, 2**21]:
            enc = encode_varint(v)
            assert len(enc) > 1


# ===========================================================================
# Zigzag
# ===========================================================================

class TestZigzag:
    CASES = [0, -1, 1, -128, 127, -32768, 32767, -2**30, 2**30]

    def test_round_trip_all(self):
        for v in self.CASES:
            enc = encode_zigzag(v)
            dec, pos = decode_zigzag(enc, 0)
            assert dec == v
            assert pos == len(enc)

    def test_zero_is_zero(self):
        assert encode_zigzag(0) == encode_varint(0)

    def test_positive_even_mapping(self):
        enc = encode_zigzag(1)
        zz, _ = decode_varint(enc, 0)
        assert zz == 2   # positive n → 2n

    def test_negative_odd_mapping(self):
        enc = encode_zigzag(-1)
        zz, _ = decode_varint(enc, 0)
        assert zz == 1   # -1 → 1

    def test_decode_with_offset(self):
        buf = bytes([0x00]) + encode_zigzag(-42)
        v, pos = decode_zigzag(buf, 1)
        assert v == -42


# ===========================================================================
# pack_string
# ===========================================================================

class TestPackString:
    def test_empty_tiny(self):
        b = pack_string('')
        assert b[0] == T_STR_TINY
        assert b[1] == 0

    def test_one_char_tiny(self):
        b = pack_string('a')
        assert b[0] == T_STR_TINY

    def test_two_char_tiny(self):
        assert pack_string('ab')[0] == T_STR_TINY

    def test_three_char_tiny(self):
        assert pack_string('abc')[0] == T_STR_TINY

    def test_four_char_regular(self):
        assert pack_string('abcd')[0] == T_STR

    def test_long_string(self):
        s = 'x' * 1000
        b = pack_string(s)
        assert b[0] == T_STR
        assert s.encode() in b

    def test_multibyte_utf8(self):
        s = '日本語'
        b = pack_string(s)
        assert s.encode('utf-8') in b

    def test_content_preserved_tiny(self):
        b = pack_string('hi')
        assert b[2:4] == b'hi'

    def test_content_preserved_regular(self):
        s = 'hello world'
        b = pack_string(s)
        assert s.encode() in b

    # tag boundary checks (from ec28/ec29)
    def test_str_tiny_boundary(self):
        for s in ['a', 'ab', 'abc']:
            assert pack_string(s)[0] == T_STR_TINY

    def test_str_regular_at_four(self):
        assert pack_string('hell')[0] == T_STR


# ===========================================================================
# pack_int
# ===========================================================================

class TestPackInt:
    def test_tag(self):      assert pack_int(0)[0] == T_INT
    def test_zero(self):     v, _ = decode_zigzag(pack_int(0), 1); assert v == 0
    def test_positive(self): v, _ = decode_zigzag(pack_int(42), 1); assert v == 42
    def test_negative(self): v, _ = decode_zigzag(pack_int(-1), 1); assert v == -1
    def test_max_int(self):  v, _ = decode_zigzag(pack_int(2**31-1), 1); assert v == 2**31-1
    def test_min_int(self):  v, _ = decode_zigzag(pack_int(-2**31), 1); assert v == -2**31


# ===========================================================================
# pack_float
# ===========================================================================

class TestPackFloat:
    def test_tag(self):
        assert pack_float(1.0)[0] == T_FLOAT

    def test_length(self):
        assert len(pack_float(1.0)) == 9  # 1 tag + 8 bytes

    def test_round_trip(self):
        v = struct.unpack('>d', pack_float(3.14)[1:])[0]
        assert v == 3.14

    def test_nan(self):
        v = struct.unpack('>d', pack_float(float('nan'))[1:])[0]
        assert math.isnan(v)

    def test_pos_inf(self):
        v = struct.unpack('>d', pack_float(float('inf'))[1:])[0]
        assert math.isinf(v) and v > 0

    def test_neg_inf(self):
        v = struct.unpack('>d', pack_float(float('-inf'))[1:])[0]
        assert math.isinf(v) and v < 0

    def test_big_endian_byte_order(self):
        b = pack_float(1.0)
        assert b[1:] == struct.pack('>d', 1.0)


# ===========================================================================
# pack_bool
# ===========================================================================

class TestPackBool:
    def test_true_tag_and_value(self):
        b = pack_bool(True)
        assert b[0] == T_BOOL
        assert b[1] == 1

    def test_false_tag_and_value(self):
        b = pack_bool(False)
        assert b[0] == T_BOOL
        assert b[1] == 0

    def test_length(self):
        assert len(pack_bool(True)) == 2


# ===========================================================================
# pack_null
# ===========================================================================

class TestPackNull:
    def test_is_null_tag(self):
        assert pack_null() == bytes([T_NULL])

    def test_length(self):
        assert len(pack_null()) == 1


# ===========================================================================
# pack_pool_ref
# ===========================================================================

class TestPackPoolRef:
    def test_tag(self):
        assert pack_pool_ref(0)[0] == T_POOL_REF

    def test_index_zero(self):
        b = pack_pool_ref(0)
        idx, _ = decode_varint(b, 1)
        assert idx == 0

    def test_index_nine(self):
        b = pack_pool_ref(9)
        idx, _ = decode_varint(b, 1)
        assert idx == 9

    def test_index_127(self):
        b = pack_pool_ref(127)
        idx, _ = decode_varint(b, 1)
        assert idx == 127

    def test_index_large(self):
        b = pack_pool_ref(300)
        idx, _ = decode_varint(b, 1)
        assert idx == 300


# ===========================================================================
# pack_bits / unpack_bits
# ===========================================================================

class TestPackBits:
    def test_tag(self):
        assert pack_bits([])[0] == T_BITS

    def test_empty_round_trip(self):
        bools, pos = unpack_bits(pack_bits([]), 1)
        assert bools == []

    def test_round_trip_various_lengths(self):
        for n in [1, 7, 8, 9, 15, 16, 100]:
            bools = [i % 2 == 0 for i in range(n)]
            packed = pack_bits(bools)
            result, _ = unpack_bits(packed, 1)
            assert result == bools

    def test_all_true(self):
        bools = [True] * 16
        result, _ = unpack_bits(pack_bits(bools), 1)
        assert result == bools

    def test_all_false(self):
        bools = [False] * 16
        result, _ = unpack_bits(pack_bits(bools), 1)
        assert result == bools

    def test_alternating(self):
        bools = [True, False] * 8
        result, _ = unpack_bits(pack_bits(bools), 1)
        assert result == bools

    def test_bit_order_lsb_first(self):
        # bit 0 of byte 0 = first bool
        packed = pack_bits([True, False, False, False, False, False, False, False])
        # skip tag + varint(8)
        data_byte = packed[2]
        assert data_byte & 1 == 1


# ===========================================================================
# pack_delta_raw / unpack_delta_raw
# ===========================================================================

class TestPackDeltaRaw:
    def test_tag(self):
        assert pack_delta_raw([])[0] == T_DELTA_RAW

    def test_empty_round_trip(self):
        vals, _ = unpack_delta_raw(pack_delta_raw([]), 1)
        assert vals == []

    def test_monotonic_increasing(self):
        ints = list(range(100))
        vals, _ = unpack_delta_raw(pack_delta_raw(ints), 1)
        assert vals == ints

    def test_monotonic_decreasing(self):
        ints = list(range(100, 0, -1))
        vals, _ = unpack_delta_raw(pack_delta_raw(ints), 1)
        assert vals == ints

    def test_negative_start(self):
        ints = [-50, -40, -30, -20, -10, 0]
        vals, _ = unpack_delta_raw(pack_delta_raw(ints), 1)
        assert vals == ints

    def test_large_jumps(self):
        ints = [0, 1_000_000, -1_000_000]
        vals, _ = unpack_delta_raw(pack_delta_raw(ints), 1)
        assert vals == ints

    def test_single_element(self):
        vals, _ = unpack_delta_raw(pack_delta_raw([42]), 1)
        assert vals == [42]

    def test_two_elements(self):
        vals, _ = unpack_delta_raw(pack_delta_raw([10, 20]), 1)
        assert vals == [10, 20]

    def test_delta_compresses_monotonic(self):
        ints = list(range(1000))
        assert len(pack_delta_raw(ints)) < sum(len(encode_zigzag(v)) for v in ints)


# ===========================================================================
# pack_rle
# ===========================================================================

class TestPackRle:
    def test_tag(self):
        assert pack_rle([])[0] == T_RLE

    def test_empty(self):
        b = pack_rle([])
        assert b[0] == T_RLE

    def test_all_same_string(self):
        b = pack_rle(['x'] * 10)
        assert len(b) < len('x' * 10)

    def test_all_same_int(self):
        b = pack_rle([5] * 10)
        assert b[0] == T_RLE

    def test_all_same_bool_true(self):
        b = pack_rle([True] * 5)
        assert b[0] == T_RLE

    def test_all_same_bool_false(self):
        b = pack_rle([False] * 5)
        assert b[0] == T_RLE

    def test_all_same_float(self):
        b = pack_rle([1.5] * 5)
        assert b[0] == T_RLE

    def test_all_none(self):
        b = pack_rle([None] * 5)
        assert b[0] == T_RLE

    def test_two_runs(self):
        b = pack_rle(['a', 'a', 'b', 'b'])
        assert b[0] == T_RLE

    def test_all_different(self):
        b = pack_rle([1, 2, 3, 4, 5])
        assert b[0] == T_RLE

    def test_compact_for_repeated(self):
        many = ['hello'] * 100
        assert len(pack_rle(many)) < len('hello' * 100)

    # branch coverage
    def test_rle_with_bool(self):
        b = pack_rle([True, True, False])
        assert b[0] == T_RLE

    def test_rle_with_float(self):
        b = pack_rle([3.14, 3.14])
        assert b[0] == T_RLE

    def test_rle_with_string(self):
        b = pack_rle(['abc', 'abc'])
        assert b[0] == T_RLE

    def test_rle_with_none(self):
        b = pack_rle([None, None])
        assert b[0] == T_RLE

    def test_rle_with_int(self):
        b = pack_rle([7, 7, 7])
        assert b[0] == T_RLE


# ===========================================================================
# Text helpers: escape / unescape
# ===========================================================================

class TestTextEscape:
    def test_plain(self):          assert _text_escape('hello') == 'hello'
    def test_tab(self):            assert _text_escape('\t') == '\\t'
    def test_newline(self):        assert _text_escape('\n') == '\\n'
    def test_cr(self):             assert _text_escape('\r') == '\\r'
    def test_backslash(self):      assert _text_escape('\\') == '\\\\'
    def test_combined(self):       assert _text_escape('a\tb\nc') == 'a\\tb\\nc'
    def test_empty(self):          assert _text_escape('') == ''
    def test_no_special_unchanged(self): assert _text_escape('abc123') == 'abc123'


class TestTextUnescape:
    def test_plain(self):          assert _text_unescape('hello') == 'hello'
    def test_tab(self):            assert _text_unescape('\\t') == '\t'
    def test_newline(self):        assert _text_unescape('\\n') == '\n'
    def test_cr(self):             assert _text_unescape('\\r') == '\r'
    def test_backslash(self):      assert _text_unescape('\\\\') == '\\'
    def test_round_trip(self):
        for s in ['hello\tworld', 'line1\nline2', 'back\\slash']:
            assert _text_unescape(_text_escape(s)) == s
    def test_unknown_escape_no_crash(self):
        result = _text_unescape('\\z')
        assert isinstance(result, str)
    def test_trailing_backslash_no_crash(self):
        result = _text_unescape('abc\\')
        assert isinstance(result, str)
    def test_empty(self):
        assert _text_unescape('') == ''
    # branch coverage
    def test_unknown_escape_preserved(self):
        result = _text_unescape('\\q')
        assert '\\' in result or 'q' in result


# ===========================================================================
# _format_float
# ===========================================================================

class TestFormatFloat:
    def test_nan(self):         assert _format_float(float('nan')) == 'nan'
    def test_pos_inf(self):     assert _format_float(float('inf')) == 'inf'
    def test_neg_inf(self):     assert _format_float(float('-inf')) == '-inf'
    def test_zero(self):        assert _format_float(0.0) == '0.0'
    def test_pi(self):          assert _format_float(3.14) == repr(3.14)
    def test_negative(self):    assert _format_float(-1.5) == repr(-1.5)
    def test_scientific(self):  assert _format_float(1e100) == repr(1e100)


# ===========================================================================
# _parse_value
# ===========================================================================

class TestParseValue:
    def test_null(self):               assert _parse_value('N') is None
    def test_true(self):               assert _parse_value('T') is True
    def test_false(self):              assert _parse_value('F') is False
    def test_empty_string_token(self): assert _parse_value('$0=') == ''
    def test_nan(self):                assert math.isnan(_parse_value('nan'))
    def test_pos_inf(self):            assert _parse_value('inf') == float('inf')
    def test_neg_inf(self):            assert _parse_value('-inf') == float('-inf')
    def test_int_positive(self):       assert _parse_value('42') == 42
    def test_int_negative(self):       assert _parse_value('-7') == -7
    def test_int_zero(self):           assert _parse_value('0') == 0
    def test_float(self):              assert _parse_value('3.14') == pytest.approx(3.14)
    def test_pool_ref_single_digit(self):
        v = _parse_value('#3')
        assert v == ('__pool_ref__', 3)
    def test_pool_ref_braces(self):
        v = _parse_value('#{12}')
        assert v == ('__pool_ref__', 12)
    def test_pool_ref_zero(self):
        v = _parse_value('#0')
        assert v == ('__pool_ref__', 0)
    def test_plain_string(self):
        assert _parse_value('hello') == 'hello'
    def test_string_with_spaces(self):
        assert _parse_value('hello world') == 'hello world'
    # branch coverage
    def test_pool_ref_digit_only(self):
        v = _parse_value('#5')
        assert v == ('__pool_ref__', 5)
    def test_hash_non_digit_non_brace(self):
        v = _parse_value('#abc')
        # falls through to string
        assert isinstance(v, (str, tuple))
    def test_float_scientific(self):
        v = _parse_value('1e10')
        assert isinstance(v, float)


# ===========================================================================
# _encode_value_text
# ===========================================================================

class TestEncodeValueText:
    def test_none(self):         assert _encode_value_text(None, {}) == 'N'
    def test_true(self):         assert _encode_value_text(True, {}) == 'T'
    def test_false(self):        assert _encode_value_text(False, {}) == 'F'
    def test_zero(self):         assert _encode_value_text(0, {}) == '0'
    def test_pos_int(self):      assert _encode_value_text(42, {}) == '42'
    def test_neg_int(self):      assert _encode_value_text(-7, {}) == '-7'
    def test_empty_str(self):    assert _encode_value_text('', {}) == '$0='
    def test_plain_str(self):    assert _encode_value_text('hello', {}) == 'hello'
    def test_nan(self):          assert _encode_value_text(float('nan'), {}) == 'nan'
    def test_inf(self):          assert _encode_value_text(float('inf'), {}) == 'inf'
    def test_neg_inf(self):      assert _encode_value_text(float('-inf'), {}) == '-inf'

    def test_pool_ref_le9(self):
        pm = {'hello': 3}
        assert _encode_value_text('hello', pm) == '#3'

    def test_pool_ref_gt9(self):
        pm = {'hello': 10}
        assert _encode_value_text('hello', pm) == '#{10}'

    def test_pool_ref_exactly_9(self):
        pm = {'x': 9}
        assert _encode_value_text('x', pm) == '#9'

    def test_no_pool_no_ref(self):
        assert _encode_value_text('hello', {}) == 'hello'

    def test_float_value(self):
        assert _encode_value_text(1.5, {}) == repr(1.5)

    # branch coverage: non-string fallback
    def test_non_string_fallback(self):
        from tests.conftest import CustomStr
        result = _encode_value_text(CustomStr('hi'), {})
        assert 'hi' in result

    def test_pool_ref_index_0_to_9(self):
        for i in range(10):
            pm = {'word': i}
            enc = _encode_value_text('word', pm)
            assert enc == f'#{i}'

    def test_pool_ref_index_gt_9(self):
        pm = {'word': 11}
        assert _encode_value_text('word', pm) == '#{11}'


# ===========================================================================
# _encode_obj_iterative_text
# ===========================================================================

class TestEncodeObjIterativeText:
    def test_none(self):        assert _encode_obj_iterative_text(None, {}) == 'N'
    def test_true(self):        assert _encode_obj_iterative_text(True, {}) == 'T'
    def test_false(self):       assert _encode_obj_iterative_text(False, {}) == 'F'
    def test_int(self):         assert _encode_obj_iterative_text(42, {}) == '42'
    def test_float(self):       assert _encode_obj_iterative_text(1.5, {}) == repr(1.5)
    def test_empty_dict(self):  assert _encode_obj_iterative_text({}, {}) == '{}'
    def test_empty_list(self):  assert _encode_obj_iterative_text([], {}) == '[]'

    def test_simple_dict(self):
        result = _encode_obj_iterative_text({'a': 1}, {})
        assert 'a' in result and '1' in result

    def test_simple_list(self):
        result = _encode_obj_iterative_text([1, 2, 3], {})
        assert '1' in result and '3' in result

    def test_nested_dict(self):
        result = _encode_obj_iterative_text({'a': {'b': 1}}, {})
        assert 'b' in result

    def test_list_of_mixed(self):
        result = _encode_obj_iterative_text([1, 'x', None], {})
        assert 'N' in result

    def test_tuple_like_list(self):
        result = _encode_obj_iterative_text((1, 2), {})
        assert '1' in result

    def test_pool_map_used(self):
        pm = {'hello': 0}
        result = _encode_obj_iterative_text('hello', pm)
        assert result == '#0'

    def test_deeply_nested(self):
        obj = {'a': {'b': {'c': {'d': 99}}}}
        result = _encode_obj_iterative_text(obj, {})
        assert '99' in result

    def test_float_nan(self):
        result = _encode_obj_iterative_text(float('nan'), {})
        assert result == 'nan'

    def test_str_value(self):
        assert _encode_obj_iterative_text('world', {}) == 'world'

    def test_str_with_pool(self):
        pm = {'world': 1}
        assert _encode_obj_iterative_text('world', pm) == '#1'

    def test_non_standard_type(self):
        from tests.conftest import CustomStr
        result = _encode_obj_iterative_text(CustomStr('hi'), {})
        assert 'hi' in result

    def test_empty_string_in_obj(self):
        result = _encode_obj_iterative_text('', {})
        assert result == '$0='

    def test_dict_with_pool_key(self):
        pm = {'key': 0}
        result = _encode_obj_iterative_text({'key': 'val'}, pm)
        assert '#0' in result
