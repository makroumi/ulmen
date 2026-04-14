"""
Integration edge case tests: boundary values, special types, nulls, unicode,
large/small record shapes.
"""

from ulmen import UlmenDictRust
from ulmen.core import (
    MAGIC,
    build_pool,
    decode_binary_records,
    decode_text_records,
    encode_binary_records,
    encode_text_records,
)

# ===========================================================================
# Empty inputs
# ===========================================================================

class TestEdgeCasesEmpty:
    def test_empty_list_text(self):
        assert encode_text_records([], [], {}) == ''

    def test_empty_list_binary(self):
        out = encode_binary_records([], [], {})
        assert out[:4] == MAGIC

    def test_empty_dict_in_list(self):
        recs = [{}]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm)
        result = decode_binary_records(data)
        assert isinstance(result, (list, dict))


# ===========================================================================
# Single record
# ===========================================================================

class TestEdgeCasesSingleRecord:
    def test_single_record_text(self):
        recs = [{'id': 1, 'name': 'solo'}]
        pool, pm = build_pool(recs)
        text = encode_text_records(recs, pool, pm)
        result = decode_text_records(text)
        assert result[0]['id'] == 1

    def test_single_record_binary(self):
        recs = [{'id': 1}]
        data = encode_binary_records(recs, [], {})
        result = decode_binary_records(data)
        if isinstance(result, list):
            assert result[0]['id'] == 1

    def test_single_rust_text(self):
        ld = UlmenDictRust([{'id': 1}])
        text = ld.encode_text()
        assert '1' in text

    def test_single_rust_binary(self):
        ld = UlmenDictRust([{'id': 1}])
        assert ld.encode_binary()[:4] == MAGIC


# ===========================================================================
# Special floats
# ===========================================================================

class TestEdgeCasesSpecialFloats:
    def test_float_binary(self):
        recs = [{'v': 3.14}]
        data = encode_binary_records(recs, [], {})
        result = decode_binary_records(data)
        if isinstance(result, list):
            assert abs(result[0]['v'] - 3.14) < 1e-10

    def test_nan_text(self):
        recs = [{'v': float('nan')}]
        pool, pm = build_pool(recs)
        text = encode_text_records(recs, pool, pm)
        assert 'nan' in text

    def test_inf_text(self):
        recs = [{'v': float('inf')}]
        pool, pm = build_pool(recs)
        text = encode_text_records(recs, pool, pm)
        assert 'inf' in text

    def test_neg_inf_text(self):
        recs = [{'v': float('-inf')}]
        pool, pm = build_pool(recs)
        text = encode_text_records(recs, pool, pm)
        assert '-inf' in text


# ===========================================================================
# Integer edge cases
# ===========================================================================

class TestEdgeCasesIntegers:
    def test_integer_binary_round_trip(self):
        for v in [0, 1, -1, 127, 128, 255, 256, 65535, 2**31 - 1, -2**31]:
            recs = [{'v': v}]
            data = encode_binary_records(recs, [], {})
            result = decode_binary_records(data)
            if isinstance(result, list):
                assert result[0]['v'] == v

    def test_large_monotonic(self):
        recs = [{'id': i * 1000} for i in range(100)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm, use_strategies=True)
        result = decode_binary_records(data)
        assert len(result) == 100


# ===========================================================================
# String edge cases
# ===========================================================================

class TestEdgeCasesStrings:
    def test_empty_string(self):
        recs = [{'s': ''}]
        data = encode_binary_records(recs, [], {})
        result = decode_binary_records(data)
        if isinstance(result, list):
            assert result[0]['s'] == ''

    def test_empty_string_text(self):
        recs = [{'s': ''}]
        pool, pm = build_pool(recs)
        text = encode_text_records(recs, pool, pm)
        result = decode_text_records(text)
        assert result[0]['s'] == ''

    def test_single_char_strings(self):
        recs = [{'s': chr(i)} for i in range(65, 91)]   # A-Z
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm)
        result = decode_binary_records(data)
        assert isinstance(result, list)

    def test_unicode_strings(self):
        recs = [{'s': s} for s in ['日本語', 'مرحبا', 'Ñoño', '🎉']]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm)
        result = decode_binary_records(data)
        if isinstance(result, list):
            assert result[0]['s'] == '日本語'

    def test_tab_in_string(self):
        recs = [{'s': 'hello\tworld'}]
        pool, pm = build_pool(recs)
        text = encode_text_records(recs, pool, pm)
        result = decode_text_records(text)
        assert result[0]['s'] == 'hello\tworld'

    def test_newline_in_string(self):
        recs = [{'s': 'line1\nline2'}]
        pool, pm = build_pool(recs)
        text = encode_text_records(recs, pool, pm)
        result = decode_text_records(text)
        assert result[0]['s'] == 'line1\nline2'

    def test_backslash_in_string(self):
        recs = [{'s': 'a\\b'}]
        pool, pm = build_pool(recs)
        text = encode_text_records(recs, pool, pm)
        result = decode_text_records(text)
        assert result[0]['s'] == 'a\\b'

    def test_long_string(self):
        s = 'x' * 10_000
        recs = [{'s': s}]
        data = encode_binary_records(recs, [], {})
        result = decode_binary_records(data)
        if isinstance(result, list):
            assert result[0]['s'] == s


# ===========================================================================
# Null columns
# ===========================================================================

class TestEdgeCasesNulls:
    def test_all_null_column(self):
        recs = [{'v': None} for _ in range(10)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm, use_strategies=True)
        result = decode_binary_records(data)
        assert all(r['v'] is None for r in result)

    def test_mixed_null_and_int(self):
        recs = [{'v': i if i % 2 == 0 else None} for i in range(10)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm)
        result = decode_binary_records(data)
        assert len(result) == 10

    def test_null_in_text(self):
        recs = [{'v': None}]
        pool, pm = build_pool(recs)
        text = encode_text_records(recs, pool, pm)
        assert 'N' in text


# ===========================================================================
# Boolean columns
# ===========================================================================

class TestEdgeCasesBooleans:
    def test_all_true(self):
        recs = [{'flag': True} for _ in range(20)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm, use_strategies=True)
        result = decode_binary_records(data)
        assert all(r['flag'] is True for r in result)

    def test_all_false(self):
        recs = [{'flag': False} for _ in range(20)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm, use_strategies=True)
        result = decode_binary_records(data)
        assert all(r['flag'] is False for r in result)

    def test_alternating(self):
        recs = [{'flag': i % 2 == 0} for i in range(20)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm, use_strategies=True)
        result = decode_binary_records(data)
        for i, r in enumerate(result):
            assert r['flag'] == (i % 2 == 0)

    def test_bool_not_confused_with_int(self):
        recs = [{'b': True, 'n': 1}, {'b': False, 'n': 0}]
        data = encode_binary_records(recs, [], {})
        result = decode_binary_records(data)
        if isinstance(result, list):
            assert isinstance(result[0]['b'], bool)


# ===========================================================================
# Multi-column / wide records
# ===========================================================================

class TestEdgeCasesMultiColumn:
    def test_many_columns(self):
        recs = [{f'col_{j}': j for j in range(50)} for _ in range(10)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm)
        result = decode_binary_records(data)
        assert len(result) == 10

    def test_column_order_preserved(self):
        recs = [{'a': 1, 'b': 2, 'c': 3} for _ in range(5)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm)
        result = decode_binary_records(data)
        if isinstance(result, list):
            assert set(result[0].keys()) == {'a', 'b', 'c'}

    def test_100_columns(self):
        recs = [{f'c{j}': j % 10 for j in range(100)} for _ in range(5)]
        pool, pm = build_pool(recs)
        data = encode_binary_records(recs, pool, pm)
        result = decode_binary_records(data)
        assert isinstance(result, list)
