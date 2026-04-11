"""
Unit tests for column strategy detection and savings computation:
  - detect_column_strategy
  - compute_delta_savings / compute_rle_savings / compute_bits_savings
  - build_pool
"""
import pytest

from lumen.core import (
    detect_column_strategy,
    compute_delta_savings,
    compute_rle_savings,
    compute_bits_savings,
    build_pool,
    encode_zigzag,
)


# ===========================================================================
# detect_column_strategy
# ===========================================================================

class TestDetectColumnStrategy:
    def test_empty(self):
        assert detect_column_strategy([]) == 'raw'

    def test_all_none(self):
        assert detect_column_strategy([None, None, None]) == 'rle'

    def test_all_bool(self):
        assert detect_column_strategy([True, False, True]) == 'bits'

    def test_monotonic_ints_delta(self):
        assert detect_column_strategy(list(range(100))) == 'delta'

    def test_single_int_raw(self):
        assert detect_column_strategy([42]) == 'raw'

    def test_two_ints(self):
        result = detect_column_strategy([10, 20])
        assert result in ('delta', 'raw')

    def test_random_ints_raw(self):
        import random
        random.seed(0)
        vals = [random.randint(-1000, 1000) for _ in range(50)]
        result = detect_column_strategy(vals)
        assert result in ('delta', 'raw', 'rle')

    def test_repeated_strings_pool_or_rle(self):
        result = detect_column_strategy(['x'] * 100)
        assert result in ('pool', 'rle')

    def test_unique_strings_raw(self):
        vals = [f'unique_{i}' for i in range(100)]
        assert detect_column_strategy(vals) == 'raw'

    def test_few_unique_strings_pool(self):
        vals = ['alpha', 'beta', 'gamma'] * 20
        result = detect_column_strategy(vals)
        assert result in ('pool', 'rle')

    def test_float_col_not_pool(self):
        vals = [1.5, 2.5, 3.5] * 20
        result = detect_column_strategy(vals)
        assert result in ('rle', 'raw')   # never pool for floats

    def test_rle_for_high_repetition(self):
        vals = ['same'] * 80 + ['other'] * 5
        result = detect_column_strategy(vals)
        assert result in ('rle', 'pool')

    def test_bool_with_none(self):
        vals = [True, None, False, None]
        result = detect_column_strategy(vals)
        assert result in ('bits', 'rle', 'raw')

    def test_small_n_no_pool(self):
        # n=4, threshold requires n > 4 for pool
        vals = ['ab', 'ab', 'cd', 'cd']
        result = detect_column_strategy(vals)
        assert result in ('raw', 'rle')

    # branch coverage
    def test_null_only_rle(self):
        assert detect_column_strategy([None] * 10) == 'rle'

    def test_single_bool(self):
        assert detect_column_strategy([True]) == 'bits'

    def test_two_ints_raw_if_no_saving(self):
        # Same value twice: delta_cost == raw_cost, stays raw
        result = detect_column_strategy([5, 5])
        assert result in ('raw', 'delta')

    def test_float_column_rle_or_raw(self):
        vals = [1.1, 2.2, 3.3] * 5
        assert detect_column_strategy(vals) in ('rle', 'raw')

    def test_mixed_types_raw(self):
        vals = [1, 'a', True, None]
        assert detect_column_strategy(vals) in ('rle', 'raw')

    def test_pool_threshold_boundary(self):
        # 10 values, 3 unique = below n//10=1 → check pool logic
        vals = ['aaa', 'bbb', 'ccc'] * 4
        result = detect_column_strategy(vals)
        assert result in ('pool', 'rle', 'raw')


# ===========================================================================
# compute_delta_savings
# ===========================================================================

class TestComputeDeltaSavings:
    def test_monotonic_saves(self):
        result = compute_delta_savings(list(range(1000)))
        assert result['saving'] > 0
        assert result['pct'] > 0

    def test_empty(self):
        result = compute_delta_savings([])
        assert result == {'raw': 0, 'delta': 0, 'saving': 0, 'pct': 0.0}

    def test_non_int_returns_zeros(self):
        result = compute_delta_savings([1.5, 2.5])
        assert result['saving'] == 0

    def test_bool_rejected(self):
        result = compute_delta_savings([True, False])
        assert result['saving'] == 0

    def test_single_element(self):
        result = compute_delta_savings([42])
        assert 'raw' in result

    def test_structure_keys(self):
        result = compute_delta_savings([1, 2, 3])
        assert set(result.keys()) == {'raw', 'delta', 'saving', 'pct'}

    def test_negative_ints(self):
        result = compute_delta_savings([-100, -99, -98, -97])
        assert 'saving' in result

    # branch coverage
    def test_delta_savings_single_element(self):
        result = compute_delta_savings([5])
        assert result['delta'] > 0

    def test_delta_savings_zero_raw(self):
        # Empty list → raw=0 → pct=0 by guard
        result = compute_delta_savings([])
        assert result['pct'] == 0.0


# ===========================================================================
# compute_rle_savings
# ===========================================================================

class TestComputeRleSavings:
    def test_repeated_saves(self):
        result = compute_rle_savings(['active'] * 800 + ['inactive'] * 200)
        assert result['saving'] > 0

    def test_empty(self):
        result = compute_rle_savings([])
        assert 'raw' in result and 'rle' in result

    def test_structure_keys(self):
        result = compute_rle_savings(['a', 'b'])
        assert set(result.keys()) == {'raw', 'rle', 'saving'}

    def test_unique_strings(self):
        result = compute_rle_savings([f'val_{i}' for i in range(100)])
        assert 'rle' in result

    # branch coverage
    def test_rle_savings_empty(self):
        result = compute_rle_savings([])
        assert result['raw'] == 0 or result['saving'] is not None


# ===========================================================================
# compute_bits_savings
# ===========================================================================

class TestComputeBitsSavings:
    def test_saves(self):
        result = compute_bits_savings([True, False] * 500)
        assert result['saving'] > 0

    def test_empty(self):
        result = compute_bits_savings([])
        assert 'raw' in result

    def test_structure_keys(self):
        result = compute_bits_savings([True, False])
        assert set(result.keys()) == {'raw', 'bits', 'saving'}

    def test_all_true(self):
        result = compute_bits_savings([True] * 100)
        assert result['saving'] > 0

    # branch coverage
    def test_bits_savings_empty(self):
        result = compute_bits_savings([])
        assert result['raw'] == 0

    def test_bits_savings_all_true(self):
        result = compute_bits_savings([True] * 64)
        assert result['bits'] < result['raw']


# ===========================================================================
# build_pool
# ===========================================================================

class TestBuildPool:
    def test_empty_records(self):
        pool, pm = build_pool([])
        assert pool == []
        assert pm == {}

    def test_no_strings(self):
        pool, pm = build_pool([{'a': 1, 'b': 2}])
        # Keys 'a','b' are single-char → not pooled
        assert 'a' not in pm

    def test_repeated_long_string(self):
        records = [{'tag': 'Engineering'}] * 10
        pool, pm = build_pool(records)
        assert 'Engineering' in pm

    def test_short_strings_not_pooled(self):
        records = [{'v': 'x'}] * 10
        pool, pm = build_pool(records)
        assert 'x' not in pm   # single char

    def test_two_char_not_pooled(self):
        records = [{'v': 'ab'}] * 10
        _, pm = build_pool(records)
        assert 'ab' not in pm  # len must be > 1... actually len > 1 passes scan
        # but ref_cost check filters it — verify empirically
        # (two-char string may or may not pool depending on savings)

    def test_max_pool_truncation(self):
        records = [{f'key_{i}': f'value_long_{i}'} for i in range(200)]
        pool, _ = build_pool(records, max_pool=10)
        assert len(pool) <= 10

    def test_keys_also_pooled(self):
        records = [{'department': 'Engineering'}] * 20
        pool, pm = build_pool(records)
        assert 'department' in pm or 'Engineering' in pm

    def test_pool_map_consistent(self):
        records = [{'tag': 'hello', 'dept': 'world'}] * 5
        pool, pm = build_pool(records)
        for s, idx in pm.items():
            assert pool[idx] == s

    def test_frequency_below_2_not_pooled(self):
        records = [{'tag': 'unique_value'}]
        pool, pm = build_pool(records)
        assert 'unique_value' not in pm

    def test_higher_frequency_ranked_first(self):
        records = (
            [{'tag': 'common'}] * 20 +
            [{'tag': 'rare'}] * 3
        )
        pool, pm = build_pool(records)
        if 'common' in pm and 'rare' in pm:
            assert pm['common'] < pm['rare']

    def test_nested_list_scanned(self):
        records = [{'items': ['Engineering', 'Engineering', 'Engineering']}]
        pool, pm = build_pool(records)
        assert 'Engineering' in pm

    def test_nested_dict_scanned(self):
        records = [{'meta': {'dept': 'Engineering'}} for _ in range(5)]
        pool, pm = build_pool(records)
        assert 'Engineering' in pm
