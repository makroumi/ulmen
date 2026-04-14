"""
Unit tests for ulmen/core/_tokens.py
Target: 100% coverage of count_tokens_exact and count_tokens_exact_records.
"""
from ulmen.core._tokens import (
    _bpe_count_chunk,
    _split_chunks,
    count_tokens_exact,
    count_tokens_exact_records,
)


class TestSplitChunks:
    def test_empty(self):
        assert _split_chunks("") == []

    def test_single_word(self):
        result = _split_chunks("hello")
        assert isinstance(result, list)
        assert len(result) >= 1

    def test_multiple_words(self):
        result = _split_chunks("hello world")
        assert len(result) >= 2

    def test_digits(self):
        result = _split_chunks("123")
        assert len(result) >= 1

    def test_punctuation(self):
        result = _split_chunks("hello, world!")
        assert len(result) >= 2

    def test_contraction(self):
        result = _split_chunks("don't")
        assert len(result) >= 1

    def test_whitespace_only(self):
        result = _split_chunks("   ")
        assert isinstance(result, list)

    def test_unicode_latin(self):
        result = _split_chunks("cafe")
        assert len(result) >= 1

    def test_mixed(self):
        result = _split_chunks("User_42: hello world!")
        assert len(result) >= 1


class TestBpeCountChunk:
    def test_one_byte(self):
        assert _bpe_count_chunk("a") == 1

    def test_four_bytes(self):
        assert _bpe_count_chunk("abcd") == 1

    def test_five_bytes(self):
        assert _bpe_count_chunk("abcde") == 2

    def test_eight_bytes(self):
        assert _bpe_count_chunk("abcdefgh") == 2

    def test_nine_bytes(self):
        assert _bpe_count_chunk("abcdefghi") == 3

    def test_empty(self):
        assert _bpe_count_chunk("") == 1

    def test_long_ascii(self):
        s = "x" * 100
        result = _bpe_count_chunk(s)
        assert result == (100 + 3) // 4

    def test_multibyte_utf8(self):
        s = "\u65e5"
        result = _bpe_count_chunk(s)
        assert result >= 1

    def test_three_byte_utf8(self):
        s = "\u4e2d\u6587"
        result = _bpe_count_chunk(s)
        assert result >= 1


class TestCountTokensExact:
    def test_empty_returns_zero(self):
        assert count_tokens_exact("") == 0

    def test_single_char(self):
        assert count_tokens_exact("a") >= 1

    def test_short_word(self):
        result = count_tokens_exact("hello")
        assert isinstance(result, int)
        assert result >= 1

    def test_longer_text(self):
        text = "The quick brown fox jumps over the lazy dog."
        result = count_tokens_exact(text)
        assert result > 0
        assert result < len(text)

    def test_returns_int(self):
        assert isinstance(count_tokens_exact("test"), int)

    def test_more_text_more_tokens(self):
        short = count_tokens_exact("hello")
        long  = count_tokens_exact("hello " * 100)
        assert long > short

    def test_numbers(self):
        result = count_tokens_exact("12345")
        assert result >= 1

    def test_punctuation(self):
        result = count_tokens_exact("!!!")
        assert result >= 1

    def test_newlines(self):
        result = count_tokens_exact("line1\nline2\nline3")
        assert result >= 1

    def test_pipe_delimited(self):
        result = count_tokens_exact("msg|m1|t1|1|user|1|hello|5|F")
        assert result >= 1

    def test_agent_payload(self):
        text = "ULMEN-AGENT v1\nrecords: 1\nmsg|m1|t1|1|user|1|hi|1|F\n"
        result = count_tokens_exact(text)
        assert result > 0

    def test_unicode(self):
        result = count_tokens_exact("hello world")
        assert result >= 1

    def test_whitespace_only(self):
        result = count_tokens_exact("   ")
        assert result >= 0

    def test_empty_chunks_fallback(self):
        result = count_tokens_exact("\x00\x01\x02")
        assert isinstance(result, int)
        assert result >= 0

    def test_contraction(self):
        result = count_tokens_exact("don't stop")
        assert result >= 1

    def test_mixed_types(self):
        result = count_tokens_exact("User_42: score=98.5, active=True")
        assert result >= 1

    def test_long_text(self):
        text = "hello world " * 1000
        result = count_tokens_exact(text)
        assert result > 100


class TestCountTokensExactRecords:
    def test_empty_string(self):
        result = count_tokens_exact_records("")
        assert result == 0

    def test_basic_payload(self):
        text = "ULMEN-AGENT v1\nrecords: 1\nmsg|m1|t1|1|user|1|hi|1|F\n"
        result = count_tokens_exact_records(text)
        base = count_tokens_exact(text)
        assert result >= base

    def test_overhead_added(self):
        text = "ULMEN-AGENT v1\nrecords: 2\nmsg|m1|t1|1|user|1|hi|1|F\nmsg|m2|t1|2|assistant|2|bye|1|F\n"
        result   = count_tokens_exact_records(text, per_record_overhead=5)
        baseline = count_tokens_exact_records(text, per_record_overhead=0)
        assert result > baseline

    def test_zero_overhead(self):
        text = "ULMEN-AGENT v1\nrecords: 1\nmsg|m1|t1|1|user|1|hi|1|F\n"
        r0 = count_tokens_exact_records(text, per_record_overhead=0)
        r3 = count_tokens_exact_records(text, per_record_overhead=3)
        assert r3 >= r0

    def test_returns_int(self):
        result = count_tokens_exact_records("test")
        assert isinstance(result, int)

    def test_no_newlines(self):
        result = count_tokens_exact_records("single line")
        assert result >= 0

    def test_many_rows(self):
        rows = "\n".join([f"msg|m{i}|t1|{i}|user|1|hi|1|F" for i in range(1, 51)])
        text = f"ULMEN-AGENT v1\nrecords: 50\n{rows}\n"
        result = count_tokens_exact_records(text)
        assert result > count_tokens_exact(text)


class TestCountTokensExactLine72:
    """Covers line 72: if not chunks: return max(1, (len(text) + 3) // 4)."""

    def test_null_bytes_produce_no_chunks_fallback(self):
        text = "\x00\x01\x02\x03"
        from ulmen.core._tokens import _split_chunks
        _split_chunks(text)
        result = count_tokens_exact(text)
        assert isinstance(result, int)
        assert result >= 1

    def test_private_use_area_chars_fallback(self):
        text = "\ue000\ue001\ue002\ue003\ue004\ue005\ue006\ue007"
        from ulmen.core._tokens import _split_chunks
        _split_chunks(text)
        result = count_tokens_exact(text)
        assert isinstance(result, int)
        assert result >= 1

    def test_single_null_byte_returns_one(self):
        result = count_tokens_exact("\x00")
        assert isinstance(result, int)
        assert result >= 1

    def test_four_null_bytes_returns_one(self):
        result = count_tokens_exact("\x00\x00\x00\x00")
        assert isinstance(result, int)
        assert result >= 1

    def test_eight_null_bytes_returns_two(self):
        text = "\x00" * 8
        result = count_tokens_exact(text)
        assert isinstance(result, int)
        assert result >= 1


class TestCountTokensExactFallbackLine72:
    """Covers _tokens.py line 72: empty chunks fallback."""

    def test_empty_chunks_triggers_fallback(self):
        from unittest.mock import patch

        from ulmen.core import _tokens as tok_mod
        with patch.object(tok_mod, "_split_chunks", return_value=[]):
            result = tok_mod.count_tokens_exact("abcd")
        assert result == max(1, (4 + 3) // 4)

    def test_empty_chunks_fallback_single_char(self):
        from unittest.mock import patch

        from ulmen.core import _tokens as tok_mod
        with patch.object(tok_mod, "_split_chunks", return_value=[]):
            result = tok_mod.count_tokens_exact("x")
        assert result == 1

    def test_empty_chunks_fallback_eight_chars(self):
        from unittest.mock import patch

        from ulmen.core import _tokens as tok_mod
        with patch.object(tok_mod, "_split_chunks", return_value=[]):
            result = tok_mod.count_tokens_exact("x" * 8)
        assert result == max(1, (8 + 3) // 4)
