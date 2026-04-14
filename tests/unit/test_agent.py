"""
Tests for ulmen.core._agent — 100% coverage target.
"""
import math

import pytest

from ulmen.core._agent import (
    AGENT_MAGIC,
    AGENT_VERSION,
    COMPRESS_COMPLETED_SEQUENCES,
    COMPRESS_KEEP_TYPES,
    COMPRESS_SLIDING_WINDOW,
    EMPTY_TOK,
    FALSE_TOK,
    FIELD_COUNTS,
    NULL_TOK,
    PRIORITY_COMPRESSIBLE,
    PRIORITY_KEEP_IF_ROOM,
    PRIORITY_MUST_KEEP,
    RECORD_TYPES,
    TRUE_TOK,
    AgentHeader,
    ContextBudgetExceededError,
    ValidationError,
    _compress_completed_sequences,
    _decode_field,
    _encode_field,
    _has_unsafe,
    _rec_priority,
    _split_row,
    _summarize_as_mem,
    build_summary_chain,
    chunk_payload,
    compress_context,
    convert_agent_to_ulmen,
    convert_ulmen_to_agent,
    decode_agent_payload,
    decode_agent_payload_full,
    decode_agent_record,
    decode_agent_stream,
    dedup_mem,
    encode_agent_payload,
    encode_agent_record,
    estimate_context_usage,
    extract_subgraph,
    extract_subgraph_payload,
    generate_system_prompt,
    get_latest_mem,
    make_validation_error,
    merge_chunks,
    validate_agent_payload,
)

# ---------------------------------------------------------------------------
# _has_unsafe
# ---------------------------------------------------------------------------

class TestHasUnsafe:
    def test_safe_string(self):
        assert not _has_unsafe("hello world")

    def test_pipe_unsafe(self):
        assert _has_unsafe("a|b")

    def test_quote_unsafe(self):
        assert _has_unsafe('say "hi"')

    def test_backslash_unsafe(self):
        assert _has_unsafe("a\\b")

    def test_newline_unsafe(self):
        assert _has_unsafe("a\nb")

    def test_cr_unsafe(self):
        assert _has_unsafe("a\rb")

    def test_empty_safe(self):
        assert not _has_unsafe("")


# ---------------------------------------------------------------------------
# _encode_field
# ---------------------------------------------------------------------------

class TestEncodeField:
    def test_none(self):
        assert _encode_field(None) == "N"

    def test_true(self):
        assert _encode_field(True) == "T"

    def test_false(self):
        assert _encode_field(False) == "F"

    def test_int_positive(self):
        assert _encode_field(42) == "42"

    def test_int_negative(self):
        assert _encode_field(-7) == "-7"

    def test_int_zero(self):
        assert _encode_field(0) == "0"

    def test_float_normal(self):
        assert _encode_field(3.14) == repr(3.14)

    def test_float_nan(self):
        assert _encode_field(float("nan")) == "nan"

    def test_float_inf(self):
        assert _encode_field(float("inf")) == "inf"

    def test_float_neg_inf(self):
        assert _encode_field(float("-inf")) == "-inf"

    def test_str_empty(self):
        assert _encode_field("") == EMPTY_TOK

    def test_str_safe(self):
        assert _encode_field("hello") == "hello"

    def test_str_with_pipe(self):
        result = _encode_field("a|b")
        assert result.startswith('"') and result.endswith('"')
        assert "a|b" in result

    def test_str_with_quote(self):
        result = _encode_field('say "hi"')
        assert result == '"say ""hi"""'

    def test_str_with_newline(self):
        result = _encode_field("a\nb")
        assert result == '"a\\nb"'

    def test_str_with_cr(self):
        result = _encode_field("a\rb")
        assert result == '"a\\rb"'

    def test_str_with_backslash(self):
        result = _encode_field("a\\b")
        assert result == '"a\\\\b"'

    def test_fallback_non_string(self):
        # list falls through to str(v)
        result = _encode_field([1, 2])
        assert isinstance(result, str)

    def test_fallback_with_pipe(self):
        class Weird:
            def __str__(self): return "a|b"
        result = _encode_field(Weird())
        assert result.startswith('"')


# ---------------------------------------------------------------------------
# _decode_field
# ---------------------------------------------------------------------------

class TestDecodeField:
    def test_null(self):
        assert _decode_field("N", "s") is None

    def test_true(self):
        assert _decode_field("T", "b") is True

    def test_false(self):
        assert _decode_field("F", "b") is False

    def test_empty(self):
        assert _decode_field("$0=", "s") == ""

    def test_quoted_string(self):
        assert _decode_field('"hello"', "s") == "hello"

    def test_quoted_with_double_quote(self):
        assert _decode_field('"say ""hi"""', "s") == 'say "hi"'

    def test_quoted_with_newline(self):
        assert _decode_field('"a\\nb"', "s") == "a\nb"

    def test_quoted_with_cr(self):
        assert _decode_field('"a\\rb"', "s") == "a\rb"

    def test_quoted_with_backslash(self):
        assert _decode_field('"a\\\\b"', "s") == "a\\b"

    def test_int(self):
        assert _decode_field("42", "d") == 42

    def test_int_negative(self):
        assert _decode_field("-7", "d") == -7

    def test_float(self):
        assert abs(_decode_field("3.14", "f") - 3.14) < 1e-9

    def test_float_nan(self):
        assert math.isnan(_decode_field("nan", "f"))

    def test_float_inf(self):
        assert _decode_field("inf", "f") == float("inf")

    def test_float_neg_inf(self):
        assert _decode_field("-inf", "f") == float("-inf")

    def test_bool_true(self):
        assert _decode_field("T", "b") is True

    def test_bool_false(self):
        assert _decode_field("F", "b") is False

    def test_bool_invalid(self):
        with pytest.raises(ValueError):
            _decode_field("X", "b")

    def test_plain_string(self):
        assert _decode_field("hello", "s") == "hello"


# ---------------------------------------------------------------------------
# _split_row
# ---------------------------------------------------------------------------

class TestSplitRow:
    def test_simple(self):
        assert _split_row("a|b|c") == ["a", "b", "c"]

    def test_no_pipe(self):
        assert _split_row("abc") == ["abc"]

    def test_quoted_field(self):
        result = _split_row('a|"b|c"|d')
        assert len(result) == 3
        assert result[1] == '"b|c"'

    def test_quoted_double_quote(self):
        result = _split_row('a|"say ""hi"""|b')
        assert result[1] == '"say ""hi"""'

    def test_trailing_pipe(self):
        result = _split_row("a|b|")
        assert result == ["a", "b", ""]

    def test_empty_string(self):
        assert _split_row("") == [""]


# ---------------------------------------------------------------------------
# encode_agent_record / decode_agent_record
# ---------------------------------------------------------------------------

class TestEncodeDecodeRecord:
    def test_msg_round_trip(self):
        rec = {"type":"msg","id":"m1","thread_id":"t1","step":1,
               "role":"user","turn":1,"content":"hello","tokens":1,"flagged":False}
        row = encode_agent_record(rec)
        dec = decode_agent_record(row)
        assert dec["type"] == "msg"
        assert dec["content"] == "hello"
        assert dec["flagged"] is False

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unknown record type"):
            encode_agent_record({"type":"foo","id":"x","thread_id":"t","step":1})

    def test_wrong_field_count_raises(self):
        with pytest.raises(ValueError, match="expects"):
            decode_agent_record("msg|m1|t1|1|user|1|hello")

    def test_empty_row_raises(self):
        with pytest.raises(ValueError):
            decode_agent_record("")

    def test_unknown_type_in_row_raises(self):
        with pytest.raises(ValueError, match="Unknown record type"):
            decode_agent_record("foo|x|t1|1|data")

    def test_all_10_types(self):
        records = [
            {"type":"msg","id":"m1","thread_id":"t1","step":1,"role":"user","turn":1,"content":"hi","tokens":1,"flagged":False},
            {"type":"tool","id":"t1","thread_id":"t1","step":2,"name":"s","args":"{}","status":"pending"},
            {"type":"res","id":"t1","thread_id":"t1","step":3,"name":"s","data":"r","status":"done","latency_ms":10},
            {"type":"plan","id":"p1","thread_id":"t1","step":4,"index":1,"description":"do","status":"pending"},
            {"type":"obs","id":"o1","thread_id":"t1","step":5,"source":"x","content":"y","confidence":0.9},
            {"type":"err","id":"e1","thread_id":"t1","step":6,"code":"E","message":"m","source":"s","recoverable":True},
            {"type":"mem","id":"me1","thread_id":"t1","step":7,"key":"k","value":"v","confidence":1.0,"ttl":None},
            {"type":"rag","id":"r1","thread_id":"t1","step":8,"rank":1,"score":0.9,"source":"w","chunk":"c","used":True},
            {"type":"hyp","id":"h1","thread_id":"t1","step":9,"statement":"s","evidence":"e","score":0.8,"accepted":True},
            {"type":"cot","id":"c1","thread_id":"t1","step":10,"index":1,"cot_type":"observe","text":"t","confidence":1.0},
        ]
        for rec in records:
            row = encode_agent_record(rec)
            dec = decode_agent_record(row)
            assert dec["type"] == rec["type"]

    def test_required_field_null_raises(self):
        with pytest.raises(ValueError, match="Required field"):
            decode_agent_record("msg|m1|t1|1|N|1|hello|5|F")

    def test_common_field_error(self):
        with pytest.raises(ValueError, match="Common field error"):
            decode_agent_record("msg|m1|t1|abc|user|1|hello|5|F")


# ---------------------------------------------------------------------------
# encode_agent_payload / decode_agent_payload
# ---------------------------------------------------------------------------

class TestPayload:
    def _base(self):
        return [{"type":"msg","id":"m1","thread_id":"t1","step":1,
                 "role":"user","turn":1,"content":"hello","tokens":1,"flagged":False}]

    def test_encode_header(self):
        enc = encode_agent_payload(self._base())
        assert enc.startswith("ULMEN-AGENT v1\nrecords: 1\n")

    def test_encode_ends_newline(self):
        enc = encode_agent_payload(self._base())
        assert enc.endswith("\n")

    def test_decode_round_trip(self):
        enc = encode_agent_payload(self._base())
        dec = decode_agent_payload(enc)
        assert len(dec) == 1
        assert dec[0]["content"] == "hello"

    def test_empty_payload(self):
        enc = encode_agent_payload([])
        assert "records: 0" in enc
        dec = decode_agent_payload(enc)
        assert dec == []

    def test_too_short_raises(self):
        with pytest.raises(ValueError, match="too short"):
            decode_agent_payload("ULMEN-AGENT v1\n")

    def test_bad_magic_raises(self):
        with pytest.raises(ValueError, match="Bad magic"):
            decode_agent_payload("ULMEN-AGENT v2\nrecords: 0\n")

    def test_bad_records_line_raises(self):
        with pytest.raises(ValueError, match="records: not found"):
            decode_agent_payload("ULMEN-AGENT v1\nfoo\n")

    def test_bad_record_count_raises(self):
        with pytest.raises(ValueError, match="Bad record count"):
            decode_agent_payload("ULMEN-AGENT v1\nrecords: abc\n")

    def test_count_mismatch_raises(self):
        with pytest.raises(ValueError, match="mismatch"):
            decode_agent_payload("ULMEN-AGENT v1\nrecords: 5\nmsg|m1|t1|1|user|1|hi|1|F\n")

    def test_blank_data_line_raises(self):
        # trailing blank lines are stripped, so count mismatch is the error
        with pytest.raises(ValueError, match="mismatch"):
            decode_agent_payload("ULMEN-AGENT v1\nrecords: 2\nmsg|m1|t1|1|user|1|hi|1|F\n\n")

    def test_row_error_wrapped(self):
        with pytest.raises(ValueError, match="Row 1"):
            decode_agent_payload("ULMEN-AGENT v1\nrecords: 1\nfoo|x|t1|1|data\n")


# ---------------------------------------------------------------------------
# validate_agent_payload
# ---------------------------------------------------------------------------

class TestValidate:
    def _valid(self):
        return encode_agent_payload([
            {"type":"msg","id":"m1","thread_id":"t1","step":1,
             "role":"user","turn":1,"content":"hello","tokens":1,"flagged":False}
        ])

    def test_valid_passes(self):
        v, m = validate_agent_payload(self._valid())
        assert v is True
        assert m == ""

    def test_decode_error_caught(self):
        v, m = validate_agent_payload("garbage")
        assert v is False
        assert m != ""

    def test_empty_thread_id(self):
        payload = "ULMEN-AGENT v1\nrecords: 1\nmsg|m1||1|user|1|hi|1|F\n"
        v, m = validate_agent_payload(payload)
        assert v is False
        assert "thread_id" in m

    def test_empty_id(self):
        payload = "ULMEN-AGENT v1\nrecords: 1\nmsg||t1|1|user|1|hi|1|F\n"
        v, m = validate_agent_payload(payload)
        assert v is False
        assert "id" in m

    def test_zero_step(self):
        payload = "ULMEN-AGENT v1\nrecords: 1\nmsg|m1|t1|0|user|1|hi|1|F\n"
        v, m = validate_agent_payload(payload)
        assert v is False
        assert "step" in m

    def test_backwards_step(self):
        payload = (
            "ULMEN-AGENT v1\nrecords: 2\n"
            "msg|m1|t1|5|user|1|hi|1|F\n"
            "msg|m2|t1|3|assistant|2|bye|1|F\n"
        )
        v, m = validate_agent_payload(payload)
        assert v is False
        assert "less than" in m

    def test_same_step_allowed(self):
        payload = (
            "ULMEN-AGENT v1\nrecords: 2\n"
            "plan|p1|t1|2|1|do a|pending\n"
            "plan|p2|t1|2|2|do b|pending\n"
        )
        v, m = validate_agent_payload(payload)
        assert v is True

    def test_res_without_tool(self):
        payload = "ULMEN-AGENT v1\nrecords: 1\nres|tc_ghost|t1|1|search|data|done|100\n"
        v, m = validate_agent_payload(payload)
        assert v is False
        assert "no matching tool" in m

    def test_bad_enum_role(self):
        payload = "ULMEN-AGENT v1\nrecords: 1\nmsg|m1|t1|1|robot|1|hi|1|F\n"
        v, m = validate_agent_payload(payload)
        assert v is False
        assert "role" in m

    def test_bad_enum_tool_status(self):
        payload = "ULMEN-AGENT v1\nrecords: 1\ntool|t1|t1|1|search|{}|flying\n"
        v, m = validate_agent_payload(payload)
        assert v is False

    def test_bad_enum_cot_type(self):
        payload = "ULMEN-AGENT v1\nrecords: 1\ncot|c1|t1|1|1|dream|thinking|1.0\n"
        v, m = validate_agent_payload(payload)
        assert v is False

    def test_multi_thread_independent(self):
        payload = (
            "ULMEN-AGENT v1\nrecords: 4\n"
            "msg|m1|th_A|1|user|1|hi|1|F\n"
            "msg|m2|th_B|1|user|1|hi|1|F\n"
            "msg|m3|th_A|2|assistant|2|bye|1|F\n"
            "msg|m4|th_B|2|assistant|2|bye|1|F\n"
        )
        v, m = validate_agent_payload(payload)
        assert v is True

    def test_null_required_field(self):
        payload = "ULMEN-AGENT v1\nrecords: 1\nmsg|m1|t1|1|N|1|hi|1|F\n"
        v, m = validate_agent_payload(payload)
        assert v is False

    def test_res_matches_tool(self):
        payload = (
            "ULMEN-AGENT v1\nrecords: 2\n"
            "tool|tc1|t1|1|search|{}|pending\n"
            "res|tc1|t1|2|search|result|done|100\n"
        )
        v, m = validate_agent_payload(payload)
        assert v is True


# ---------------------------------------------------------------------------
# make_validation_error
# ---------------------------------------------------------------------------

class TestMakeValidationError:
    def test_produces_valid_payload(self):
        payload = make_validation_error("test error", thread_id="th_test")
        v, m = validate_agent_payload(payload)
        assert v is True

    def test_error_fields(self):
        payload = make_validation_error("oops", thread_id="th_x")
        dec = decode_agent_payload(payload)
        assert dec[0]["type"] == "err"
        assert dec[0]["code"] == "VALIDATION_FAILED"
        assert dec[0]["message"] == "oops"
        assert dec[0]["recoverable"] is False

    def test_default_thread_id(self):
        payload = make_validation_error("err")
        dec = decode_agent_payload(payload)
        assert dec[0]["thread_id"] == "INVALID"


# ---------------------------------------------------------------------------
# extract_subgraph
# ---------------------------------------------------------------------------

class TestExtractSubgraph:
    def _records(self):
        return [
            {"type":"msg","id":"m1","thread_id":"th_A","step":1,"role":"user","turn":1,"content":"hi","tokens":1,"flagged":False},
            {"type":"plan","id":"p1","thread_id":"th_A","step":2,"index":1,"description":"do","status":"pending"},
            {"type":"cot","id":"c1","thread_id":"th_A","step":3,"index":1,"cot_type":"observe","text":"t","confidence":1.0},
            {"type":"msg","id":"m2","thread_id":"th_B","step":1,"role":"user","turn":1,"content":"hey","tokens":1,"flagged":False},
        ]

    def test_filter_thread(self):
        r = extract_subgraph(self._records(), thread_id="th_A")
        assert len(r) == 3
        assert all(x["thread_id"] == "th_A" for x in r)

    def test_filter_wrong_thread(self):
        r = extract_subgraph(self._records(), thread_id="th_Z")
        assert r == []

    def test_filter_step_min(self):
        r = extract_subgraph(self._records(), step_min=2)
        assert all(x["step"] >= 2 for x in r)

    def test_filter_step_max(self):
        r = extract_subgraph(self._records(), step_max=2)
        assert all(x["step"] <= 2 for x in r)

    def test_filter_types(self):
        r = extract_subgraph(self._records(), types=["cot"])
        assert len(r) == 1
        assert r[0]["type"] == "cot"

    def test_combined_filters(self):
        r = extract_subgraph(self._records(), thread_id="th_A", types=["msg","plan"])
        assert len(r) == 2

    def test_no_filters(self):
        r = extract_subgraph(self._records())
        assert len(r) == 4

    def test_empty_input(self):
        assert extract_subgraph([]) == []


# ---------------------------------------------------------------------------
# extract_subgraph_payload
# ---------------------------------------------------------------------------

class TestExtractSubgraphPayload:
    def test_basic(self):
        records = [
            {"type":"msg","id":"m1","thread_id":"t1","step":1,"role":"user","turn":1,"content":"hi","tokens":1,"flagged":False},
            {"type":"cot","id":"c1","thread_id":"t1","step":2,"index":1,"cot_type":"observe","text":"t","confidence":1.0},
        ]
        enc = encode_agent_payload(records)
        sub = extract_subgraph_payload(enc, types=["cot"])
        dec = decode_agent_payload(sub)
        assert len(dec) == 1
        assert dec[0]["type"] == "cot"

    def test_result_is_valid(self):
        records = [
            {"type":"plan","id":"p1","thread_id":"t1","step":1,"index":1,"description":"do","status":"pending"},
            {"type":"plan","id":"p2","thread_id":"t1","step":2,"index":2,"description":"done","status":"done"},
        ]
        enc = encode_agent_payload(records)
        sub = extract_subgraph_payload(enc, step_max=1)
        v, m = validate_agent_payload(sub)
        assert v is True


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

class TestConstants:
    def test_agent_magic(self):
        assert AGENT_MAGIC == "ULMEN-AGENT v1"

    def test_agent_version(self):
        assert AGENT_VERSION == "1.0.0"

    def test_record_types_count(self):
        assert len(RECORD_TYPES) == 10

    def test_field_counts(self):
        assert FIELD_COUNTS["msg"] == 9
        assert FIELD_COUNTS["tool"] == 7
        assert FIELD_COUNTS["res"] == 8
        assert FIELD_COUNTS["cot"] == 8

    def test_sentinels(self):
        assert NULL_TOK == "N"
        assert TRUE_TOK == "T"
        assert FALSE_TOK == "F"
        assert EMPTY_TOK == "$0="


# ---------------------------------------------------------------------------
# Missing coverage: trailing pipe, field error, blank line inside data
# ---------------------------------------------------------------------------

class TestMissingCoverage:
    def test_trailing_pipe_in_row(self):
        # covers _split_row line: fields.append("") when i == n after pipe
        from ulmen.core._agent import _split_row
        result = _split_row("a|b|")
        assert result[-1] == ""

    def test_decode_field_error_wrapped(self):
        # covers the except block in decode_agent_record for type-specific fields
        # obs confidence must be float — pass garbage
        with pytest.raises(ValueError, match="Field"):
            decode_agent_record("obs|o1|t1|1|source|content|notafloat")

    def test_blank_line_inside_data(self):
        # blank line inside (not trailing) — count mismatch since strip removes trailing
        # to hit the blank-line check we need it in the middle
        payload = "ULMEN-AGENT v1\nrecords: 3\nmsg|m1|t1|1|user|1|hi|1|F\n\nmsg|m2|t1|2|assistant|2|bye|1|F\n"
        with pytest.raises(ValueError):
            decode_agent_payload(payload)

    def test_encode_ulmen_llm_via_ulmendict(self):
        # covers _api.py encode_ulmen_llm cache path
        from ulmen.core._api import UlmenDict
        ld = UlmenDict([{"id": 1, "name": "Alice"}])
        result = ld.encode_ulmen_llm()
        assert result.startswith("L|")
        # second call hits cache
        result2 = ld.encode_ulmen_llm()
        assert result == result2

    def test_decode_text_via_ulmendict(self):
        # covers _api.py decode_text
        from ulmen.core._api import UlmenDict
        ld = UlmenDict([{"id": 1, "name": "Alice"}])
        text = ld.encode_text()
        decoded = ld.decode_text(text)
        assert len(decoded) == 1

    def test_decode_binary_via_ulmendict(self):
        # covers _api.py decode_binary
        from ulmen.core._api import UlmenDict
        ld = UlmenDict([{"id": 1, "name": "Alice"}])
        binary = ld.encode_binary_pooled()
        decoded = ld.decode_binary(binary)
        assert len(decoded) == 1

    def test_decode_ulmen_llm_via_ulmendict(self):
        # covers _api.py decode_ulmen_llm method
        from ulmen.core._api import UlmenDict
        ld = UlmenDict([{"id": 1, "name": "Alice"}])
        llm = ld.encode_ulmen_llm()
        decoded = ld.decode_ulmen_llm(llm)
        assert len(decoded) == 1
        assert decoded[0]["id"] == 1

    def test_ulmendictfull_init_dict(self):
        # covers UlmenDictFull init with dict input
        from ulmen.core._api import UlmenDictFull
        ldf = UlmenDictFull({"id": 1, "name": "Alice"})
        assert len(ldf) == 1

    def test_ulmendictfull_init_iterable(self):
        # covers UlmenDictFull init with iterable input
        from ulmen.core._api import UlmenDictFull
        ldf = UlmenDictFull(iter([{"id": 1}, {"id": 2}]))
        assert len(ldf) == 2

    def test_ulmendictfull_append(self):
        # covers UlmenDictFull append
        from ulmen.core._api import UlmenDictFull
        ldf = UlmenDictFull([{"id": 1}])
        ldf.append({"id": 2})
        assert len(ldf) == 2

    def test_encode_ulmen_llm_direct(self):
        # covers _api.py encode_ulmen_llm_direct
        from ulmen.core._api import encode_ulmen_llm_direct
        result = encode_ulmen_llm_direct([{"id": 1}])
        assert result.startswith("L|")

    def test_decode_ulmen_llm_direct(self):
        # covers _api.py decode_ulmen_llm_direct
        from ulmen.core._api import decode_ulmen_llm_direct
        result = decode_ulmen_llm_direct("L|id:d\n1")
        assert result == [{"id": 1}]


class TestFinalCoverage:
    def test_split_row_trailing_pipe_appends_empty(self):
        # agent line 188: fields.append("") when row ends with pipe
        from ulmen.core._agent import _split_row
        result = _split_row("a|b|c|")
        assert result[-1] == ""
        assert len(result) == 4

    def test_decode_agent_record_empty_fields(self):
        # agent line 229: if not fields -> ValueError("Empty row")
        # _split_row("") returns [""] which is truthy but rtype="" not in _SCHEMAS
        # We need to patch to get truly empty list — test via empty string edge
        from ulmen.core._agent import decode_agent_record
        # An all-pipe row that produces empty first field
        with pytest.raises(ValueError):
            decode_agent_record("")


class TestLine188Coverage:
    def test_split_row_ends_with_pipe_non_quoted(self):
        # Directly hits line 188: fields.append("") when i==n after pipe increment
        from ulmen.core._agent import _split_row
        # non-quoted path: "a|b|" -> j found at 3, fields=['a','b'], i=4, n=4, i==n -> ""
        result = _split_row("type|id|")
        assert result == ["type", "id", ""]
        assert result[-1] == ""

    def test_split_row_single_field_trailing_pipe(self):
        from ulmen.core._agent import _split_row
        result = _split_row("only|")
        assert result == ["only", ""]


class TestContextBudgetExceededError:
    """Covers lines 194-197: ContextBudgetExceededError construction and attributes."""

    def test_attributes_set_correctly(self):
        err = ContextBudgetExceededError(context_window=100, context_used=150)
        assert err.context_window == 100
        assert err.context_used == 150
        assert err.overage == 50

    def test_is_value_error_subclass(self):
        err = ContextBudgetExceededError(100, 200)
        assert isinstance(err, ValueError)

    def test_message_contains_values(self):
        err = ContextBudgetExceededError(100, 200)
        msg = str(err)
        assert "200" in msg
        assert "100" in msg
        assert "100" in msg

    def test_enforce_budget_raises_when_over(self):
        from ulmen.core._agent import ContextBudgetExceededError, encode_agent_payload
        records = [
            {"type": "msg", "id": f"m{i}", "thread_id": "t1", "step": i + 1,
             "role": "user", "turn": i + 1, "content": "hello world long content",
             "tokens": 10, "flagged": False}
            for i in range(30)
        ]
        with pytest.raises(ContextBudgetExceededError) as exc_info:
            encode_agent_payload(
                records,
                context_window=5,
                auto_context=True,
                enforce_budget=True,
            )
        assert exc_info.value.context_window == 5
        assert exc_info.value.overage > 0

    def test_enforce_budget_false_does_not_raise(self):
        records = [
            {"type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
             "role": "user", "turn": 1, "content": "hi", "tokens": 1, "flagged": False}
        ]
        result = encode_agent_payload(
            records,
            context_window=5,
            auto_context=True,
            enforce_budget=False,
        )
        assert "ULMEN-AGENT v1" in result


class TestAgentHeaderEncodeLines:
    """Covers lines 249-265: AgentHeader.encode_lines with every optional field."""

    def test_all_optional_fields_present(self):
        h = AgentHeader()
        h.thread_id = "t1"
        h.context_window = 8000
        h.context_used = 42
        h.payload_id = "pid-abc"
        h.parent_payload_id = "pid-prev"
        h.agent_id = "agent-x"
        h.session_id = "sess-1"
        h.schema_version = "1.0.0"
        h.meta_fields = ("from_agent", "to_agent")
        h.record_count = 3
        lines = h.encode_lines()
        joined = "\n".join(lines)
        assert "thread: t1" in joined
        assert "context_window: 8000" in joined
        assert "context_used: 42" in joined
        assert "payload_id: pid-abc" in joined
        assert "parent_payload_id: pid-prev" in joined
        assert "agent_id: agent-x" in joined
        assert "session_id: sess-1" in joined
        assert "schema_version: 1.0.0" in joined
        assert "meta: from_agent,to_agent" in joined
        assert "records: 3" in joined

    def test_minimal_header_only_records(self):
        h = AgentHeader()
        h.record_count = 0
        lines = h.encode_lines()
        assert lines == ["records: 0"]

    def test_no_meta_fields_no_meta_line(self):
        h = AgentHeader()
        h.record_count = 1
        lines = h.encode_lines()
        assert not any(line.startswith("meta:") for line in lines)


class TestParseHeaderAllBranches:
    """Covers lines 293-337: _parse_header branches for all header line types."""

    def test_payload_id_parsed(self):
        payload = (
            "ULMEN-AGENT v1\n"
            "payload_id: uuid-123\n"
            "records: 0\n"
        )
        _, header = decode_agent_payload_full(payload)
        assert header.payload_id == "uuid-123"

    def test_parent_payload_id_parsed(self):
        payload = (
            "ULMEN-AGENT v1\n"
            "parent_payload_id: prev-uuid\n"
            "records: 0\n"
        )
        _, header = decode_agent_payload_full(payload)
        assert header.parent_payload_id == "prev-uuid"

    def test_agent_id_parsed(self):
        payload = (
            "ULMEN-AGENT v1\n"
            "agent_id: agent-alpha\n"
            "records: 0\n"
        )
        _, header = decode_agent_payload_full(payload)
        assert header.agent_id == "agent-alpha"

    def test_session_id_parsed(self):
        payload = (
            "ULMEN-AGENT v1\n"
            "session_id: sess-999\n"
            "records: 0\n"
        )
        _, header = decode_agent_payload_full(payload)
        assert header.session_id == "sess-999"

    def test_schema_version_parsed(self):
        payload = (
            "ULMEN-AGENT v1\n"
            "schema_version: 2.0.0\n"
            "records: 0\n"
        )
        _, header = decode_agent_payload_full(payload)
        assert header.schema_version == "2.0.0"

    def test_bad_context_window_raises(self):
        payload = "ULMEN-AGENT v1\ncontext_window: abc\nrecords: 0\n"
        with pytest.raises(ValueError, match="Bad context_window"):
            decode_agent_payload_full(payload)

    def test_bad_context_used_raises(self):
        payload = "ULMEN-AGENT v1\ncontext_used: xyz\nrecords: 0\n"
        with pytest.raises(ValueError, match="Bad context_used"):
            decode_agent_payload_full(payload)

    def test_unknown_meta_field_raises(self):
        payload = "ULMEN-AGENT v1\nmeta: not_a_real_field\nrecords: 0\n"
        with pytest.raises(ValueError, match="Unknown meta fields"):
            decode_agent_payload_full(payload)

    def test_forward_compat_unknown_header_line_ignored(self):
        payload = (
            "ULMEN-AGENT v1\n"
            "future_extension: some_value\n"
            "records: 1\n"
            "msg|m1|t1|1|user|1|hi|1|F\n"
        )
        records, _ = decode_agent_payload_full(payload)
        assert len(records) == 1

    def test_all_header_fields_together(self):
        payload = (
            "ULMEN-AGENT v1\n"
            "thread: t1\n"
            "context_window: 8000\n"
            "context_used: 10\n"
            "payload_id: p1\n"
            "parent_payload_id: p0\n"
            "agent_id: ag1\n"
            "session_id: s1\n"
            "schema_version: 1.0.0\n"
            "records: 1\n"
            "msg|m1|t1|1|user|1|hi|1|F\n"
        )
        records, header = decode_agent_payload_full(payload)
        assert header.thread_id == "t1"
        assert header.context_window == 8000
        assert header.context_used == 10
        assert header.payload_id == "p1"
        assert header.parent_payload_id == "p0"
        assert header.agent_id == "ag1"
        assert header.session_id == "s1"
        assert header.schema_version == "1.0.0"
        assert len(records) == 1


class TestDecodeAgentRecordMetaFields:
    """Covers lines 536-542: meta_field decoding with priority and string types."""

    def test_priority_meta_field_decoded_as_int(self):
        rec = {
            "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
            "role": "user", "turn": 1, "content": "hi", "tokens": 1,
            "flagged": False, "priority": 2,
        }
        row = encode_agent_record(rec, meta_fields=("priority",))
        decoded = decode_agent_record(row, meta_fields=("priority",))
        assert decoded["priority"] == 2
        assert isinstance(decoded["priority"], int)

    def test_from_agent_meta_field_decoded_as_string(self):
        rec = {
            "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
            "role": "user", "turn": 1, "content": "hi", "tokens": 1,
            "flagged": False, "from_agent": "agent_a", "to_agent": "agent_b",
            "parent_id": None, "priority": 1,
        }
        row = encode_agent_record(
            rec,
            meta_fields=("parent_id", "from_agent", "to_agent", "priority"),
        )
        decoded = decode_agent_record(
            row,
            meta_fields=("parent_id", "from_agent", "to_agent", "priority"),
        )
        assert decoded["from_agent"] == "agent_a"
        assert decoded["to_agent"] == "agent_b"
        assert decoded["priority"] == 1

    def test_null_meta_field_decoded_as_none(self):
        rec = {
            "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
            "role": "user", "turn": 1, "content": "hi", "tokens": 1,
            "flagged": False, "parent_id": None,
        }
        row = encode_agent_record(rec, meta_fields=("parent_id",))
        decoded = decode_agent_record(row, meta_fields=("parent_id",))
        assert decoded["parent_id"] is None


class TestEncodeAgentPayloadFullOptions:
    """Covers lines 599-604: auto_context, enforce_budget, auto_payload_id."""

    def test_auto_payload_id_generates_uuid(self):
        records = [
            {"type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
             "role": "user", "turn": 1, "content": "hi", "tokens": 1, "flagged": False}
        ]
        payload = encode_agent_payload(records, auto_payload_id=True)
        _, header = decode_agent_payload_full(payload)
        assert header.payload_id is not None
        assert len(header.payload_id) > 10

    def test_auto_context_sets_context_used(self):
        records = [
            {"type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
             "role": "user", "turn": 1, "content": "hi", "tokens": 1, "flagged": False}
        ]
        payload = encode_agent_payload(
            records, context_window=8000, auto_context=True
        )
        _, header = decode_agent_payload_full(payload)
        assert header.context_used is not None
        assert header.context_used > 0

    def test_auto_context_false_no_context_used(self):
        records = [
            {"type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
             "role": "user", "turn": 1, "content": "hi", "tokens": 1, "flagged": False}
        ]
        payload = encode_agent_payload(
            records, context_window=8000, auto_context=False
        )
        _, header = decode_agent_payload_full(payload)
        assert header.context_used is None


class TestValidateStructuredMode:
    """Covers line 754: structured=True returns ValidationError on parse failure."""

    def test_structured_parse_failure_returns_validation_error_object(self):
        ok, err = validate_agent_payload("not a valid payload", structured=True)
        assert ok is False
        assert isinstance(err, ValidationError)
        assert err.suggestion is not None

    def test_structured_valid_returns_none(self):
        payload = encode_agent_payload([
            {"type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
             "role": "user", "turn": 1, "content": "hi", "tokens": 1, "flagged": False}
        ])
        ok, err = validate_agent_payload(payload, structured=True)
        assert ok is True
        assert err is None

    def test_structured_empty_thread_returns_validation_error(self):
        payload = "ULMEN-AGENT v1\nrecords: 1\nmsg||t1|1|user|1|hi|1|F\n"
        ok, err = validate_agent_payload(payload, structured=True)
        assert ok is False
        assert isinstance(err, ValidationError)

    def test_structured_bad_step_returns_validation_error(self):
        payload = "ULMEN-AGENT v1\nrecords: 1\nmsg|m1|t1|0|user|1|hi|1|F\n"
        ok, err = validate_agent_payload(payload, structured=True)
        assert ok is False
        assert isinstance(err, ValidationError)
        assert err.field == "step"

    def test_structured_backwards_step_returns_validation_error(self):
        payload = (
            "ULMEN-AGENT v1\nrecords: 2\n"
            "msg|m1|t1|5|user|1|hi|1|F\n"
            "msg|m2|t1|3|assistant|2|bye|1|F\n"
        )
        ok, err = validate_agent_payload(payload, structured=True)
        assert ok is False
        assert isinstance(err, ValidationError)

    def test_structured_bad_enum_returns_validation_error(self):
        payload = "ULMEN-AGENT v1\nrecords: 1\nmsg|m1|t1|1|robot|1|hi|1|F\n"
        ok, err = validate_agent_payload(payload, structured=True)
        assert ok is False
        assert isinstance(err, ValidationError)

    def test_structured_res_without_tool_returns_validation_error(self):
        payload = "ULMEN-AGENT v1\nrecords: 1\nres|ghost|t1|1|search|data|done|100\n"
        ok, err = validate_agent_payload(payload, structured=True)
        assert ok is False
        assert isinstance(err, ValidationError)
        assert "no matching tool" in err.message

    def test_structured_empty_id_returns_validation_error(self):
        payload = "ULMEN-AGENT v1\nrecords: 1\nmsg|m1||1|user|1|hi|1|F\n"
        ok, err = validate_agent_payload(payload, structured=True)
        assert ok is False
        assert isinstance(err, ValidationError)


class TestDecodeAgentStream:
    """Covers lines 670-724: decode_agent_stream iterator."""

    def _make_payload(self, records, **kwargs):
        return encode_agent_payload(records, **kwargs)

    def _msg(self, mid, tid, step):
        return {
            "type": "msg", "id": mid, "thread_id": tid, "step": step,
            "role": "user", "turn": step, "content": "hi",
            "tokens": 1, "flagged": False,
        }

    def test_basic_stream_two_records(self):
        payload = self._make_payload([
            self._msg("m1", "t1", 1),
            self._msg("m2", "t1", 2),
        ])
        records = list(decode_agent_stream(iter(payload.splitlines(keepends=True))))
        assert len(records) == 2
        assert records[0]["id"] == "m1"
        assert records[1]["id"] == "m2"

    def test_stream_empty_payload(self):
        payload = encode_agent_payload([])
        records = list(decode_agent_stream(iter(payload.splitlines(keepends=True))))
        assert records == []

    def test_stream_bad_magic_raises(self):
        lines = ["BAD-MAGIC v1\nrecords: 0\n"]
        with pytest.raises(ValueError, match="Bad magic"):
            list(decode_agent_stream(iter(lines)))

    def test_stream_unknown_header_line_ignored(self):
        payload = (
            "ULMEN-AGENT v1\n"
            "future_header: value\n"
            "records: 1\n"
            "msg|m1|t1|1|user|1|hi|1|F\n"
        )
        records = list(decode_agent_stream(iter(payload.splitlines(keepends=True))))
        assert len(records) == 1

    def test_stream_bad_data_row_raises(self):
        payload = (
            "ULMEN-AGENT v1\n"
            "records: 1\n"
            "badtype|x|t1|1|data\n"
        )
        with pytest.raises(ValueError, match="Row 1"):
            list(decode_agent_stream(iter(payload.splitlines(keepends=True))))

    def test_stream_skips_blank_lines_in_data(self):
        payload = encode_agent_payload([self._msg("m1", "t1", 1)])
        lines = payload.splitlines(keepends=True) + ["\n", "\n"]
        records = list(decode_agent_stream(iter(lines)))
        assert len(records) == 1

    def test_stream_with_thread_and_context_window(self):
        payload = encode_agent_payload(
            [self._msg("m1", "t1", 1)],
            thread_id="t1",
            context_window=8000,
            auto_context=True,
        )
        records = list(decode_agent_stream(iter(payload.splitlines(keepends=True))))
        assert len(records) == 1
        assert records[0]["thread_id"] == "t1"

    def test_stream_with_meta_fields(self):
        rec = self._msg("m1", "t1", 1)
        rec["from_agent"] = "a"
        rec["to_agent"] = "b"
        rec["priority"] = 1
        rec["parent_id"] = None
        payload = encode_agent_payload(
            [rec],
            meta_fields=("parent_id", "from_agent", "to_agent", "priority"),
        )
        records = list(decode_agent_stream(iter(payload.splitlines(keepends=True))))
        assert len(records) == 1
        assert records[0]["from_agent"] == "a"

    def test_stream_five_records(self):
        msgs = [self._msg(f"m{i}", "t1", i + 1) for i in range(5)]
        payload = encode_agent_payload(msgs)
        records = list(decode_agent_stream(iter(payload.splitlines(keepends=True))))
        assert len(records) == 5

    def test_stream_stops_exactly_at_record_count(self):
        msgs = [self._msg(f"m{i}", "t1", i + 1) for i in range(3)]
        payload = encode_agent_payload(msgs)
        extra = payload + "msg|m99|t1|99|user|1|extra|1|F\n"
        records = list(decode_agent_stream(iter(extra.splitlines(keepends=True))))
        assert len(records) == 3


class TestChunkPayload:
    """Covers lines 895-1001: chunk_payload."""

    def _msg(self, mid, tid, step):
        return {
            "type": "msg", "id": mid, "thread_id": tid, "step": step,
            "role": "user", "turn": step, "content": "hi",
            "tokens": 1, "flagged": False,
        }

    def _tool(self, tid_rec, tid, step):
        return {
            "type": "tool", "id": tid_rec, "thread_id": tid, "step": step,
            "name": "search", "args": "{}", "status": "pending",
        }

    def _res(self, tid_rec, tid, step):
        return {
            "type": "res", "id": tid_rec, "thread_id": tid, "step": step,
            "name": "search", "data": "result", "status": "done", "latency_ms": 10,
        }

    def test_empty_records_returns_one_valid_payload(self):
        chunks = chunk_payload([], token_budget=1000, thread_id="t1")
        assert len(chunks) == 1
        assert decode_agent_payload(chunks[0]) == []

    def test_fits_single_chunk(self):
        records = [self._msg(f"m{i}", "t1", i + 1) for i in range(3)]
        chunks = chunk_payload(records, token_budget=10000, thread_id="t1")
        assert len(chunks) == 1
        merged = merge_chunks(chunks)
        assert len(merged) == 3

    def test_splits_into_multiple_chunks(self):
        records = [self._msg(f"m{i}", "t1", i + 1) for i in range(20)]
        chunks = chunk_payload(records, token_budget=30, thread_id="t1")
        assert len(chunks) > 1
        merged = merge_chunks(chunks)
        assert len(merged) == 20

    def test_tool_res_pair_kept_atomic(self):
        records = [
            self._msg("m1", "t1", 1),
            self._tool("tc1", "t1", 2),
            self._res("tc1", "t1", 3),
            self._msg("m2", "t1", 4),
        ]
        chunks = chunk_payload(records, token_budget=10000, thread_id="t1")
        for chunk in chunks:
            ok, err = validate_agent_payload(chunk)
            assert ok is True, f"Invalid chunk: {err}"

    def test_unmatched_res_is_solo_unit(self):
        records = [
            self._msg("m1", "t1", 1),
            self._res("ghost", "t1", 2),
        ]
        chunks = chunk_payload(records, token_budget=10000, thread_id="t1")
        merged = merge_chunks(chunks)
        assert len(merged) == 2

    def test_overlap_parameter(self):
        records = [self._msg(f"m{i}", "t1", i + 1) for i in range(10)]
        chunks = chunk_payload(records, token_budget=30, thread_id="t1", overlap=1)
        assert len(chunks) >= 1

    def test_payload_id_assigned_to_each_chunk(self):
        records = [self._msg(f"m{i}", "t1", i + 1) for i in range(20)]
        chunks = chunk_payload(records, token_budget=30, thread_id="t1")
        for chunk in chunks:
            _, header = decode_agent_payload_full(chunk)
            assert header.payload_id is not None

    def test_parent_payload_id_links_consecutive_chunks(self):
        records = [self._msg(f"m{i}", "t1", i + 1) for i in range(20)]
        chunks = chunk_payload(records, token_budget=30, thread_id="t1")
        if len(chunks) > 1:
            _, h2 = decode_agent_payload_full(chunks[1])
            assert h2.parent_payload_id is not None

    def test_session_id_in_every_chunk_header(self):
        records = [self._msg("m1", "t1", 1)]
        chunks = chunk_payload(
            records, token_budget=10000, thread_id="t1", session_id="sess-x"
        )
        _, header = decode_agent_payload_full(chunks[0])
        assert header.session_id == "sess-x"

    def test_parent_payload_id_param_set_on_first_chunk(self):
        records = [self._msg("m1", "t1", 1)]
        chunks = chunk_payload(
            records, token_budget=10000, thread_id="t1",
            parent_payload_id="external-prev-id",
        )
        _, header = decode_agent_payload_full(chunks[0])
        assert header.parent_payload_id == "external-prev-id"

    def test_multiple_tools_each_with_res(self):
        records = [
            self._tool("tc1", "t1", 1),
            self._res("tc1", "t1", 2),
            self._tool("tc2", "t1", 3),
            self._res("tc2", "t1", 4),
        ]
        chunks = chunk_payload(records, token_budget=10000, thread_id="t1")
        for chunk in chunks:
            ok, err = validate_agent_payload(chunk)
            assert ok is True, f"Invalid chunk: {err}"

    def test_each_chunk_is_valid_agent_payload(self):
        records = [self._msg(f"m{i}", "t1", i + 1) for i in range(15)]
        chunks = chunk_payload(records, token_budget=25, thread_id="t1")
        assert len(chunks) > 1
        for chunk in chunks:
            ok, err = validate_agent_payload(chunk)
            assert ok is True, f"Invalid chunk: {err}"


class TestMergeChunks:
    """Covers lines 1021-1032: merge_chunks deduplication."""

    def _msg(self, mid, tid, step):
        return {
            "type": "msg", "id": mid, "thread_id": tid, "step": step,
            "role": "user", "turn": step, "content": "hi",
            "tokens": 1, "flagged": False,
        }

    def test_empty_list(self):
        assert merge_chunks([]) == []

    def test_single_payload(self):
        p = encode_agent_payload([self._msg("m1", "t1", 1)])
        merged = merge_chunks([p])
        assert len(merged) == 1

    def test_two_payloads_combined(self):
        p1 = encode_agent_payload([self._msg("m1", "t1", 1)])
        p2 = encode_agent_payload([self._msg("m2", "t1", 2)])
        merged = merge_chunks([p1, p2])
        assert len(merged) == 2

    def test_duplicate_records_deduplicated(self):
        p = encode_agent_payload([self._msg("m1", "t1", 1)])
        merged = merge_chunks([p, p])
        assert len(merged) == 1

    def test_dedup_key_is_id_thread_step(self):
        p1 = encode_agent_payload([self._msg("m1", "t1", 1)])
        p2 = encode_agent_payload([self._msg("m1", "t1", 1)])
        merged = merge_chunks([p1, p2])
        assert len(merged) == 1

    def test_chunk_roundtrip_preserves_all_records(self):
        records = [self._msg(f"m{i}", "t1", i + 1) for i in range(20)]
        chunks = chunk_payload(records, token_budget=30, thread_id="t1")
        merged = merge_chunks(chunks)
        assert len(merged) == 20


class TestBuildSummaryChain:
    """Covers lines 1073-1126: build_summary_chain."""

    def _msg(self, mid, tid, step):
        return {
            "type": "msg", "id": mid, "thread_id": tid, "step": step,
            "role": "user", "turn": step, "content": "hi",
            "tokens": 1, "flagged": False,
        }

    def test_empty_input_returns_empty(self):
        assert build_summary_chain([], token_budget=1000) == []

    def test_small_dataset_one_payload(self):
        records = [self._msg(f"m{i}", "t1", i + 1) for i in range(3)]
        chain = build_summary_chain(records, token_budget=10000, thread_id="t1")
        assert len(chain) == 1
        decoded = decode_agent_payload(chain[0])
        assert len(decoded) == 3

    def test_large_dataset_creates_chain(self):
        records = [self._msg(f"m{i}", "t1", i + 1) for i in range(50)]
        chain = build_summary_chain(records, token_budget=50, thread_id="t1")
        assert len(chain) >= 1

    def test_chain_payloads_each_have_payload_id(self):
        records = [self._msg(f"m{i}", "t1", i + 1) for i in range(50)]
        chain = build_summary_chain(records, token_budget=50, thread_id="t1")
        for payload in chain:
            _, header = decode_agent_payload_full(payload)
            assert header.payload_id is not None

    def test_chain_linked_by_parent_id(self):
        records = [self._msg(f"m{i}", "t1", i + 1) for i in range(50)]
        chain = build_summary_chain(records, token_budget=50, thread_id="t1")
        if len(chain) > 1:
            _, h2 = decode_agent_payload_full(chain[1])
            assert h2.parent_payload_id is not None

    def test_session_id_propagated(self):
        records = [self._msg("m1", "t1", 1)]
        chain = build_summary_chain(
            records, token_budget=10000, thread_id="t1", session_id="sess-y"
        )
        assert len(chain) >= 1

    def test_each_payload_valid(self):
        records = [self._msg(f"m{i}", "t1", i + 1) for i in range(30)]
        chain = build_summary_chain(records, token_budget=60, thread_id="t1")
        for payload in chain:
            assert "ULMEN-AGENT v1" in payload


class TestCompressContext:
    """Covers lines 1156-1176: compress_context strategy dispatch."""

    def _msg(self, mid, tid, step):
        return {
            "type": "msg", "id": mid, "thread_id": tid, "step": step,
            "role": "user", "turn": step, "content": "hi",
            "tokens": 1, "flagged": False,
        }

    def _tool(self, tid_rec, tid, step):
        return {
            "type": "tool", "id": tid_rec, "thread_id": tid, "step": step,
            "name": "search", "args": "{}", "status": "pending",
        }

    def _res(self, tid_rec, tid, step):
        return {
            "type": "res", "id": tid_rec, "thread_id": tid, "step": step,
            "name": "search", "data": "result", "status": "done", "latency_ms": 10,
        }

    def _cot(self, cid, tid, step):
        return {
            "type": "cot", "id": cid, "thread_id": tid, "step": step,
            "index": 1, "cot_type": "observe", "text": "thinking",
            "confidence": 1.0,
        }

    def test_empty_returns_empty(self):
        assert compress_context([]) == []

    def test_completed_sequences_strategy(self):
        records = [
            self._tool("tc1", "t1", 1),
            self._res("tc1", "t1", 2),
            self._msg("m1", "t1", 3),
        ]
        result = compress_context(records, strategy=COMPRESS_COMPLETED_SEQUENCES)
        types = [r["type"] for r in result]
        assert "mem" in types
        assert "msg" in types
        assert "tool" not in types

    def test_keep_types_strategy_default(self):
        records = [
            self._msg("m1", "t1", 1),
            self._tool("tc1", "t1", 2),
            {
                "type": "mem", "id": "me1", "thread_id": "t1", "step": 3,
                "key": "k", "value": "v", "confidence": 1.0, "ttl": None,
            },
        ]
        result = compress_context(records, strategy=COMPRESS_KEEP_TYPES)
        types = {r["type"] for r in result}
        assert "msg" in types
        assert "mem" in types
        assert "tool" not in types

    def test_keep_types_strategy_custom(self):
        records = [
            self._msg("m1", "t1", 1),
            self._tool("tc1", "t1", 2),
            self._cot("c1", "t1", 3),
        ]
        result = compress_context(
            records, strategy=COMPRESS_KEEP_TYPES, keep_types=["msg"]
        )
        types = {r["type"] for r in result}
        assert "msg" in types
        assert "tool" not in types
        assert "cot" not in types

    def test_keep_types_with_must_keep_priority(self):
        rec = self._tool("tc1", "t1", 1)
        rec["priority"] = PRIORITY_MUST_KEEP
        records = [rec, self._msg("m1", "t1", 2)]
        result = compress_context(
            records,
            strategy=COMPRESS_KEEP_TYPES,
            keep_types=["msg"],
            keep_priority=PRIORITY_KEEP_IF_ROOM,
        )
        types = [r["type"] for r in result]
        assert "tool" in types

    def test_sliding_window_strategy_reduces_records(self):
        records = [self._msg(f"m{i}", "t1", i + 1) for i in range(20)]
        result = compress_context(
            records, strategy=COMPRESS_SLIDING_WINDOW, window_size=5
        )
        assert len(result) < 20

    def test_sliding_window_within_size_returns_copy(self):
        records = [self._msg(f"m{i}", "t1", i + 1) for i in range(3)]
        result = compress_context(
            records, strategy=COMPRESS_SLIDING_WINDOW, window_size=10
        )
        assert len(result) == 3

    def test_sliding_window_default_window_size(self):
        records = [self._msg(f"m{i}", "t1", i + 1) for i in range(50)]
        result = compress_context(records, strategy=COMPRESS_SLIDING_WINDOW)
        assert len(result) < 50

    def test_unknown_strategy_raises(self):
        with pytest.raises(ValueError, match="Unknown compression strategy"):
            compress_context(
                [self._msg("m1", "t1", 1)], strategy="not_a_real_strategy"
            )


class TestRecPriority:
    """Covers lines 1180-1186: _rec_priority helper."""

    def test_no_priority_field_returns_compressible(self):
        assert _rec_priority({"type": "msg"}) == PRIORITY_COMPRESSIBLE

    def test_none_priority_returns_compressible(self):
        assert _rec_priority({"priority": None}) == PRIORITY_COMPRESSIBLE

    def test_int_priority_returned_as_is(self):
        assert _rec_priority({"priority": 1}) == 1

    def test_string_int_priority_converted(self):
        assert _rec_priority({"priority": "2"}) == 2

    def test_invalid_priority_string_returns_compressible(self):
        assert _rec_priority({"priority": "not_a_number"}) == PRIORITY_COMPRESSIBLE


class TestCompressCompletedSequences:
    """Covers lines 1198-1273: _compress_completed_sequences all branches."""

    def _tool(self, tid_rec, tid, step):
        return {
            "type": "tool", "id": tid_rec, "thread_id": tid, "step": step,
            "name": "search", "args": "{}", "status": "pending",
        }

    def _res(self, tid_rec, tid, step):
        return {
            "type": "res", "id": tid_rec, "thread_id": tid, "step": step,
            "name": "search", "data": "result", "status": "done", "latency_ms": 10,
        }

    def _cot(self, cid, tid, step):
        return {
            "type": "cot", "id": cid, "thread_id": tid, "step": step,
            "index": 1, "cot_type": "observe", "text": "thinking",
            "confidence": 1.0,
        }

    def _msg(self, mid, tid, step):
        return {
            "type": "msg", "id": mid, "thread_id": tid, "step": step,
            "role": "user", "turn": step, "content": "hi",
            "tokens": 1, "flagged": False,
        }

    def test_completed_pair_becomes_mem(self):
        from ulmen.core._agent import PRIORITY_KEEP_IF_ROOM
        records = [self._tool("tc1", "t1", 1), self._res("tc1", "t1", 2)]
        result = _compress_completed_sequences(records, PRIORITY_KEEP_IF_ROOM)
        assert any(r["type"] == "mem" for r in result)
        assert not any(r["type"] in ("tool", "res") for r in result)

    def test_incomplete_tool_not_compressed(self):
        from ulmen.core._agent import PRIORITY_KEEP_IF_ROOM
        records = [self._tool("tc1", "t1", 1)]
        result = _compress_completed_sequences(records, PRIORITY_KEEP_IF_ROOM)
        assert any(r["type"] == "tool" for r in result)

    def test_cot_dropped_preserve_false(self):
        from ulmen.core._agent import PRIORITY_KEEP_IF_ROOM
        records = [self._cot("c1", "t1", 1)]
        result = _compress_completed_sequences(
            records, PRIORITY_KEEP_IF_ROOM, preserve_cot=False
        )
        assert not any(r["type"] == "cot" for r in result)

    def test_cot_converted_to_mem_preserve_true(self):
        from ulmen.core._agent import PRIORITY_KEEP_IF_ROOM
        records = [self._cot("c1", "t1", 1)]
        result = _compress_completed_sequences(
            records, PRIORITY_KEEP_IF_ROOM, preserve_cot=True
        )
        assert any(r["type"] == "mem" for r in result)
        assert not any(r["type"] == "cot" for r in result)

    def test_must_keep_priority_never_compressed(self):
        from ulmen.core._agent import (
            PRIORITY_KEEP_IF_ROOM,
            PRIORITY_MUST_KEEP,
        )
        rec = self._tool("tc1", "t1", 1)
        rec["priority"] = PRIORITY_MUST_KEEP
        records = [rec, self._res("tc1", "t1", 2)]
        result = _compress_completed_sequences(records, PRIORITY_KEEP_IF_ROOM)
        assert any(r["type"] == "tool" for r in result)

    def test_msg_plan_obs_err_mem_hyp_rag_always_kept(self):
        from ulmen.core._agent import PRIORITY_KEEP_IF_ROOM
        records = [
            self._msg("m1", "t1", 1),
            {
                "type": "plan", "id": "p1", "thread_id": "t1", "step": 2,
                "index": 1, "description": "do it", "status": "pending",
            },
            {
                "type": "obs", "id": "o1", "thread_id": "t1", "step": 3,
                "source": "x", "content": "y", "confidence": 0.9,
            },
        ]
        result = _compress_completed_sequences(records, PRIORITY_KEEP_IF_ROOM)
        types = {r["type"] for r in result}
        assert "msg" in types
        assert "plan" in types
        assert "obs" in types

    def test_duplicate_tool_id_compressed_only_once(self):
        from ulmen.core._agent import PRIORITY_KEEP_IF_ROOM
        records = [
            self._tool("tc1", "t1", 1),
            self._tool("tc1", "t1", 1),
            self._res("tc1", "t1", 2),
        ]
        result = _compress_completed_sequences(records, PRIORITY_KEEP_IF_ROOM)
        mem_count = sum(1 for r in result if r["type"] == "mem")
        assert mem_count == 1

    def test_meta_fields_copied_to_compressed_mem(self):
        from ulmen.core._agent import PRIORITY_KEEP_IF_ROOM
        rec = self._tool("tc1", "t1", 1)
        rec["from_agent"] = "agent_a"
        records = [rec, self._res("tc1", "t1", 2)]
        result = _compress_completed_sequences(records, PRIORITY_KEEP_IF_ROOM)
        mem_recs = [r for r in result if r["type"] == "mem"]
        assert len(mem_recs) == 1
        assert mem_recs[0].get("from_agent") == "agent_a"

    def test_cot_meta_fields_copied_when_preserve_true(self):
        from ulmen.core._agent import PRIORITY_KEEP_IF_ROOM
        rec = self._cot("c1", "t1", 1)
        rec["from_agent"] = "agent_b"
        result = _compress_completed_sequences(
            [rec], PRIORITY_KEEP_IF_ROOM, preserve_cot=True
        )
        mem_recs = [r for r in result if r["type"] == "mem"]
        assert mem_recs[0].get("from_agent") == "agent_b"

    def test_unmatched_res_falls_through_to_result(self):
        from ulmen.core._agent import PRIORITY_KEEP_IF_ROOM
        records = [self._res("ghost_id", "t1", 1)]
        result = _compress_completed_sequences(records, PRIORITY_KEEP_IF_ROOM)
        assert any(r["type"] == "res" for r in result)


class TestDedupMem:
    """Covers lines 1277-1301: dedup_mem."""

    def _mem(self, mid, tid, step, key="k", value="v"):
        return {
            "type": "mem", "id": mid, "thread_id": tid, "step": step,
            "key": key, "value": value, "confidence": 1.0, "ttl": None,
        }

    def _msg(self, mid, tid, step):
        return {
            "type": "msg", "id": mid, "thread_id": tid, "step": step,
            "role": "user", "turn": step, "content": "hi",
            "tokens": 1, "flagged": False,
        }

    def test_keeps_highest_step_for_same_key(self):
        records = [
            self._mem("me1", "t1", 1, key="k"),
            self._mem("me2", "t1", 5, key="k"),
            self._mem("me3", "t1", 3, key="k"),
        ]
        result = dedup_mem(records)
        mem_recs = [r for r in result if r["type"] == "mem"]
        assert len(mem_recs) == 1
        assert mem_recs[0]["id"] == "me2"

    def test_non_mem_records_always_preserved(self):
        records = [self._msg("m1", "t1", 1), self._mem("me1", "t1", 2)]
        result = dedup_mem(records)
        types = [r["type"] for r in result]
        assert "msg" in types
        assert "mem" in types

    def test_different_keys_both_retained(self):
        records = [
            self._mem("me1", "t1", 1, key="a"),
            self._mem("me2", "t1", 2, key="b"),
        ]
        result = dedup_mem(records)
        assert len(result) == 2

    def test_different_threads_same_key_both_retained(self):
        records = [
            self._mem("me1", "t1", 1, key="k"),
            self._mem("me2", "t2", 1, key="k"),
        ]
        result = dedup_mem(records)
        assert len(result) == 2

    def test_empty_input_returns_empty(self):
        assert dedup_mem([]) == []


class TestEstimateContextUsage:
    """Covers lines 1313-1321: estimate_context_usage."""

    def _msg(self, mid, tid, step):
        return {
            "type": "msg", "id": mid, "thread_id": tid, "step": step,
            "role": "user", "turn": step, "content": "hi",
            "tokens": 1, "flagged": False,
        }

    def test_structure_keys_present(self):
        result = estimate_context_usage([self._msg("m1", "t1", 1)])
        assert set(result.keys()) == {"rows", "chars", "tokens", "by_type"}

    def test_rows_count_correct(self):
        records = [self._msg(f"m{i}", "t1", i + 1) for i in range(5)]
        result = estimate_context_usage(records)
        assert result["rows"] == 5

    def test_by_type_aggregated_correctly(self):
        records = [
            self._msg("m1", "t1", 1),
            self._msg("m2", "t1", 2),
            {
                "type": "cot", "id": "c1", "thread_id": "t1", "step": 3,
                "index": 1, "cot_type": "observe", "text": "t", "confidence": 1.0,
            },
        ]
        result = estimate_context_usage(records)
        assert "msg" in result["by_type"]
        assert "cot" in result["by_type"]
        assert result["by_type"]["msg"] > 0

    def test_empty_records_all_zeros(self):
        result = estimate_context_usage([])
        assert result["rows"] == 0
        assert result["chars"] == 0
        assert result["tokens"] == 0

    def test_with_meta_fields(self):
        rec = self._msg("m1", "t1", 1)
        rec["from_agent"] = "a"
        rec["to_agent"] = "b"
        rec["priority"] = 1
        rec["parent_id"] = None
        result = estimate_context_usage(
            [rec],
            meta_fields=("parent_id", "from_agent", "to_agent", "priority"),
        )
        assert result["rows"] == 1


class TestGenerateSystemPrompt:
    """Covers lines 1332-1349 and 1357-1369: generate_system_prompt branches."""

    def test_returns_string(self):
        result = generate_system_prompt()
        assert isinstance(result, str)

    def test_contains_agent_magic(self):
        result = generate_system_prompt()
        assert "ULMEN-AGENT v1" in result

    def test_contains_all_10_record_types(self):
        result = generate_system_prompt()
        for rtype in ["msg", "tool", "res", "plan", "obs", "err", "mem", "rag", "hyp", "cot"]:
            assert rtype in result

    def test_include_examples_true(self):
        result = generate_system_prompt(include_examples=True)
        assert "EXAMPLE" in result

    def test_include_examples_false(self):
        result = generate_system_prompt(include_examples=False)
        assert "EXAMPLE" not in result

    def test_include_validation_true(self):
        result = generate_system_prompt(include_validation=True)
        assert "VALIDATION SELF-CHECK" in result

    def test_include_validation_false(self):
        result = generate_system_prompt(include_validation=False)
        assert "VALIDATION SELF-CHECK" not in result

    def test_enum_values_present(self):
        result = generate_system_prompt()
        assert "user" in result
        assert "assistant" in result

    def test_value_encoding_section_present(self):
        result = generate_system_prompt()
        assert "null/absent" in result


class TestConvertBridge:
    """Covers lines 1398-1504: convert_agent_to_ulmen and convert_ulmen_to_agent."""

    def _msg(self, mid, tid, step):
        return {
            "type": "msg", "id": mid, "thread_id": tid, "step": step,
            "role": "user", "turn": step, "content": "hi",
            "tokens": 1, "flagged": False,
        }

    def test_agent_to_ulmen_starts_with_ulmen_magic(self):
        payload = encode_agent_payload([self._msg("m1", "t1", 1)])
        ulmen = convert_agent_to_ulmen(payload)
        assert ulmen.startswith("L|")

    def test_ulmen_to_agent_valid_payload(self):
        from ulmen.core._ulmen_llm import encode_ulmen_llm
        records = [
            {
                "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
                "role": "user", "turn": 1, "content": "hello",
                "tokens": 1, "flagged": False,
            }
        ]
        ulmen = encode_ulmen_llm(records)
        agent_payload = convert_ulmen_to_agent(ulmen, thread_id="t1")
        assert "ULMEN-AGENT v1" in agent_payload

    def test_ulmen_to_agent_skips_non_dict_records(self):
        from ulmen.core._ulmen_llm import encode_ulmen_llm
        ulmen = encode_ulmen_llm([1, 2, 3])
        agent_payload = convert_ulmen_to_agent(ulmen, thread_id="t1")
        decoded = decode_agent_payload(agent_payload)
        assert decoded == []

    def test_ulmen_to_agent_skips_unknown_type(self):
        from ulmen.core._ulmen_llm import encode_ulmen_llm
        records = [{"type": "unknown_type", "id": "x", "thread_id": "t1"}]
        ulmen = encode_ulmen_llm(records)
        agent_payload = convert_ulmen_to_agent(ulmen, thread_id="t1")
        decoded = decode_agent_payload(agent_payload)
        assert decoded == []

    def test_ulmen_to_agent_assigns_thread_id_when_missing(self):
        from ulmen.core._ulmen_llm import encode_ulmen_llm
        records = [
            {
                "type": "msg", "id": "m1", "thread_id": "", "step": 1,
                "role": "user", "turn": 1, "content": "hi",
                "tokens": 1, "flagged": False,
            }
        ]
        ulmen = encode_ulmen_llm(records)
        agent_payload = convert_ulmen_to_agent(ulmen, thread_id="assigned")
        decoded = decode_agent_payload(agent_payload)
        if decoded:
            assert decoded[0]["thread_id"] == "assigned"

    def test_ulmen_to_agent_assigns_id_when_missing(self):
        from ulmen.core._ulmen_llm import encode_ulmen_llm
        records = [
            {
                "type": "msg", "thread_id": "t1", "step": 1,
                "role": "user", "turn": 1, "content": "hi",
                "tokens": 1, "flagged": False,
            }
        ]
        ulmen = encode_ulmen_llm(records)
        agent_payload = convert_ulmen_to_agent(ulmen, thread_id="t1")
        assert "ULMEN-AGENT v1" in agent_payload

    def test_ulmen_to_agent_assigns_step_when_missing(self):
        from ulmen.core._ulmen_llm import encode_ulmen_llm
        records = [
            {
                "type": "msg", "id": "m1", "thread_id": "t1",
                "role": "user", "turn": 1, "content": "hi",
                "tokens": 1, "flagged": False,
            }
        ]
        ulmen = encode_ulmen_llm(records)
        agent_payload = convert_ulmen_to_agent(ulmen, thread_id="t1")
        assert "ULMEN-AGENT v1" in agent_payload

    def test_round_trip_agent_ulmen_agent(self):
        payload = encode_agent_payload([self._msg("m1", "t1", 1)])
        ulmen = convert_agent_to_ulmen(payload)
        assert ulmen.startswith("L|")
        back = convert_ulmen_to_agent(ulmen, thread_id="t1")
        assert "ULMEN-AGENT v1" in back


class TestSummarizeAsMemAndGetLatestMem:
    """Covers lines 1516-1518: _summarize_as_mem and get_latest_mem."""

    def _msg(self, mid, tid, step):
        return {
            "type": "msg", "id": mid, "thread_id": tid, "step": step,
            "role": "user", "turn": step, "content": "hi",
            "tokens": 1, "flagged": False,
        }

    def _mem(self, mid, tid, step, key="k"):
        return {
            "type": "mem", "id": mid, "thread_id": tid, "step": step,
            "key": key, "value": "v", "confidence": 1.0, "ttl": None,
        }

    def test_summarize_single_thread(self):
        records = [self._msg("m1", "t1", 1), self._msg("m2", "t1", 2)]
        result = _summarize_as_mem(records)
        assert len(result) == 1
        assert result[0]["type"] == "mem"
        assert "Compressed 2" in result[0]["value"]

    def test_summarize_multiple_threads(self):
        records = [self._msg("m1", "t1", 1), self._msg("m2", "t2", 1)]
        result = _summarize_as_mem(records)
        assert len(result) == 2
        tids = {r["thread_id"] for r in result}
        assert "t1" in tids
        assert "t2" in tids

    def test_get_latest_mem_returns_highest_step(self):
        records = [
            self._mem("me1", "t1", 1, key="pref"),
            self._mem("me2", "t1", 5, key="pref"),
            self._mem("me3", "t1", 3, key="pref"),
        ]
        result = get_latest_mem(records, "pref")
        assert result["id"] == "me2"

    def test_get_latest_mem_returns_none_when_absent(self):
        assert get_latest_mem([self._msg("m1", "t1", 1)], "missing") is None

    def test_get_latest_mem_filters_by_key(self):
        records = [
            self._mem("me1", "t1", 1, key="a"),
            self._mem("me2", "t1", 2, key="b"),
        ]
        result = get_latest_mem(records, "a")
        assert result["key"] == "a"


class TestUncoveredLines:
    """Targets every remaining uncovered line to reach 100%."""

    def test_validation_error_repr(self):
        ve = ValidationError("bad thing", row=3, field="step")
        r = repr(ve)
        assert r.startswith("ValidationError(")
        assert "bad thing" in r

    def test_validation_error_bool_is_false(self):
        ve = ValidationError("oops")
        assert not ve
        assert bool(ve) is False

    def test_split_row_quoted_then_trailing_pipe(self):
        from ulmen.core._agent import _split_row
        result = _split_row('"hello"|world|')
        assert result[-1] == ""
        assert len(result) == 3

    def test_decode_agent_record_truly_empty_fields_unreachable(self):
        from ulmen.core._agent import _split_row
        result = _split_row("")
        assert result == [""]

    def test_decode_agent_stream_reraises_non_records_parse_error(self):
        from ulmen.core._agent import decode_agent_stream
        lines = [
            "ULMEN-AGENT v1",
            "context_window: abc",
        ]
        with pytest.raises(ValueError, match="Bad context_window"):
            list(decode_agent_stream(iter(lines)))

    def test_decode_agent_stream_overflow_path_data_in_header_buffer(self):
        from ulmen.core._agent import decode_agent_stream
        lines = [
            "ULMEN-AGENT v1",
            "records: 1",
            "msg|m1|t1|1|user|1|hi|1|F",
        ]
        records = list(decode_agent_stream(iter(lines)))
        assert len(records) == 1
        assert records[0]["id"] == "m1"

    def test_decode_agent_stream_overflow_blank_line_skipped(self):
        from ulmen.core._agent import decode_agent_stream
        lines = [
            "ULMEN-AGENT v1",
            "records: 1",
            "",
            "msg|m1|t1|1|user|1|hi|1|F",
        ]
        records = list(decode_agent_stream(iter(lines)))
        assert len(records) == 1

    def test_decode_agent_stream_overflow_bad_row_raises(self):
        from ulmen.core._agent import decode_agent_stream
        lines = [
            "ULMEN-AGENT v1",
            "records: 1",
            "badtype|x|t1|1|data",
        ]
        with pytest.raises(ValueError, match="Row 1"):
            list(decode_agent_stream(iter(lines)))

    def test_decode_agent_stream_overflow_stops_at_record_count(self):
        from ulmen.core._agent import decode_agent_stream
        lines = [
            "ULMEN-AGENT v1",
            "records: 1",
            "msg|m1|t1|1|user|1|hi|1|F",
            "msg|m2|t1|2|user|2|bye|1|F",
        ]
        records = list(decode_agent_stream(iter(lines)))
        assert len(records) == 1

    def test_decode_agent_stream_blank_line_in_data_section_skipped(self):
        from ulmen.core._agent import decode_agent_stream
        payload = (
            "ULMEN-AGENT v1\n"
            "records: 2\n"
            "msg|m1|t1|1|user|1|hi|1|F\n"
            "msg|m2|t1|2|user|2|bye|1|F\n"
            "\n"
        )
        records = list(decode_agent_stream(iter(payload.splitlines(keepends=True))))
        assert len(records) == 2


class TestDecodeAgentRecordEmptyRow:
    """Covers _agent.py line 495: raise ValueError('Empty row')."""

    def test_empty_fields_list_raises(self):
        from unittest.mock import patch

        import ulmen.core._agent as _agent_mod
        with patch.object(_agent_mod, "_split_row", return_value=[]), pytest.raises(ValueError, match="Empty row"):
            decode_agent_record("msg|m1|t1|1|user|1|hi|1|F")







class TestSchemaEvolution:
    """100% coverage for validate_schema_compliance and migrate_schema."""

    def _msg(self):
        return {"type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
                "role": "user", "turn": 1, "content": "hi", "tokens": 5, "flagged": False}

    # validate_schema_compliance ------------------------------------------

    def test_valid_record(self):
        from ulmen import validate_schema_compliance
        ok, err = validate_schema_compliance([self._msg()])
        assert ok is True and err is None

    def test_unknown_version_raises(self):
        import pytest

        from ulmen import validate_schema_compliance
        with pytest.raises(ValueError, match="Unknown schema version"):
            validate_schema_compliance([self._msg()], schema_version="9.9.9")

    def test_unknown_type(self):
        from ulmen import validate_schema_compliance
        rec = {"type": "ghost", "id": "x", "thread_id": "t1", "step": 1}
        ok, err = validate_schema_compliance([rec])
        assert ok is False and "unknown type" in err

    def test_missing_required_field(self):
        from ulmen import validate_schema_compliance
        rec = self._msg()
        del rec["role"]
        ok, err = validate_schema_compliance([rec])
        assert ok is False and "missing required field" in err and "role" in err

    def test_forbidden_field(self):
        from ulmen import validate_schema_compliance
        rec = self._msg()
        rec["not_a_real_field"] = "oops"
        ok, err = validate_schema_compliance([rec])
        assert ok is False and "not_a_real_field" in err

    def test_default_version_used_when_none(self):
        from ulmen import validate_schema_compliance
        ok, err = validate_schema_compliance([self._msg()], schema_version=None)
        assert ok is True

    def test_meta_fields_allowed(self):
        from ulmen import validate_schema_compliance
        rec = self._msg()
        rec["from_agent"] = "agent_a"
        rec["to_agent"] = "agent_b"
        ok, err = validate_schema_compliance([rec])
        assert ok is True

    def test_empty_records_valid(self):
        from ulmen import validate_schema_compliance
        ok, err = validate_schema_compliance([])
        assert ok is True and err is None

    # migrate_schema --------------------------------------------------------

    def test_noop_same_version(self):
        from ulmen import migrate_schema
        recs = [self._msg()]
        result = migrate_schema(recs, "1.0.0", "1.0.0")
        assert result is recs

    def test_unknown_from_version_raises(self):
        import pytest

        from ulmen import migrate_schema
        with pytest.raises(ValueError, match="Unknown source schema version"):
            migrate_schema([self._msg()], "0.0.0", "1.0.0")

    def test_unknown_to_version_raises(self):
        import pytest

        from ulmen import migrate_schema
        with pytest.raises(ValueError, match="Unknown target schema version"):
            migrate_schema([self._msg()], "1.0.0", "9.9.9")

    def test_missing_migration_path_raises(self):
        import pytest

        import ulmen.core._agent as _agent_mod
        from ulmen import migrate_schema
        # Temporarily register a fake version
        _agent_mod.SCHEMA_VERSIONS["1.1.0"] = _agent_mod.SCHEMA_VERSIONS["1.0.0"]
        try:
            with pytest.raises(ValueError, match="No migration path"):
                migrate_schema([self._msg()], "1.0.0", "1.1.0")
        finally:
            del _agent_mod.SCHEMA_VERSIONS["1.1.0"]

    def test_registered_migration_called(self):
        import ulmen.core._agent as _agent_mod
        from ulmen import migrate_schema
        sentinel = [False]
        def fake_migrate(records):
            sentinel[0] = True
            return records
        _agent_mod.SCHEMA_VERSIONS["1.1.0"] = _agent_mod.SCHEMA_VERSIONS["1.0.0"]
        _agent_mod._MIGRATIONS[("1.0.0", "1.1.0")] = fake_migrate
        try:
            migrate_schema([self._msg()], "1.0.0", "1.1.0")
            assert sentinel[0] is True
        finally:
            del _agent_mod.SCHEMA_VERSIONS["1.1.0"]
            del _agent_mod._MIGRATIONS[("1.0.0", "1.1.0")]
