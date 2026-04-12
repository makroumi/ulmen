"""
Tests for lumen.core._agent — 100% coverage target.
"""
import math

import pytest

from lumen.core._agent import (
    AGENT_MAGIC,
    AGENT_VERSION,
    EMPTY_TOK,
    FALSE_TOK,
    FIELD_COUNTS,
    NULL_TOK,
    RECORD_TYPES,
    TRUE_TOK,
    _decode_field,
    _encode_field,
    _has_unsafe,
    _split_row,
    decode_agent_payload,
    decode_agent_record,
    encode_agent_payload,
    encode_agent_record,
    extract_subgraph,
    extract_subgraph_payload,
    make_validation_error,
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
        assert enc.startswith("LUMEN-AGENT v1\nrecords: 1\n")

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
            decode_agent_payload("LUMEN-AGENT v1\n")

    def test_bad_magic_raises(self):
        with pytest.raises(ValueError, match="Bad magic"):
            decode_agent_payload("LUMEN-AGENT v2\nrecords: 0\n")

    def test_bad_records_line_raises(self):
        with pytest.raises(ValueError, match="Bad records line"):
            decode_agent_payload("LUMEN-AGENT v1\nfoo\n")

    def test_bad_record_count_raises(self):
        with pytest.raises(ValueError, match="Bad record count"):
            decode_agent_payload("LUMEN-AGENT v1\nrecords: abc\n")

    def test_count_mismatch_raises(self):
        with pytest.raises(ValueError, match="mismatch"):
            decode_agent_payload("LUMEN-AGENT v1\nrecords: 5\nmsg|m1|t1|1|user|1|hi|1|F\n")

    def test_blank_data_line_raises(self):
        # trailing blank lines are stripped, so count mismatch is the error
        with pytest.raises(ValueError, match="mismatch"):
            decode_agent_payload("LUMEN-AGENT v1\nrecords: 2\nmsg|m1|t1|1|user|1|hi|1|F\n\n")

    def test_row_error_wrapped(self):
        with pytest.raises(ValueError, match="Row 1"):
            decode_agent_payload("LUMEN-AGENT v1\nrecords: 1\nfoo|x|t1|1|data\n")


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
        payload = "LUMEN-AGENT v1\nrecords: 1\nmsg|m1||1|user|1|hi|1|F\n"
        v, m = validate_agent_payload(payload)
        assert v is False
        assert "thread_id" in m

    def test_empty_id(self):
        payload = "LUMEN-AGENT v1\nrecords: 1\nmsg||t1|1|user|1|hi|1|F\n"
        v, m = validate_agent_payload(payload)
        assert v is False
        assert "id" in m

    def test_zero_step(self):
        payload = "LUMEN-AGENT v1\nrecords: 1\nmsg|m1|t1|0|user|1|hi|1|F\n"
        v, m = validate_agent_payload(payload)
        assert v is False
        assert "step" in m

    def test_backwards_step(self):
        payload = (
            "LUMEN-AGENT v1\nrecords: 2\n"
            "msg|m1|t1|5|user|1|hi|1|F\n"
            "msg|m2|t1|3|assistant|2|bye|1|F\n"
        )
        v, m = validate_agent_payload(payload)
        assert v is False
        assert "less than" in m

    def test_same_step_allowed(self):
        payload = (
            "LUMEN-AGENT v1\nrecords: 2\n"
            "plan|p1|t1|2|1|do a|pending\n"
            "plan|p2|t1|2|2|do b|pending\n"
        )
        v, m = validate_agent_payload(payload)
        assert v is True

    def test_res_without_tool(self):
        payload = "LUMEN-AGENT v1\nrecords: 1\nres|tc_ghost|t1|1|search|data|done|100\n"
        v, m = validate_agent_payload(payload)
        assert v is False
        assert "no matching tool" in m

    def test_bad_enum_role(self):
        payload = "LUMEN-AGENT v1\nrecords: 1\nmsg|m1|t1|1|robot|1|hi|1|F\n"
        v, m = validate_agent_payload(payload)
        assert v is False
        assert "role" in m

    def test_bad_enum_tool_status(self):
        payload = "LUMEN-AGENT v1\nrecords: 1\ntool|t1|t1|1|search|{}|flying\n"
        v, m = validate_agent_payload(payload)
        assert v is False

    def test_bad_enum_cot_type(self):
        payload = "LUMEN-AGENT v1\nrecords: 1\ncot|c1|t1|1|1|dream|thinking|1.0\n"
        v, m = validate_agent_payload(payload)
        assert v is False

    def test_multi_thread_independent(self):
        payload = (
            "LUMEN-AGENT v1\nrecords: 4\n"
            "msg|m1|th_A|1|user|1|hi|1|F\n"
            "msg|m2|th_B|1|user|1|hi|1|F\n"
            "msg|m3|th_A|2|assistant|2|bye|1|F\n"
            "msg|m4|th_B|2|assistant|2|bye|1|F\n"
        )
        v, m = validate_agent_payload(payload)
        assert v is True

    def test_null_required_field(self):
        payload = "LUMEN-AGENT v1\nrecords: 1\nmsg|m1|t1|1|N|1|hi|1|F\n"
        v, m = validate_agent_payload(payload)
        assert v is False

    def test_res_matches_tool(self):
        payload = (
            "LUMEN-AGENT v1\nrecords: 2\n"
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
        assert AGENT_MAGIC == "LUMEN-AGENT v1"

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
        from lumen.core._agent import _split_row
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
        payload = "LUMEN-AGENT v1\nrecords: 3\nmsg|m1|t1|1|user|1|hi|1|F\n\nmsg|m2|t1|2|assistant|2|bye|1|F\n"
        with pytest.raises(ValueError):
            decode_agent_payload(payload)

    def test_encode_lumen_llm_via_lumendict(self):
        # covers _api.py encode_lumen_llm cache path
        from lumen.core._api import LumenDict
        ld = LumenDict([{"id": 1, "name": "Alice"}])
        result = ld.encode_lumen_llm()
        assert result.startswith("L|")
        # second call hits cache
        result2 = ld.encode_lumen_llm()
        assert result == result2

    def test_decode_text_via_lumendict(self):
        # covers _api.py decode_text
        from lumen.core._api import LumenDict
        ld = LumenDict([{"id": 1, "name": "Alice"}])
        text = ld.encode_text()
        decoded = ld.decode_text(text)
        assert len(decoded) == 1

    def test_decode_binary_via_lumendict(self):
        # covers _api.py decode_binary
        from lumen.core._api import LumenDict
        ld = LumenDict([{"id": 1, "name": "Alice"}])
        binary = ld.encode_binary_pooled()
        decoded = ld.decode_binary(binary)
        assert len(decoded) == 1

    def test_decode_lumen_llm_via_lumendict(self):
        # covers _api.py decode_lumen_llm method
        from lumen.core._api import LumenDict
        ld = LumenDict([{"id": 1, "name": "Alice"}])
        llm = ld.encode_lumen_llm()
        decoded = ld.decode_lumen_llm(llm)
        assert len(decoded) == 1
        assert decoded[0]["id"] == 1

    def test_lumendictfull_init_dict(self):
        # covers LumenDictFull init with dict input
        from lumen.core._api import LumenDictFull
        ldf = LumenDictFull({"id": 1, "name": "Alice"})
        assert len(ldf) == 1

    def test_lumendictfull_init_iterable(self):
        # covers LumenDictFull init with iterable input
        from lumen.core._api import LumenDictFull
        ldf = LumenDictFull(iter([{"id": 1}, {"id": 2}]))
        assert len(ldf) == 2

    def test_lumendictfull_append(self):
        # covers LumenDictFull append
        from lumen.core._api import LumenDictFull
        ldf = LumenDictFull([{"id": 1}])
        ldf.append({"id": 2})
        assert len(ldf) == 2

    def test_encode_lumen_llm_direct(self):
        # covers _api.py encode_lumen_llm_direct
        from lumen.core._api import encode_lumen_llm_direct
        result = encode_lumen_llm_direct([{"id": 1}])
        assert result.startswith("L|")

    def test_decode_lumen_llm_direct(self):
        # covers _api.py decode_lumen_llm_direct
        from lumen.core._api import decode_lumen_llm_direct
        result = decode_lumen_llm_direct("L|id:d\n1")
        assert result == [{"id": 1}]


class TestFinalCoverage:
    def test_split_row_trailing_pipe_appends_empty(self):
        # agent line 188: fields.append("") when row ends with pipe
        from lumen.core._agent import _split_row
        result = _split_row("a|b|c|")
        assert result[-1] == ""
        assert len(result) == 4

    def test_decode_agent_record_empty_fields(self):
        # agent line 229: if not fields -> ValueError("Empty row")
        # _split_row("") returns [""] which is truthy but rtype="" not in _SCHEMAS
        # We need to patch to get truly empty list — test via empty string edge
        from lumen.core._agent import decode_agent_record
        # An all-pipe row that produces empty first field
        with pytest.raises(ValueError):
            decode_agent_record("")


class TestLine188Coverage:
    def test_split_row_ends_with_pipe_non_quoted(self):
        # Directly hits line 188: fields.append("") when i==n after pipe increment
        from lumen.core._agent import _split_row
        # non-quoted path: "a|b|" -> j found at 3, fields=['a','b'], i=4, n=4, i==n -> ""
        result = _split_row("type|id|")
        assert result == ["type", "id", ""]
        assert result[-1] == ""

    def test_split_row_single_field_trailing_pipe(self):
        from lumen.core._agent import _split_row
        result = _split_row("only|")
        assert result == ["only", ""]
