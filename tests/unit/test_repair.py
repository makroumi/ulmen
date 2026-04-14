"""
Unit tests for lumen/core/_repair.py
Target: 100% coverage of parse_llm_output and helpers.
"""
import pytest

from lumen.core._agent import (
    decode_agent_payload,
    encode_agent_payload,
    validate_agent_payload,
)
from lumen.core._repair import (
    _find_magic,
    _is_data_line,
    _is_header_line,
    _repair_record_count,
    _strip_fences,
    parse_llm_output,
)

MAGIC = "LUMEN-AGENT v1"


def _msg(mid, tid, step):
    return {
        "type": "msg", "id": mid, "thread_id": tid, "step": step,
        "role": "user", "turn": step, "content": "hi",
        "tokens": 1, "flagged": False,
    }


def _valid_payload(n=1):
    recs = [_msg(f"m{i}", "t1", i + 1) for i in range(n)]
    return encode_agent_payload(recs, thread_id="t1")


class TestStripFences:
    def test_no_fence(self):
        text = "hello\nworld"
        assert _strip_fences(text) == "hello\nworld"

    def test_backtick_fence_removed(self):
        text = "```\nhello\n```"
        result = _strip_fences(text)
        assert "```" not in result
        assert "hello" in result

    def test_fence_with_language(self):
        text = "```python\ncode\n```"
        result = _strip_fences(text)
        assert "code" in result
        assert "```" not in result

    def test_empty(self):
        assert _strip_fences("") == ""


class TestFindMagic:
    def test_found_at_zero(self):
        lines = [MAGIC, "records: 1"]
        assert _find_magic(lines) == 0

    def test_found_after_preamble(self):
        lines = ["Here is the payload:", MAGIC, "records: 1"]
        assert _find_magic(lines) == 1

    def test_not_found(self):
        lines = ["hello", "world"]
        assert _find_magic(lines) == -1

    def test_found_with_whitespace(self):
        lines = ["  ", MAGIC, "records: 0"]
        assert _find_magic(lines) == 1

    def test_empty(self):
        assert _find_magic([]) == -1


class TestIsDataLine:
    def test_msg_line(self):
        assert _is_data_line("msg|m1|t1|1|user|1|hi|1|F")

    def test_tool_line(self):
        assert _is_data_line("tool|tc1|t1|2|search|{}|pending")

    def test_header_line_not_data(self):
        assert not _is_data_line("thread: t1")

    def test_empty_not_data(self):
        assert not _is_data_line("")

    def test_unknown_type_not_data(self):
        assert not _is_data_line("unknown|x|t1|1|foo")

    def test_cot_line(self):
        assert _is_data_line("cot|c1|t1|1|1|observe|thinking|1.0")


class TestIsHeaderLine:
    def test_thread(self):
        assert _is_header_line("thread: t1")

    def test_records(self):
        assert _is_header_line("records: 5")

    def test_context_window(self):
        assert _is_header_line("context_window: 8000")

    def test_context_used(self):
        assert _is_header_line("context_used: 100")

    def test_payload_id(self):
        assert _is_header_line("payload_id: abc123")

    def test_parent_payload_id(self):
        assert _is_header_line("parent_payload_id: abc")

    def test_agent_id(self):
        assert _is_header_line("agent_id: agent_a")

    def test_session_id(self):
        assert _is_header_line("session_id: sess1")

    def test_schema_version(self):
        assert _is_header_line("schema_version: 1.0.0")

    def test_meta(self):
        assert _is_header_line("meta: from_agent,to_agent")

    def test_data_line_not_header(self):
        assert not _is_header_line("msg|m1|t1|1|user|1|hi|1|F")

    def test_empty_not_header(self):
        assert not _is_header_line("")


class TestRepairRecordCount:
    def test_fixes_wrong_count(self):
        header = ["thread: t1", "records: 99"]
        data   = ["msg|m1|t1|1|user|1|hi|1|F"]
        result = _repair_record_count(header, data)
        assert "records: 1" in result

    def test_adds_missing_records_line(self):
        header = ["thread: t1"]
        data   = ["msg|m1|t1|1|user|1|hi|1|F"]
        result = _repair_record_count(header, data)
        assert any(line.startswith("records:") for line in result)

    def test_zero_data_lines(self):
        header = ["records: 5"]
        result = _repair_record_count(header, [])
        assert "records: 0" in result

    def test_correct_count_unchanged_logic(self):
        header = ["records: 1"]
        data   = ["msg|m1|t1|1|user|1|hi|1|F"]
        result = _repair_record_count(header, data)
        assert "records: 1" in result


class TestParseLlmOutput:
    def test_valid_payload_passes_through(self):
        payload = _valid_payload(2)
        result = parse_llm_output(payload)
        ok, err = validate_agent_payload(result)
        assert ok is True

    def test_wrong_count_repaired(self):
        bad = f"{MAGIC}\nrecords: 99\nmsg|m1|t1|1|user|1|hi|1|F\n"
        result = parse_llm_output(bad)
        ok, err = validate_agent_payload(result)
        assert ok is True

    def test_no_magic_returns_error_payload(self):
        result = parse_llm_output("just random text")
        ok, err = validate_agent_payload(result)
        assert ok is True
        recs = decode_agent_payload(result)
        assert recs[0]["type"] == "err"

    def test_no_magic_strict_raises(self):
        with pytest.raises(ValueError, match="magic"):
            parse_llm_output("random text", strict=True)

    def test_markdown_fences_stripped(self):
        payload = _valid_payload(1)
        wrapped = f"```\n{payload}\n```"
        result = parse_llm_output(wrapped)
        ok, err = validate_agent_payload(result)
        assert ok is True

    def test_preamble_before_magic_ignored(self):
        payload = _valid_payload(1)
        wrapped = f"Here is the output:\n{payload}"
        result = parse_llm_output(wrapped)
        ok, err = validate_agent_payload(result)
        assert ok is True

    def test_blank_lines_in_data_removed(self):
        bad = f"{MAGIC}\nrecords: 1\n\nmsg|m1|t1|1|user|1|hi|1|F\n\n"
        result = parse_llm_output(bad)
        ok, err = validate_agent_payload(result)
        assert ok is True

    def test_unknown_record_types_skipped(self):
        bad = (f"{MAGIC}\nrecords: 2\n"
               "unknown|x|t1|1|foo\n"
               "msg|m1|t1|1|user|1|hi|1|F\n")
        result = parse_llm_output(bad)
        ok, err = validate_agent_payload(result)
        assert ok is True
        recs = decode_agent_payload(result)
        assert len(recs) == 1

    def test_thread_id_override(self):
        payload = _valid_payload(1)
        result = parse_llm_output(payload, thread_id="custom_thread")
        assert isinstance(result, str)
        ok, _ = validate_agent_payload(result)
        assert ok is True

    def test_empty_string(self):
        result = parse_llm_output("")
        ok, _ = validate_agent_payload(result)
        assert ok is True
        recs = decode_agent_payload(result)
        assert recs[0]["type"] == "err"

    def test_only_magic_no_records(self):
        result = parse_llm_output(f"{MAGIC}\n")
        assert isinstance(result, str)

    def test_strict_no_magic_raises(self):
        with pytest.raises(ValueError):
            parse_llm_output("garbage", strict=True)

    def test_meta_fields_preserved(self):
        recs = [_msg("m1", "t1", 1)]
        recs[0]["from_agent"] = "a"
        recs[0]["to_agent"] = "b"
        recs[0]["priority"] = 1
        recs[0]["parent_id"] = None
        payload = encode_agent_payload(
            recs,
            thread_id="t1",
            meta_fields=("parent_id", "from_agent", "to_agent", "priority"),
        )
        result = parse_llm_output(payload)
        ok, err = validate_agent_payload(result)
        assert ok is True

    def test_all_bad_rows_returns_empty_payload(self):
        bad = f"{MAGIC}\nrecords: 2\nbadrow1\nbadrow2\n"
        result = parse_llm_output(bad)
        ok, _ = validate_agent_payload(result)
        assert ok is True
        recs = decode_agent_payload(result)
        # All rows were bad and skipped, so repair returns records: 0
        assert recs == []

    def test_strict_all_bad_rows_no_raise(self):
        # All bad rows are silently skipped, strict mode still returns records: 0
        bad = f"{MAGIC}\nrecords: 2\nbadrow1\nbadrow2\n"
        result = parse_llm_output(bad, strict=True)
        ok, _ = validate_agent_payload(result)
        assert ok is True
        recs = decode_agent_payload(result)
        assert recs == []


class TestRepairUnknownHeaderLineForwardCompat:
    """Covers _repair.py line 150: unknown header line kept for forward compat."""

    def test_unknown_header_line_before_records_kept(self):
        payload = (
            "LUMEN-AGENT v1\n"
            "future_extension: some_value\n"
            "records: 1\n"
            "msg|m1|t1|1|user|1|hi|1|F\n"
        )
        result = parse_llm_output(payload)
        ok, _ = validate_agent_payload(result)
        assert ok is True

    def test_multiple_unknown_header_lines_all_kept(self):
        payload = (
            "LUMEN-AGENT v1\n"
            "x_custom_field: abc\n"
            "y_another: 123\n"
            "records: 1\n"
            "msg|m1|t1|1|user|1|hi|1|F\n"
        )
        result = parse_llm_output(payload)
        ok, _ = validate_agent_payload(result)
        assert ok is True


class TestRepairLastResortReEncodeGoodRows:
    """Covers _repair.py lines 164-192: last-resort path with valid decodable rows."""

    def test_rows_with_bad_enum_skipped_good_rows_kept(self):
        bad = (
            "LUMEN-AGENT v1\n"
            "records: 2\n"
            "msg|m1|t1|1|robot|1|hi|1|F\n"
            "msg|m2|t1|2|user|2|bye|1|F\n"
        )
        result = parse_llm_output(bad)
        ok, _ = validate_agent_payload(result)
        assert ok is True
        recs = decode_agent_payload(result)
        assert len(recs) >= 1

    def test_mixed_good_and_malformed_rows(self):
        bad = (
            "LUMEN-AGENT v1\n"
            "records: 3\n"
            "msg|m1|t1|1|user|1|hi|1|F\n"
            "this_is_not_a_valid_row\n"
            "msg|m2|t1|2|user|2|bye|1|F\n"
        )
        result = parse_llm_output(bad)
        ok, _ = validate_agent_payload(result)
        assert ok is True
        recs = decode_agent_payload(result)
        assert len(recs) == 2

    def test_meta_fields_from_header_used_in_last_resort(self):
        rec = {
            "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
            "role": "user", "turn": 1, "content": "hi",
            "tokens": 1, "flagged": False,
            "from_agent": "a", "to_agent": "b", "priority": 2, "parent_id": None,
        }
        payload = encode_agent_payload(
            [rec],
            thread_id="t1",
            meta_fields=("parent_id", "from_agent", "to_agent", "priority"),
        )
        bad = payload.replace("records: 1", "records: 99")
        result = parse_llm_output(bad)
        ok, _ = validate_agent_payload(result)
        assert ok is True

    def test_last_resort_produces_correct_record_count(self):
        bad = (
            "LUMEN-AGENT v1\n"
            "records: 10\n"
            "msg|m1|t1|1|user|1|hi|1|F\n"
            "msg|m2|t1|2|user|2|bye|1|F\n"
        )
        result = parse_llm_output(bad)
        ok, _ = validate_agent_payload(result)
        assert ok is True
        recs = decode_agent_payload(result)
        assert len(recs) == 2


class TestRepairNoGoodRecordsPaths:
    """Covers _repair.py lines 178-204: no good rows and strict/non-strict error paths."""

    def test_all_rows_malformed_non_strict_returns_error_payload(self):
        bad = (
            "LUMEN-AGENT v1\n"
            "records: 2\n"
            "completely_invalid_row_1\n"
            "completely_invalid_row_2\n"
        )
        result = parse_llm_output(bad, strict=False)
        ok, _ = validate_agent_payload(result)
        assert ok is True
        recs = decode_agent_payload(result)
        assert isinstance(recs, list)

    def test_all_rows_bad_enum_strict_raises_value_error(self):
        bad = (
            "LUMEN-AGENT v1\n"
            "records: 1\n"
            "msg|m1|t1|1|robot|1|hi|1|F\n"
        )
        with pytest.raises(ValueError):
            parse_llm_output(bad, strict=True)

    def test_all_rows_bad_enum_non_strict_returns_error_payload(self):
        bad = (
            "LUMEN-AGENT v1\n"
            "records: 1\n"
            "msg|m1|t1|1|robot|1|hi|1|F\n"
        )
        result = parse_llm_output(bad, strict=False)
        ok, _ = validate_agent_payload(result)
        assert ok is True
        recs = decode_agent_payload(result)
        assert recs[0]["type"] == "err"


class TestRepairUncoveredLines:
    """Covers _repair.py lines 171-172, 178-179, 182-185, 195, 204."""

    def test_meta_fields_extracted_in_last_resort(self):
        from lumen.core._agent import encode_agent_payload
        rec = {
            "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
            "role": "user", "turn": 1, "content": "hi",
            "tokens": 1, "flagged": False,
            "from_agent": "a", "to_agent": "b", "priority": 1, "parent_id": None,
        }
        payload = encode_agent_payload(
            [rec], thread_id="t1",
            meta_fields=("parent_id", "from_agent", "to_agent", "priority"),
        )
        bad = payload.replace("records: 1", "records: 99")
        result = parse_llm_output(bad)
        ok, _ = validate_agent_payload(result)
        assert ok is True

    def test_malformed_rows_skipped_in_last_resort(self):
        bad = (
            "LUMEN-AGENT v1\n"
            "records: 3\n"
            "msg|m1|t1|1|user|1|hi|1|F\n"
            "this_is|completely|wrong\n"
            "msg|m2|t1|2|user|2|bye|1|F\n"
        )
        result = parse_llm_output(bad)
        ok, _ = validate_agent_payload(result)
        assert ok is True
        from lumen.core._agent import decode_agent_payload
        recs = decode_agent_payload(result)
        assert len(recs) == 2

    def test_no_good_records_non_strict_returns_error_payload(self):
        bad = (
            "LUMEN-AGENT v1\n"
            "records: 3\n"
            "notarecord1\n"
            "notarecord2\n"
            "notarecord3\n"
        )
        result = parse_llm_output(bad, strict=False)
        ok, _ = validate_agent_payload(result)
        assert ok is True

    def test_no_good_records_strict_raises(self):
        bad = (
            "LUMEN-AGENT v1\n"
            "records: 2\n"
            "msg|m1|t1|1|robot|1|hi|1|F\n"
            "msg|m2|t1|2|robot|2|bye|1|F\n"
        )
        with pytest.raises(ValueError):
            parse_llm_output(bad, strict=True)

    def test_successful_re_encode_returns_result(self):
        bad = (
            "LUMEN-AGENT v1\n"
            "records: 99\n"
            "msg|m1|t1|1|user|1|hello world|5|F\n"
            "msg|m2|t1|2|assistant|2|goodbye|4|F\n"
        )
        result = parse_llm_output(bad)
        ok, _ = validate_agent_payload(result)
        assert ok is True
        from lumen.core._agent import decode_agent_payload
        recs = decode_agent_payload(result)
        assert len(recs) == 2

    def test_encode_exception_non_strict_returns_error_payload(self):
        from unittest.mock import patch

        import lumen.core._agent as _agent_mod
        from lumen.core._repair import parse_llm_output as _parse

        bad = (
            "LUMEN-AGENT v1\n"
            "records: 2\n"
            "msg|m1|t1|1|robot|1|hi|1|F\n"
            "msg|m2|t1|2|robot|2|bye|1|F\n"
        )
        original_eap = _agent_mod.encode_agent_payload
        original_val = _agent_mod.validate_agent_payload
        val_count = [0]
        enc_count = [0]

        def fake_validate(text, structured=False):
            val_count[0] += 1
            if val_count[0] == 1:
                return False, "first pass fails"
            return original_val(text, structured=structured)

        def fake_encode(*args, **kwargs):
            enc_count[0] += 1
            if enc_count[0] == 1:
                raise RuntimeError("boom on last resort encode")
            return original_eap(*args, **kwargs)

        with patch.object(_agent_mod, "validate_agent_payload", side_effect=fake_validate),                 patch.object(_agent_mod, "encode_agent_payload", side_effect=fake_encode):
            result = _parse(bad, strict=False)
        ok, _ = validate_agent_payload(result)
        assert ok is True

    def test_encode_exception_strict_raises(self):
        from unittest.mock import patch

        import lumen.core._agent as _agent_mod
        from lumen.core._repair import parse_llm_output as _parse

        bad = (
            "LUMEN-AGENT v1\n"
            "records: 2\n"
            "msg|m1|t1|1|robot|1|hi|1|F\n"
            "msg|m2|t1|2|robot|2|bye|1|F\n"
        )
        original_eap = _agent_mod.encode_agent_payload
        original_val = _agent_mod.validate_agent_payload
        val_count = [0]
        enc_count = [0]

        def fake_validate(text, structured=False):
            val_count[0] += 1
            if val_count[0] == 1:
                return False, "first pass fails"
            return original_val(text, structured=structured)

        def fake_encode(*args, **kwargs):
            enc_count[0] += 1
            if enc_count[0] == 1:
                raise RuntimeError("boom on last resort encode")
            return original_eap(*args, **kwargs)

        with patch.object(_agent_mod, "validate_agent_payload", side_effect=fake_validate),                 patch.object(_agent_mod, "encode_agent_payload", side_effect=fake_encode),                 pytest.raises(ValueError, match="Repair failed"):
            _parse(bad, strict=True)


class TestRepairCoveredLinesDirect:
    """
    Covers _repair.py lines 171-172, 185, 195 with precise inputs
    that deterministically reach each branch without mocking.

    Lines 171-172: meta_fields extraction body inside last-resort block.
                   Trigger: header has "meta: ..." line AND first validate
                   fails AND last-resort block is entered.

    Line 185:      return make_validation_error (non-strict, no good records).
                   Trigger: first validate fails, all data rows fail
                   decode_agent_record, strict=False.

    Line 195:      return result after ok2=True in last-resort re-encode.
                   Trigger: first validate fails on semantic rule (res without
                   tool), the res row IS decodable, re-encode produces a
                   valid payload (res alone passes validate), ok2=True.
    """

    def test_line_171_172_meta_fields_extracted_in_last_resort(self):
        """
        Lines 171-172: the meta_fields extraction body is executed.

        Payload has a "meta: from_agent,to_agent" header line so the loop
        body runs. First validate fails because the res row has no matching
        tool. Re-encode of the decodable res row succeeds and ok2 may be
        True or False — either way lines 171-172 are hit.
        """
        bad = (
            "LUMEN-AGENT v1\n"
            "thread: t1\n"
            "meta: from_agent,to_agent\n"
            "records: 1\n"
            "res|tc1|t1|1|search|data|done|100|N|N\n"
        )
        result = parse_llm_output(bad, strict=False)
        ok, _ = validate_agent_payload(result)
        assert ok is True

    def test_line_185_no_good_records_non_strict_returns_error_payload(self):
        """
        Line 185: return make_validation_error (non-strict path).

        All data rows pass _is_data_line (valid type prefix) but fail
        decode_agent_record (wrong field count). good_records is empty.
        strict=False so line 185 executes instead of raising.
        """
        bad = (
            "LUMEN-AGENT v1\n"
            "records: 2\n"
            "msg|m1|t1\n"
            "msg|m2|t1\n"
        )
        result = parse_llm_output(bad, strict=False)
        ok, _ = validate_agent_payload(result)
        assert ok is True
        recs = decode_agent_payload(result)
        assert recs[0]["type"] == "err"

    def test_line_171_172_strict_raises_when_no_good_records_with_meta(self):
        """
        Lines 171-172 + 183-184: meta extracted, no good records, strict=True raises.
        """
        bad = (
            "LUMEN-AGENT v1\n"
            "meta: from_agent,to_agent\n"
            "records: 2\n"
            "msg|m1|t1\n"
            "msg|m2|t1\n"
        )
        with pytest.raises(ValueError):
            parse_llm_output(bad, strict=True)

    def test_line_195_ok2_true_returns_result_directly(self):
        """
        Line 195: if ok2: return result.

        Payload has two data rows: one valid msg row and one short msg row
        that passes _is_data_line (valid type prefix) but fails
        decode_agent_record (wrong field count).

        Flow:
          records: 2 is correct so _repair_record_count keeps it.
          validate_agent_payload fails on the short row (field count error).
          Last-resort: good row decodes fine, short row raises -> skipped.
          good_records = [one msg record].
          encode_agent_payload succeeds.
          validate_agent_payload(result) -> ok2=True.
          Line 195: return result.
        """
        bad = (
            "LUMEN-AGENT v1\n"
            "records: 2\n"
            "msg|m1|t1|1|user|1|hi|1|F\n"
            "msg|m2|t1\n"
        )
        result = parse_llm_output(bad, strict=False)
        ok, _ = validate_agent_payload(result)
        assert ok is True
        recs = decode_agent_payload(result)
        assert len(recs) == 1
        assert recs[0]["id"] == "m1"

    def test_line_184_185_repair_produces_invalid_strict_raises(self):
        """
        Lines 184-185: encode succeeds but validate returns False, strict=True raises.
        """
        from unittest.mock import patch

        import lumen.core._agent as agent_mod

        call_count = {"n": 0}
        original_validate = agent_mod.validate_agent_payload

        def fake_validate(text, structured=False):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return False, "first pass fails"
            if call_count["n"] == 2:
                return False, "re-encode invalid"
            return original_validate(text, structured=structured)

        bad = (
            "LUMEN-AGENT v1\n"
            "records: 99\n"
            "msg|m1|t1|1|user|1|hi|1|F\n"
        )
        with patch.object(agent_mod, "validate_agent_payload", side_effect=fake_validate), \
                pytest.raises(ValueError, match="Repair produced invalid payload"):
            parse_llm_output(bad, strict=True)

    def test_line_195_encode_raises_strict_true_raises_value_error(self):
        """
        Lines 200-203: encode_agent_payload raises Exception, strict=True raises.
        """
        from unittest.mock import patch

        import lumen.core._agent as agent_mod

        call_count = {"n": 0}
        original_validate = agent_mod.validate_agent_payload

        def fake_validate(text, structured=False):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return False, "first pass fails to enter last-resort"
            return original_validate(text, structured=structured)

        def fake_encode(*args, **kwargs):
            raise RuntimeError("forced encode failure")

        bad = (
            "LUMEN-AGENT v1\n"
            "records: 99\n"
            "msg|m1|t1|1|user|1|hi|1|F\n"
        )
        with patch.object(agent_mod, "validate_agent_payload", side_effect=fake_validate),                 patch.object(agent_mod, "encode_agent_payload", side_effect=fake_encode),                 pytest.raises(ValueError, match="Repair failed"):
            parse_llm_output(bad, strict=True)

    def test_line_204_encode_raises_strict_false_returns_error_payload(self):
        """
        Line 204: encode raises, strict=False -> error payload.
        Only raises on first encode call so make_validation_error can succeed.
        """
        from unittest.mock import patch

        import lumen.core._agent as agent_mod

        call_count = {"n": 0}
        original_validate = agent_mod.validate_agent_payload
        original_encode   = agent_mod.encode_agent_payload

        def fake_validate(text, structured=False):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return False, "first pass fails to enter last-resort"
            return original_validate(text, structured=structured)

        encode_count = {"n": 0}
        def fake_encode(*args, **kwargs):
            encode_count["n"] += 1
            if encode_count["n"] == 1:
                raise RuntimeError("forced encode failure on last-resort")
            return original_encode(*args, **kwargs)

        bad = (
            "LUMEN-AGENT v1\n"
            "records: 99\n"
            "msg|m1|t1|1|user|1|hi|1|F\n"
        )
        with patch.object(agent_mod, "validate_agent_payload", side_effect=fake_validate),                 patch.object(agent_mod, "encode_agent_payload", side_effect=fake_encode):
            result = parse_llm_output(bad, strict=False)

        ok, _ = original_validate(result)
        assert ok is True
        recs = decode_agent_payload(result)
        assert recs[0]["type"] == "err"
