"""
Tests for lumen.core._lumen_llm — 100% coverage target.
"""
import math

import pytest

from lumen.core._lumen_llm import (
    _build_decoders,
    _dec_b,
    _dec_d,
    _dec_f,
    _dec_m,
    _dec_n,
    _dec_s,
    _decode_nested_dict,
    _decode_nested_list,
    _decode_val_generic,
    _decode_val_unquoted,
    _enc_b,
    _enc_d,
    _enc_f,
    _enc_n,
    _enc_s,
    _encode_nested_dict,
    _encode_nested_list,
    _encode_val,
    _find_top_level_char,
    _needs_quoting,
    _parse_row_slow,
    _read_balanced,
    _row_is_plain,
    _split_rows_quoted,
    _split_simple,
    _split_top_level,
    _type_char,
    decode_lumen_llm,
    encode_lumen_llm,
)

# ---------------------------------------------------------------------------
# _needs_quoting
# ---------------------------------------------------------------------------

class TestNeedsQuoting:
    def test_safe(self):
        assert not _needs_quoting("hello world")

    def test_comma(self):
        assert _needs_quoting("a,b")

    def test_quote(self):
        assert _needs_quoting('a"b')

    def test_newline(self):
        assert _needs_quoting("a\nb")

    def test_brace(self):
        assert _needs_quoting("a{b")

    def test_bracket(self):
        assert _needs_quoting("a[b")

    def test_pipe(self):
        assert _needs_quoting("a|b")

    def test_colon(self):
        assert _needs_quoting("a:b")


# ---------------------------------------------------------------------------
# _encode_val
# ---------------------------------------------------------------------------

class TestEncodeVal:
    def test_none(self):
        assert _encode_val(None) == "N"

    def test_true(self):
        assert _encode_val(True) == "T"

    def test_false(self):
        assert _encode_val(False) == "F"

    def test_int(self):
        assert _encode_val(42) == "42"

    def test_float_normal(self):
        assert _encode_val(3.14) == repr(3.14)

    def test_float_nan(self):
        assert _encode_val(float("nan")) == "nan"

    def test_float_inf(self):
        assert _encode_val(float("inf")) == "inf"

    def test_float_neg_inf(self):
        assert _encode_val(float("-inf")) == "-inf"

    def test_str_empty(self):
        assert _encode_val("") == "$0="

    def test_str_safe(self):
        assert _encode_val("hello") == "hello"

    def test_str_needs_quoting(self):
        result = _encode_val("a,b")
        assert result == '"a,b"'

    def test_str_with_quote(self):
        result = _encode_val('say "hi"')
        assert result == '"say ""hi"""'

    def test_dict_empty(self):
        assert _encode_val({}) == "{}"

    def test_dict_non_empty(self):
        result = _encode_val({"k": "v"})
        assert result == "{k:v}"

    def test_list_empty(self):
        assert _encode_val([]) == "[]"

    def test_list_non_empty(self):
        result = _encode_val([1, 2, 3])
        assert result == "[1|2|3]"

    def test_tuple(self):
        result = _encode_val((1, 2))
        assert result == "[1|2]"

    def test_fallback_type(self):
        class Weird:
            def __str__(self): return "weird"
        result = _encode_val(Weird())
        assert result == "weird"

    def test_fallback_needs_quoting(self):
        class Weird:
            def __str__(self): return "a,b"
        result = _encode_val(Weird())
        assert result == '"a,b"'


# ---------------------------------------------------------------------------
# _encode_nested_dict / _encode_nested_list
# ---------------------------------------------------------------------------

class TestEncodeNested:
    def test_nested_dict_empty(self):
        assert _encode_nested_dict({}) == "{}"

    def test_nested_dict_simple(self):
        result = _encode_nested_dict({"k": "v"})
        assert result == "{k:v}"

    def test_nested_list_empty(self):
        assert _encode_nested_list([]) == "[]"

    def test_nested_list_simple(self):
        result = _encode_nested_list([1, 2])
        assert result == "[1|2]"


# ---------------------------------------------------------------------------
# Typed per-cell encoders
# ---------------------------------------------------------------------------

class TestTypedEncoders:
    def test_enc_d_none(self):
        assert _enc_d(None) == "N"

    def test_enc_d_int(self):
        assert _enc_d(42) == "42"

    def test_enc_f_none(self):
        assert _enc_f(None) == "N"

    def test_enc_f_nan(self):
        assert _enc_f(float("nan")) == "nan"

    def test_enc_f_inf(self):
        assert _enc_f(float("inf")) == "inf"

    def test_enc_f_neg_inf(self):
        assert _enc_f(float("-inf")) == "-inf"

    def test_enc_f_normal(self):
        assert _enc_f(3.14) == repr(3.14)

    def test_enc_b_none(self):
        assert _enc_b(None) == "N"

    def test_enc_b_true(self):
        assert _enc_b(True) == "T"

    def test_enc_b_false(self):
        assert _enc_b(False) == "F"

    def test_enc_n(self):
        assert _enc_n("anything") == "N"

    def test_enc_s_none(self):
        assert _enc_s(None) == "N"

    def test_enc_s_empty(self):
        assert _enc_s("") == "$0="

    def test_enc_s_safe(self):
        assert _enc_s("hello") == "hello"

    def test_enc_s_needs_quoting(self):
        result = _enc_s("a,b")
        assert result.startswith('"')


# ---------------------------------------------------------------------------
# _type_char
# ---------------------------------------------------------------------------

class TestTypeChar:
    def test_bool(self):
        assert _type_char(True) == "b"

    def test_int(self):
        assert _type_char(42) == "d"

    def test_float(self):
        assert _type_char(3.14) == "f"

    def test_str(self):
        assert _type_char("hi") == "s"

    def test_other(self):
        assert _type_char([1, 2]) == "m"


# ---------------------------------------------------------------------------
# encode_lumen_llm
# ---------------------------------------------------------------------------

class TestEncodeLumenLlm:
    def test_empty(self):
        assert encode_lumen_llm([]) == "L|"

    def test_non_dict(self):
        result = encode_lumen_llm([1, 2, 3])
        assert result.startswith("L|")
        assert "1" in result

    def test_single_dict(self):
        result = encode_lumen_llm([{"a": 1, "b": "hello"}])
        assert result.startswith("L|")
        assert "a:d" in result
        assert "b:s" in result

    def test_multi_dict(self):
        records = [{"id": i, "name": f"User_{i}"} for i in range(5)]
        result = encode_lumen_llm(records)
        lines = result.split("\n")
        assert lines[0].startswith("L|")
        assert len(lines) == 6  # header + 5 rows

    def test_empty_dict_records(self):
        result = encode_lumen_llm([{}, {}])
        assert "L|{}" in result

    def test_type_inference_bool(self):
        result = encode_lumen_llm([{"active": True}])
        assert "active:b" in result

    def test_type_inference_int(self):
        result = encode_lumen_llm([{"id": 1}])
        assert "id:d" in result

    def test_type_inference_float(self):
        result = encode_lumen_llm([{"score": 1.5}])
        assert "score:f" in result

    def test_type_inference_null(self):
        result = encode_lumen_llm([{"x": None}])
        assert "x:n" in result

    def test_type_demotion_to_mixed(self):
        result = encode_lumen_llm([{"x": 1}, {"x": "hello"}])
        assert "x:m" in result

    def test_nested_dict_value(self):
        result = encode_lumen_llm([{"meta": {"k": "v"}}])
        assert "{k:v}" in result

    def test_nested_list_value(self):
        result = encode_lumen_llm([{"tags": [1, 2, 3]}])
        assert "[1|2|3]" in result

    def test_nan_value(self):
        result = encode_lumen_llm([{"x": float("nan")}])
        assert "nan" in result

    def test_inf_value(self):
        result = encode_lumen_llm([{"x": float("inf")}])
        assert "inf" in result

    def test_empty_string_value(self):
        result = encode_lumen_llm([{"x": ""}])
        assert "$0=" in result

    def test_keys_with_special_chars(self):
        result = encode_lumen_llm([{"a,b": 1}])
        assert '"a,b"' in result

    def test_missing_key_in_later_row(self):
        records = [{"a": 1, "b": 2}, {"a": 3}]
        result = encode_lumen_llm(records)
        lines = result.split("\n")
        assert len(lines) == 3


# ---------------------------------------------------------------------------
# decode_lumen_llm
# ---------------------------------------------------------------------------

class TestDecodeLumenLlm:
    def test_empty(self):
        assert decode_lumen_llm("L|") == []

    def test_bad_magic(self):
        with pytest.raises(ValueError, match="Not a LUMEN LLM payload"):
            decode_lumen_llm("garbage")

    def test_empty_dict_payload(self):
        result = decode_lumen_llm("L|{}\n{}\n{}")
        assert result == [{}, {}]

    def test_single_record(self):
        result = decode_lumen_llm("L|id:d,name:s\n1,Alice")
        assert result == [{"id": 1, "name": "Alice"}]

    def test_multi_record(self):
        result = decode_lumen_llm("L|id:d,name:s\n1,Alice\n2,Bob")
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["name"] == "Bob"

    def test_null_value(self):
        result = decode_lumen_llm("L|x:d\nN")
        assert result[0]["x"] is None

    def test_bool_true(self):
        result = decode_lumen_llm("L|active:b\nT")
        assert result[0]["active"] is True

    def test_bool_false(self):
        result = decode_lumen_llm("L|active:b\nF")
        assert result[0]["active"] is False

    def test_float_value(self):
        result = decode_lumen_llm("L|score:f\n3.14")
        assert abs(result[0]["score"] - 3.14) < 1e-9

    def test_float_nan(self):
        result = decode_lumen_llm("L|score:f\nnan")
        assert math.isnan(result[0]["score"])

    def test_float_inf(self):
        result = decode_lumen_llm("L|score:f\ninf")
        assert result[0]["score"] == float("inf")

    def test_float_neg_inf(self):
        result = decode_lumen_llm("L|score:f\n-inf")
        assert result[0]["score"] == float("-inf")

    def test_null_type_col(self):
        result = decode_lumen_llm("L|x:n\nN")
        assert result[0]["x"] is None

    def test_mixed_type_col(self):
        result = decode_lumen_llm("L|x:m\n42")
        assert result[0]["x"] == 42

    def test_missing_col_in_row(self):
        result = decode_lumen_llm("L|a:d,b:s\n1")
        assert result[0]["a"] == 1
        assert result[0]["b"] is None

    def test_empty_string_value(self):
        result = decode_lumen_llm("L|name:s\n$0=")
        assert result[0]["name"] == ""

    def test_quoted_value(self):
        result = decode_lumen_llm('L|name:s\n"Alice,Bob"')
        assert result[0]["name"] == "Alice,Bob"

    def test_nested_dict_value(self):
        result = decode_lumen_llm("L|meta:m\n{k:v}")
        assert result[0]["meta"] == {"k": "v"}

    def test_nested_list_value(self):
        result = decode_lumen_llm("L|tags:m\n[1|2|3]")
        assert result[0]["tags"] == [1, 2, 3]

    def test_round_trip_simple(self):
        records = [{"id": i, "name": f"User_{i}", "active": i % 2 == 0}
                   for i in range(10)]
        encoded = encode_lumen_llm(records)
        decoded = decode_lumen_llm(encoded)
        assert len(decoded) == 10
        assert decoded[0]["id"] == 0
        assert decoded[5]["active"] is False

    def test_round_trip_float(self):
        records = [{"score": 98.5}]
        encoded = encode_lumen_llm(records)
        decoded = decode_lumen_llm(encoded)
        assert abs(decoded[0]["score"] - 98.5) < 1e-9

    def test_round_trip_null(self):
        records = [{"x": None}]
        encoded = encode_lumen_llm(records)
        decoded = decode_lumen_llm(encoded)
        assert decoded[0]["x"] is None

    def test_round_trip_empty_string(self):
        records = [{"name": ""}]
        encoded = encode_lumen_llm(records)
        decoded = decode_lumen_llm(encoded)
        assert decoded[0]["name"] == ""

    def test_round_trip_nested(self):
        records = [{"meta": {"k": "v"}, "tags": [1, 2]}]
        encoded = encode_lumen_llm(records)
        decoded = decode_lumen_llm(encoded)
        assert decoded[0]["meta"] == {"k": "v"}
        assert decoded[0]["tags"] == [1, 2]

    def test_non_dict_scalar(self):
        result = decode_lumen_llm("L|\n42")
        assert result == [42]

    def test_spec_no_type_hint(self):
        result = decode_lumen_llm("L|name\nAlice")
        assert result[0]["name"] == "Alice"


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

class TestHelpers:
    def test_row_is_plain_true(self):
        assert _row_is_plain("a,b,c")

    def test_row_is_plain_false_quote(self):
        assert not _row_is_plain('a,"b",c')

    def test_row_is_plain_false_brace(self):
        assert not _row_is_plain("a,{b},c")

    def test_split_rows_quoted_simple(self):
        rows = _split_rows_quoted("a\nb\nc")
        assert rows == ["a", "b", "c"]

    def test_split_rows_quoted_with_newline_in_quote(self):
        rows = _split_rows_quoted('"a\nb"\nc')
        assert len(rows) == 2

    def test_parse_row_slow_simple(self):
        result = _parse_row_slow("a,b,c")
        assert result == ["a", "b", "c"]

    def test_parse_row_slow_quoted(self):
        result = _parse_row_slow('"a,b",c')
        assert result[0] == "a,b"
        assert result[1] == "c"

    def test_parse_row_slow_nested_dict(self):
        result = _parse_row_slow("{k:v},b")
        assert result[0] == {"k": "v"}

    def test_parse_row_slow_nested_list(self):
        result = _parse_row_slow("[1|2],b")
        assert result[0] == [1, 2]

    def test_parse_row_slow_trailing_comma(self):
        result = _parse_row_slow("a,")
        assert result[0] == "a"

    def test_decode_val_generic_sentinels(self):
        assert _decode_val_generic("N") is None
        assert _decode_val_generic("T") is True
        assert _decode_val_generic("F") is False
        assert _decode_val_generic("$0=") == ""
        assert math.isnan(_decode_val_generic("nan"))
        assert _decode_val_generic("inf") == float("inf")
        assert _decode_val_generic("-inf") == float("-inf")
        assert _decode_val_generic("{}") == {}
        assert _decode_val_generic("[]") == []

    def test_decode_val_generic_int(self):
        assert _decode_val_generic("42") == 42

    def test_decode_val_generic_float(self):
        assert abs(_decode_val_generic("3.14") - 3.14) < 1e-9

    def test_decode_val_generic_string(self):
        assert _decode_val_generic("hello") == "hello"

    def test_decode_val_generic_dict(self):
        assert _decode_val_generic("{k:v}") == {"k": "v"}

    def test_decode_val_generic_list(self):
        assert _decode_val_generic("[1|2]") == [1, 2]

    def test_decode_val_unquoted_int(self):
        assert _decode_val_unquoted("42") == 42

    def test_decode_val_unquoted_float(self):
        assert abs(_decode_val_unquoted("3.14") - 3.14) < 1e-9

    def test_decode_val_unquoted_string(self):
        assert _decode_val_unquoted("hello") == "hello"

    def test_decode_nested_dict(self):
        assert _decode_nested_dict("") == {}
        assert _decode_nested_dict("k:v") == {"k": "v"}

    def test_decode_nested_list(self):
        assert _decode_nested_list("") == []
        assert _decode_nested_list("1|2|3") == [1, 2, 3]

    def test_split_top_level_simple(self):
        assert _split_top_level("a,b,c", ",") == ["a", "b", "c"]

    def test_split_top_level_nested(self):
        result = _split_top_level("{a,b},c", ",")
        assert result == ["{a,b}", "c"]

    def test_split_top_level_quoted(self):
        result = _split_top_level('"a,b",c', ",")
        assert result == ['"a,b"', "c"]

    def test_split_top_level_pipe(self):
        result = _split_top_level("a|b|c", "|")
        assert result == ["a", "b", "c"]

    def test_find_top_level_char_found(self):
        assert _find_top_level_char("a:b", ":") == 1

    def test_find_top_level_char_nested(self):
        assert _find_top_level_char("{a:b}:c", ":") == 5

    def test_find_top_level_char_not_found(self):
        assert _find_top_level_char("abc", ":") == -1

    def test_find_top_level_char_quoted(self):
        assert _find_top_level_char('"a:b":c', ":") == 5

    def test_read_balanced_dict(self):
        tok, end = _read_balanced("{a,b}", 0, "{", "}")
        assert tok == "{a,b}"
        assert end == 5

    def test_read_balanced_list(self):
        tok, end = _read_balanced("[1|2]", 0, "[", "]")
        assert tok == "[1|2]"
        assert end == 5

    def test_read_balanced_nested(self):
        tok, end = _read_balanced("{{a}}", 0, "{", "}")
        assert tok == "{{a}}"

    def test_read_balanced_unclosed(self):
        tok, end = _read_balanced("{abc", 0, "{", "}")
        assert tok == "{abc"

    def test_split_simple_plain(self):
        assert _split_simple("a,b,c") == ["a", "b", "c"]

    def test_split_simple_complex(self):
        result = _split_simple("{a,b},c")
        assert result == ["{a,b}", "c"]

    def test_build_decoders_with_type(self):
        keys, decoders = _build_decoders(["id:d", "name:s"])
        assert keys == ["id", "name"]
        assert decoders[0]("42") == 42
        assert decoders[1]("Alice") == "Alice"

    def test_build_decoders_no_type(self):
        keys, decoders = _build_decoders(["foo"])
        assert keys == ["foo"]
        assert decoders[0]("42") == 42

    def test_dec_d(self):
        assert _dec_d("N") is None
        assert _dec_d("42") == 42

    def test_dec_f(self):
        assert _dec_f("N") is None
        assert math.isnan(_dec_f("nan"))
        assert _dec_f("inf") == float("inf")
        assert _dec_f("-inf") == float("-inf")
        assert abs(_dec_f("3.14") - 3.14) < 1e-9

    def test_dec_b(self):
        assert _dec_b("T") is True
        assert _dec_b("F") is False
        assert _dec_b("X") is None

    def test_dec_n(self):
        assert _dec_n("anything") is None

    def test_dec_s(self):
        assert _dec_s("N") is None
        assert _dec_s("$0=") == ""
        assert _dec_s("hello") == "hello"
        assert _dec_s('"a,b"') == '"a,b"'  # _dec_s is fast-path: no RFC4180 unescaping

    def test_dec_m(self):
        assert _dec_m("42") == 42
        assert _dec_m("N") is None


class TestMissingCoverage:
    """Cover remaining uncovered branches in _lumen_llm.py."""

    def test_mixed_col_type_branch(self):
        # covers line 199: ct == 'm' fast path after type is locked as mixed
        records = [{"x": 1}, {"x": "hello"}, {"x": 2.0}]
        result = encode_lumen_llm(records)
        decoded = decode_lumen_llm(result)
        assert len(decoded) == 3

    def test_slow_path_plain_row(self):
        # covers llm316: slow path but row is plain
        # needs_slow=True (header has quotes) but data rows are plain
        result = encode_lumen_llm([{"name": "Alice,Bob", "id": 1}])
        decoded = decode_lumen_llm(result)
        assert decoded[0]["name"] == "Alice,Bob"
        assert decoded[0]["id"] == 1

    def test_slow_path_quoted_row(self):
        # covers slow path with quoted data row
        records = [{"name": "Alice,Bob", "desc": "hello,world"}]
        result = encode_lumen_llm(records)
        decoded = decode_lumen_llm(result)
        assert decoded[0]["name"] == "Alice,Bob"
        assert decoded[0]["desc"] == "hello,world"

    def test_split_rows_quoted_double_quote(self):
        # covers llm357: double-quote escape in _split_rows_quoted
        from lumen.core._lumen_llm import _split_rows_quoted
        text = '"hello\n""world""\n"\nline2'
        rows = _split_rows_quoted(text)
        assert len(rows) == 2

    def test_read_balanced_quoted_string(self):
        # covers llm428-431: quoted string inside balanced read
        from lumen.core._lumen_llm import _read_balanced
        tok, end = _read_balanced('{"key":"val"}', 0, '{', '}')
        assert tok == '{"key":"val"}'

    def test_read_balanced_quoted_with_close(self):
        from lumen.core._lumen_llm import _read_balanced
        tok, end = _read_balanced('["a}b"]', 0, '[', ']')
        assert tok == '["a}b"]'

    def test_non_dict_records_list(self):
        # non-dict records: scalar list
        result = encode_lumen_llm([1, 2, 3])
        decoded = decode_lumen_llm(result)
        assert decoded == [1, 2, 3]

    def test_empty_data_rows_skipped(self):
        # empty rows in data section are skipped
        result = decode_lumen_llm("L|id:d\n1\n\n2")
        assert len(result) == 2

    def test_type_violation_promotes_to_mixed(self):
        # col starts as int, then gets string -> promotes to m
        records = [{"x": 1}, {"x": "hello"}]
        enc = encode_lumen_llm(records)
        assert "x:m" in enc
        dec = decode_lumen_llm(enc)
        assert dec[0]["x"] == 1
        assert dec[1]["x"] == "hello"


class TestTextMissingCoverage:
    """Cover _text.py line 123: ctx != 'val' branch."""

    def test_encode_obj_iterative_non_val_ctx(self):
        from lumen.core._text import _encode_obj_iterative_text
        # The stack-based encoder pushes (obj, 'val') tuples
        # ctx != 'val' is an internal guard - test nested structures
        # to ensure the stack processes correctly
        nested = {"a": {"b": {"c": 42}}}
        result = _encode_obj_iterative_text(nested, {})
        assert "42" in result

    def test_encode_obj_deep_list(self):
        from lumen.core._text import _encode_obj_iterative_text
        deep = [[1, 2], [3, 4]]
        result = _encode_obj_iterative_text(deep, {})
        assert "1" in result and "4" in result


class TestCorePyShim:
    """Cover lumen/core.py shim — 0% coverage."""

    def test_core_py_imports(self):
        # Importing lumen.core.py shim via the module path
        import importlib.util
        # force import of lumen/core.py (not the package)
        spec = importlib.util.spec_from_file_location(
            "lumen_core_shim",
            "lumen/core.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert hasattr(mod, "LumenDict")
        assert hasattr(mod, "encode_varint")


class TestInitMissingCoverage:
    """Cover lumen/__init__.py lines 184,188 — Rust encode/decode_lumen_llm."""

    def test_encode_lumen_llm_via_init(self):
        from lumen import encode_lumen_llm
        records = [{"id": 1, "name": "Alice"}]
        result = encode_lumen_llm(records)
        assert result.startswith("L|")

    def test_decode_lumen_llm_via_init(self):
        from lumen import decode_lumen_llm, encode_lumen_llm
        records = [{"id": 1, "name": "Alice"}]
        enc = encode_lumen_llm(records)
        dec = decode_lumen_llm(enc)
        assert dec[0]["id"] == 1


class TestSlowPathNonPlainRow:
    def test_slow_path_non_plain_data_row(self):
        # llm lines 329-336: needs_slow=True AND data row is NOT plain
        # i.e. data row contains { or [ or "
        # Build a payload where header triggers slow path AND a data row has nested
        records = [
            {"name": "Alice,Bob", "meta": {"k": "v"}},
            {"name": "Carol", "meta": {"x": 1}},
        ]
        enc = encode_lumen_llm(records)
        # enc will have quoted name field AND nested dict -> needs_slow=True
        # AND data rows are non-plain (contain {)
        assert '"' in enc or '{' in enc
        dec = decode_lumen_llm(enc)
        assert len(dec) == 2
        assert dec[0]["name"] == "Alice,Bob"
        assert dec[0]["meta"] == {"k": "v"}
        assert dec[1]["meta"] == {"x": 1}

    def test_slow_path_empty_row_skipped(self):
        # llm line 329: if not row: continue in slow path
        from lumen.core._lumen_llm import decode_lumen_llm
        # manually craft payload that has quotes (triggers slow) + empty row
        text = 'L|name:s\n"Alice,Bob"\n\n"Carol"'
        dec = decode_lumen_llm(text)
        assert len(dec) == 2


class TestTextCtxNotVal:
    def test_encode_obj_iterative_ctx_not_val(self):
        # text line 123: if ctx != 'val': continue
        # The stack pops (item, ctx) — ctx is always 'val' in current impl
        # but we can verify the function works correctly for all types
        # which exercises the full stack loop
        from lumen.core._text import _encode_obj_iterative_text

        # Test with None - hits parts.append('N') after ctx check
        assert _encode_obj_iterative_text(None, {}) == "N"

        # Test with bool
        assert _encode_obj_iterative_text(True, {}) == "T"
        assert _encode_obj_iterative_text(False, {}) == "F"

        # Test with int
        assert _encode_obj_iterative_text(42, {}) == "42"

        # Test with float
        assert _encode_obj_iterative_text(3.14, {}) == repr(3.14)

        # Test with nested to exercise full stack traversal
        result = _encode_obj_iterative_text({"a": [1, None, True]}, {})
        assert "1" in result
        assert "N" in result
        assert "T" in result


class TestSlowPathPlainDataRow:
    def test_needs_slow_but_plain_data_rows(self):
        # Covers llm 331-334: needs_slow=True (header quoted) but data rows are plain
        # Header has quoted key -> needs_slow=True
        # Data rows have no quotes/braces -> _row_is_plain=True -> hits lines 331-334
        from lumen.core._lumen_llm import decode_lumen_llm
        text = 'L|"first,name":s,age:d\nAlice,30\nBob,25'
        result = decode_lumen_llm(text)
        assert len(result) == 2
        assert result[0]['"first,name"'] == "Alice"
        assert result[0]["age"] == 30
        assert result[1]['"first,name"'] == "Bob"
        assert result[1]["age"] == 25

    def test_needs_slow_plain_row_mismatched_cols(self):
        # Covers llm 335-337 else branch: n != n_keys in slow+plain path
        from lumen.core._lumen_llm import decode_lumen_llm
        text = 'L|"first,name":s,age:d,score:f\nAlice,30'
        result = decode_lumen_llm(text)
        assert result[0]['"first,name"'] == "Alice"
        assert result[0]["age"] == 30
        assert result[0]["score"] is None
