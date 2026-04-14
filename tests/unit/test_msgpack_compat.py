"""
Unit tests for ulmen/core/_msgpack_compat.py
Target: 100% coverage of the minimal pure-Python msgpack packer.
"""
import struct

from ulmen.core._msgpack_compat import packb


class TestPackbNone:
    def test_none(self):
        assert packb(None) == b"\xc0"


class TestPackbBool:
    def test_true(self):
        assert packb(True) == b"\xc3"

    def test_false(self):
        assert packb(False) == b"\xc2"


class TestPackbInt:
    def test_zero(self):
        assert packb(0) == b"\x00"

    def test_positive_fixint(self):
        assert packb(42) == b"\x2a"

    def test_max_fixint(self):
        assert packb(0x7f) == b"\x7f"

    def test_negative_fixint(self):
        b = packb(-1)
        assert b[0] == 0xff

    def test_negative_minus_32(self):
        b = packb(-32)
        assert b[0] == 0xe0

    def test_uint8(self):
        b = packb(200)
        assert b[0] == 0xcc

    def test_uint16(self):
        b = packb(1000)
        assert b[0] == 0xcd

    def test_uint32(self):
        b = packb(100000)
        assert b[0] == 0xce

    def test_int8_negative(self):
        b = packb(-100)
        assert b[0] == 0xd0

    def test_int16_negative(self):
        b = packb(-1000)
        assert b[0] == 0xd1

    def test_int32_negative(self):
        b = packb(-100000)
        assert b[0] == 0xd2

    def test_int64_large(self):
        b = packb(-(2**32))
        assert b[0] == 0xd3

    def test_large_positive(self):
        b = packb(2**32 + 1)
        assert b[0] == 0xd3


class TestPackbFloat:
    def test_float(self):
        b = packb(3.14)
        assert b[0] == 0xcb
        val = struct.unpack(">d", b[1:])[0]
        assert abs(val - 3.14) < 1e-10

    def test_zero_float(self):
        b = packb(0.0)
        assert b[0] == 0xcb


class TestPackbStr:
    def test_empty_string(self):
        b = packb("")
        assert b[0] == 0xa0

    def test_short_string(self):
        b = packb("hi")
        assert b[0] == 0xa2
        assert b[1:] == b"hi"

    def test_fixstr_max(self):
        s = "x" * 31
        b = packb(s)
        assert b[0] == 0xbf

    def test_str8(self):
        s = "x" * 32
        b = packb(s)
        assert b[0] == 0xd9

    def test_str16(self):
        s = "x" * 256
        b = packb(s)
        assert b[0] == 0xda

    def test_str32(self):
        s = "x" * 65536
        b = packb(s)
        assert b[0] == 0xdb

    def test_unicode_string(self):
        b = packb("hello")
        assert b"hello" in b


class TestPackbList:
    def test_empty_list(self):
        b = packb([])
        assert b[0] == 0x90

    def test_fixarray(self):
        b = packb([1, 2, 3])
        assert b[0] == 0x93

    def test_array16(self):
        b = packb(list(range(16)))
        assert b[0] == 0xdc

    def test_array32(self):
        b = packb(list(range(65536)))
        assert b[0] == 0xdd

    def test_nested_list(self):
        b = packb([[1, 2], [3, 4]])
        assert isinstance(b, bytes)

    def test_tuple_as_list(self):
        b = packb((1, 2, 3))
        assert isinstance(b, bytes)


class TestPackbDict:
    def test_empty_dict(self):
        b = packb({})
        assert b[0] == 0x80

    def test_fixmap(self):
        b = packb({"a": 1})
        assert b[0] == 0x81

    def test_map16(self):
        d = {str(i): i for i in range(16)}
        b = packb(d)
        assert b[0] == 0xde

    def test_map32(self):
        d = {str(i): i for i in range(65536)}
        b = packb(d)
        assert b[0] == 0xdf

    def test_nested_dict(self):
        b = packb({"meta": {"k": "v"}})
        assert isinstance(b, bytes)


class TestPackbFallback:
    def test_unknown_type_serialized_as_str(self):
        class Weird:
            def __str__(self): return "weird"
        b = packb(Weird())
        assert b"weird" in b


class TestPackbRealData:
    def test_ulmen_benchmark_records(self):
        records = [{"id": i, "name": f"u{i}", "score": 1.5} for i in range(100)]
        b = packb(records)
        assert isinstance(b, bytes)
        assert len(b) > 0

    def test_mixed_types(self):
        b = packb({"none": None, "bool": True, "int": 42,
                   "float": 3.14, "str": "hello", "list": [1, 2]})
        assert isinstance(b, bytes)

    def test_msgpack_shim_via_import(self):
        import msgpack
        records = [{"id": i} for i in range(10)]
        b = msgpack.packb(records)
        assert isinstance(b, bytes)
