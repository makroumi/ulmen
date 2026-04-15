# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
#
# This file is part of ULMEN.
# ULMEN is licensed under the Business Source License 1.1.
# See the LICENSE file in the project root for full license information.

"""
Minimal pure-Python MessagePack packer — zero external dependencies.

Implements only packb() for the subset used in ULMEN benchmarks:
lists of dicts with str keys and str/int/float/bool/None values.

Not a full msgpack implementation — benchmark use only.
Conforms to MessagePack spec for the types used in ULMEN benchmarks.
"""

from __future__ import annotations

import struct


def _pack_one(val, out: bytearray) -> None:
    if val is None:
        out.append(0xc0)
    elif isinstance(val, bool):
        out.append(0xc3 if val else 0xc2)
    elif isinstance(val, int):
        if 0 <= val <= 0x7f:
            out.append(val)
        elif -32 <= val < 0:
            out.append(val & 0xff)
        elif 0 <= val <= 0xff:
            out += b'\xcc' + struct.pack('B', val)
        elif 0 <= val <= 0xffff:
            out += b'\xcd' + struct.pack('>H', val)
        elif 0 <= val <= 0xffffffff:
            out += b'\xce' + struct.pack('>I', val)
        elif -0x80 <= val < 0:
            out += b'\xd0' + struct.pack('b', val)
        elif -0x8000 <= val < 0:
            out += b'\xd1' + struct.pack('>h', val)
        elif -0x80000000 <= val < 0:
            out += b'\xd2' + struct.pack('>i', val)
        else:
            out += b'\xd3' + struct.pack('>q', val)
    elif isinstance(val, float):
        out += b'\xcb' + struct.pack('>d', val)
    elif isinstance(val, str):
        b = val.encode('utf-8')
        n = len(b)
        if n <= 31:
            out.append(0xa0 | n)
        elif n <= 0xff:
            out += b'\xd9' + struct.pack('B', n)
        elif n <= 0xffff:
            out += b'\xda' + struct.pack('>H', n)
        else:
            out += b'\xdb' + struct.pack('>I', n)
        out += b
    elif isinstance(val, (list, tuple)):
        n = len(val)
        if n <= 15:
            out.append(0x90 | n)
        elif n <= 0xffff:
            out += b'\xdc' + struct.pack('>H', n)
        else:
            out += b'\xdd' + struct.pack('>I', n)
        for item in val:
            _pack_one(item, out)
    elif isinstance(val, dict):
        n = len(val)
        if n <= 15:
            out.append(0x80 | n)
        elif n <= 0xffff:
            out += b'\xde' + struct.pack('>H', n)
        else:
            out += b'\xdf' + struct.pack('>I', n)
        for k, v in val.items():
            _pack_one(k, out)
            _pack_one(v, out)
    else:
        # fallback: encode as string
        _pack_one(str(val), out)


def packb(data, use_bin_type: bool = True) -> bytes:
    """
    Pack data to MessagePack bytes.
    Supports: None, bool, int, float, str, list, tuple, dict.
    """
    out = bytearray()
    _pack_one(data, out)
    return bytes(out)
