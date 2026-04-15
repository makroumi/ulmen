# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
#
# This file is part of ULMEN.
# ULMEN is licensed under the Business Source License 1.1.
# See the LICENSE file in the project root for full license information.

"""
ULMEN text codec -- encoder and decoder for the line-oriented text format.

Text format summary:

    [POOL:s1,s2,...]                   optional pool definition line
    records[N]:col:type,...            matrix header for multi-record dicts
    @col=v1;v2;...                     inline column (rle or pool strategy)
    v1 TAB v2 TAB ...                  data row for remaining columns

    SCHEMA:col:type,...                single-record dict header
    v1 TAB v2 TAB ...                  data row

    <encoded_value>                    plain line for non-dict records

Token vocabulary (text format):
    N           None
    T / F       True / False
    $0=         empty string sentinel
    nan / inf / -inf   float specials
    #N          pool reference single digit index
    #{N}        pool reference multi-digit index
    integer     plain integer literal
    float       plain float literal
    other       escaped string with backslash sequences for tab/newline/cr/backslash
"""

import math
from typing import Any

from ulmen.core._strategies import detect_column_strategy


def _text_escape(s: str) -> str:
    """Escape control characters for safe embedding in tab-delimited lines."""
    return (
        s.replace('\\', '\\\\')
         .replace('\t', '\\t')
         .replace('\n', '\\n')
         .replace('\r', '\\r')
    )


def _text_unescape(s: str) -> str:
    """Reverse of _text_escape. Unknown escape sequences pass through unchanged."""
    result = []
    i = 0
    while i < len(s):
        if s[i] == '\\' and i + 1 < len(s):
            c = s[i + 1]
            if   c == 't':  result.append('\t'); i += 2
            elif c == 'n':  result.append('\n'); i += 2
            elif c == 'r':  result.append('\r'); i += 2
            elif c == '\\': result.append('\\'); i += 2
            else:           result.append(s[i]); i += 1
        else:
            result.append(s[i]); i += 1
    return ''.join(result)


def _format_float(f: float) -> str:
    """Format a float for text output, handling NaN and signed infinities."""
    if math.isnan(f):  return 'nan'
    if math.isinf(f):  return 'inf' if f > 0 else '-inf'
    return repr(f)


def _parse_value(tok: str) -> Any:
    """
    Parse a single text token back to a Python value.

    Pool references are returned as ('__pool_ref__', index) tuples
    for deferred resolution once the pool is known.
    """
    if tok == 'N':    return None
    if tok == 'T':    return True
    if tok == 'F':    return False
    if tok == '$0=':  return ''
    if tok == 'nan':  return float('nan')
    if tok == 'inf':  return float('inf')
    if tok == '-inf': return float('-inf')
    if tok.startswith('#'):
        inner = tok[1:]
        if inner.startswith('{') and inner.endswith('}'):
            return ('__pool_ref__', int(inner[1:-1]))
        if inner.isdigit():
            return ('__pool_ref__', int(inner))
    try:   return int(tok)
    except ValueError: pass
    try:   return float(tok)
    except ValueError: pass
    return _text_unescape(tok)


def _encode_value_text(v: Any, pool_map: dict) -> str:
    """Encode a scalar value to its text token representation."""
    if v is None:            return 'N'
    if isinstance(v, bool):  return 'T' if v else 'F'
    if isinstance(v, int):   return str(v)
    if isinstance(v, float): return _format_float(v)
    if isinstance(v, str):
        if v == '':  return '$0='
        if v in pool_map:
            idx = pool_map[v]
            return f'#{idx}' if idx <= 9 else f'#{{{idx}}}'
        return _text_escape(v)
    return _text_escape(str(v))


def _encode_obj_iterative_text(obj: Any, pool_map: dict) -> str:
    """
    Encode an arbitrary Python object to ULMEN text format.

    Supports nested dicts, lists, tuples, and all scalar types.
    Uses an explicit stack to avoid Python recursion limits on deeply
    nested structures.
    """
    parts = []
    stack = [(obj, 'val')]
    while stack:
        item, ctx = stack.pop()
        if ctx != 'val':  # pragma: no cover
            continue
        if item is None:
            parts.append('N')
        elif isinstance(item, bool):
            parts.append('T' if item else 'F')
        elif isinstance(item, int):
            parts.append(str(item))
        elif isinstance(item, float):
            parts.append(_format_float(item))
        elif isinstance(item, str):
            parts.append(_encode_value_text(item, pool_map))
        elif isinstance(item, dict):
            if not item:
                parts.append('{}')
            else:
                inner = [
                    f"{_encode_obj_iterative_text(k, pool_map)}"
                    f":{_encode_obj_iterative_text(v, pool_map)}"
                    for k, v in item.items()
                ]
                parts.append('{' + ','.join(inner) + '}')
        elif isinstance(item, (list, tuple)):
            if not item:
                parts.append('[]')
            else:
                elems = [_encode_obj_iterative_text(x, pool_map) for x in item]
                parts.append('[' + ','.join(elems) + ']')
        else:
            parts.append(_text_escape(str(item)))
    return ''.join(parts)


def encode_text_records(
    records: list,
    pool: list,
    pool_map: dict,
    schema: dict = None,
    matrix_mode: bool = True,
) -> str:
    """
    Encode a list of records to the ULMEN text format.

    Selection logic:
        empty records              -> empty string
        matrix_mode + multi-dict  -> matrix format records[N]:...
        single dict or no matrix  -> schema format SCHEMA:...
        non-dict records           -> one encoded value per line
    """
    if not records:
        return ''

    lines = []
    if pool:
        lines.append('POOL:' + ','.join(_text_escape(s) for s in pool))

    is_matrix = (
        matrix_mode
        and len(records) > 1
        and all(isinstance(r, dict) for r in records)
    )

    if is_matrix:
        lines += _encode_matrix_text(records, pool_map)
    elif records and isinstance(records[0], dict):
        lines += _encode_schema_text(records, pool_map)
    else:
        for r in records:
            lines.append(_encode_obj_iterative_text(r, pool_map))

    return '\n'.join(lines)


def _encode_matrix_text(records: list, pool_map: dict) -> list:
    """Encode multiple dict records in columnar matrix text format."""
    all_keys = []
    seen_keys: set = set()
    for r in records:
        for k in r:
            if k not in seen_keys:
                all_keys.append(k)
                seen_keys.add(k)

    col_types: dict = {}
    col_strategies: dict = {}
    for col in all_keys:
        vals     = [r.get(col) for r in records]
        non_null = [v for v in vals if v is not None]
        if not non_null:
            col_types[col] = 'n'
        elif all(isinstance(v, bool) for v in non_null):
            col_types[col] = 'b'
        elif all(isinstance(v, int) and not isinstance(v, bool) for v in non_null):
            col_types[col] = 'd'
        elif all(isinstance(v, float) for v in non_null):
            col_types[col] = 'f'
        else:
            col_types[col] = 's'
        col_strategies[col] = detect_column_strategy(vals)

    col_specs = ','.join(f"{col}:{col_types[col]}" for col in all_keys)
    lines = [f"records[{len(records)}]:{col_specs}"]

    col_inline = {col for col in all_keys if col_strategies[col] in ('rle', 'pool')}
    for col in all_keys:
        if col in col_inline:
            vals = [r.get(col) for r in records]
            toks = [_encode_value_text(v, pool_map) for v in vals]
            lines.append(f"@{col}={';'.join(toks)}")

    remaining = [c for c in all_keys if c not in col_inline]
    if remaining:
        for r in records:
            row = [_encode_value_text(r.get(col), pool_map) for col in remaining]
            lines.append('\t'.join(row))

    return lines


def _encode_schema_text(records: list, pool_map: dict) -> list:
    """
    Encode dict records using the SCHEMA: header.

    Type inference uses all records rather than only the first so that
    a None in the first record does not produce a wrong type label.
    """
    cols = list(records[0].keys())

    def _type_char(col: str) -> str:
        non_null = [r.get(col) for r in records if r.get(col) is not None]
        if not non_null:                                              return 'n'
        if all(isinstance(v, bool) for v in non_null):               return 'b'
        if all(isinstance(v, int) and not isinstance(v, bool)
               for v in non_null):                                    return 'd'
        if all(isinstance(v, float) for v in non_null):              return 'f'
        return 's'

    schema_toks = ','.join(f"{c}:{_type_char(c)}" for c in cols)
    lines = [f'SCHEMA:{schema_toks}']
    for r in records:
        row = [_encode_value_text(r.get(col), pool_map) for col in cols]
        lines.append('\t'.join(row))
    return lines


def decode_text_records(text: str) -> list:
    """
    Decode a ULMEN text payload back to a list of Python objects.

    The row fill index is tracked with a plain counter instead of a
    linear scan so large datasets decode in O(n) rather than O(n^2).
    """
    pool:        list = []
    schema_cols: list = None
    matrix_mode: bool = False
    matrix_cols: list = None
    matrix_n:    int  = 0
    inline_cols: dict = {}
    results:     list = []
    row_cursor:  int  = 0

    def _resolve(tok: str) -> Any:
        v = _parse_value(tok)
        if isinstance(v, tuple) and v[0] == '__pool_ref__':
            idx = v[1]
            return pool[idx] if 0 <= idx < len(pool) else None
        return v

    for line in text.split('\n'):
        line = line.rstrip('\r')
        if not line:
            continue

        if line.startswith('POOL:'):
            pool = [_text_unescape(s) for s in line[5:].split(',') if s]
            continue

        if line.startswith('SCHEMA:'):
            schema_cols = []
            for part in line[7:].split(','):
                if ':' in part:
                    name, typ = part.split(':', 1)
                    schema_cols.append((name.strip(), typ.strip()))
            continue

        if line.startswith('records['):
            matrix_mode = True
            bracket_end = line.index(']')
            matrix_n    = int(line[8:bracket_end])
            rest        = line[bracket_end + 2:]
            matrix_cols = []
            for spec in rest.split(','):
                if ':' in spec:
                    cn, _ct = spec.split(':', 1)
                    matrix_cols.append(cn.strip())
                else:
                    matrix_cols.append(spec.strip())
            inline_cols = {}
            results     = [{} for _ in range(matrix_n)]
            row_cursor  = 0
            continue

        if matrix_mode and line.startswith('@'):
            eq       = line.index('=')
            col_name = line[1:eq]
            vals     = [_resolve(t) for t in line[eq + 1:].split(';')]
            inline_cols[col_name] = vals
            for i, rec in enumerate(results):
                if i < len(vals):
                    rec[col_name] = vals[i]
            continue

        if matrix_mode:
            data_cols = [c for c in matrix_cols if c not in inline_cols]
            if not data_cols:
                continue
            if row_cursor >= matrix_n:
                continue
            toks = line.split('\t')
            for ci, col in enumerate(data_cols):
                if ci < len(toks):
                    results[row_cursor][col] = _resolve(toks[ci])
            row_cursor += 1
            continue

        if schema_cols:
            toks = line.split('\t')
            rec  = {
                cn: _resolve(toks[i])
                for i, (cn, _ct) in enumerate(schema_cols)
                if i < len(toks)
            }
            results.append(rec)
            continue

        results.append(_resolve(line))

    return results
