"""
LUMEN-AGENT v1 encoder, decoder, validator, and subgraph extractor.

Wire format:
    LUMEN-AGENT v1
    records: N
    type|id|thread_id|step|field...|field...

Zero runtime dependencies. Pure Python reference implementation.
Rust acceleration layer to be added in v1.1.
"""

from __future__ import annotations

import math
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

AGENT_MAGIC   = "LUMEN-AGENT v1"
AGENT_VERSION = "1.0.0"

SEP = "|"

NULL_TOK  = "N"
TRUE_TOK  = "T"
FALSE_TOK = "F"
EMPTY_TOK = "$0="

RECORD_TYPES = frozenset(["msg","tool","res","plan","obs","err","mem","rag","hyp","cot"])

_SCHEMAS: dict[str, list[tuple[str,str,bool]]] = {
    "msg":  [("role","s",True), ("turn","d",True), ("content","s",True),
             ("tokens","d",True), ("flagged","b",True)],
    "tool": [("name","s",True), ("args","s",True), ("status","s",True)],
    "res":  [("name","s",True), ("data","s",False), ("status","s",True),
             ("latency_ms","d",True)],
    "plan": [("index","d",True), ("description","s",True), ("status","s",True)],
    "obs":  [("source","s",True), ("content","s",True), ("confidence","f",True)],
    "err":  [("code","s",True), ("message","s",True), ("source","s",True),
             ("recoverable","b",True)],
    "mem":  [("key","s",True), ("value","s",True), ("confidence","f",True),
             ("ttl","d",False)],
    "rag":  [("rank","d",True), ("score","f",True), ("source","s",True),
             ("chunk","s",True), ("used","b",True)],
    "hyp":  [("statement","s",True), ("evidence","s",True), ("score","f",True),
             ("accepted","b",True)],
    "cot":  [("index","d",True), ("cot_type","s",True), ("text","s",True),
             ("confidence","f",True)],
}

FIELD_COUNTS = {t: 4 + len(s) for t, s in _SCHEMAS.items()}

_ENUMS: dict[str, frozenset] = {
    "msg.role":      frozenset(["user","assistant","system"]),
    "msg.flagged":   frozenset(["T","F"]),
    "tool.status":   frozenset(["pending","running","done","error"]),
    "res.status":    frozenset(["done","error","timeout"]),
    "plan.status":   frozenset(["pending","active","done","skipped"]),
    "err.recoverable": frozenset(["T","F"]),
    "rag.used":      frozenset(["T","F"]),
    "hyp.accepted":  frozenset(["T","F"]),
    "cot.cot_type":  frozenset(["observe","plan","compute","verify","conclude"]),
}

# Pipe, newline, carriage-return, backslash, quote require quoting
_UNSAFE_CHARS = frozenset('|\\\"')


def _has_unsafe(s: str) -> bool:
    if not _UNSAFE_CHARS.isdisjoint(s):
        return True
    return '\n' in s or '\r' in s


# ---------------------------------------------------------------------------
# Field encoding / decoding
# ---------------------------------------------------------------------------

def _encode_field(v: Any) -> str:
    """Encode a single Python value to a LUMEN-AGENT field token."""
    if v is None:
        return NULL_TOK
    if isinstance(v, bool):
        return TRUE_TOK if v else FALSE_TOK
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if math.isnan(v):  return "nan"
        if math.isinf(v):  return "inf" if v > 0 else "-inf"
        return repr(v)
    if isinstance(v, str):
        if not v:
            return EMPTY_TOK
        needs_quote = (
            '|' in v or '"' in v or '\\' in v or
            '\n' in v or '\r' in v
        )
        if needs_quote:
            # escape newlines and carriage returns as literal backslash sequences
            safe = (v
                    .replace('\\', '\\\\')
                    .replace('"', '""')
                    .replace('\n', '\\n')
                    .replace('\r', '\\r'))
            return '"' + safe + '"'
        return v
    s = str(v)
    needs_quote = (
        '|' in s or '"' in s or '\\' in s or
        '\n' in s or '\r' in s
    )
    if needs_quote:
        safe = (s
                .replace('\\', '\\\\')
                .replace('"', '""')
                .replace('\n', '\\n')
                .replace('\r', '\\r'))
        return '"' + safe + '"'
    return s


def _decode_field(tok: str, type_char: str) -> Any:
    """Decode a single field token to a Python value."""
    if tok == NULL_TOK:
        return None
    if tok == TRUE_TOK:
        return True
    if tok == FALSE_TOK:
        return False
    if tok == EMPTY_TOK:
        return ""
    if tok.startswith('"') and tok.endswith('"') and len(tok) >= 2:
        inner = tok[1:-1]
        # unescape in reverse order of encoding
        inner = inner.replace('\\\\', '\\')
        inner = inner.replace('\\n', '\n').replace('\\r', '\r')
        inner = inner.replace('""', '"')
        return inner
    if type_char == "d":
        return int(tok)
    if type_char == "f":
        if tok == "nan":  return float("nan")
        if tok == "inf":  return float("inf")
        if tok == "-inf": return float("-inf")
        return float(tok)
    if type_char == "b":
        if tok == TRUE_TOK:  return True
        if tok == FALSE_TOK: return False
        raise ValueError(f"Invalid bool token: {tok!r}")
    return tok


# ---------------------------------------------------------------------------
# Row splitting (pipe-aware, respects RFC 4180 quoting)
# ---------------------------------------------------------------------------

def _split_row(line: str) -> list[str]:
    """Split a pipe-delimited row respecting quoted fields."""
    if '"' not in line:
        return line.split(SEP)
    fields = []
    i = 0
    n = len(line)
    while i < n:
        if line[i] == '"':
            i += 1
            buf = []
            while i < n:
                c = line[i]
                if c == '"':
                    if i + 1 < n and line[i+1] == '"':
                        buf.append('"'); i += 2
                    else:
                        i += 1; break
                else:
                    buf.append(c); i += 1
            raw = ''.join(buf).replace('"', '""')
            fields.append('"' + raw + '"')
            if i < n and line[i] == SEP:
                i += 1
        else:
            j = line.find(SEP, i)
            if j == -1:
                fields.append(line[i:]); break
            fields.append(line[i:j]); i = j + 1
            if i == n:
                fields.append("")  # pragma: no cover
    return fields


# ---------------------------------------------------------------------------
# Encode
# ---------------------------------------------------------------------------

def encode_agent_record(rec: dict) -> str:
    """Encode a single agent record dict to a LUMEN-AGENT row string."""
    rtype = rec["type"]
    if rtype not in _SCHEMAS:
        raise ValueError(f"Unknown record type: {rtype!r}")
    schema = _SCHEMAS[rtype]
    parts = [
        rtype,
        _encode_field(rec["id"]),
        _encode_field(rec["thread_id"]),
        _encode_field(rec["step"]),
    ]
    for fname, ftype, _ in schema:
        parts.append(_encode_field(rec.get(fname)))
    return SEP.join(parts)


def encode_agent_payload(records: list[dict]) -> str:
    """Encode a list of agent record dicts to a complete LUMEN-AGENT v1 payload."""
    lines = [AGENT_MAGIC, f"records: {len(records)}"]
    for rec in records:
        lines.append(encode_agent_record(rec))
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Decode
# ---------------------------------------------------------------------------

def decode_agent_record(line: str) -> dict:
    """Decode a single LUMEN-AGENT row string to a dict."""
    fields = _split_row(line)
    if not fields:  # pragma: no cover
        raise ValueError("Empty row")

    rtype = fields[0]
    if rtype not in _SCHEMAS:
        raise ValueError(f"Unknown record type: {rtype!r}")

    expected = FIELD_COUNTS[rtype]
    if len(fields) != expected:
        raise ValueError(
            f"Row type {rtype!r} expects {expected} fields, got {len(fields)}: {line!r}"
        )

    schema = _SCHEMAS[rtype]

    try:
        rec: dict = {
            "type":      rtype,
            "id":        _decode_field(fields[1], "s"),
            "thread_id": _decode_field(fields[2], "s"),
            "step":      _decode_field(fields[3], "d"),
        }
    except (ValueError, IndexError) as e:
        raise ValueError(f"Common field error in row {line!r}: {e}") from e

    for i, (fname, ftype, required) in enumerate(schema):
        tok = fields[4 + i]
        if required and tok == NULL_TOK:
            raise ValueError(f"Required field {fname!r} is null in row {line!r}")
        try:
            rec[fname] = _decode_field(tok, ftype)
        except (ValueError, IndexError) as e:
            raise ValueError(
                f"Field {fname!r} (type={ftype}) error in row {line!r}: {e}"
            ) from e

    return rec


def decode_agent_payload(text: str) -> list[dict]:
    """Decode a complete LUMEN-AGENT v1 payload to a list of record dicts."""
    lines = text.split("\n")
    while lines and lines[-1] == "":
        lines = lines[:-1]

    if len(lines) < 2:
        raise ValueError("Payload too short: missing header lines")

    if lines[0] != AGENT_MAGIC:
        raise ValueError(f"Bad magic: expected {AGENT_MAGIC!r}, got {lines[0]!r}")

    if not lines[1].startswith("records: "):
        raise ValueError(f"Bad records line: {lines[1]!r}")

    try:
        declared_n = int(lines[1][9:])
    except ValueError:
        raise ValueError(f"Bad record count: {lines[1]!r}")

    data_lines = lines[2:]
    actual_n   = len(data_lines)

    if actual_n != declared_n:
        raise ValueError(
            f"Record count mismatch: declared {declared_n}, found {actual_n}"
        )

    records = []
    for i, line in enumerate(data_lines):
        if not line:
            raise ValueError(f"Blank line at data row {i+1}")
        try:
            records.append(decode_agent_record(line))
        except ValueError as e:
            raise ValueError(f"Row {i+1}: {e}") from e

    return records


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_agent_payload(text: str) -> tuple[bool, str]:
    """
    Validate a LUMEN-AGENT v1 payload.
    Returns (True, "") on success.
    Returns (False, error_message) on failure.
    """
    try:
        records = decode_agent_payload(text)
    except ValueError as e:
        return False, str(e)

    thread_steps: dict[str, int] = {}
    tool_ids:     set[str]       = set()
    res_ids:      list[tuple[str,str]] = []

    for i, rec in enumerate(records):
        tid   = rec.get("thread_id", "")
        step  = rec.get("step", 0)
        rtype = rec["type"]
        rid   = rec.get("id", "")

        if not tid:
            return False, f"Row {i+1}: thread_id is empty"
        if not rid:
            return False, f"Row {i+1}: id is empty"
        if not isinstance(step, int) or step < 1:
            return False, f"Row {i+1}: step must be positive integer, got {step!r}"

        prev = thread_steps.get(tid, 0)
        # Same step is allowed (multiple records at same logical step).
        # Only strictly backwards is rejected.
        if step < prev:
            return False, (
                f"Row {i+1}: step {step} is less than "
                f"previous step {prev} in thread {tid!r}"
            )
        thread_steps[tid] = step

        if rtype == "tool":
            tool_ids.add(rid)
        if rtype == "res":
            res_ids.append((rid, f"row {i+1}"))

        for field_key, valid_vals in _ENUMS.items():
            etype, fname = field_key.split(".", 1)
            if etype == rtype and fname in rec:
                val = rec[fname]
                if val is not None:
                    tok = TRUE_TOK if val is True else FALSE_TOK if val is False else str(val)
                    if tok not in valid_vals:
                        return False, (
                            f"Row {i+1}: field {fname!r} value {val!r} "
                            f"not in {sorted(valid_vals)}"
                        )

    for res_id, loc in res_ids:
        if res_id not in tool_ids:
            return False, f"{loc}: res id {res_id!r} has no matching tool row"

    return True, ""


def make_validation_error(error_msg: str, thread_id: str = "INVALID") -> str:
    """Produce a single-record error payload describing a validation failure."""
    rec = {
        "type":        "err",
        "id":          "er_val_001",
        "thread_id":   thread_id,
        "step":        1,
        "code":        "VALIDATION_FAILED",
        "message":     error_msg,
        "source":      "validator",
        "recoverable": False,
    }
    return encode_agent_payload([rec])


# ---------------------------------------------------------------------------
# Subgraph extraction
# ---------------------------------------------------------------------------

def extract_subgraph(
    records:   list[dict],
    thread_id: str | None       = None,
    step_min:  int | None       = None,
    step_max:  int | None       = None,
    types:     list[str] | None = None,
) -> list[dict]:
    """Extract a filtered subset of records (all filters combined with AND)."""
    type_set = frozenset(types) if types else None
    result   = []
    for rec in records:
        if thread_id is not None and rec.get("thread_id") != thread_id:
            continue
        step = rec.get("step", 0)
        if step_min is not None and step < step_min:
            continue
        if step_max is not None and step > step_max:
            continue
        if type_set is not None and rec.get("type") not in type_set:
            continue
        result.append(rec)
    return result


def extract_subgraph_payload(
    text:      str,
    thread_id: str | None       = None,
    step_min:  int | None       = None,
    step_max:  int | None       = None,
    types:     list[str] | None = None,
) -> str:
    """Decode, filter, and re-encode a LUMEN-AGENT payload."""
    records  = decode_agent_payload(text)
    filtered = extract_subgraph(records, thread_id, step_min, step_max, types)
    return encode_agent_payload(filtered)
