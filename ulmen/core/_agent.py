"""
ULMEN-AGENT v1 encoder, decoder, validator, compressor, and streaming decoder.

Wire format:
    ULMEN-AGENT v1
    [thread: <thread_id>]
    [context_window: <n>]
    [context_used: <n>]
    [payload_id: <id>]
    [parent_payload_id: <id>]
    [agent_id: <id>]
    [session_id: <id>]
    [meta: parent_id,from_agent,to_agent,priority]
    records: N
    type|id|thread_id|step|field...|[meta_fields...]

Design principles:
    Zero runtime dependencies.
    Strict all-or-nothing validation.
    Backward compatible: old payloads parse with new parser.
    Forward compatible: unknown header lines are silently ignored.
    Streaming: every row is independently parseable.
    Tree-structured: parent_id enables hierarchical reasoning.
    Routing: from_agent/to_agent enables multi-agent message passing.
    Priority: controls context compression pass behavior.
    Unlimited context: chunk_payload/merge_chunks + summary chain.
"""

from __future__ import annotations

import math
import uuid
from typing import Any, Callable, Iterator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

AGENT_MAGIC   = "ULMEN-AGENT v1"
AGENT_VERSION = "1.0.0"

SEP = "|"

NULL_TOK  = "N"
TRUE_TOK  = "T"
FALSE_TOK = "F"
EMPTY_TOK = "$0="

RECORD_TYPES = frozenset([
    "msg", "tool", "res", "plan", "obs",
    "err", "mem", "rag", "hyp", "cot",
])

# Meta fields appended after type-specific fields when declared in header.
META_FIELDS = ("parent_id", "from_agent", "to_agent", "priority")

# Priority values
PRIORITY_MUST_KEEP    = 1
PRIORITY_KEEP_IF_ROOM = 2
PRIORITY_COMPRESSIBLE = 3

# Compression strategies
COMPRESS_COMPLETED_SEQUENCES = "completed_sequences"
COMPRESS_KEEP_TYPES          = "keep_types"
COMPRESS_SLIDING_WINDOW      = "sliding_window"

# Known header line prefixes — anything else is silently ignored
# for forward compatibility with future versions
_KNOWN_HEADER_PREFIXES = frozenset([
    "thread: ",
    "context_window: ",
    "context_used: ",
    "meta: ",
    "records: ",
    "payload_id: ",
    "parent_payload_id: ",
    "agent_id: ",
    "session_id: ",
    "schema_version: ",
])

# Type-specific field schemas: (field_name, type_char, required)
_SCHEMAS: dict[str, list[tuple[str, str, bool]]] = {
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

# ---------------------------------------------------------------------------
# Schema versioning — schema_version header field now gates field changes
# ---------------------------------------------------------------------------

DEFAULT_SCHEMA_VERSION = "1.0.0"

# Registry of versioned schemas. Add new versions here when fields change.
# Minor (1.x): new optional fields only — decoders remain backward compatible.
# Major (x.0): breaking field changes — migration path required.
SCHEMA_VERSIONS: dict[str, dict] = {
    "1.0.0": _SCHEMAS,
    # "1.1.0": _SCHEMAS_1_1_0,  # add optional fields
    # "2.0.0": _SCHEMAS_2_0_0,  # breaking changes
}

_COMMON_FIELDS = frozenset({"type", "id", "thread_id", "step"})


def validate_schema_compliance(
    records: list[dict],
    schema_version: str | None = None,
) -> tuple[bool, str | None]:
    """
    Validate records conform to the declared schema version.

    Returns (True, None) on success, (False, error_message) on failure.
    Unknown schema_version raises ValueError immediately — callers must
    explicitly handle version negotiation.
    """
    version = schema_version or DEFAULT_SCHEMA_VERSION
    if version not in SCHEMA_VERSIONS:
        raise ValueError(
            f"Unknown schema version: {version!r}. "
            f"Available: {sorted(SCHEMA_VERSIONS.keys())}"
        )
    schemas = SCHEMA_VERSIONS[version]
    for i, record in enumerate(records):
        rtype = record.get("type")
        if rtype not in schemas:
            return False, f"record[{i}]: unknown type {rtype!r} in schema {version}"
        required = {name for name, _, req in schemas[rtype] if req}
        allowed  = (
            {name for name, _, _ in schemas[rtype]}
            | _COMMON_FIELDS
            | set(META_FIELDS)
        )
        for field in required:
            if field not in record:
                return False, (
                    f"record[{i}] type={rtype!r}: "
                    f"missing required field {field!r} (schema {version})"
                )
        for field in record:
            if field not in allowed:
                return False, (
                    f"record[{i}] type={rtype!r}: "
                    f"field {field!r} not in schema {version}"
                )
    return True, None


def migrate_schema(
    records: list[dict],
    from_version: str,
    to_version: str,
) -> list[dict]:
    """
    Migrate records from one schema version to another.

    Same-version is a no-op. Cross-version migration is registered
    in _MIGRATIONS. Raises ValueError for unknown versions or missing
    migration paths.

    Add migration callables to _MIGRATIONS when introducing new versions::

        _MIGRATIONS[("1.0.0", "1.1.0")] = _migrate_1_0_to_1_1
    """
    if from_version not in SCHEMA_VERSIONS:
        raise ValueError(f"Unknown source schema version: {from_version!r}")
    if to_version not in SCHEMA_VERSIONS:
        raise ValueError(f"Unknown target schema version: {to_version!r}")
    if from_version == to_version:
        return records
    key = (from_version, to_version)
    if key not in _MIGRATIONS:
        raise ValueError(
            f"No migration path from {from_version!r} to {to_version!r}. "
            f"Registered paths: {list(_MIGRATIONS.keys())}"
        )
    return _MIGRATIONS[key](records)


# Migration registry: (from_version, to_version) -> callable(records) -> records
_MIGRATIONS: dict[tuple[str, str], Callable[[list[dict]], list[dict]]] = {}
# Example: _MIGRATIONS[("1.0.0", "1.1.0")] = _migrate_1_0_to_1_1


_ENUMS: dict[str, frozenset] = {
    "msg.role":        frozenset(["user", "assistant", "system"]),
    "msg.flagged":     frozenset(["T", "F"]),
    "tool.status":     frozenset(["pending", "running", "done", "error"]),
    "res.status":      frozenset(["done", "error", "timeout"]),
    "plan.status":     frozenset(["pending", "active", "done", "skipped"]),
    "err.recoverable": frozenset(["T", "F"]),
    "rag.used":        frozenset(["T", "F"]),
    "hyp.accepted":    frozenset(["T", "F"]),
    "cot.cot_type":    frozenset(["observe", "plan", "compute", "verify", "conclude"]),
}

_UNSAFE_CHARS = frozenset('|\\\"')


# ---------------------------------------------------------------------------
# Structured validation error
# ---------------------------------------------------------------------------

class ValidationError:
    """
    Structured validation error returned by validate_agent_payload.

    Attributes
    ----------
    message        : str   human-readable description
    row            : int or None   1-based row number where error occurred
    field          : str or None   field name that caused the error
    expected       : str or None   what was expected
    got            : str or None   what was actually found
    suggestion     : str or None   how to fix it
    """

    __slots__ = ("message", "row", "field", "expected", "got", "suggestion")

    def __init__(
        self,
        message: str,
        row: int | None = None,
        field: str | None = None,
        expected: str | None = None,
        got: str | None = None,
        suggestion: str | None = None,
    ):
        self.message    = message
        self.row        = row
        self.field      = field
        self.expected   = expected
        self.got        = got
        self.suggestion = suggestion

    def __str__(self) -> str:
        parts = [self.message]
        if self.row is not None:
            parts.append(f"row={self.row}")
        if self.field is not None:
            parts.append(f"field={self.field!r}")
        if self.expected is not None:
            parts.append(f"expected={self.expected!r}")
        if self.got is not None:
            parts.append(f"got={self.got!r}")
        if self.suggestion is not None:
            parts.append(f"hint={self.suggestion!r}")
        return " | ".join(parts)

    def __repr__(self) -> str:
        return f"ValidationError({str(self)})"

    def __bool__(self) -> bool:
        return False  # always falsy — error is a failure


# ---------------------------------------------------------------------------
# Context budget exceeded error
# ---------------------------------------------------------------------------

class ContextBudgetExceededError(ValueError):
    """
    Raised when encode_agent_payload would produce a payload that
    exceeds the declared context_window budget.

    Attributes
    ----------
    context_window : int   declared budget in tokens
    context_used   : int   actual tokens in the payload
    overage        : int   how many tokens over budget
    """

    def __init__(self, context_window: int, context_used: int):
        self.context_window = context_window
        self.context_used   = context_used
        self.overage        = context_used - context_window
        super().__init__(
            f"Payload uses {context_used} tokens but context_window is "
            f"{context_window} (overage: {self.overage} tokens). "
            f"Use compress_context() or chunk_payload() to reduce size."
        )


# ---------------------------------------------------------------------------
# Payload header
# ---------------------------------------------------------------------------

class AgentHeader:
    """
    Parsed representation of the ULMEN-AGENT payload header.

    Attributes
    ----------
    thread_id         : str or None
    context_window    : int or None
    context_used      : int or None
    meta_fields       : tuple
    record_count      : int
    payload_id        : str or None   unique ID for this payload
    parent_payload_id : str or None   links to previous payload in chain
    agent_id          : str or None   ID of the producing agent
    session_id        : str or None   session this payload belongs to
    schema_version    : str or None   protocol version for negotiation
    """

    __slots__ = (
        "thread_id", "context_window", "context_used",
        "meta_fields", "record_count",
        "payload_id", "parent_payload_id",
        "agent_id", "session_id", "schema_version",
    )

    def __init__(self):
        self.thread_id         = None
        self.context_window    = None
        self.context_used      = None
        self.meta_fields       = ()
        self.record_count      = 0
        self.payload_id        = None
        self.parent_payload_id = None
        self.agent_id          = None
        self.session_id        = None
        self.schema_version    = None

    def encode_lines(self) -> list[str]:
        """Produce header lines (excluding magic, including records:)."""
        lines = []
        if self.thread_id is not None:
            lines.append(f"thread: {self.thread_id}")
        if self.context_window is not None:
            lines.append(f"context_window: {self.context_window}")
        if self.context_used is not None:
            lines.append(f"context_used: {self.context_used}")
        if self.payload_id is not None:
            lines.append(f"payload_id: {self.payload_id}")
        if self.parent_payload_id is not None:
            lines.append(f"parent_payload_id: {self.parent_payload_id}")
        if self.agent_id is not None:
            lines.append(f"agent_id: {self.agent_id}")
        if self.session_id is not None:
            lines.append(f"session_id: {self.session_id}")
        if self.schema_version is not None:
            lines.append(f"schema_version: {self.schema_version}")
        if self.meta_fields:
            lines.append(f"meta: {','.join(self.meta_fields)}")
        lines.append(f"records: {self.record_count}")
        return lines


# ---------------------------------------------------------------------------
# Header parser — forward compatible: unknown lines silently ignored
# ---------------------------------------------------------------------------

def _parse_header(lines: list[str]) -> tuple[AgentHeader, int]:
    """
    Parse header lines starting after the magic line.
    Returns (AgentHeader, number_of_header_lines_consumed).

    Forward compatible: unknown header lines are silently ignored.
    This allows future versions to add new header fields without
    breaking older parsers.

    Raises ValueError when records: line not yet seen (streaming signal).
    Raises ValueError on malformed values for known fields.
    """
    h   = AgentHeader()
    idx = 0

    while idx < len(lines):
        line = lines[idx]

        if line.startswith("thread: "):
            h.thread_id = line[8:].strip()
            idx += 1

        elif line.startswith("context_window: "):
            try:
                h.context_window = int(line[16:].strip())
            except ValueError:
                raise ValueError(f"Bad context_window line: {line!r}")
            idx += 1

        elif line.startswith("context_used: "):
            try:
                h.context_used = int(line[14:].strip())
            except ValueError:
                raise ValueError(f"Bad context_used line: {line!r}")
            idx += 1

        elif line.startswith("payload_id: "):
            h.payload_id = line[12:].strip()
            idx += 1

        elif line.startswith("parent_payload_id: "):
            h.parent_payload_id = line[19:].strip()
            idx += 1

        elif line.startswith("agent_id: "):
            h.agent_id = line[10:].strip()
            idx += 1

        elif line.startswith("session_id: "):
            h.session_id = line[12:].strip()
            idx += 1

        elif line.startswith("schema_version: "):
            h.schema_version = line[16:].strip()
            idx += 1

        elif line.startswith("meta: "):
            raw = line[6:].strip()
            fields = tuple(f.strip() for f in raw.split(",") if f.strip())
            unknown = [f for f in fields if f not in META_FIELDS]
            if unknown:
                raise ValueError(f"Unknown meta fields: {unknown}")
            h.meta_fields = fields
            idx += 1

        elif line.startswith("records: "):
            try:
                h.record_count = int(line[9:].strip())
            except ValueError:
                raise ValueError(f"Bad record count: {line!r}")
            idx += 1
            return h, idx   # records: found — header complete

        else:
            # Forward compatibility: silently ignore unknown header lines
            # This allows future protocol versions to add new fields
            idx += 1

    # records: not yet seen
    raise ValueError("records: not found")


# ---------------------------------------------------------------------------
# Field encoding helpers
# ---------------------------------------------------------------------------

def _has_unsafe(s: str) -> bool:
    if not _UNSAFE_CHARS.isdisjoint(s):
        return True
    return "\n" in s or "\r" in s


def _encode_field(v: Any) -> str:
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
        if "|" in v or '"' in v or "\\" in v or "\n" in v or "\r" in v:
            safe = (v
                    .replace("\\", "\\\\")
                    .replace('"', '""')
                    .replace("\n", "\\n")
                    .replace("\r", "\\r"))
            return '"' + safe + '"'
        return v
    s = str(v)
    if "|" in s or '"' in s or "\\" in s or "\n" in s or "\r" in s:
        safe = (s
                .replace("\\", "\\\\")
                .replace('"', '""')
                .replace("\n", "\\n")
                .replace("\r", "\\r"))
        return '"' + safe + '"'
    return s


def _decode_field(tok: str, type_char: str) -> Any:
    if tok == NULL_TOK:   return None
    if tok == TRUE_TOK:   return True
    if tok == FALSE_TOK:  return False
    if tok == EMPTY_TOK:  return ""
    if tok.startswith('"') and tok.endswith('"') and len(tok) >= 2:
        inner = tok[1:-1]
        inner = inner.replace("\\\\", "\\")
        inner = inner.replace("\\n", "\n").replace("\\r", "\r")
        inner = inner.replace('""', '"')
        return inner
    if type_char == "d":
        try:
            return int(tok)
        except ValueError:
            raise ValueError(f"Invalid int token: {tok!r}")
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
# Row splitting (pipe-aware, RFC 4180 quoting)
# ---------------------------------------------------------------------------

def _split_row(line: str) -> list[str]:
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
                    if i + 1 < n and line[i + 1] == '"':
                        buf.append('"'); i += 2
                    else:
                        i += 1; break
                else:
                    buf.append(c); i += 1
            raw = "".join(buf).replace('"', '""')
            fields.append('"' + raw + '"')
            if i < n and line[i] == SEP:
                i += 1
        else:
            j = line.find(SEP, i)
            if j == -1:
                fields.append(line[i:]); break
            fields.append(line[i:j]); i = j + 1
            if i == n:
                fields.append("")
    return fields


# ---------------------------------------------------------------------------
# Single record encode / decode
# ---------------------------------------------------------------------------

def encode_agent_record(
    rec: dict,
    meta_fields: tuple = (),
) -> str:
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
    for mf in meta_fields:
        parts.append(_encode_field(rec.get(mf)))
    return SEP.join(parts)


def decode_agent_record(
    line: str,
    meta_fields: tuple = (),
) -> dict:
    fields = _split_row(line)
    if not fields:
        raise ValueError("Empty row")

    rtype = fields[0]
    if rtype not in _SCHEMAS:
        raise ValueError(f"Unknown record type: {rtype!r}")

    base_count = FIELD_COUNTS[rtype]
    meta_count = len(meta_fields)
    expected   = base_count + meta_count

    if len(fields) != expected:
        raise ValueError(
            f"Row type {rtype!r} expects {expected} fields, "
            f"got {len(fields)}: {line!r}"
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
            raise ValueError(
                f"Required field {fname!r} is null in row {line!r}"
            )
        try:
            rec[fname] = _decode_field(tok, ftype)
        except (ValueError, IndexError) as e:
            raise ValueError(
                f"Field {fname!r} (type={ftype}) error in row {line!r}: {e}"
            ) from e

    meta_offset = base_count
    for i, mf in enumerate(meta_fields):
        tok = fields[meta_offset + i]
        if mf == "priority":
            rec[mf] = _decode_field(tok, "d")
        else:
            rec[mf] = _decode_field(tok, "s")

    return rec


# ---------------------------------------------------------------------------
# Payload encode / decode
# ---------------------------------------------------------------------------

def encode_agent_payload(
    records: list[dict],
    thread_id: str | None = None,
    context_window: int | None = None,
    meta_fields: tuple = (),
    auto_context: bool = True,
    enforce_budget: bool = False,
    payload_id: str | None = None,
    parent_payload_id: str | None = None,
    agent_id: str | None = None,
    session_id: str | None = None,
    schema_version: str | None = None,
    auto_payload_id: bool = False,
) -> str:
    """
    Encode a list of agent record dicts to a complete ULMEN-AGENT v1 payload.

    Parameters
    ----------
    records           : list of record dicts
    thread_id         : optional thread identifier
    context_window    : optional context window size in tokens
    meta_fields       : tuple of meta field names to include on every row
    auto_context      : compute context_used automatically when True
    enforce_budget    : raise ContextBudgetExceededError if over context_window
    payload_id        : unique ID for this payload (for chain protocol)
    parent_payload_id : ID of prior payload in chain (for unlimited context)
    agent_id          : ID of the producing agent
    session_id        : session this payload belongs to
    schema_version    : protocol version for negotiation
    auto_payload_id   : generate a UUID payload_id automatically when True
    """
    h = AgentHeader()
    h.thread_id         = thread_id
    h.context_window    = context_window
    h.meta_fields       = meta_fields
    h.record_count      = len(records)
    h.payload_id        = (str(uuid.uuid4()) if auto_payload_id else payload_id)
    h.parent_payload_id = parent_payload_id
    h.agent_id          = agent_id
    h.session_id        = session_id
    h.schema_version    = schema_version

    data_lines = []
    for rec in records:
        data_lines.append(encode_agent_record(rec, meta_fields=meta_fields))

    if auto_context and context_window is not None:
        from ulmen.core._utils import estimate_tokens
        body = "\n".join(data_lines)
        h.context_used = estimate_tokens(body)

        if enforce_budget and h.context_used > context_window:
            raise ContextBudgetExceededError(context_window, h.context_used)

    lines = [AGENT_MAGIC] + h.encode_lines() + data_lines
    return "\n".join(lines) + "\n"


def decode_agent_payload(text: str) -> list[dict]:
    records, _ = decode_agent_payload_full(text)
    return records


def decode_agent_payload_full(text: str) -> tuple[list[dict], AgentHeader]:
    lines = text.split("\n")
    while lines and lines[-1] == "":
        lines = lines[:-1]

    if len(lines) < 2:
        raise ValueError("Payload too short: missing header lines")

    if lines[0] != AGENT_MAGIC:
        raise ValueError(
            f"Bad magic: expected {AGENT_MAGIC!r}, got {lines[0]!r}"
        )

    try:
        header, consumed = _parse_header(lines[1:])
    except ValueError:
        raise
    data_start = 1 + consumed

    if header.record_count == 0 and data_start >= len(lines):
        return [], header

    data_lines  = lines[data_start:]
    actual_n    = len(data_lines)
    declared_n  = header.record_count

    if actual_n != declared_n:
        raise ValueError(
            f"Record count mismatch: declared {declared_n}, found {actual_n}"
        )

    records = []
    for i, line in enumerate(data_lines):
        if not line:
            raise ValueError(f"Blank line at data row {i + 1}")
        try:
            records.append(
                decode_agent_record(line, meta_fields=header.meta_fields)
            )
        except ValueError as e:
            raise ValueError(f"Row {i + 1}: {e}") from e

    return records, header


# ---------------------------------------------------------------------------
# Streaming decoder
# ---------------------------------------------------------------------------

def decode_agent_stream(lines: Iterator[str]) -> Iterator[dict]:
    """
    Stream-decode a ULMEN-AGENT v1 payload one record at a time.
    Header is buffered until records: line is found, then data rows stream.
    Unknown header lines are silently ignored (forward compatible).
    """
    header: AgentHeader | None = None
    seen     = 0
    expected = None
    raw_header_lines: list[str] = []

    for raw_line in lines:
        line = raw_line.rstrip("\n").rstrip("\r")

        if header is None:
            if not raw_header_lines:
                if line != AGENT_MAGIC:
                    raise ValueError(
                        f"Bad magic: expected {AGENT_MAGIC!r}, got {line!r}"
                    )
                raw_header_lines.append(line)
                continue

            raw_header_lines.append(line)

            try:
                h, consumed = _parse_header(raw_header_lines[1:])
            except ValueError as e:
                if "records: not found" in str(e):
                    continue
                raise

            header   = h
            expected = h.record_count
            continue

        if not line:
            continue

        try:
            rec = decode_agent_record(line, meta_fields=header.meta_fields)
        except ValueError as e:
            raise ValueError(f"Row {seen + 1}: {e}") from e

        seen += 1
        yield rec

        if expected is not None and seen >= expected:
            return


# ---------------------------------------------------------------------------
# Validation — returns (bool, str | ValidationError)
# ---------------------------------------------------------------------------

def validate_agent_payload(
    text: str,
    structured: bool = False,
) -> tuple[bool, str | ValidationError | None]:
    """
    Validate a ULMEN-AGENT v1 payload.

    Parameters
    ----------
    text       : payload string to validate
    structured : when True return ValidationError object instead of string

    Returns
    -------
    (True, None)             on success
    (False, str)             on failure when structured=False
    (False, ValidationError) on failure when structured=True
    """
    try:
        records, header = decode_agent_payload_full(text)
    except ValueError as e:
        err_str = str(e)
        if structured:
            return False, ValidationError(
                message=err_str,
                suggestion="Check payload structure and header lines",
            )
        return False, err_str

    thread_steps: dict[str, int]        = {}
    tool_ids:     set[str]              = set()
    res_ids:      list[tuple[str, str]] = []

    for i, rec in enumerate(records):
        tid   = rec.get("thread_id", "")
        step  = rec.get("step", 0)
        rtype = rec["type"]
        rid   = rec.get("id", "")

        if not tid:
            ve = ValidationError(
                message=f"Row {i + 1}: thread_id is empty",
                row=i + 1, field="thread_id",
                expected="non-empty string", got="empty string",
                suggestion="Set thread_id to a unique thread identifier",
            )
            return False, ve if structured else str(ve)

        if not rid:
            ve = ValidationError(
                message=f"Row {i + 1}: id is empty",
                row=i + 1, field="id",
                expected="non-empty string", got="empty string",
                suggestion="Set id to a unique record identifier",
            )
            return False, ve if structured else str(ve)

        if not isinstance(step, int) or step < 1:
            ve = ValidationError(
                message=f"Row {i + 1}: step must be positive integer, got {step!r}",
                row=i + 1, field="step",
                expected="positive integer >= 1", got=repr(step),
                suggestion="step starts at 1 and increments monotonically",
            )
            return False, ve if structured else str(ve)

        prev = thread_steps.get(tid, 0)
        if step < prev:
            ve = ValidationError(
                message=(
                    f"Row {i + 1}: step {step} is less than "
                    f"previous step {prev} in thread {tid!r}"
                ),
                row=i + 1, field="step",
                expected=f">= {prev}", got=str(step),
                suggestion="steps must be non-decreasing within a thread",
            )
            return False, ve if structured else str(ve)
        thread_steps[tid] = step

        if rtype == "tool":
            tool_ids.add(rid)
        if rtype == "res":
            res_ids.append((rid, f"row {i + 1}"))

        for field_key, valid_vals in _ENUMS.items():
            etype, fname = field_key.split(".", 1)
            if etype == rtype and fname in rec:
                val = rec[fname]
                if val is not None:
                    tok = (
                        TRUE_TOK  if val is True  else
                        FALSE_TOK if val is False else
                        str(val)
                    )
                    if tok not in valid_vals:
                        ve = ValidationError(
                            message=(
                                f"Row {i + 1}: field {fname!r} value {val!r} "
                                f"not in {sorted(valid_vals)}"
                            ),
                            row=i + 1, field=fname,
                            expected=str(sorted(valid_vals)), got=repr(val),
                            suggestion=(
                                f"Use one of: {', '.join(sorted(valid_vals))}"
                            ),
                        )
                        return False, ve if structured else str(ve)

    for res_id, loc in res_ids:
        if res_id not in tool_ids:
            ve = ValidationError(
                message=f"{loc}: res id {res_id!r} has no matching tool row",
                field="id",
                expected=f"tool row with id={res_id!r}",
                got="no matching tool row",
                suggestion=(
                    f"Add a tool row with id={res_id!r} before the res row, "
                    f"or fix the res id to match an existing tool"
                ),
            )
            return False, ve if structured else str(ve)

    return True, None if structured else ""


# ---------------------------------------------------------------------------
# Chunked payload — unlimited context window
# ---------------------------------------------------------------------------

def chunk_payload(
    records: list[dict],
    token_budget: int,
    thread_id: str | None = None,
    meta_fields: tuple = (),
    overlap: int = 0,
    parent_payload_id: str | None = None,
    session_id: str | None = None,
) -> list[str]:
    """
    Split a large list of records into multiple payloads each fitting
    within token_budget. Enables unlimited context window by chaining
    payloads via parent_payload_id.

    tool+res pairs are kept atomic: a tool record and its matching res
    record are always placed in the same chunk so every chunk passes
    validate_agent_payload independently.

    Parameters
    ----------
    records           : full list of records to chunk
    token_budget      : max tokens per chunk (uses estimate_tokens)
    thread_id         : thread_id written to every chunk header
    meta_fields       : meta fields passed through to every chunk
    overlap           : number of atomic units to repeat at start of
                        next chunk for context continuity (default 0)
    parent_payload_id : payload_id to set as parent of the first chunk
    session_id        : session_id written to every chunk header

    Returns
    -------
    List of payload strings. Each payload is a valid ULMEN-AGENT v1
    payload. Payloads are linked via payload_id / parent_payload_id.
    """
    from ulmen.core._utils import estimate_tokens

    if not records:
        return [encode_agent_payload(
            [], thread_id=thread_id, context_window=token_budget,
            meta_fields=meta_fields, session_id=session_id,
            auto_payload_id=True,
        )]

    # ------------------------------------------------------------------
    # Step 1: group records into atomic units.
    # A unit is either:
    #   - a solo record (msg, plan, obs, err, mem, rag, hyp, cot)
    #   - a tool record bundled with its matching res record(s)
    #
    # Single forward pass: when we see a tool, we open a unit.
    # When we see a res whose id matches a pending tool, we close
    # the pair into one unit. Unmatched res records are solo.
    # ------------------------------------------------------------------

    pending_tools: dict[str, list[int]] = {}   # tool_id -> list of unit indices
    units: list[list[dict]] = []

    for rec in records:
        rtype = rec.get("type")
        rid   = rec.get("id", "")

        if rtype == "tool":
            unit_idx = len(units)
            units.append([rec])
            pending_tools.setdefault(rid, []).append(unit_idx)

        elif rtype == "res" and rid in pending_tools and pending_tools[rid]:
            # Attach res to its matching tool unit
            unit_idx = pending_tools[rid].pop(0)
            units[unit_idx].append(rec)
            if not pending_tools[rid]:
                del pending_tools[rid]

        else:
            units.append([rec])

    # ------------------------------------------------------------------
    # Step 2: token cost per atomic unit
    # ------------------------------------------------------------------

    def _unit_tokens(unit: list[dict]) -> int:
        return sum(
            estimate_tokens(encode_agent_record(r, meta_fields=meta_fields))
            for r in unit
        )

    unit_costs = [_unit_tokens(u) for u in units]

    # ------------------------------------------------------------------
    # Step 3: header overhead
    # ------------------------------------------------------------------

    header_overhead = estimate_tokens(
        f"{AGENT_MAGIC}\nthread: {thread_id or ''}\n"
        f"context_window: {token_budget}\nrecords: 9999\n"
    )
    effective_budget = max(10, token_budget - header_overhead)

    # ------------------------------------------------------------------
    # Step 4: pack units into chunks greedily
    # ------------------------------------------------------------------

    def _flush(batch_units: list[list[dict]], prev_id: str | None) -> tuple[str, str]:
        flat = [r for unit in batch_units for r in unit]
        pid  = str(uuid.uuid4())
        payload = encode_agent_payload(
            flat,
            thread_id=thread_id,
            context_window=token_budget,
            meta_fields=meta_fields,
            auto_context=True,
            payload_id=pid,
            parent_payload_id=prev_id,
            session_id=session_id,
        )
        return payload, pid

    chunks: list[str]   = []
    current_units: list = []
    current_tokens: int = 0
    prev_payload_id     = parent_payload_id

    for unit, cost in zip(units, unit_costs):
        if current_units and current_tokens + cost > effective_budget:
            payload, prev_payload_id = _flush(current_units, prev_payload_id)
            chunks.append(payload)
            if overlap > 0:
                current_units  = current_units[-overlap:]
                current_tokens = sum(_unit_tokens(u) for u in current_units)
            else:
                current_units  = []
                current_tokens = 0

        current_units.append(unit)
        current_tokens += cost

    if current_units:
        payload, _ = _flush(current_units, prev_payload_id)
        chunks.append(payload)

    return chunks



def merge_chunks(payloads: list[str]) -> list[dict]:
    """
    Decode and merge a list of chunked ULMEN-AGENT payloads back into
    a single flat list of records.

    Payloads are merged in the order provided. Records from overlapping
    chunks are deduplicated by (id, thread_id, step).

    Parameters
    ----------
    payloads : list of ULMEN-AGENT v1 payload strings

    Returns
    -------
    Flat list of unique records in order
    """
    seen: set[tuple] = set()
    result: list[dict] = []

    for payload in payloads:
        records, _ = decode_agent_payload_full(payload)
        for rec in records:
            key = (rec.get("id"), rec.get("thread_id"), rec.get("step"))
            if key not in seen:
                seen.add(key)
                result.append(rec)

    return result


# ---------------------------------------------------------------------------
# Summary chain — compressed history for unlimited context
# ---------------------------------------------------------------------------

def build_summary_chain(
    records: list[dict],
    token_budget: int,
    thread_id: str | None = None,
    meta_fields: tuple = (),
    session_id: str | None = None,
) -> list[str]:
    """
    Build a chain of payloads where older records are progressively
    compressed into mem summary records. Enables unlimited context by
    keeping recent records verbatim and summarizing history.

    The chain is structured as:
        payload_1: summary of oldest records
        payload_2: summary of next batch (parent=payload_1)
        ...
        payload_N: most recent records verbatim (parent=payload_N-1)

    Each payload in the chain is independently valid ULMEN-AGENT v1.

    Parameters
    ----------
    records      : full conversation history
    token_budget : max tokens per payload
    thread_id    : thread_id for all payloads
    meta_fields  : meta fields for all records
    session_id   : session_id for all payloads

    Returns
    -------
    List of payload strings forming the chain.
    Feed the LAST payload to the LLM — it contains the full context
    via parent_payload_id references to prior summaries.
    """
    from ulmen.core._utils import estimate_tokens

    if not records:
        return []

    # Estimate how many records fit in one payload
    sample_row = encode_agent_record(records[0], meta_fields=meta_fields)
    row_tokens = max(1, estimate_tokens(sample_row))
    header_overhead = 20  # conservative estimate
    records_per_chunk = max(1, (token_budget - header_overhead) // row_tokens)

    if len(records) <= records_per_chunk:
        return [encode_agent_payload(
            records,
            thread_id=thread_id,
            context_window=token_budget,
            meta_fields=meta_fields,
            auto_context=True,
            auto_payload_id=True,
            session_id=session_id,
        )]

    chain: list[str] = []
    prev_id = None

    # Process in chunks, compressing older ones
    i = 0
    while i < len(records):
        batch = records[i:i + records_per_chunk]
        is_last = (i + records_per_chunk) >= len(records)

        chunk_records = batch if is_last else _summarize_as_mem(batch)

        pid = str(uuid.uuid4())
        payload = encode_agent_payload(
            chunk_records,
            thread_id=thread_id,
            context_window=token_budget,
            meta_fields=meta_fields,
            auto_context=True,
            payload_id=pid,
            parent_payload_id=prev_id,
            session_id=session_id,
        )
        chain.append(payload)
        prev_id = pid
        i += records_per_chunk

    return chain


# ---------------------------------------------------------------------------
# Context compression
# ---------------------------------------------------------------------------

def compress_context(
    records: list[dict],
    strategy: str = COMPRESS_COMPLETED_SEQUENCES,
    keep_priority: int = PRIORITY_KEEP_IF_ROOM,
    target_reduction: float = 0.5,
    keep_types: list[str] | None = None,
    window_size: int | None = None,
    preserve_cot: bool = False,
) -> list[dict]:
    """
    Compress a list of agent records to reduce context window usage.

    Parameters
    ----------
    records          : input records
    strategy         : one of the COMPRESS_* constants
    keep_priority    : records with priority <= this are never compressed
    target_reduction : target fraction to remove (informational)
    keep_types       : for keep_types strategy
    window_size      : for sliding_window strategy
    preserve_cot     : when True, cot records are converted to mem instead
                       of dropped — preserves reasoning trace losslessly
    """
    if not records:
        return []

    if strategy == COMPRESS_COMPLETED_SEQUENCES:
        return _compress_completed_sequences(records, keep_priority, preserve_cot)

    if strategy == COMPRESS_KEEP_TYPES:
        kt = set(keep_types or ["msg", "err", "mem"])
        return [r for r in records if r.get("type") in kt or
                _rec_priority(r) <= keep_priority]

    if strategy == COMPRESS_SLIDING_WINDOW:
        ws = window_size or max(10, len(records) // 4)
        if len(records) <= ws:
            return list(records)
        recent  = records[-ws:]
        earlier = records[:-ws]
        summary = _summarize_as_mem(earlier)
        return summary + recent

    raise ValueError(f"Unknown compression strategy: {strategy!r}")


def _rec_priority(rec: dict) -> int:
    p = rec.get("priority")
    if p is None:
        return PRIORITY_COMPRESSIBLE
    try:
        return int(p)
    except (TypeError, ValueError):
        return PRIORITY_COMPRESSIBLE


def _compress_completed_sequences(
    records: list[dict],
    keep_priority: int,
    preserve_cot: bool = False,
) -> list[dict]:
    """
    Replace completed tool+res sequences with mem records.
    When preserve_cot=True, cot records are converted to mem instead of dropped.
    """
    tool_by_id: dict[str, dict] = {}
    res_by_tool_id: dict[str, dict] = {}
    for rec in records:
        if rec.get("type") == "tool":
            tool_by_id[rec["id"]] = rec
        if rec.get("type") == "res":
            res_by_tool_id[rec["id"]] = rec

    completed_tool_ids = set(tool_by_id.keys()) & set(res_by_tool_id.keys())

    result = []
    compressed_tool_ids: set[str] = set()
    seq_counter = 0
    cot_counter = 0

    for rec in records:
        rtype    = rec.get("type")
        rid      = rec.get("id", "")
        priority = _rec_priority(rec)

        if priority <= keep_priority:
            result.append(rec)
            continue

        if rtype in ("msg", "plan", "obs", "err", "mem", "hyp", "rag"):
            result.append(rec)
            continue

        if rtype == "tool" and rid in completed_tool_ids:
            if rid in compressed_tool_ids:
                continue
            compressed_tool_ids.add(rid)
            res_rec     = res_by_tool_id[rid]
            seq_counter += 1
            mem_rec = {
                "type":       "mem",
                "id":         f"mem_cmp_{seq_counter:03d}",
                "thread_id":  rec.get("thread_id", ""),
                "step":       rec.get("step", 0),
                "key":        f"tool_result_{rec.get('name', rid)}",
                "value":      str(res_rec.get("data", ""))[:500],
                "confidence": 1.0,
                "ttl":        None,
            }
            for mf in META_FIELDS:
                if mf in rec:
                    mem_rec[mf] = rec[mf]
            result.append(mem_rec)
            continue

        if rtype == "res" and rid in completed_tool_ids:
            continue

        if rtype == "cot":
            if preserve_cot:
                # Convert cot to mem to preserve reasoning trace
                cot_counter += 1
                mem_rec = {
                    "type":       "mem",
                    "id":         f"mem_cot_{cot_counter:03d}",
                    "thread_id":  rec.get("thread_id", ""),
                    "step":       rec.get("step", 0),
                    "key":        f"cot_{rec.get('cot_type', 'step')}_{rec.get('index', cot_counter)}",
                    "value":      str(rec.get("text", ""))[:500],
                    "confidence": float(rec.get("confidence", 1.0)),
                    "ttl":        None,
                }
                for mf in META_FIELDS:
                    if mf in rec:
                        mem_rec[mf] = rec[mf]
                result.append(mem_rec)
            continue

        result.append(rec)

    return result


def _summarize_as_mem(records: list[dict]) -> list[dict]:
    by_thread: dict[str, list[dict]] = {}
    for rec in records:
        tid = rec.get("thread_id", "")
        by_thread.setdefault(tid, []).append(rec)

    result = []
    for tid, recs in by_thread.items():
        types    = list({r["type"] for r in recs})
        max_step = max((r.get("step", 0) for r in recs), default=0)
        summary  = (
            f"Compressed {len(recs)} records "
            f"(types: {','.join(sorted(types))}) "
            f"up to step {max_step}"
        )
        result.append({
            "type":       "mem",
            "id":         f"mem_summary_{tid}",
            "thread_id":  tid,
            "step":       max_step,
            "key":        f"context_summary_{tid}",
            "value":      summary,
            "confidence": 0.9,
            "ttl":        None,
        })
    return result


# ---------------------------------------------------------------------------
# Memory deduplication
# ---------------------------------------------------------------------------

def get_latest_mem(records: list[dict], key: str) -> dict | None:
    """
    Return the most recent mem record with the given key, or None.
    "Most recent" means highest step value among mem records with that key.
    """
    best: dict | None = None
    best_step = -1
    for rec in records:
        if rec.get("type") == "mem" and rec.get("key") == key:
            step = rec.get("step", 0)
            if step > best_step:
                best_step = step
                best = rec
    return best


def dedup_mem(records: list[dict]) -> list[dict]:
    """
    Deduplicate mem records by key — keep only the most recent value
    per key per thread. All non-mem records are preserved unchanged.

    Returns a new list. Original is not modified.
    """
    # Find the winning mem record per (thread_id, key)
    best_mem: dict[tuple, dict] = {}
    for rec in records:
        if rec.get("type") == "mem":
            k = (rec.get("thread_id", ""), rec.get("key", ""))
            existing = best_mem.get(k)
            if existing is None or rec.get("step", 0) > existing.get("step", 0):
                best_mem[k] = rec

    winning_ids = {id(v) for v in best_mem.values()}

    result = []
    for rec in records:
        if rec.get("type") == "mem":
            if id(rec) in winning_ids:
                result.append(rec)
        else:
            result.append(rec)
    return result


# ---------------------------------------------------------------------------
# Context token estimation
# ---------------------------------------------------------------------------

def estimate_context_usage(records: list[dict], meta_fields: tuple = ()) -> dict:
    from ulmen.core._utils import estimate_tokens

    by_type: dict[str, int] = {}
    total_chars = 0

    for rec in records:
        row   = encode_agent_record(rec, meta_fields=meta_fields)
        chars = len(row)
        total_chars += chars
        rtype = rec.get("type", "unknown")
        by_type[rtype] = by_type.get(rtype, 0) + estimate_tokens(row)

    return {
        "rows":    len(records),
        "chars":   total_chars,
        "tokens":  estimate_tokens("x" * total_chars),
        "by_type": by_type,
    }


# ---------------------------------------------------------------------------
# Programmatic system prompt generator
# ---------------------------------------------------------------------------

def generate_system_prompt(
    include_examples: bool = True,
    include_validation: bool = True,
) -> str:
    """
    Generate the ULMEN-AGENT v1 system prompt programmatically from
    the live schema. Always reflects the current record types and fields.

    Parameters
    ----------
    include_examples   : include example payloads in the prompt
    include_validation : include self-check validation rules

    Returns
    -------
    Complete system prompt string ready to inject into LLM system message.
    """
    lines = [
        "You communicate using ULMEN-AGENT v1, a strict typed pipe-delimited format.",
        "Every response you produce (except the final answer to the user) must be",
        "valid ULMEN-AGENT v1. Never produce free-form JSON, XML, or prose for",
        "internal reasoning or tool calls.",
        "",
        "PAYLOAD STRUCTURE",
        "",
        "Every payload starts with a header:",
        f"  {AGENT_MAGIC}",
        "  records: N",
        "",
        "N is the exact number of data rows that follow.",
        "No blank lines. Lines end with newline only.",
        "",
        "ROW FORMAT",
        "",
        "  type|id|thread_id|step|field|field|...",
        "",
        "Common fields on every row:",
        "  id        unique record ID",
        "  thread_id groups all rows in one task",
        "  step      positive integer, non-decreasing within a thread",
        "",
        "RECORD TYPES AND SCHEMAS",
        "",
    ]

    for rtype, schema in sorted(_SCHEMAS.items()):
        fields = ["id", "thread_id", "step"] + [f[0] for f in schema]
        field_str = "|".join(fields)
        lines.append(f"  {rtype}: {rtype}|{field_str}")

        # Add enum constraints
        for fname, ftype, required in schema:
            key = f"{rtype}.{fname}"
            if key in _ENUMS:
                vals = " / ".join(sorted(_ENUMS[key]))
                req  = "required" if required else "optional"
                lines.append(f"    {fname} ({req}): {vals}")
            else:
                type_name = {"s": "string", "d": "integer", "f": "float", "b": "bool"}.get(ftype, ftype)
                req = "required" if required else "optional"
                lines.append(f"    {fname} ({req}): {type_name}")
        lines.append("")

    lines += [
        "VALUE ENCODING",
        "",
        "  null/absent  -> N",
        "  True         -> T",
        "  False        -> F",
        "  empty string -> $0=",
        "  integer      -> 42 or -7",
        "  float        -> 3.14 or nan or inf or -inf",
        "  safe string  -> write as-is",
        '  unsafe string -> wrap in "..." double internal quotes to ""',
        '  unsafe chars: | newline carriage-return backslash quote',
        "",
        "STRICT RULES",
        "",
        "  1. Field count must exactly match the schema for the record type.",
        "  2. Required fields must not be N.",
        "  3. step must be a positive integer, non-decreasing within each thread.",
        "  4. Every res id must match a prior tool id.",
        "  5. records: N must equal the exact number of rows that follow.",
        "  6. No blank lines. No trailing whitespace.",
        "  7. Unknown record types are forbidden.",
        "",
        "WHAT YOU MUST NEVER DO",
        "",
        "  Never produce JSON for tool calls. Use tool rows.",
        "  Never produce free-form prose for reasoning. Use cot rows.",
        "  Never skip the header lines.",
        '  Never produce records: 0 when there are rows.',
        "  Never use tab or semicolon as delimiter. Only pipe.",
        "  Never omit required fields. Use N only for optional fields.",
    ]

    if include_validation:
        lines += [
            "",
            "VALIDATION SELF-CHECK",
            "",
            "  Before outputting, verify:",
            f"  1. Line 1 is exactly: {AGENT_MAGIC}",
            "  2. Line 2 is exactly: records: N where N equals actual row count",
            "  3. Every row has the correct number of pipe-delimited fields",
            "  4. No required field contains N",
            "  5. step is non-decreasing within each thread",
            "  6. Every res id matches a prior tool id",
            "  7. No blank lines",
        ]

    if include_examples:
        lines += [
            "",
            "EXAMPLE",
            "",
            f"  {AGENT_MAGIC}",
            "  records: 3",
            "  msg|msg_001|th_001|1|user|1|What is 2+2?|5|F",
            "  cot|ct_001|th_001|2|1|compute|2+2=4|1.0",
            "  msg|msg_002|th_001|3|assistant|2|The answer is 4.|4|F",
        ]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# ULMEN <-> ULMEN-AGENT bridge
# ---------------------------------------------------------------------------

def convert_agent_to_ulmen(payload: str) -> str:
    """
    Convert a ULMEN-AGENT v1 payload to ULMEN format for LLM consumption.
    The ULMEN output contains all records as typed rows for easy LLM reading.
    """
    from ulmen.core._ulmen_llm import encode_ulmen_llm
    records, _ = decode_agent_payload_full(payload)
    return encode_ulmen_llm(records)


def convert_ulmen_to_agent(
    ulmen: str,
    thread_id: str = "t1",
) -> str:
    """
    Convert a ULMEN payload to ULMEN-AGENT v1 format.
    Records must contain a 'type' field matching a valid ULMEN-AGENT record type.
    Records missing required agent fields are skipped with a warning.

    Parameters
    ----------
    ulmen     : ULMEN payload string
    thread_id : thread_id to assign if records don't have one
    """
    from ulmen.core._ulmen_llm import decode_ulmen_llm
    records = decode_ulmen_llm(ulmen)
    agent_records = []
    step = 1
    for rec in records:
        if not isinstance(rec, dict):
            continue
        rtype = rec.get("type")
        if rtype not in _SCHEMAS:
            continue
        if "thread_id" not in rec or not rec["thread_id"]:
            rec = dict(rec)
            rec["thread_id"] = thread_id
        if "id" not in rec or not rec["id"]:
            rec = dict(rec)
            rec["id"] = f"{rtype}_{step:03d}"
        if "step" not in rec or not rec["step"]:
            rec = dict(rec)
            rec["step"] = step
        step = max(step + 1, rec.get("step", step) + 1)
        agent_records.append(rec)
    return encode_agent_payload(agent_records, thread_id=thread_id)


# ---------------------------------------------------------------------------
# Validation error helper
# ---------------------------------------------------------------------------

def make_validation_error(
    error_msg: str,
    thread_id: str = "INVALID",
) -> str:
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
    records, header = decode_agent_payload_full(text)
    filtered = extract_subgraph(records, thread_id, step_min, step_max, types)
    return encode_agent_payload(
        filtered,
        thread_id=header.thread_id,
        context_window=header.context_window,
        meta_fields=header.meta_fields,
        auto_context=header.context_window is not None,
    )
