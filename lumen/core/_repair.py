"""
LUMEN-AGENT LLM output parser and auto-repair.

parse_llm_output accepts raw text from an LLM (which may have minor
formatting errors) and returns a valid LUMEN-AGENT v1 payload string.

Auto-repair strategies applied in order:
    1. Strip leading/trailing whitespace and markdown fences.
    2. Locate the LUMEN-AGENT v1 magic line.
    3. Fix wrong records: count (off-by-one, under/over count).
    4. Remove blank data lines.
    5. Skip lines with unknown record types.
    6. Re-encode the header with the corrected record count.

The repaired payload is validated with validate_agent_payload before
being returned. If it cannot be repaired, a ValidationError payload
is returned instead.
"""

from __future__ import annotations

AGENT_MAGIC = "LUMEN-AGENT v1"


def _strip_fences(text: str) -> str:
    """Remove markdown code fences (```...```) if present."""
    lines = text.splitlines()
    out   = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("```"):
            continue
        out.append(line)
    return "\n".join(out)


def _find_magic(lines: list[str]) -> int:
    """Return the index of the LUMEN-AGENT v1 magic line, or -1."""
    for i, line in enumerate(lines):
        if line.strip() == AGENT_MAGIC:
            return i
    return -1


def _repair_record_count(header_lines: list[str], data_lines: list[str]) -> list[str]:
    """
    Fix the records: N line to match the actual number of data lines.
    Returns updated header lines.
    """
    actual = len(data_lines)
    new_header = []
    found = False
    for line in header_lines:
        if line.startswith("records: "):
            new_header.append(f"records: {actual}")
            found = True
        else:
            new_header.append(line)
    if not found:
        new_header.append(f"records: {actual}")
    return new_header


def _is_data_line(line: str) -> bool:
    """Return True if line looks like a LUMEN-AGENT data row."""
    from lumen.core._agent import RECORD_TYPES
    if not line or line.startswith("thread: ") or line.startswith("context"):
        return False
    parts = line.split("|", 1)
    return parts[0].strip() in RECORD_TYPES


def _is_header_line(line: str) -> bool:
    """Return True if line is a known header line."""
    prefixes = (
        "thread: ", "context_window: ", "context_used: ",
        "payload_id: ", "parent_payload_id: ", "agent_id: ",
        "session_id: ", "schema_version: ", "meta: ", "records: ",
    )
    return any(line.startswith(p) for p in prefixes)


def parse_llm_output(
    raw_text: str,
    thread_id: str | None = None,
    strict: bool = False,
) -> str:
    """
    Parse raw LLM output and return a valid LUMEN-AGENT v1 payload.

    Applies auto-repair for common LLM output errors:
    - Markdown fences stripped
    - Magic line located even if not on line 1
    - Wrong records: count corrected
    - Blank lines removed
    - Lines with unknown record types skipped
    - Header reconstructed with correct count

    Parameters
    ----------
    raw_text  : raw text output from an LLM
    thread_id : override thread_id for all records (optional)
    strict    : if True, raise ValueError instead of returning error payload

    Returns
    -------
    Valid LUMEN-AGENT v1 payload string.
    If repair fails and strict=False, returns a validation error payload.
    If repair fails and strict=True, raises ValueError.
    """
    from lumen.core._agent import (
        encode_agent_payload,
        make_validation_error,
        validate_agent_payload,
    )

    # Step 1: strip markdown fences
    cleaned = _strip_fences(raw_text).strip()

    # Step 2: find magic line
    lines   = cleaned.splitlines()
    magic_i = _find_magic(lines)

    if magic_i == -1:
        msg = f"No '{AGENT_MAGIC}' magic line found in LLM output"
        if strict:
            raise ValueError(msg)
        return make_validation_error(msg, thread_id=thread_id or "REPAIR")

    # Discard everything before magic
    lines = lines[magic_i:]

    # Step 3: separate header from data
    header_lines: list[str] = []
    data_lines:   list[str] = []
    past_records  = False

    for line in lines[1:]:  # skip magic itself
        stripped = line.strip()
        if not stripped:
            continue  # drop blank lines
        if not past_records and _is_header_line(stripped):
            header_lines.append(stripped)
            if stripped.startswith("records: "):
                past_records = True
        elif _is_data_line(stripped):
            data_lines.append(stripped)
        elif not past_records:
            # Unknown header line — keep for forward compat
            header_lines.append(stripped)

    # Step 4: fix record count
    header_lines = _repair_record_count(header_lines, data_lines)

    # Step 5: reassemble
    repaired = "\n".join([AGENT_MAGIC] + header_lines + data_lines) + "\n"

    # Step 6: validate
    ok, err = validate_agent_payload(repaired)
    if ok:
        return repaired

    # Step 7: last-resort — try to decode individual rows and re-encode
    from lumen.core._agent import decode_agent_record
    good_records = []
    meta_fields  = ()

    # Extract meta_fields from header if present
    for hl in header_lines:
        if hl.startswith("meta: "):
            raw = hl[6:].strip()
            meta_fields = tuple(f.strip() for f in raw.split(",") if f.strip())

    for row_line in data_lines:
        try:
            rec = decode_agent_record(row_line, meta_fields=meta_fields)
            good_records.append(rec)
        except ValueError:
            continue  # skip malformed rows

    if not good_records:
        msg = f"Could not repair LLM output: {err}"
        if strict:
            raise ValueError(msg)
        return make_validation_error(str(err), thread_id=thread_id or "REPAIR")

    try:
        result = encode_agent_payload(
            good_records,
            thread_id=thread_id,
            meta_fields=meta_fields,
        )
        ok2, err2 = validate_agent_payload(result)
        if ok2:
            return result
        msg = f"Repair produced invalid payload: {err2}"
        if strict:
            raise ValueError(msg)
        return make_validation_error(str(err2), thread_id=thread_id or "REPAIR")
    except Exception as e:
        msg = f"Repair failed: {e}"
        if strict:
            raise ValueError(msg)
        return make_validation_error(msg, thread_id=thread_id or "REPAIR")
