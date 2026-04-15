# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
#
# This file is part of ULMEN.
# ULMEN is licensed under the Business Source License 1.1.
# See the LICENSE file in the project root for full license information.

"""
ULMEN-AGENT append-only replay log.

ReplayLog provides an immutable audit trail of agent payloads. Once
a payload is appended it cannot be modified or removed. The log can
be replayed from any position to reconstruct agent state.
"""

from __future__ import annotations

import time
from typing import Iterator


class ReplayLog:
    """
    Append-only audit trail for ULMEN-AGENT payloads.

    Each entry records the payload string, a monotonically increasing
    sequence number, a wall-clock timestamp, and optional metadata.

    The log is stored in memory. Use save() / load() for persistence.

    Usage
    -----
    log = ReplayLog()
    log.append(payload_str, meta={"source": "agent_a"})
    log.append(payload_str2)

    for entry in log.replay():
        print(entry["seq"], entry["payload"][:40])

    since = log.replay_from(seq=2)
    """

    def __init__(self, name: str = "default") -> None:
        self._name:    str          = name
        self._entries: list[dict]   = []
        self._seq:     int          = 0

    # ------------------------------------------------------------------
    # Mutation — append only
    # ------------------------------------------------------------------

    def append(
        self,
        payload: str,
        meta: dict | None = None,
    ) -> int:
        """
        Append a payload to the log.

        Parameters
        ----------
        payload : ULMEN-AGENT v1 payload string
        meta    : optional dict of metadata (e.g. source agent, timestamp)

        Returns
        -------
        Sequence number assigned to this entry (1-based).
        """
        self._seq += 1
        entry = {
            "seq":       self._seq,
            "timestamp": time.time(),
            "payload":   payload,
            "meta":      dict(meta) if meta else {},
        }
        self._entries.append(entry)
        return self._seq

    # ------------------------------------------------------------------
    # Replay
    # ------------------------------------------------------------------

    def replay(self) -> Iterator[dict]:
        """Yield all log entries in order from oldest to newest."""
        yield from self._entries

    def replay_from(self, seq: int) -> Iterator[dict]:
        """
        Yield all entries with seq >= the given sequence number.

        Parameters
        ----------
        seq : starting sequence number (inclusive)
        """
        for entry in self._entries:
            if entry["seq"] >= seq:
                yield entry

    def replay_decoded(self) -> Iterator[tuple[dict, list[dict]]]:
        """
        Yield (entry, records) pairs for every log entry.
        Records are decoded from each payload on demand.
        """
        from ulmen.core._agent import decode_agent_payload_full
        for entry in self._entries:
            try:
                records, _ = decode_agent_payload_full(entry["payload"])
            except ValueError:
                records = []
            yield entry, records

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._entries)

    @property
    def latest_seq(self) -> int:
        """Sequence number of the most recently appended entry, or 0."""
        return self._seq

    @property
    def name(self) -> str:
        return self._name

    def get(self, seq: int) -> dict | None:
        """Return the entry with the given sequence number, or None."""
        for entry in self._entries:
            if entry["seq"] == seq:
                return entry
        return None

    def all_records(self) -> list[dict]:
        """
        Decode and merge all records across all log entries.
        Deduplicated by (id, thread_id, step).
        """
        from ulmen.core._agent import decode_agent_payload_full
        seen:   set[tuple]  = set()
        result: list[dict]  = []
        for entry in self._entries:
            try:
                records, _ = decode_agent_payload_full(entry["payload"])
            except ValueError:
                continue
            for rec in records:
                key = (rec.get("id"), rec.get("thread_id"), rec.get("step"))
                if key not in seen:
                    seen.add(key)
                    result.append(rec)
        return result

    # ------------------------------------------------------------------
    # Serialisation — zero-dep, newline-delimited payload blocks
    # ------------------------------------------------------------------

    def save(self, path: str) -> None:
        """
        Save the log to a file as newline-delimited base64-encoded entries.
        Zero external dependencies — uses only stdlib base64.
        """
        import base64
        import json
        with open(path, "w", encoding="utf-8") as f:
            for entry in self._entries:
                rec = {
                    "seq":       entry["seq"],
                    "timestamp": entry["timestamp"],
                    "meta":      entry["meta"],
                    "payload":   base64.b64encode(
                        entry["payload"].encode("utf-8")
                    ).decode("ascii"),
                }
                f.write(json.dumps(rec, separators=(",", ":")) + "\n")

    @classmethod
    def load(cls, path: str, name: str = "loaded") -> ReplayLog:
        """Load a previously saved ReplayLog from a file."""
        import base64
        import json
        log = cls(name=name)
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec                = json.loads(line)
                entry              = {
                    "seq":       rec["seq"],
                    "timestamp": rec["timestamp"],
                    "meta":      rec.get("meta", {}),
                    "payload":   base64.b64decode(rec["payload"]).decode("utf-8"),
                }
                log._entries.append(entry)
                log._seq = max(log._seq, rec["seq"])
        return log

    def __repr__(self) -> str:
        return f"ReplayLog(name={self._name!r}, entries={len(self._entries)}, latest_seq={self._seq})"
