# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
#
# This file is part of ULMEN.
# ULMEN is licensed under the Business Source License 1.1.
# See the LICENSE file in the project root for full license information.

"""
ULMEN-AGENT cross-payload thread persistence.

ThreadRegistry tracks records across multiple payloads, keyed by
(session_id, thread_id). merge_threads merges records from multiple
agent payloads into a single deduplicated ordered list.
"""

from __future__ import annotations


class ThreadRegistry:
    """
    Persistent registry of agent records across multiple payloads.

    Records are stored keyed by (session_id, thread_id) and deduplicated
    by (id, thread_id, step). The registry is append-only: records are
    never removed, only added.

    Usage
    -----
    registry = ThreadRegistry()
    registry.ingest(payload_str, session_id="sess_1")
    registry.ingest(payload_str2, session_id="sess_1")

    all_recs  = registry.get_thread("t1", session_id="sess_1")
    all_recs  = registry.all_records(session_id="sess_1")
    """

    def __init__(self) -> None:
        # (session_id, thread_id) -> ordered list of records
        self._store: dict[tuple[str, str], list[dict]] = {}
        # dedup key set per session: (session_id, id, thread_id, step)
        self._seen: set[tuple] = set()

    def ingest(self, payload: str, session_id: str = "") -> int:
        """
        Decode and ingest a ULMEN-AGENT v1 payload string.

        Parameters
        ----------
        payload    : ULMEN-AGENT v1 payload string
        session_id : session identifier (groups payloads together)

        Returns
        -------
        Number of new (non-duplicate) records added.
        """
        from ulmen.core._agent import decode_agent_payload_full
        records, _ = decode_agent_payload_full(payload)
        return self._add_records(records, session_id)

    def ingest_records(self, records: list[dict], session_id: str = "") -> int:
        """
        Ingest pre-decoded records directly.

        Returns number of new records added.
        """
        return self._add_records(records, session_id)

    def _add_records(self, records: list[dict], session_id: str) -> int:
        added = 0
        for rec in records:
            tid  = rec.get("thread_id", "")
            key  = (session_id, rec.get("id", ""), tid, rec.get("step", 0))
            if key in self._seen:
                continue
            self._seen.add(key)
            store_key = (session_id, tid)
            if store_key not in self._store:
                self._store[store_key] = []
            self._store[store_key].append(rec)
            added += 1
        return added

    def get_thread(self, thread_id: str, session_id: str = "") -> list[dict]:
        """Return all records for (session_id, thread_id) in ingestion order."""
        return list(self._store.get((session_id, thread_id), []))

    def all_records(self, session_id: str = "") -> list[dict]:
        """Return all records for a session across all threads, in ingestion order."""
        result = []
        for (sid, _tid), recs in self._store.items():
            if sid == session_id:
                result.extend(recs)
        return result

    def thread_ids(self, session_id: str = "") -> list[str]:
        """Return all thread_ids seen for a session."""
        return [tid for (sid, tid) in self._store if sid == session_id]

    def session_ids(self) -> list[str]:
        """Return all session_ids seen."""
        return list({sid for (sid, _) in self._store})

    @property
    def total_records(self) -> int:
        """Total number of unique records stored across all sessions and threads."""
        return len(self._seen)

    def __repr__(self) -> str:
        return (
            f"ThreadRegistry("
            f"sessions={len(self.session_ids())}, "
            f"threads={len(self._store)}, "
            f"records={self.total_records})"
        )


# ---------------------------------------------------------------------------
# merge_threads — merge records from multiple payloads
# ---------------------------------------------------------------------------

def merge_threads(payloads: list[str], session_id: str = "") -> list[dict]:
    """
    Decode and merge records from multiple ULMEN-AGENT v1 payloads.

    Records are deduplicated by (id, thread_id, step). The merged list
    is ordered by (thread_id, step) for deterministic output.

    Parameters
    ----------
    payloads   : list of ULMEN-AGENT v1 payload strings
    session_id : optional session label (for logging only)

    Returns
    -------
    Flat deduplicated list of records sorted by (thread_id, step).
    """
    registry = ThreadRegistry()
    for payload in payloads:
        registry.ingest(payload, session_id=session_id)

    # Sort by (thread_id, step) for deterministic ordering
    all_recs = registry.all_records(session_id=session_id)
    all_recs.sort(key=lambda r: (r.get("thread_id", ""), r.get("step", 0)))
    return all_recs
