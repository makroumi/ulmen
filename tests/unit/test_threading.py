"""
Unit tests for ulmen/core/_threading.py
Target: 100% coverage of ThreadRegistry and merge_threads.
"""
from ulmen.core._agent import encode_agent_payload
from ulmen.core._threading import ThreadRegistry, merge_threads


def _make_payload(records, thread_id="t1"):
    return encode_agent_payload(records, thread_id=thread_id)


def _msg(mid, tid, step):
    return {
        "type": "msg", "id": mid, "thread_id": tid, "step": step,
        "role": "user", "turn": step, "content": "hi",
        "tokens": 1, "flagged": False,
    }


class TestThreadRegistryConstruction:
    def test_empty(self):
        r = ThreadRegistry()
        assert r.total_records == 0

    def test_repr(self):
        r = ThreadRegistry()
        assert "ThreadRegistry" in repr(r)

    def test_session_ids_empty(self):
        r = ThreadRegistry()
        assert r.session_ids() == []

    def test_thread_ids_empty(self):
        r = ThreadRegistry()
        assert r.thread_ids() == []


class TestThreadRegistryIngest:
    def test_ingest_returns_count(self):
        r = ThreadRegistry()
        recs = [_msg("m1", "t1", 1)]
        payload = _make_payload(recs)
        n = r.ingest(payload, session_id="s1")
        assert n == 1

    def test_ingest_multiple(self):
        r = ThreadRegistry()
        recs = [_msg(f"m{i}", "t1", i + 1) for i in range(5)]
        payload = _make_payload(recs)
        n = r.ingest(payload, session_id="s1")
        assert n == 5

    def test_ingest_dedup(self):
        r = ThreadRegistry()
        recs = [_msg("m1", "t1", 1)]
        payload = _make_payload(recs)
        n1 = r.ingest(payload, session_id="s1")
        n2 = r.ingest(payload, session_id="s1")
        assert n1 == 1
        assert n2 == 0

    def test_ingest_records_direct(self):
        r = ThreadRegistry()
        recs = [_msg("m1", "t1", 1), _msg("m2", "t1", 2)]
        n = r.ingest_records(recs, session_id="s1")
        assert n == 2

    def test_ingest_records_dedup(self):
        r = ThreadRegistry()
        recs = [_msg("m1", "t1", 1)]
        r.ingest_records(recs, session_id="s1")
        n = r.ingest_records(recs, session_id="s1")
        assert n == 0

    def test_total_records(self):
        r = ThreadRegistry()
        r.ingest_records([_msg("m1", "t1", 1)], session_id="s1")
        r.ingest_records([_msg("m2", "t1", 2)], session_id="s1")
        assert r.total_records == 2

    def test_different_sessions(self):
        r = ThreadRegistry()
        recs = [_msg("m1", "t1", 1)]
        r.ingest_records(recs, session_id="s1")
        r.ingest_records(recs, session_id="s2")
        assert r.total_records == 2

    def test_default_session_empty_string(self):
        r = ThreadRegistry()
        recs = [_msg("m1", "t1", 1)]
        r.ingest_records(recs)
        assert r.total_records == 1

    def test_multiple_threads(self):
        r = ThreadRegistry()
        r.ingest_records([_msg("m1", "t1", 1)], session_id="s1")
        r.ingest_records([_msg("m2", "t2", 1)], session_id="s1")
        assert r.total_records == 2
        assert "t1" in r.thread_ids(session_id="s1")
        assert "t2" in r.thread_ids(session_id="s1")


class TestThreadRegistryGet:
    def test_get_thread(self):
        r = ThreadRegistry()
        recs = [_msg("m1", "t1", 1), _msg("m2", "t1", 2)]
        r.ingest_records(recs, session_id="s1")
        result = r.get_thread("t1", session_id="s1")
        assert len(result) == 2

    def test_get_thread_wrong_session(self):
        r = ThreadRegistry()
        r.ingest_records([_msg("m1", "t1", 1)], session_id="s1")
        result = r.get_thread("t1", session_id="s2")
        assert result == []

    def test_get_thread_missing(self):
        r = ThreadRegistry()
        result = r.get_thread("missing", session_id="s1")
        assert result == []

    def test_all_records_for_session(self):
        r = ThreadRegistry()
        r.ingest_records([_msg("m1", "t1", 1)], session_id="s1")
        r.ingest_records([_msg("m2", "t2", 1)], session_id="s1")
        r.ingest_records([_msg("m3", "t1", 1)], session_id="s2")
        result = r.all_records(session_id="s1")
        assert len(result) == 2

    def test_all_records_default_session(self):
        r = ThreadRegistry()
        r.ingest_records([_msg("m1", "t1", 1)])
        result = r.all_records()
        assert len(result) == 1

    def test_session_ids_returned(self):
        r = ThreadRegistry()
        r.ingest_records([_msg("m1", "t1", 1)], session_id="s1")
        r.ingest_records([_msg("m2", "t2", 1)], session_id="s2")
        sessions = r.session_ids()
        assert "s1" in sessions
        assert "s2" in sessions

    def test_thread_ids_for_session(self):
        r = ThreadRegistry()
        r.ingest_records([_msg("m1", "t1", 1)], session_id="s1")
        r.ingest_records([_msg("m2", "t2", 1)], session_id="s1")
        tids = r.thread_ids(session_id="s1")
        assert "t1" in tids
        assert "t2" in tids

    def test_thread_ids_other_session_excluded(self):
        r = ThreadRegistry()
        r.ingest_records([_msg("m1", "t1", 1)], session_id="s1")
        r.ingest_records([_msg("m2", "t2", 1)], session_id="s2")
        tids = r.thread_ids(session_id="s1")
        assert "t2" not in tids

    def test_get_thread_returns_copy(self):
        r = ThreadRegistry()
        r.ingest_records([_msg("m1", "t1", 1)], session_id="s1")
        result1 = r.get_thread("t1", session_id="s1")
        result2 = r.get_thread("t1", session_id="s1")
        assert result1 is not result2


class TestMergeThreads:
    def test_empty(self):
        result = merge_threads([])
        assert result == []

    def test_single_payload(self):
        recs = [_msg("m1", "t1", 1), _msg("m2", "t1", 2)]
        payload = _make_payload(recs)
        result = merge_threads([payload])
        assert len(result) == 2

    def test_two_payloads_merged(self):
        recs1 = [_msg("m1", "t1", 1)]
        recs2 = [_msg("m2", "t1", 2)]
        p1 = _make_payload(recs1)
        p2 = _make_payload(recs2)
        result = merge_threads([p1, p2])
        assert len(result) == 2

    def test_dedup_overlapping(self):
        recs = [_msg("m1", "t1", 1)]
        payload = _make_payload(recs)
        result = merge_threads([payload, payload])
        assert len(result) == 1

    def test_sorted_by_thread_then_step(self):
        recs1 = [_msg("m3", "t2", 1)]
        recs2 = [_msg("m1", "t1", 1), _msg("m2", "t1", 2)]
        p1 = _make_payload(recs1, thread_id="t2")
        p2 = _make_payload(recs2)
        result = merge_threads([p1, p2])
        assert len(result) == 3
        tids = [r["thread_id"] for r in result]
        assert tids == sorted(tids)

    def test_session_id_passed(self):
        recs = [_msg("m1", "t1", 1)]
        payload = _make_payload(recs)
        result = merge_threads([payload], session_id="sess_1")
        assert len(result) == 1

    def test_multiple_threads_in_one_payload(self):
        recs = [_msg("m1", "t1", 1), _msg("m2", "t2", 1)]
        payload = encode_agent_payload(recs)
        result = merge_threads([payload])
        assert len(result) == 2
