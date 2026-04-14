"""
Unit tests for lumen/core/_replay.py
Target: 100% coverage of ReplayLog.
"""
import os
import tempfile

from lumen.core._agent import encode_agent_payload
from lumen.core._replay import ReplayLog


def _msg(mid, tid, step):
    return {
        "type": "msg", "id": mid, "thread_id": tid, "step": step,
        "role": "user", "turn": step, "content": "hi",
        "tokens": 1, "flagged": False,
    }


def _payload(n=2, tid="t1"):
    recs = [_msg(f"m{i}", tid, i + 1) for i in range(n)]
    return encode_agent_payload(recs, thread_id=tid)


class TestReplayLogConstruction:
    def test_default_name(self):
        log = ReplayLog()
        assert log.name == "default"

    def test_custom_name(self):
        log = ReplayLog(name="audit")
        assert log.name == "audit"

    def test_empty_len(self):
        assert len(ReplayLog()) == 0

    def test_latest_seq_zero(self):
        assert ReplayLog().latest_seq == 0

    def test_repr(self):
        log = ReplayLog(name="test")
        assert "ReplayLog" in repr(log)
        assert "test" in repr(log)


class TestReplayLogAppend:
    def test_append_returns_seq(self):
        log = ReplayLog()
        seq = log.append(_payload())
        assert seq == 1

    def test_append_increments_seq(self):
        log = ReplayLog()
        s1 = log.append(_payload())
        s2 = log.append(_payload())
        assert s1 == 1
        assert s2 == 2

    def test_append_increases_len(self):
        log = ReplayLog()
        log.append(_payload())
        log.append(_payload())
        assert len(log) == 2

    def test_latest_seq_tracks(self):
        log = ReplayLog()
        log.append(_payload())
        log.append(_payload())
        assert log.latest_seq == 2

    def test_append_with_meta(self):
        log = ReplayLog()
        log.append(_payload(), meta={"source": "agent_a"})
        entry = log.get(1)
        assert entry["meta"]["source"] == "agent_a"

    def test_append_no_meta(self):
        log = ReplayLog()
        log.append(_payload())
        entry = log.get(1)
        assert entry["meta"] == {}

    def test_meta_is_copied(self):
        log = ReplayLog()
        meta = {"key": "val"}
        log.append(_payload(), meta=meta)
        meta["key"] = "changed"
        entry = log.get(1)
        assert entry["meta"]["key"] == "val"

    def test_timestamp_present(self):
        log = ReplayLog()
        log.append(_payload())
        entry = log.get(1)
        assert entry["timestamp"] > 0


class TestReplayLogReplay:
    def test_replay_empty(self):
        log = ReplayLog()
        assert list(log.replay()) == []

    def test_replay_order(self):
        log = ReplayLog()
        p1 = _payload(n=1, tid="t1")
        p2 = _payload(n=2, tid="t2")
        log.append(p1, meta={"i": 1})
        log.append(p2, meta={"i": 2})
        entries = list(log.replay())
        assert entries[0]["meta"]["i"] == 1
        assert entries[1]["meta"]["i"] == 2

    def test_replay_from_seq1(self):
        log = ReplayLog()
        log.append(_payload())
        log.append(_payload())
        entries = list(log.replay_from(seq=1))
        assert len(entries) == 2

    def test_replay_from_seq2(self):
        log = ReplayLog()
        log.append(_payload())
        log.append(_payload())
        entries = list(log.replay_from(seq=2))
        assert len(entries) == 1
        assert entries[0]["seq"] == 2

    def test_replay_from_beyond_end(self):
        log = ReplayLog()
        log.append(_payload())
        entries = list(log.replay_from(seq=99))
        assert entries == []

    def test_replay_decoded(self):
        log = ReplayLog()
        log.append(_payload(n=2))
        for entry, records in log.replay_decoded():
            assert len(records) == 2
            assert records[0]["type"] == "msg"

    def test_replay_decoded_bad_payload(self):
        log = ReplayLog()
        log._entries.append({
            "seq": 1, "timestamp": 0.0, "meta": {},
            "payload": "INVALID PAYLOAD",
        })
        for entry, records in log.replay_decoded():
            assert records == []

    def test_get_existing(self):
        log = ReplayLog()
        log.append(_payload())
        entry = log.get(1)
        assert entry is not None
        assert entry["seq"] == 1

    def test_get_missing(self):
        log = ReplayLog()
        assert log.get(99) is None

    def test_all_records_merged(self):
        log = ReplayLog()
        log.append(_payload(n=2, tid="t1"))
        log.append(_payload(n=2, tid="t2"))
        recs = log.all_records()
        assert len(recs) == 4

    def test_all_records_dedup(self):
        log = ReplayLog()
        p = _payload(n=2)
        log.append(p)
        log.append(p)
        recs = log.all_records()
        assert len(recs) == 2

    def test_all_records_bad_payload_skipped(self):
        log = ReplayLog()
        log._entries.append({
            "seq": 1, "timestamp": 0.0, "meta": {},
            "payload": "BAD",
        })
        recs = log.all_records()
        assert recs == []


class TestReplayLogSaveLoad:
    def test_save_and_load(self):
        log = ReplayLog(name="test")
        log.append(_payload(n=2, tid="t1"), meta={"src": "a"})
        log.append(_payload(n=1, tid="t2"), meta={"src": "b"})

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            path = f.name

        try:
            log.save(path)
            loaded = ReplayLog.load(path, name="loaded")
            assert len(loaded) == 2
            assert loaded.latest_seq == 2
            assert loaded.name == "loaded"
            e1 = loaded.get(1)
            assert e1["meta"]["src"] == "a"
            recs = loaded.all_records()
            assert len(recs) == 3
        finally:
            os.unlink(path)

    def test_load_empty_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            path = f.name
        try:
            log = ReplayLog.load(path)
            assert len(log) == 0
        finally:
            os.unlink(path)

    def test_save_load_roundtrip_payload(self):
        log = ReplayLog()
        p = _payload(n=3)
        log.append(p)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            path = f.name
        try:
            log.save(path)
            loaded = ReplayLog.load(path)
            orig_entry  = list(log.replay())[0]
            load_entry  = list(loaded.replay())[0]
            assert orig_entry["payload"] == load_entry["payload"]
        finally:
            os.unlink(path)


class TestReplayLogLoadSkipBlankLines:
    """Covers _replay.py line 180: blank lines in saved file are skipped."""

    def test_load_skips_blank_lines_in_file(self):
        import base64
        import json
        import os
        import tempfile
        import time

        from lumen.core._agent import encode_agent_payload
        from lumen.core._replay import ReplayLog

        rec = {
            "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
            "role": "user", "turn": 1, "content": "hi",
            "tokens": 1, "flagged": False,
        }
        payload = encode_agent_payload([rec])
        entry = {
            "seq": 1,
            "timestamp": time.time(),
            "meta": {},
            "payload": base64.b64encode(payload.encode()).decode("ascii"),
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            path = f.name
            f.write("\n")
            f.write(json.dumps(entry, separators=(",", ":")) + "\n")
            f.write("\n")
            f.write("\n")
        try:
            log = ReplayLog.load(path)
            assert len(log) == 1
            assert log.latest_seq == 1
        finally:
            os.unlink(path)
