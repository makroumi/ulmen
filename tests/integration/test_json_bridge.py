"""
Tests for the JSON bridge functions: from_json, to_json, compare_sizes.

These test both Rust and Python paths since the interface is identical.
"""
import json

import pytest

import ulmen


class TestFromJson:
    """Test ulmen.from_json()."""

    def test_basic_agent_payload(self):
        raw = json.dumps([{
            "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
            "role": "user", "turn": 1, "content": "hi", "tokens": 1, "flagged": False,
        }])
        payload = ulmen.from_json(raw)
        assert payload.startswith("ULMEN-AGENT v1\n")
        ok, err = ulmen.validate_agent_payload(payload)
        assert ok is True, err

    def test_with_thread_id(self):
        raw = json.dumps([{
            "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
            "role": "user", "turn": 1, "content": "hi", "tokens": 1, "flagged": False,
        }])
        payload = ulmen.from_json(raw, thread_id="override_thread")
        assert "thread: override_thread\n" in payload

    def test_with_context_window(self):
        raw = json.dumps([{
            "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
            "role": "user", "turn": 1, "content": "hi", "tokens": 1, "flagged": False,
        }])
        payload = ulmen.from_json(raw, context_window=256)
        assert "context_window: 256\n" in payload

    def test_multiple_records(self):
        raw = json.dumps([
            {"type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
             "role": "user", "turn": 1, "content": "hello", "tokens": 1, "flagged": False},
            {"type": "msg", "id": "m2", "thread_id": "t1", "step": 2,
             "role": "assistant", "turn": 2, "content": "hi there", "tokens": 2, "flagged": False},
        ])
        payload = ulmen.from_json(raw)
        assert "records: 2\n" in payload
        ok, _ = ulmen.validate_agent_payload(payload)
        assert ok

    def test_empty_records(self):
        raw = json.dumps([])
        payload = ulmen.from_json(raw)
        assert "records: 0\n" in payload


class TestToJson:
    """Test ulmen.to_json()."""

    def test_basic_roundtrip(self):
        payload = ulmen.encode_agent_payload([{
            "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
            "role": "user", "turn": 1, "content": "hi", "tokens": 1, "flagged": False,
        }])
        raw = ulmen.to_json(payload)
        data = json.loads(raw)
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["type"] == "msg"
        assert data[0]["id"] == "m1"
        assert data[0]["content"] == "hi"

    def test_pretty_output(self):
        payload = ulmen.encode_agent_payload([{
            "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
            "role": "user", "turn": 1, "content": "hi", "tokens": 1, "flagged": False,
        }])
        raw = ulmen.to_json(payload, pretty=True)
        assert "\n" in raw
        assert "  " in raw  # indentation
        data = json.loads(raw)
        assert data[0]["thread_id"] == "t1"

    def test_multiple_records(self):
        payload = ulmen.encode_agent_payload([
            {"type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
             "role": "user", "turn": 1, "content": "a", "tokens": 1, "flagged": False},
            {"type": "msg", "id": "m2", "thread_id": "t1", "step": 2,
             "role": "assistant", "turn": 2, "content": "b", "tokens": 1, "flagged": False},
        ])
        raw = ulmen.to_json(payload)
        data = json.loads(raw)
        assert len(data) == 2

    def test_preserves_special_chars(self):
        payload = ulmen.encode_agent_payload([{
            "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
            "role": "user", "turn": 1, "content": "hello\nworld", "tokens": 2, "flagged": False,
        }])
        raw = ulmen.to_json(payload)
        data = json.loads(raw)
        assert data[0]["content"] == "hello\nworld"


class TestCompareSizes:
    """Test ulmen.compare_sizes()."""

    def test_returns_tuple(self):
        raw = json.dumps([{
            "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
            "role": "user", "turn": 1, "content": "hi", "tokens": 1, "flagged": False,
        }])
        result = ulmen.compare_sizes(raw)
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_types(self):
        raw = json.dumps([{
            "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
            "role": "user", "turn": 1, "content": "hi", "tokens": 1, "flagged": False,
        }])
        json_bytes, ulmen_bytes, saving = ulmen.compare_sizes(raw)
        assert isinstance(json_bytes, int)
        assert isinstance(ulmen_bytes, int)
        assert isinstance(saving, float)

    def test_positive_values(self):
        raw = json.dumps([{
            "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
            "role": "user", "turn": 1, "content": "hi", "tokens": 1, "flagged": False,
        }])
        json_bytes, ulmen_bytes, saving = ulmen.compare_sizes(raw)
        assert json_bytes > 0
        assert ulmen_bytes > 0

    def test_ulmen_is_smaller(self):
        raw = json.dumps([{
            "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
            "role": "user", "turn": 1, "content": "hello world this is a test message",
            "tokens": 10, "flagged": False,
        }])
        json_bytes, ulmen_bytes, saving = ulmen.compare_sizes(raw)
        assert ulmen_bytes < json_bytes
        assert saving > 0


class TestRoundtrip:
    """Test full JSON -> ULMEN -> JSON roundtrip."""

    def test_data_preserved(self):
        original = [{
            "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
            "role": "user", "turn": 1, "content": "test", "tokens": 1, "flagged": False,
        }]
        raw = json.dumps(original)
        payload = ulmen.from_json(raw)
        back = json.loads(ulmen.to_json(payload))
        assert back[0]["type"] == original[0]["type"]
        assert back[0]["id"] == original[0]["id"]
        assert back[0]["content"] == original[0]["content"]
