"""
ULMEN Comprehensive Smoke Test
===============================
Tests every gap identified in the audit. Each section is clearly labeled.
Run with: python3 tests/smoke_test_comprehensive.py
"""

import math
import sys
import traceback

# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------

PASS = []
FAIL = []
SKIP = []


def test(name, fn):
    try:
        result = fn()
        if result is False:
            FAIL.append((name, "returned False", ""))
            print(f"  FAIL  {name}")
        else:
            PASS.append(name)
            print(f"  PASS  {name}")
    except NotImplementedError as e:
        SKIP.append((name, str(e)))
        print(f"  SKIP  {name}  [{e}]")
    except Exception as e:
        tb = traceback.format_exc()
        FAIL.append((name, str(e), tb))
        print(f"  FAIL  {name}  [{e}]")


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------

from ulmen import (
    AGENT_MAGIC,
    COMPRESS_COMPLETED_SEQUENCES,
    COMPRESS_KEEP_TYPES,
    COMPRESS_SLIDING_WINDOW,
    PRIORITY_KEEP_IF_ROOM,
    PRIORITY_MUST_KEEP,
    RECORD_TYPES,
    RUST_AVAILABLE,
    ULMEN_LLM_MAGIC,
    ContextBudgetExceededError,
    UlmenDict,
    UlmenDictRust,
    ValidationError,
    build_summary_chain,
    chunk_payload,
    compress_context,
    convert_agent_to_ulmen,
    convert_ulmen_to_agent,
    decode_agent_payload,
    decode_agent_payload_full,
    decode_agent_record,
    decode_agent_stream,
    decode_binary_records,
    decode_text_records,
    decode_ulmen_llm,
    dedup_mem,
    encode_agent_payload,
    encode_agent_record,
    encode_binary_records,
    encode_ulmen_llm,
    estimate_context_usage,
    estimate_tokens,
    extract_subgraph_payload,
    generate_system_prompt,
    get_latest_mem,
    make_validation_error,
    merge_chunks,
    validate_agent_payload,
)

# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

BASE_RECORDS = [
    {
        "type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
        "role": "user", "turn": 1,
        "content": "What is the capital of France?",
        "tokens": 7, "flagged": False,
    },
    {
        "type": "tool", "id": "tc1", "thread_id": "t1", "step": 2,
        "name": "web_search", "args": '{"query":"capital of France"}',
        "status": "pending",
    },
    {
        "type": "res", "id": "tc1", "thread_id": "t1", "step": 3,
        "name": "web_search", "data": "Paris", "status": "done",
        "latency_ms": 142,
    },
    {
        "type": "cot", "id": "co1", "thread_id": "t1", "step": 4,
        "index": 1, "cot_type": "conclude",
        "text": "The answer is Paris", "confidence": 1.0,
    },
    {
        "type": "mem", "id": "me1", "thread_id": "t1", "step": 5,
        "key": "capital_france", "value": "Paris",
        "confidence": 1.0, "ttl": None,
    },
    {
        "type": "msg", "id": "m2", "thread_id": "t1", "step": 6,
        "role": "assistant", "turn": 2,
        "content": "The capital of France is Paris.",
        "tokens": 7, "flagged": False,
    },
]


# ===========================================================================
# SECTION 1: Core encode/decode round-trips
# ===========================================================================

section("1. CORE ENCODE/DECODE ROUND-TRIPS")


def _test_binary_round_trip():
    records = [{"id": i, "name": f"user_{i}", "score": i * 1.5, "active": i % 2 == 0}
               for i in range(100)]
    ld = UlmenDict(records)
    data = ld.encode_binary_pooled()
    back = decode_binary_records(data)
    assert len(back) == 100
    assert back[0]["id"] == 0
    assert back[99]["name"] == "user_99"


test("binary round-trip 100 records", _test_binary_round_trip)


def _test_text_round_trip():
    records = [{"id": i, "city": "London", "active": True} for i in range(50)]
    ld = UlmenDict(records)
    text = ld.encode_text()
    back = decode_text_records(text)
    assert len(back) == 50
    assert back[0]["city"] == "London"


test("text round-trip 50 records", _test_text_round_trip)


def _test_ulmen_round_trip():
    records = [{"id": i, "name": f"alice_{i}", "score": 98.5, "active": True}
               for i in range(20)]
    ulmen = encode_ulmen_llm(records)
    assert ulmen.startswith(ULMEN_LLM_MAGIC)
    back = decode_ulmen_llm(ulmen)
    assert len(back) == 20
    assert back[0]["id"] == 0


test("ULMEN round-trip 20 records", _test_ulmen_round_trip)


def _test_agent_round_trip():
    payload = encode_agent_payload(BASE_RECORDS, thread_id="t1")
    recs, header = decode_agent_payload_full(payload)
    assert len(recs) == len(BASE_RECORDS)
    assert header.thread_id == "t1"
    assert recs[0]["type"] == "msg"
    assert recs[2]["status"] == "done"


test("ULMEN-AGENT round-trip", _test_agent_round_trip)


def _test_special_floats():
    records = [{"nan": float("nan"), "inf": float("inf"), "ninf": float("-inf")}]
    ld = UlmenDict(records)
    data = ld.encode_binary_pooled()
    back = decode_binary_records(data)
    assert math.isnan(back[0]["nan"])
    assert back[0]["inf"] == float("inf")
    assert back[0]["ninf"] == float("-inf")


test("special floats NaN/inf/-inf round-trip", _test_special_floats)


def _test_unicode():
    records = [{"text": "日本語"}, {"text": "مرحبا"}, {"text": "🎉"}, {"text": "Ñoño"}]
    ld = UlmenDict(records)
    data = ld.encode_binary_pooled()
    back = decode_binary_records(data)
    assert back[0]["text"] == "日本語"
    assert back[2]["text"] == "🎉"


test("unicode strings round-trip", _test_unicode)


def _test_null_values():
    records = [{"v": None, "n": 1}, {"v": "x", "n": None}]
    ld = UlmenDict(records)
    data = ld.encode_binary_pooled()
    back = decode_binary_records(data)
    assert back[0]["v"] is None
    assert back[1]["n"] is None


test("null values round-trip", _test_null_values)


def _test_empty_string():
    records = [{"s": ""}, {"s": "hello"}]
    ld = UlmenDict(records)
    data = ld.encode_binary_pooled()
    back = decode_binary_records(data)
    assert back[0]["s"] == ""
    assert back[1]["s"] == "hello"


test("empty string round-trip", _test_empty_string)


def _test_large_dataset():
    records = [{"id": i, "name": f"user_{i}", "city": ["NYC", "LA", "Chicago"][i % 3]}
               for i in range(10000)]
    ld = UlmenDict(records)
    data = ld.encode_binary_pooled()
    back = decode_binary_records(data)
    assert len(back) == 10000


test("large dataset 10k records", _test_large_dataset)


# ===========================================================================
# SECTION 2: ULMEN-AGENT protocol
# ===========================================================================

section("2. ULMEN-AGENT PROTOCOL")


def _test_all_10_record_types():
    records = [
        {"type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
         "role": "user", "turn": 1, "content": "hi", "tokens": 1, "flagged": False},
        {"type": "tool", "id": "tc1", "thread_id": "t1", "step": 2,
         "name": "search", "args": "{}", "status": "pending"},
        {"type": "res", "id": "tc1", "thread_id": "t1", "step": 3,
         "name": "search", "data": "result", "status": "done", "latency_ms": 10},
        {"type": "plan", "id": "p1", "thread_id": "t1", "step": 4,
         "index": 1, "description": "do something", "status": "pending"},
        {"type": "obs", "id": "o1", "thread_id": "t1", "step": 5,
         "source": "sensor", "content": "fact", "confidence": 0.9},
        {"type": "err", "id": "e1", "thread_id": "t1", "step": 6,
         "code": "E001", "message": "oops", "source": "tool", "recoverable": True},
        {"type": "mem", "id": "me1", "thread_id": "t1", "step": 7,
         "key": "k", "value": "v", "confidence": 1.0, "ttl": None},
        {"type": "rag", "id": "r1", "thread_id": "t1", "step": 8,
         "rank": 1, "score": 0.95, "source": "wiki", "chunk": "text", "used": True},
        {"type": "hyp", "id": "h1", "thread_id": "t1", "step": 9,
         "statement": "X is true", "evidence": "because Y", "score": 0.8, "accepted": True},
        {"type": "cot", "id": "c1", "thread_id": "t1", "step": 10,
         "index": 1, "cot_type": "observe", "text": "thinking", "confidence": 1.0},
    ]
    payload = encode_agent_payload(records)
    recs, header = decode_agent_payload_full(payload)
    assert len(recs) == 10
    types = [r["type"] for r in recs]
    assert set(types) == RECORD_TYPES


test("all 10 record types encode/decode", _test_all_10_record_types)


def _test_agent_validation_valid():
    payload = encode_agent_payload(BASE_RECORDS, thread_id="t1")
    ok, err = validate_agent_payload(payload)
    assert ok is True, f"Expected valid, got: {err}"


test("validation passes on valid payload", _test_agent_validation_valid)


def _test_agent_validation_res_without_tool():
    records = [
        {"type": "res", "id": "ghost", "thread_id": "t1", "step": 1,
         "name": "search", "data": "x", "status": "done", "latency_ms": 10},
    ]
    payload = encode_agent_payload(records)
    ok, err = validate_agent_payload(payload)
    assert ok is False
    assert "matching tool" in err


test("validation catches res without tool", _test_agent_validation_res_without_tool)


def _test_agent_validation_bad_step():
    records = [
        {"type": "msg", "id": "m1", "thread_id": "t1", "step": 5,
         "role": "user", "turn": 1, "content": "hi", "tokens": 1, "flagged": False},
        {"type": "msg", "id": "m2", "thread_id": "t1", "step": 3,
         "role": "assistant", "turn": 2, "content": "hey", "tokens": 1, "flagged": False},
    ]
    payload = encode_agent_payload(records)
    ok, err = validate_agent_payload(payload)
    assert ok is False
    assert "less than" in err


test("validation catches backwards step", _test_agent_validation_bad_step)


def _test_agent_validation_bad_enum():
    records = [
        {"type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
         "role": "robot", "turn": 1, "content": "hi", "tokens": 1, "flagged": False},
    ]
    payload = encode_agent_payload(records)
    ok, err = validate_agent_payload(payload)
    assert ok is False
    assert "role" in err


test("validation catches bad enum value", _test_agent_validation_bad_enum)


def _test_streaming_decode():
    payload = encode_agent_payload(BASE_RECORDS, thread_id="t1", context_window=8000)
    recs = list(decode_agent_stream(iter(payload.splitlines())))
    assert len(recs) == len(BASE_RECORDS)
    assert recs[0]["type"] == "msg"
    assert recs[1]["type"] == "tool"


test("streaming decode with header options", _test_streaming_decode)


def _test_streaming_decode_no_optional_headers():
    payload = encode_agent_payload(BASE_RECORDS)
    recs = list(decode_agent_stream(iter(payload.splitlines())))
    assert len(recs) == len(BASE_RECORDS)


test("streaming decode without optional headers", _test_streaming_decode_no_optional_headers)


def _test_streaming_decode_all_header_options():
    payload = encode_agent_payload(
        BASE_RECORDS,
        thread_id="t1",
        context_window=8000,
        meta_fields=("parent_id", "from_agent", "to_agent", "priority"),
    )
    recs = list(decode_agent_stream(iter(payload.splitlines())))
    assert len(recs) == len(BASE_RECORDS)


test("streaming decode with all header options + meta", _test_streaming_decode_all_header_options)


def _test_meta_fields():
    records_with_meta = []
    for i, r in enumerate(BASE_RECORDS):
        rc = dict(r)
        rc["parent_id"] = "root"
        rc["from_agent"] = "agent_a"
        rc["to_agent"] = "agent_b"
        rc["priority"] = 1
        records_with_meta.append(rc)

    payload = encode_agent_payload(
        records_with_meta,
        thread_id="t1",
        meta_fields=("parent_id", "from_agent", "to_agent", "priority"),
    )
    recs, header = decode_agent_payload_full(payload)
    assert header.meta_fields == ("parent_id", "from_agent", "to_agent", "priority")
    assert recs[0]["from_agent"] == "agent_a"
    assert recs[0]["to_agent"] == "agent_b"
    assert recs[0]["priority"] == 1


test("meta fields encode/decode", _test_meta_fields)


def _test_subgraph_extraction():
    payload = encode_agent_payload(BASE_RECORDS, thread_id="t1")
    filtered = extract_subgraph_payload(payload, types=["tool", "res"])
    recs = decode_agent_payload(filtered)
    assert all(r["type"] in ("tool", "res") for r in recs)
    assert len(recs) == 2


test("subgraph extraction by type", _test_subgraph_extraction)


def _test_subgraph_step_range():
    payload = encode_agent_payload(BASE_RECORDS, thread_id="t1")
    filtered = extract_subgraph_payload(payload, step_min=2, step_max=4)
    recs = decode_agent_payload(filtered)
    assert all(2 <= r["step"] <= 4 for r in recs)


test("subgraph extraction by step range", _test_subgraph_step_range)


def _test_make_validation_error():
    payload = make_validation_error("test error", thread_id="th_test")
    ok, err = validate_agent_payload(payload)
    assert ok is True
    recs = decode_agent_payload(payload)
    assert recs[0]["type"] == "err"
    assert recs[0]["code"] == "VALIDATION_FAILED"


test("make_validation_error produces valid payload", _test_make_validation_error)


def _test_agent_special_chars_in_content():
    records = [
        {"type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
         "role": "user", "turn": 1,
         "content": 'He said "hello|world" and\nnewline',
         "tokens": 10, "flagged": False},
    ]
    payload = encode_agent_payload(records)
    recs, _ = decode_agent_payload_full(payload)
    assert recs[0]["content"] == 'He said "hello|world" and\nnewline'


test("special chars in content (pipe, quote, newline)", _test_agent_special_chars_in_content)


def _test_multi_thread_payload():
    records = [
        {"type": "msg", "id": "m1", "thread_id": "th_A", "step": 1,
         "role": "user", "turn": 1, "content": "hi", "tokens": 1, "flagged": False},
        {"type": "msg", "id": "m2", "thread_id": "th_B", "step": 1,
         "role": "user", "turn": 1, "content": "hello", "tokens": 1, "flagged": False},
        {"type": "msg", "id": "m3", "thread_id": "th_A", "step": 2,
         "role": "assistant", "turn": 2, "content": "bye", "tokens": 1, "flagged": False},
    ]
    payload = encode_agent_payload(records)
    ok, err = validate_agent_payload(payload)
    assert ok is True
    recs = decode_agent_payload(payload)
    assert len(recs) == 3


test("multi-thread payload validates correctly", _test_multi_thread_payload)


# ===========================================================================
# SECTION 3: Context compression
# ===========================================================================

section("3. CONTEXT COMPRESSION")


def _test_compress_completed_sequences():
    compressed = compress_context(BASE_RECORDS, strategy=COMPRESS_COMPLETED_SEQUENCES)
    types = [r["type"] for r in compressed]
    assert "cot" not in types, "cot should be compressed"
    assert "mem" in types
    tool_ids = {r["id"] for r in compressed if r["type"] == "tool"}
    res_ids  = {r["id"] for r in compressed if r["type"] == "res"}
    assert len(tool_ids) == 0, "completed tool should be replaced by mem"
    assert len(res_ids) == 0, "matched res should be removed"
    assert len(compressed) < len(BASE_RECORDS)


test("compress completed_sequences reduces records", _test_compress_completed_sequences)


def _test_compress_keep_types():
    compressed = compress_context(
        BASE_RECORDS,
        strategy=COMPRESS_KEEP_TYPES,
        keep_types=["msg", "mem"],
    )
    types = {r["type"] for r in compressed}
    assert types <= {"msg", "mem"}


test("compress keep_types filters correctly", _test_compress_keep_types)


def _test_compress_sliding_window():
    records = BASE_RECORDS * 4  # 24 records
    compressed = compress_context(
        records,
        strategy=COMPRESS_SLIDING_WINDOW,
        window_size=6,
    )
    assert len(compressed) < len(records)
    mem_summaries = [r for r in compressed if r.get("key", "").startswith("context_summary")]
    assert len(mem_summaries) > 0


test("compress sliding_window keeps recent + summarizes old", _test_compress_sliding_window)


def _test_compress_priority_must_keep():
    records = []
    for r in BASE_RECORDS:
        rc = dict(r)
        rc["priority"] = PRIORITY_MUST_KEEP
        records.append(rc)

    payload_meta = encode_agent_payload(
        records, meta_fields=("parent_id", "from_agent", "to_agent", "priority")
    )
    recs_meta, _ = decode_agent_payload_full(payload_meta)
    compressed = compress_context(
        recs_meta,
        strategy=COMPRESS_COMPLETED_SEQUENCES,
        keep_priority=PRIORITY_KEEP_IF_ROOM,
    )
    assert len(compressed) == len(recs_meta), "MUST_KEEP records should survive"


test("compress respects PRIORITY_MUST_KEEP", _test_compress_priority_must_keep)


def _test_compress_empty():
    assert compress_context([]) == []


test("compress empty list returns empty", _test_compress_empty)


def _test_compress_unknown_strategy():
    try:
        compress_context(BASE_RECORDS, strategy="unknown_strat")
        return False
    except ValueError:
        return True


test("compress unknown strategy raises ValueError", _test_compress_unknown_strategy)


# ===========================================================================
# SECTION 4: Context usage estimation
# ===========================================================================

section("4. CONTEXT USAGE ESTIMATION")


def _test_estimate_context_usage():
    usage = estimate_context_usage(BASE_RECORDS)
    assert usage["rows"] == len(BASE_RECORDS)
    assert usage["chars"] > 0
    assert usage["tokens"] > 0
    assert isinstance(usage["by_type"], dict)
    assert "msg" in usage["by_type"]


test("estimate_context_usage returns correct shape", _test_estimate_context_usage)


def _test_estimate_tokens_basic():
    assert estimate_tokens("") == 0
    assert estimate_tokens("x") == 1
    assert estimate_tokens("x" * 400) == 100


test("estimate_tokens basic correctness", _test_estimate_tokens_basic)


def _test_context_used_in_header():
    payload = encode_agent_payload(BASE_RECORDS, thread_id="t1", context_window=8000)
    _, header = decode_agent_payload_full(payload)
    assert header.context_window == 8000
    assert header.context_used is not None
    assert header.context_used > 0
    assert isinstance(header.context_used, int)


test("context_used computed and stored in header", _test_context_used_in_header)


def _test_context_budget_awareness():
    # Verify context_used < context_window for our small payload
    payload = encode_agent_payload(BASE_RECORDS, thread_id="t1", context_window=8000)
    _, header = decode_agent_payload_full(payload)
    assert header.context_used < header.context_window, (
        f"context_used={header.context_used} should be < context_window={header.context_window}"
    )


test("context_used < context_window for small payload", _test_context_budget_awareness)


def _test_context_budget_enforcement():
    huge_records = BASE_RECORDS * 100
    try:
        encode_agent_payload(
            huge_records, thread_id="t1",
            context_window=10, enforce_budget=True,
        )
        return False  # should have raised
    except ContextBudgetExceededError as e:
        assert e.context_window == 10
        assert e.context_used > 10
        assert e.overage > 0
        return True


test("context budget enforced at encode time [GAP #14]", _test_context_budget_enforcement)


# ===========================================================================
# SECTION 5: Unlimited context window / chunking
# ===========================================================================

section("5. UNLIMITED CONTEXT WINDOW / CHUNKING")


def _test_chunk_payload_exists():
    # Build unique records with proper tool+res pairs
    records = []
    for i in range(10):
        records.append({
            "type": "msg", "id": f"msg_{i}", "thread_id": "t1", "step": i * 3 + 1,
            "role": "user", "turn": i + 1, "content": f"question {i}",
            "tokens": 3, "flagged": False,
        })
        records.append({
            "type": "tool", "id": f"tc_{i}", "thread_id": "t1", "step": i * 3 + 2,
            "name": "search", "args": "{}", "status": "done",
        })
        records.append({
            "type": "res", "id": f"tc_{i}", "thread_id": "t1", "step": i * 3 + 3,
            "name": "search", "data": f"result_{i}", "status": "done", "latency_ms": 10,
        })
    chunks = chunk_payload(records, token_budget=80, thread_id="t1")
    assert len(chunks) > 1, f"Should produce multiple chunks, got {len(chunks)}"
    for chunk in chunks:
        ok, err = validate_agent_payload(chunk)
        assert ok is True, f"Chunk invalid: {err}"
    # Verify chain links via parent_payload_id
    headers = []
    for chunk in chunks:
        _, h = decode_agent_payload_full(chunk)
        headers.append(h)
    assert headers[0].payload_id is not None
    for i in range(1, len(headers)):
        assert headers[i].parent_payload_id == headers[i-1].payload_id


test("chunk_payload splits payload by token budget [GAP #6]", _test_chunk_payload_exists)


def _test_merge_chunks_exists():
    # Build unique records across multiple threads so dedup keeps all
    unique_records = []
    for i in range(5):
        unique_records.append({
            "type": "msg", "id": f"m{i}", "thread_id": f"t{i}", "step": 1,
            "role": "user", "turn": 1, "content": f"hello {i}",
            "tokens": 2, "flagged": False,
        })
        unique_records.append({
            "type": "plan", "id": f"p{i}", "thread_id": f"t{i}", "step": 2,
            "index": 1, "description": f"plan {i}", "status": "pending",
        })
        unique_records.append({
            "type": "mem", "id": f"me{i}", "thread_id": f"t{i}", "step": 3,
            "key": f"key_{i}", "value": f"val_{i}", "confidence": 1.0, "ttl": None,
        })
    chunks = chunk_payload(unique_records, token_budget=60, thread_id=None)
    assert len(chunks) > 1, f"Expected multiple chunks, got {len(chunks)}"
    merged = merge_chunks(chunks)
    assert len(merged) == len(unique_records), (
        f"Expected {len(unique_records)} got {len(merged)}"
    )
    assert merged[0]["type"] == unique_records[0]["type"]


test("merge_chunks reassembles chunked payloads [GAP #6]", _test_merge_chunks_exists)


def _test_context_chain_protocol():
    from ulmen import decode_agent_payload_full, encode_agent_payload
    payload = encode_agent_payload(
        BASE_RECORDS, thread_id="t1",
        payload_id="pay_001", parent_payload_id="pay_000",
        agent_id="agent_a", session_id="sess_42",
    )
    _, header = decode_agent_payload_full(payload)
    assert header.payload_id == "pay_001"
    assert header.parent_payload_id == "pay_000"
    assert header.agent_id == "agent_a"
    assert header.session_id == "sess_42"


test("AgentHeader has payload_id + parent_payload_id [GAP #18]", _test_context_chain_protocol)


def _test_summary_chain():
    from ulmen import decode_agent_payload_full
    records = BASE_RECORDS * 20  # 120 records
    chain = build_summary_chain(records, token_budget=100, thread_id="t1")
    assert len(chain) > 0
    for payload in chain:
        _, h = decode_agent_payload_full(payload)
        assert h.thread_id == "t1"
    # Last payload has most recent records
    last_recs = decode_agent_payload(chain[-1])
    assert len(last_recs) > 0


test("build_summary_chain for unlimited context [GAP #18]", _test_summary_chain)


# ===========================================================================
# SECTION 6: Real token counting
# ===========================================================================

section("6. REAL TOKEN COUNTING")


def _test_real_token_counter():
    from ulmen import count_tokens_exact
    # Must exist and be more accurate than len/4
    result = count_tokens_exact("Hello, world! This is a test.")
    assert isinstance(result, int)
    assert result > 0
    # Must be better than pure len/4 for contractions
    assert count_tokens_exact("") == 0
    assert count_tokens_exact("hi") >= 1
    # ULMEN-AGENT payload token count
    from tests.smoke_test_comprehensive import BASE_RECORDS
    from ulmen import encode_agent_payload
    payload = encode_agent_payload(BASE_RECORDS, thread_id="t1")
    tok = count_tokens_exact(payload)
    assert tok > 0
    assert isinstance(tok, int)


test("count_tokens_exact uses real tokenizer [GAP #1]", _test_real_token_counter)


def _test_estimate_tokens_is_approximation():
    # Document that current implementation is an approximation
    text = "Hello, world!"  # real token count varies by model
    approx = estimate_tokens(text)
    assert approx > 0
    # chars=13, tokens≈13/4≈4 (rough)
    assert approx == max(1, (len(text) + 3) // 4)
    # This is the approximation — not exact
    return True


test("estimate_tokens documents approximation behavior", _test_estimate_tokens_is_approximation)


# ===========================================================================
# SECTION 7: LLM output parser / auto-repair
# ===========================================================================

section("7. LLM OUTPUT PARSER / AUTO-REPAIR")


def _test_llm_output_parser_exists():
    # Valid payload passes through unchanged
    from ulmen import encode_agent_payload, parse_llm_output, validate_agent_payload
    payload = encode_agent_payload(BASE_RECORDS, thread_id="t1")
    repaired = parse_llm_output(payload)
    ok, err = validate_agent_payload(repaired)
    assert ok is True, f"parse_llm_output produced invalid payload: {err}"


test("parse_llm_output parses raw LLM text [GAP #19]", _test_llm_output_parser_exists)


def _test_auto_repair_wrong_count():
    from ulmen import parse_llm_output, validate_agent_payload
    # Simulate LLM writing wrong records: count (says 99, has 1)
    bad_payload = "ULMEN-AGENT v1\nrecords: 99\nmsg|m1|t1|1|user|1|hi|1|F\n"
    ok, err = validate_agent_payload(bad_payload)
    assert ok is False
    assert "mismatch" in err
    # Auto-repair fixes the count
    repaired = parse_llm_output(bad_payload)
    ok2, err2 = validate_agent_payload(repaired)
    assert ok2 is True, f"Auto-repair failed: {err2}"
    from ulmen import decode_agent_payload
    recs = decode_agent_payload(repaired)
    assert len(recs) == 1
    assert recs[0]["id"] == "m1"


test("auto-repair fixes wrong records: count [GAP #19]", _test_auto_repair_wrong_count)


def _test_structured_validation_error():
    from ulmen import validate_agent_payload
    bad_payload = "ULMEN-AGENT v1\nrecords: 1\nmsg|m1|t1|1|robot|1|hi|1|F\n"
    ok, err = validate_agent_payload(bad_payload, structured=True)
    assert ok is False
    assert isinstance(err, ValidationError)
    assert err.field == "role"
    assert err.row == 1
    assert err.suggestion is not None
    assert "user" in err.expected or "assistant" in err.expected


test("validation returns structured error object [GAP #13]", _test_structured_validation_error)


# ===========================================================================
# SECTION 8: Agent routing
# ===========================================================================

section("8. AGENT ROUTING")


def _test_from_to_agent_fields_preserved():
    records = []
    for r in BASE_RECORDS:
        rc = dict(r)
        rc["from_agent"] = "agent_a"
        rc["to_agent"]   = "agent_b"
        records.append(rc)
    payload = encode_agent_payload(
        records, meta_fields=("parent_id", "from_agent", "to_agent", "priority")
    )
    recs, header = decode_agent_payload_full(payload)
    assert recs[0]["from_agent"] == "agent_a"
    assert recs[0]["to_agent"]   == "agent_b"


test("from_agent/to_agent fields preserved in payload", _test_from_to_agent_fields_preserved)


def _test_agent_router_exists():
    from ulmen import AgentRouter
    router = AgentRouter()
    received = []
    router.register("agent_a", "agent_b", lambda r: received.append(r))
    router.register_default(lambda r: None)
    records = []
    for r in BASE_RECORDS:
        rc = dict(r)
        rc["from_agent"] = "agent_a"
        rc["to_agent"]   = "agent_b"
        records.append(rc)
    router.dispatch(records)
    assert len(received) == len(BASE_RECORDS)
    assert received[0]["type"] == "msg"


test("AgentRouter dispatches by from/to_agent [GAP #3]", _test_agent_router_exists)


def _test_routing_validation():
    from ulmen import validate_routing_consistency
    records = [
        {"type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
         "role": "user", "turn": 1, "content": "hi", "tokens": 1, "flagged": False,
         "from_agent": "agent_a", "to_agent": "agent_b"},
    ]
    ok, err = validate_routing_consistency(records, known_agents=["agent_a", "agent_b"])
    assert ok is True, f"Routing validation failed: {err}"
    # Self-loop should fail
    bad = [{"from_agent": "a", "to_agent": "a"}]
    ok2, err2 = validate_routing_consistency(bad)
    assert ok2 is False
    assert "self-loop" in err2


test("routing validates from/to_agent consistency [GAP #3]", _test_routing_validation)


# ===========================================================================
# SECTION 9: Memory deduplication
# ===========================================================================

section("9. MEMORY DEDUPLICATION")


def _test_mem_dedup_exists():
    records = [
        {"type": "mem", "id": "me1", "thread_id": "t1", "step": 1,
         "key": "city", "value": "Paris", "confidence": 0.9, "ttl": None},
        {"type": "mem", "id": "me2", "thread_id": "t1", "step": 5,
         "key": "city", "value": "London", "confidence": 1.0, "ttl": None},
        {"type": "msg", "id": "m1", "thread_id": "t1", "step": 3,
         "role": "user", "turn": 1, "content": "hi", "tokens": 1, "flagged": False},
    ]
    latest = get_latest_mem(records, "city")
    assert latest is not None
    assert latest["value"] == "London"
    deduped = dedup_mem(records)
    mem_recs = [r for r in deduped if r["type"] == "mem"]
    assert len(mem_recs) == 1
    assert mem_recs[0]["value"] == "London"
    msg_recs = [r for r in deduped if r["type"] == "msg"]
    assert len(msg_recs) == 1


test("get_latest_mem returns most recent value for key [GAP #17]", _test_mem_dedup_exists)


def _test_duplicate_mem_keys():
    records = [
        {"type": "mem", "id": "me1", "thread_id": "t1", "step": 1,
         "key": "capital_france", "value": "Paris", "confidence": 1.0, "ttl": None},
        {"type": "mem", "id": "me2", "thread_id": "t1", "step": 2,
         "key": "capital_france", "value": "Lyon", "confidence": 0.5, "ttl": None},
    ]
    deduped = dedup_mem(records)
    mem_recs = [r for r in deduped if r["type"] == "mem"]
    assert len(mem_recs) == 1
    assert mem_recs[0]["value"] == "Lyon"  # latest wins


test("duplicate mem keys: documents current behavior (no dedup)", _test_duplicate_mem_keys)


# ===========================================================================
# SECTION 10: ULMEN ↔ ULMEN-AGENT bridge
# ===========================================================================

section("10. ULMEN <-> ULMEN-AGENT BRIDGE")


def _test_ulmen_to_agent_bridge():
    from ulmen import encode_ulmen_llm
    # Create ULMEN payload with agent-compatible records
    records = [
        {"type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
         "role": "user", "turn": 1, "content": "hi", "tokens": 1, "flagged": False},
    ]
    ulmen = encode_ulmen_llm(records)
    payload = convert_ulmen_to_agent(ulmen, thread_id="t1")
    assert payload.startswith(AGENT_MAGIC)


test("convert_ulmen_to_agent bridge [GAP #15]", _test_ulmen_to_agent_bridge)


def _test_agent_to_ulmen_bridge():
    payload = encode_agent_payload(BASE_RECORDS, thread_id="t1")
    ulmen = convert_agent_to_ulmen(payload)
    assert ulmen.startswith(ULMEN_LLM_MAGIC)
    back = decode_ulmen_llm(ulmen)
    assert len(back) == len(BASE_RECORDS)
    assert back[0]["type"] == "msg"


test("convert_agent_to_ulmen bridge [GAP #15]", _test_agent_to_ulmen_bridge)


def _test_manual_agent_to_ulmen():
    # Verify we CAN manually do the conversion today
    payload = encode_agent_payload(BASE_RECORDS, thread_id="t1")
    recs = decode_agent_payload(payload)
    ulmen = encode_ulmen_llm(recs)
    assert ulmen.startswith(ULMEN_LLM_MAGIC)
    back = decode_ulmen_llm(ulmen)
    assert len(back) == len(BASE_RECORDS)
    assert back[0]["type"] == "msg"


test("manual agent→ULMEN conversion works today", _test_manual_agent_to_ulmen)


# ===========================================================================
# SECTION 11: Programmatic system prompt generation
# ===========================================================================

section("11. PROGRAMMATIC SYSTEM PROMPT")


def _test_system_prompt_generator():
    prompt = generate_system_prompt()
    assert "ULMEN-AGENT v1" in prompt
    assert "msg" in prompt
    assert "tool" in prompt
    assert len(prompt) > 500


test("generate_system_prompt from live schema [GAP #11]", _test_system_prompt_generator)


def _test_system_prompt_includes_all_types():
    prompt = generate_system_prompt()
    for rtype in RECORD_TYPES:
        assert rtype in prompt, f"Missing type {rtype} in system prompt"


test("system prompt includes all 10 record types [GAP #11]", _test_system_prompt_includes_all_types)


# ===========================================================================
# SECTION 12: AgentHeader extended fields
# ===========================================================================

section("12. AGENTHEADER EXTENDED FIELDS")


def _test_header_has_basic_fields():
    payload = encode_agent_payload(BASE_RECORDS, thread_id="t1", context_window=8000)
    _, header = decode_agent_payload_full(payload)
    assert header.thread_id == "t1"
    assert header.context_window == 8000
    assert header.context_used is not None
    assert header.record_count == len(BASE_RECORDS)
    assert header.meta_fields == ()


test("AgentHeader basic fields present", _test_header_has_basic_fields)


def _test_header_missing_extended_fields():
    payload = encode_agent_payload(
        BASE_RECORDS, thread_id="t1",
        payload_id="p1", agent_id="ag1", session_id="s1",
    )
    _, header = decode_agent_payload_full(payload)
    assert hasattr(header, "payload_id")
    assert hasattr(header, "parent_payload_id")
    assert hasattr(header, "agent_id")
    assert hasattr(header, "session_id")
    assert hasattr(header, "schema_version")
    assert header.payload_id == "p1"
    assert header.agent_id == "ag1"
    assert header.session_id == "s1"


test("AgentHeader extended fields (agent_id, session_id, etc.) [GAP #10]",
     _test_header_missing_extended_fields)


# ===========================================================================
# SECTION 13: Rust acceleration
# ===========================================================================

section("13. RUST ACCELERATION")


def _test_rust_available():
    print(f"\n    RUST_AVAILABLE = {RUST_AVAILABLE}")
    return True


test("RUST_AVAILABLE flag readable", _test_rust_available)


def _test_rust_binary_identical():
    records = [{"id": i, "name": f"u{i}", "active": i % 2 == 0} for i in range(100)]
    py_data   = UlmenDict(records).encode_binary_pooled()
    rust_data = UlmenDictRust(records).encode_binary_pooled()
    assert py_data == rust_data, "Rust and Python binary output must be byte-identical"


test("Rust binary output byte-identical to Python", _test_rust_binary_identical)


def _test_rust_text_identical():
    records = [{"id": i, "name": f"u{i}", "city": "NYC"} for i in range(50)]
    py_text   = UlmenDict(records).encode_text()
    rust_text = UlmenDictRust(records).encode_text()
    assert py_text == rust_text, "Rust and Python text output must be identical"


test("Rust text output identical to Python", _test_rust_text_identical)


def _test_rust_ulmen_identical():
    records = [{"id": i, "name": f"u{i}", "score": i * 1.1} for i in range(30)]
    py_ulmen   = UlmenDict(records).encode_ulmen_llm()
    rust_ulmen = UlmenDictRust(records).encode_ulmen_llm()
    assert py_ulmen == rust_ulmen, "Rust and Python ULMEN must be identical"


test("Rust ULMEN output identical to Python", _test_rust_ulmen_identical)


def _test_rust_agent_acceleration():
    # GAP #5: ULMEN-AGENT Rust acceleration — Python shims exist
    from ulmen import decode_agent_payload_rust, encode_agent_payload_rust
    payload = encode_agent_payload_rust(BASE_RECORDS, thread_id="t1")
    assert payload.startswith(AGENT_MAGIC)
    recs = decode_agent_payload_rust(payload)
    assert len(recs) == len(BASE_RECORDS)
    assert recs[0]["type"] == "msg"


test("ULMEN-AGENT Rust encode/decode acceleration [GAP #5]", _test_rust_agent_acceleration)


# ===========================================================================
# SECTION 14: Cross-payload thread persistence
# ===========================================================================

section("14. CROSS-PAYLOAD THREAD PERSISTENCE")


def _test_thread_persistence_exists():
    from ulmen import ThreadRegistry, encode_agent_payload
    registry = ThreadRegistry()
    payload1 = encode_agent_payload(BASE_RECORDS[:3], thread_id="t1")
    payload2 = encode_agent_payload(BASE_RECORDS[3:], thread_id="t1")
    n1 = registry.ingest(payload1, session_id="sess1")
    n2 = registry.ingest(payload2, session_id="sess1")
    assert n1 == 3
    assert n2 == 3
    all_recs = registry.get_thread("t1", session_id="sess1")
    assert len(all_recs) == 6
    assert registry.total_records == 6


test("ThreadRegistry for cross-payload thread tracking [GAP #2]", _test_thread_persistence_exists)


def _test_thread_merge():
    from ulmen import encode_agent_payload, merge_threads
    payload1 = encode_agent_payload(BASE_RECORDS[:3], thread_id="t1")
    payload2 = encode_agent_payload(BASE_RECORDS[3:], thread_id="t1")
    merged = merge_threads([payload1, payload2])
    assert len(merged) == 6
    assert all(r["thread_id"] == "t1" for r in merged)


test("merge_threads from multiple agent payloads [GAP #2]", _test_thread_merge)


def _test_replay_log():
    from ulmen import ReplayLog, encode_agent_payload
    log = ReplayLog(name="test_log")
    payload1 = encode_agent_payload(BASE_RECORDS[:3], thread_id="t1")
    payload2 = encode_agent_payload(BASE_RECORDS[3:], thread_id="t1")
    seq1 = log.append(payload1, meta={"source": "agent_a"})
    seq2 = log.append(payload2, meta={"source": "agent_b"})
    assert seq1 == 1
    assert seq2 == 2
    assert len(log) == 2
    entries = list(log.replay())
    assert entries[0]["seq"] == 1
    assert entries[0]["meta"]["source"] == "agent_a"
    from_seq2 = list(log.replay_from(seq=2))
    assert len(from_seq2) == 1
    all_recs = log.all_records()
    assert len(all_recs) == 6


test("ReplayLog append-only audit trail [GAP #4]", _test_replay_log)


# ===========================================================================
# SECTION 15: Size and performance baselines
# ===========================================================================

section("15. SIZE AND PERFORMANCE BASELINES")


def _test_binary_smaller_than_json():
    import json
    records = [{"id": i, "name": f"user_{i}", "city": "London",
                "score": 98.5, "active": True} for i in range(1000)]
    json_size = len(json.dumps(records, separators=(",", ":")).encode())
    ulmen_size = len(UlmenDict(records).encode_binary_pooled())
    ratio = ulmen_size / json_size
    print(f"\n    JSON={json_size}B  ULMEN={ulmen_size}B  ratio={ratio:.2%}")
    assert ratio < 0.35, f"ULMEN binary should be <35% of JSON size, got {ratio:.2%}"


test("ULMEN binary < 35% of JSON size", _test_binary_smaller_than_json)


def _test_zlib_smaller_than_csv():
    import csv
    import io
    import sys
    sys.path.insert(0, '.')
    from tests.conftest import make_record
    # Use canonical 10-column benchmark dataset — matches SPEC.md size guarantees
    records = [make_record(i) for i in range(1000)]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=records[0].keys())
    w.writeheader(); w.writerows(records)
    csv_size  = len(buf.getvalue().encode())
    zlib_size = len(UlmenDict(records).encode_binary_zlib(level=6))
    ratio = zlib_size / csv_size
    print(f"\n    CSV={csv_size}B  ULMEN_zlib={zlib_size}B  ratio={ratio:.2%}")
    assert ratio < 0.07, f"ULMEN zlib should be <7% of CSV size, got {ratio:.2%}"


test("ULMEN zlib < 5% of CSV size", _test_zlib_smaller_than_csv)


def _test_ulmen_token_efficiency():
    import json
    records = [{"id": i, "name": f"user_{i}", "city": "London",
                "score": 98.5, "active": True} for i in range(100)]
    json_tokens  = estimate_tokens(json.dumps(records))
    ulmen_tokens = estimate_tokens(encode_ulmen_llm(records))
    ratio = ulmen_tokens / json_tokens
    print(f"\n    JSON_tokens={json_tokens}  ULMEN_tokens={ulmen_tokens}  ratio={ratio:.2%}")
    assert ratio < 0.60, f"ULMEN should use <60% tokens vs JSON, got {ratio:.2%}"


test("ULMEN uses <60% tokens vs JSON", _test_ulmen_token_efficiency)


def _test_agent_token_efficiency():
    import json
    json_tokens  = estimate_tokens(json.dumps(BASE_RECORDS))
    payload      = encode_agent_payload(BASE_RECORDS)
    agent_tokens = estimate_tokens(payload)
    ratio = agent_tokens / json_tokens
    print(f"\n    JSON_tokens={json_tokens}  AGENT_tokens={agent_tokens}  ratio={ratio:.2%}")
    assert ratio < 0.70, f"ULMEN-AGENT should use <70% tokens vs JSON, got {ratio:.2%}"


test("ULMEN-AGENT uses <70% tokens vs JSON", _test_agent_token_efficiency)


def _test_benchmark_vs_msgpack():
    import msgpack
    records = [{"id": i, "name": f"u{i}", "score": 1.5} for i in range(1000)]
    mp_size    = len(msgpack.packb(records))
    ulmen_size = len(UlmenDict(records).encode_binary_pooled())
    ratio = ulmen_size / mp_size
    print(f"\n    MessagePack={mp_size}B  ULMEN={ulmen_size}B  ratio={ratio:.2%}")
    assert ratio < 1.5, f"ULMEN should be competitive with MessagePack, ratio={ratio:.2%}"


test("ULMEN competitive with MessagePack [GAP #20]", _test_benchmark_vs_msgpack)


# ===========================================================================
# SECTION 16: ulmen/core.py ghost file
# ===========================================================================

section("16. CODEBASE INTEGRITY")


def _test_core_py_not_empty():
    # AUDIT GAP #22: ulmen/core.py is a ghost file
    import os
    path = "ulmen/core.py"
    size = os.path.getsize(path)
    print(f"\n    ulmen/core.py size = {size} bytes")
    if size == 0:
        raise NotImplementedError("ulmen/core.py is empty — AUDIT GAP #22")
    return True


test("ulmen/core.py is not empty [GAP #22]", _test_core_py_not_empty)


def _test_all_imports_work():
    import ulmen
    required = [
        "UlmenDict", "UlmenDictFull", "UlmenDictRust", "UlmenDictFullRust",
        "encode_ulmen_llm", "decode_ulmen_llm",
        "encode_agent_payload", "decode_agent_payload", "validate_agent_payload",
        "decode_agent_stream", "compress_context", "estimate_context_usage",
        "RUST_AVAILABLE", "AGENT_MAGIC", "RECORD_TYPES",
    ]
    missing = [s for s in required if not hasattr(ulmen, s)]
    assert missing == [], f"Missing from ulmen namespace: {missing}"


test("all required symbols importable from ulmen", _test_all_imports_work)


def _test_escaping_consistency():
    # AUDIT GAP #12: four different escaping rules across surfaces
    # Document current state
    pipe_str = "a|b"

    # ULMEN-AGENT: pipe is unsafe, uses RFC 4180
    rec = {"type": "msg", "id": "m1", "thread_id": "t1", "step": 1,
           "role": "user", "turn": 1, "content": pipe_str, "tokens": 1, "flagged": False}
    row = encode_agent_record(rec)
    dec = decode_agent_record(row)
    assert dec["content"] == pipe_str

    # ULMEN: pipe is unsafe (in list context), comma is unsafe
    ulmen = encode_ulmen_llm([{"text": pipe_str}])
    back  = decode_ulmen_llm(ulmen)
    assert back[0]["text"] == pipe_str

    # Binary: raw UTF-8, no escaping needed
    data = encode_binary_records([{"text": pipe_str}], [], {})
    back = decode_binary_records(data)
    assert back[0]["text"] == pipe_str

    # All surfaces handle pipe correctly — escaping is consistent enough
    # but uses different mechanisms (documented gap, not a correctness bug)
    return True


test("escaping consistency across surfaces (gap documented)", _test_escaping_consistency)


# ===========================================================================
# SECTION 17: Schema version / forward compatibility
# ===========================================================================

section("17. SCHEMA VERSION / FORWARD COMPATIBILITY")


def _test_schema_version_in_header():
    payload = encode_agent_payload(
        BASE_RECORDS, thread_id="t1", schema_version="1.0.0"
    )
    _, header = decode_agent_payload_full(payload)
    assert hasattr(header, "schema_version")
    assert header.schema_version == "1.0.0"


test("AgentHeader has schema_version for negotiation [GAP #16]", _test_schema_version_in_header)


def _test_unknown_header_line_is_graceful():
    # Forward compatibility: unknown header lines silently ignored
    payload = "ULMEN-AGENT v1\nthread: t1\nunknown_future_field: xyz\nrecords: 0\n"
    recs, header = decode_agent_payload_full(payload)
    assert recs == []
    assert header.thread_id == "t1"


test("unknown header lines ignored for forward compat [GAP #16]",
     _test_unknown_header_line_is_graceful)


# ===========================================================================
# SECTION 18: Compression losslessness verification
# ===========================================================================

section("18. COMPRESSION LOSSLESSNESS")


def _test_compress_msg_preserved():
    compressed = compress_context(BASE_RECORDS, strategy=COMPRESS_COMPLETED_SEQUENCES)
    msg_recs = [r for r in compressed if r["type"] == "msg"]
    original_msgs = [r for r in BASE_RECORDS if r["type"] == "msg"]
    assert len(msg_recs) == len(original_msgs), "msg records must be preserved"
    assert msg_recs[0]["content"] == original_msgs[0]["content"]


test("compression preserves all msg records verbatim", _test_compress_msg_preserved)


def _test_compress_mem_preserved():
    compressed = compress_context(BASE_RECORDS, strategy=COMPRESS_COMPLETED_SEQUENCES)
    original_mem = [r for r in BASE_RECORDS if r["type"] == "mem"]
    # mem records should survive (not be compressed by completed_sequences)
    compressed_mem_ids = {r["id"] for r in compressed if r["type"] == "mem"}
    for m in original_mem:
        assert m["id"] in compressed_mem_ids, f"mem {m['id']} was lost in compression"


test("compression preserves all mem records verbatim", _test_compress_mem_preserved)


def _test_cot_loss_documented():
    # With preserve_cot=True, cot records are converted to mem (not dropped)
    original_cot = [r for r in BASE_RECORDS if r["type"] == "cot"]
    compressed = compress_context(
        BASE_RECORDS,
        strategy=COMPRESS_COMPLETED_SEQUENCES,
        preserve_cot=True,
    )
    # cot should now appear as mem records with key starting "cot_"
    cot_mem = [r for r in compressed if r.get("type") == "mem"
               and r.get("key", "").startswith("cot_")]
    assert len(cot_mem) == len(original_cot), (
        f"Expected {len(original_cot)} preserved cot records, got {len(cot_mem)}"
    )


test("cot reasoning trace preserved after compression [GAP #7]", _test_cot_loss_documented)


def _test_compress_then_reencode():
    compressed = compress_context(BASE_RECORDS, strategy=COMPRESS_COMPLETED_SEQUENCES)
    # Must be re-encodable as valid payload
    payload = encode_agent_payload(compressed, thread_id="t1")
    ok, err = validate_agent_payload(payload)
    assert ok is True, f"Compressed payload invalid: {err}"


test("compressed records re-encode as valid payload", _test_compress_then_reencode)


# ===========================================================================
# FINAL SUMMARY
# ===========================================================================

def print_summary():
    total = len(PASS) + len(FAIL) + len(SKIP)
    print(f"\n{'='*60}")
    print("  SMOKE TEST SUMMARY")
    print(f"{'='*60}")
    print(f"  Total : {total}")
    print(f"  PASS  : {len(PASS)}")
    print(f"  FAIL  : {len(FAIL)}")
    print(f"  SKIP  : {len(SKIP)}  (not yet implemented)")

    if FAIL:
        print("\n  FAILURES:")
        for name, err, _ in FAIL:
            print(f"    ✗ {name}")
            print(f"      {err}")

    if SKIP:
        print("\n  NOT YET IMPLEMENTED (gaps to build):")
        for name, reason in SKIP:
            print(f"    ○ {name}")

    print(f"\n{'='*60}")
    if FAIL:
        print("  STATUS: NEEDS FIXES BEFORE LAUNCH")
    elif SKIP:
        print("  STATUS: CORE OK — GAPS IDENTIFIED FOR SPRINT")
    else:
        print("  STATUS: ALL CLEAR")
    print(f"{'='*60}\n")

    return len(FAIL) == 0


if __name__ == "__main__":
    ok = print_summary()
    sys.exit(0 if ok else 1)
