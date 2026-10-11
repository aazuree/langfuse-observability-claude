# tests/test_otlp.py
"""Translation of the hook's internal events into an OTLP/JSON trace export.

The hook builds trace/generation/span/score dicts (the shape the legacy batch
ingestion API took). Langfuse v4 retires that API, so langfuse_otlp turns the
same dicts into OTLP spans for /api/public/otel/v1/traces, and hands scores
back separately for /api/public/scores.
"""
import json
import re

import langfuse_otlp as otlp

TRACE_ID = "0" * 31 + "1"


def _attrs(span):
    out = {}
    for kv in span["attributes"]:
        v = kv["value"]
        if "stringValue" in v:
            out[kv["key"]] = v["stringValue"]
        elif "intValue" in v:
            out[kv["key"]] = int(v["intValue"])
        elif "doubleValue" in v:
            out[kv["key"]] = v["doubleValue"]
        elif "boolValue" in v:
            out[kv["key"]] = v["boolValue"]
        elif "arrayValue" in v:
            out[kv["key"]] = [x["stringValue"] for x in v["arrayValue"]["values"]]
    return out


def _events():
    return [
        {"type": "trace-create", "body": {
            "id": TRACE_ID, "name": "fix the parser", "sessionId": "sess-1",
            "userId": "alice", "tags": ["claude-code", "opus"],
            "metadata": {"git_branch": "main", "worktree": {"name": "wt"}, "empty": None},
        }},
        {"type": "generation-create", "body": {
            "id": "gen-a", "traceId": TRACE_ID, "name": "Turn 1: fix",
            "model": "claude-opus-5-5", "input": "fix it", "output": "done",
            "startTime": "2026-10-11T10:00:00+00:00",
            "endTime": "2026-10-11T10:00:05.500+00:00",
            "completionStartTime": "2026-10-11T10:00:01+00:00",
            "usageDetails": {"input": 10, "output": 20},
            "costDetails": {"input": 0.1, "total": 0.3},
            "metadata": {"speed": "fast", "tools_used": ["Read"]},
            "attributes": {"gen_ai.request.model": "claude-opus-5-5"},
        }},
        {"type": "span-create", "body": {
            "id": "span-a-0", "traceId": TRACE_ID, "parentObservationId": "gen-a",
            "name": "Read", "startTime": "2026-10-11T10:00:02+00:00",
            "endTime": "2026-10-11T10:00:03+00:00",
            "input": {"file_path": "x.py"}, "output": "contents",
            "metadata": {"tool_use_id": "toolu_1"},
        }},
        {"type": "score-create", "body": {
            "id": "score-1", "sessionId": "sess-1", "name": "cache_hit_rate",
            "dataType": "NUMERIC", "value": 0.9,
        }},
    ]


def _spans(payload):
    return [s for rs in payload["resourceSpans"] for ss in rs["scopeSpans"] for s in ss["spans"]]


def test_ids_are_hex_and_deterministic():
    assert otlp.span_id("gen-a") == otlp.span_id("gen-a")
    assert re.fullmatch(r"[0-9a-f]{16}", otlp.span_id("gen-a"))
    assert otlp.span_id("gen-a") != otlp.span_id("gen-b")
    assert otlp.trace_id(TRACE_ID) == TRACE_ID
    assert re.fullmatch(r"[0-9a-f]{32}", otlp.trace_id("trace-sess-1"))


def test_scores_are_split_out():
    payload, scores = otlp.build_export(_events())
    assert [s["name"] for s in scores] == ["cache_hit_rate"]
    assert len(_spans(payload)) == 2


def test_generation_maps_to_langfuse_attributes():
    payload, _ = otlp.build_export(_events())
    gen = next(s for s in _spans(payload) if s["name"] == "Turn 1: fix")
    a = _attrs(gen)
    assert gen["traceId"] == TRACE_ID
    assert gen["spanId"] == otlp.span_id("gen-a")
    assert "parentSpanId" not in gen
    assert a["langfuse.observation.type"] == "generation"
    assert a["langfuse.observation.model.name"] == "claude-opus-5-5"
    assert json.loads(a["langfuse.observation.usage_details"]) == {"input": 10, "output": 20}
    assert json.loads(a["langfuse.observation.cost_details"]) == {"input": 0.1, "total": 0.3}
    assert a["langfuse.observation.input"] == "fix it"
    assert a["langfuse.observation.output"] == "done"
    assert a["langfuse.observation.completion_start_time"] == "2026-10-11T10:00:01+00:00"
    assert a["langfuse.observation.metadata.speed"] == "fast"
    assert json.loads(a["langfuse.observation.metadata.tools_used"]) == ["Read"]
    assert a["gen_ai.request.model"] == "claude-opus-5-5"
    assert gen["startTimeUnixNano"] == str(1791712800 * 10**9)
    assert gen["endTimeUnixNano"] == str(1791712805 * 10**9 + 500_000_000)


def test_root_span_carries_trace_attributes():
    payload, _ = otlp.build_export(_events())
    gen = next(s for s in _spans(payload) if s["name"] == "Turn 1: fix")
    a = _attrs(gen)
    assert a["langfuse.trace.name"] == "fix the parser"
    assert a["langfuse.trace.tags"] == ["claude-code", "opus"]
    assert a["langfuse.trace.metadata.git_branch"] == "main"
    assert json.loads(a["langfuse.trace.metadata.worktree"]) == {"name": "wt"}
    assert "langfuse.trace.metadata.empty" not in a


def test_every_span_carries_session_and_user():
    payload, _ = otlp.build_export(_events())
    for s in _spans(payload):
        a = _attrs(s)
        assert a["session.id"] == "sess-1"
        assert a["user.id"] == "alice"


def test_child_span_is_a_tool_nested_under_its_parent():
    payload, _ = otlp.build_export(_events())
    tool = next(s for s in _spans(payload) if s["name"] == "Read")
    a = _attrs(tool)
    assert tool["parentSpanId"] == otlp.span_id("gen-a")
    assert a["langfuse.observation.type"] == "tool"
    assert json.loads(a["langfuse.observation.input"]) == {"file_path": "x.py"}
    assert "langfuse.trace.name" not in a


def test_spans_without_their_trace_event_use_their_own_session():
    # A background subagent's later turns are exported without the
    # trace-create of the turn that launched it.
    events = [e for e in _events() if e["type"] == "span-create"]
    events[0]["body"].update(sessionId="sess-1", userId="alice")
    payload, _ = otlp.build_export(events)
    a = _attrs(_spans(payload)[0])
    assert a["session.id"] == "sess-1"
    assert a["user.id"] == "alice"


def test_missing_end_time_falls_back_to_start():
    events = _events()
    events[2]["body"]["endTime"] = None
    payload, _ = otlp.build_export(events)
    tool = next(s for s in _spans(payload) if s["name"] == "Read")
    assert tool["endTimeUnixNano"] == tool["startTimeUnixNano"]
