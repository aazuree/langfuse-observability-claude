#!/usr/bin/env python3
"""Translate the hook's events into an OTLP/JSON trace export for Langfuse.

The hook builds trace-create / generation-create / span-create / score-create
dicts — the shape of Langfuse's legacy batch ingestion API, which v4 retires.
This module turns those dicts into OTLP spans for /api/public/otel/v1/traces,
using the `langfuse.*` attributes Langfuse maps natively, and hands the scores
back unchanged for /api/public/scores (scores are not spans).

Langfuse v4 does not deduplicate a span exported twice, so callers must export
every observation once. IDs are derived deterministically from the hook's own
IDs, so a re-ingest after a delete reproduces the same trace and span IDs.

Stdlib only.
"""

import hashlib
import json
import re
from datetime import datetime

SCOPE_NAME = "langfuse-observability-hook"
SERVICE_NAME = "claude-code"

_HEX32 = re.compile(r"^[0-9a-f]{32}$")


def trace_id(value: str) -> str:
    """32-hex OTel trace ID. Hex IDs pass through; anything else is hashed."""
    if _HEX32.match(value):
        return value
    return hashlib.sha256(value.encode()).hexdigest()[:32]


def span_id(value: str) -> str:
    """16-hex OTel span ID derived from the hook's observation ID."""
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def _nanos(iso: str | None) -> str | None:
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:
        return None
    # Integer arithmetic: float seconds would lose the low digits.
    return str((int(dt.timestamp()) * 10**6 + dt.microsecond) * 1000)


def _value(v):
    """OTLP AnyValue. Dicts, mixed lists and None-free containers become JSON."""
    if isinstance(v, bool):
        return {"boolValue": v}
    if isinstance(v, int):
        return {"intValue": str(v)}
    if isinstance(v, float):
        return {"doubleValue": v}
    if isinstance(v, str):
        return {"stringValue": v}
    return {"stringValue": json.dumps(v, default=str)}


def _text(v) -> str:
    return v if isinstance(v, str) else json.dumps(v, default=str)


def _kv(key: str, v) -> dict:
    return {"key": key, "value": _value(v)}


def _metadata_attrs(prefix: str, metadata: dict | None) -> list:
    """One attribute per key, so each key is filterable in Langfuse.

    Non-string values are JSON-encoded: Langfuse metadata attribute values
    are strings. None values are omitted, never sent as "null".
    """
    out = []
    for key, v in (metadata or {}).items():
        if v is None:
            continue
        out.append(_kv(f"{prefix}.{key}", v if isinstance(v, str) else json.dumps(v, default=str)))
    return out


def _trace_attrs(trace: dict) -> list:
    attrs = []
    if trace.get("name"):
        attrs.append(_kv("langfuse.trace.name", trace["name"]))
    if trace.get("tags"):
        attrs.append({"key": "langfuse.trace.tags", "value": {"arrayValue": {
            "values": [{"stringValue": t} for t in trace["tags"]]}}})
    attrs += _metadata_attrs("langfuse.trace.metadata", trace.get("metadata"))
    return attrs


def _span(body: dict, obs_type: str, trace: dict) -> dict:
    start = _nanos(body.get("startTime"))
    end = _nanos(body.get("endTime")) or start
    attrs = [_kv("langfuse.observation.type", obs_type)]
    # Observations carry their own session/user: a background subagent's
    # later turns join a trace whose trace-create was sent in an earlier fire.
    sid = body.get("sessionId") or trace.get("sessionId")
    uid = body.get("userId") or trace.get("userId")
    if sid:
        attrs.append(_kv("session.id", sid))
    if uid:
        attrs.append(_kv("user.id", uid))
    if body.get("model"):
        attrs.append(_kv("langfuse.observation.model.name", body["model"]))
    if body.get("usageDetails"):
        attrs.append(_kv("langfuse.observation.usage_details", json.dumps(body["usageDetails"])))
    if body.get("costDetails"):
        attrs.append(_kv("langfuse.observation.cost_details", json.dumps(body["costDetails"])))
    if body.get("completionStartTime"):
        attrs.append(_kv("langfuse.observation.completion_start_time", body["completionStartTime"]))
    if body.get("input") is not None:
        attrs.append(_kv("langfuse.observation.input", _text(body["input"])))
    if body.get("output") is not None:
        attrs.append(_kv("langfuse.observation.output", _text(body["output"])))
    attrs += _metadata_attrs("langfuse.observation.metadata", body.get("metadata"))
    attrs += [_kv(k, v) for k, v in (body.get("attributes") or {}).items() if v is not None]

    parent = body.get("parentObservationId")
    if not parent:
        attrs += _trace_attrs(trace)

    span = {
        "traceId": trace_id(body["traceId"]),
        "spanId": span_id(body["id"]),
        "name": body.get("name") or obs_type,
        "kind": 1,  # SPAN_KIND_INTERNAL
        "attributes": attrs,
    }
    if parent:
        span["parentSpanId"] = span_id(parent)
    if start:
        span["startTimeUnixNano"] = start
        span["endTimeUnixNano"] = end
    return span


def build_export(events: list[dict]) -> tuple[dict, list]:
    """Return (OTLP/JSON export, score bodies) for a list of hook events."""
    traces = {e["body"]["id"]: e["body"] for e in events if e["type"] == "trace-create"}
    spans, scores = [], []
    for e in events:
        body = e["body"]
        if e["type"] == "score-create":
            scores.append(body)
        elif e["type"] in ("generation-create", "span-create"):
            obs_type = "generation" if e["type"] == "generation-create" else "tool"
            spans.append(_span(body, obs_type, traces.get(body["traceId"], {})))
    export = {"resourceSpans": [{
        "resource": {"attributes": [_kv("service.name", SERVICE_NAME)]},
        "scopeSpans": [{"scope": {"name": SCOPE_NAME}, "spans": spans}],
    }]}
    return export, scores
