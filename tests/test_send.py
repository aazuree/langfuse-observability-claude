# tests/test_send.py
"""send_to_langfuse: one OTLP export per fire, then scores.

The return value tells the caller whether it may advance the session's state.
Langfuse v4 does not deduplicate a re-exported span, so the trace export is
sent as a single request — it lands whole or not at all — and a retry never
duplicates part of it. Scores upsert by id and are recomputed every fire, so a
failed score send is logged but does not hold the spans back.

  * 2xx                       -> advance
  * 400 / 413 / 422           -> the payload can never be accepted: advance, log [ERROR]
  * 401 / 403 / 408 / 429 / 5xx / network -> retry next fire
"""
import gzip
import importlib.util
import io
import json
import os
from urllib.error import HTTPError, URLError

import pytest

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook",
    os.path.join(os.path.dirname(__file__), "..", "langfuse-hook.py"),
)
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)

import langfuse_common

TRACE = "a" * 32


def _batch(with_score=True):
    events = [
        {"type": "trace-create", "body": {"id": TRACE, "name": "t", "sessionId": "s"}},
        {"type": "generation-create", "body": {
            "id": "gen-1", "traceId": TRACE, "name": "Turn 1", "sessionId": "s",
            "startTime": "2026-10-11T10:00:00+00:00", "endTime": "2026-10-11T10:00:01+00:00"}},
    ]
    if with_score:
        events.append({"type": "score-create", "body": {
            "id": "score-1", "sessionId": "s", "name": "cache_hit_rate",
            "dataType": "NUMERIC", "value": 0.5}})
    return events


class _Resp:
    status = 200

    def read(self):
        return b"{}"

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _http_error(req, code):
    return HTTPError(req.full_url, code, f"status {code}", {}, io.BytesIO(b'{"error":"why"}'))


@pytest.fixture
def calls(monkeypatch):
    monkeypatch.setattr(langfuse_common, "LANGFUSE_PUBLIC_KEY", "pk")
    monkeypatch.setattr(langfuse_common, "LANGFUSE_SECRET_KEY", "sk")
    logged = []
    monkeypatch.setattr(hook, "log", logged.append)
    record = {"requests": [], "log": logged, "fail": {}}

    def opener(req, timeout=15):
        record["requests"].append(req)
        for fragment, outcome in record["fail"].items():
            if fragment in req.full_url:
                if outcome == "network":
                    raise URLError("connection refused")
                raise _http_error(req, outcome)
        return _Resp()

    monkeypatch.setattr(hook, "urlopen", opener)
    return record


def test_spans_go_to_the_otlp_endpoint_in_one_gzipped_request(calls):
    assert hook.send_to_langfuse(_batch(with_score=False)) is True
    [req] = calls["requests"]
    assert req.full_url.endswith("/api/public/otel/v1/traces")
    assert req.get_header("X-langfuse-ingestion-version") == "4"
    assert req.get_header("Content-encoding") == "gzip"
    assert req.get_header("Authorization").startswith("Basic ")
    export = json.loads(gzip.decompress(req.data))
    spans = export["resourceSpans"][0]["scopeSpans"][0]["spans"]
    assert [s["name"] for s in spans] == ["Turn 1"]


def test_scores_go_to_the_scores_api_after_the_spans(calls):
    assert hook.send_to_langfuse(_batch()) is True
    urls = [r.full_url for r in calls["requests"]]
    assert urls[0].endswith("/api/public/otel/v1/traces")
    assert urls[1].endswith("/api/public/scores")
    assert json.loads(calls["requests"][1].data)["name"] == "cache_hit_rate"


def test_a_batch_of_only_scores_sends_no_trace_export(calls):
    batch = [e for e in _batch() if e["type"] == "score-create"]
    assert hook.send_to_langfuse(batch) is True
    assert [r.full_url.rsplit("/", 1)[-1] for r in calls["requests"]] == ["scores"]


@pytest.mark.parametrize("code", [400, 413, 422])
def test_an_export_that_can_never_be_accepted_advances_loudly(calls, code):
    calls["fail"]["/otel/"] = code
    assert hook.send_to_langfuse(_batch()) is True
    assert any("[ERROR]" in m and str(code) in m and "why" in m for m in calls["log"])


@pytest.mark.parametrize("code", [401, 403, 408, 429, 500, 503])
def test_a_fixable_or_transient_export_failure_is_retried(calls, code):
    calls["fail"]["/otel/"] = code
    assert hook.send_to_langfuse(_batch()) is False


def test_a_network_error_is_retried(calls):
    calls["fail"]["/otel/"] = "network"
    assert hook.send_to_langfuse(_batch()) is False


def test_scores_are_not_sent_when_the_export_will_be_retried(calls):
    calls["fail"]["/otel/"] = 503
    hook.send_to_langfuse(_batch())
    assert not any(r.full_url.endswith("/scores") for r in calls["requests"])


def test_a_failed_score_does_not_hold_back_delivered_spans(calls):
    """Holding the state back would re-export spans Langfuse already has."""
    calls["fail"]["/scores"] = 503
    assert hook.send_to_langfuse(_batch()) is True
    assert any("[WARN]" in m and "cache_hit_rate" in m for m in calls["log"])
