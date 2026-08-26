"""Tests for session-start-hook.py (StopFailure handler)."""
import importlib.util
import io
import json
import os
import sys
import unittest.mock as mock
from pathlib import Path
import pytest

_spec = importlib.util.spec_from_file_location(
    "session_start_hook",
    os.path.join(os.path.dirname(__file__), "..", "session-start-hook.py"),
)
sh = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sh)


class TestBuildStopFailureBatch:
    def _write_transcript(self, entries, tmp_path):
        p = tmp_path / "sess.jsonl"
        p.write_text("\n".join(json.dumps(e) for e in entries))
        return str(p)

    def test_stop_failure_tags(self, tmp_path):
        path = self._write_transcript([], tmp_path)
        batch = sh.build_stop_failure_batch("sess-abc", path)
        assert len(batch) == 1
        evt = batch[0]
        assert evt["type"] == "trace-update"
        body = evt["body"]
        assert body["id"] == "trace-sess-abc"
        assert "stop-failure" in body["tags"]

    def test_captures_last_api_error(self, tmp_path):
        entries = [
            {"type": "system", "subtype": "api_error", "timestamp": "2026-01-01T00:00:00Z",
             "error": {"status": 529, "message": "Overloaded"}},
        ]
        path = self._write_transcript(entries, tmp_path)
        batch = sh.build_stop_failure_batch("sess-abc", path)
        meta = batch[0]["body"]["metadata"]
        assert meta["stop_failure"]["last_error_status"] == 529
        assert meta["stop_failure"]["last_error_message"] == "Overloaded"

    def test_no_api_errors_no_metadata(self, tmp_path):
        path = self._write_transcript([], tmp_path)
        batch = sh.build_stop_failure_batch("sess-abc", path)
        body = batch[0]["body"]
        assert "metadata" not in body

    def test_last_error_wins(self, tmp_path):
        entries = [
            {"type": "system", "subtype": "api_error", "timestamp": "2026-01-01T00:00:00Z",
             "error": {"status": 429, "message": "Rate limit"}},
            {"type": "system", "subtype": "api_error", "timestamp": "2026-01-01T00:01:00Z",
             "error": {"status": 529, "message": "Overloaded"}},
        ]
        path = self._write_transcript(entries, tmp_path)
        batch = sh.build_stop_failure_batch("sess-abc", path)
        meta = batch[0]["body"]["metadata"]
        assert meta["stop_failure"]["last_error_status"] == 529


class TestMainDispatch:
    def _run_main(self, payload, monkeypatch):
        monkeypatch.setattr(sh, "LANGFUSE_PUBLIC_KEY", "pk-test")
        monkeypatch.setattr(sh, "LANGFUSE_SECRET_KEY", "sk-test")
        monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
        sent = []
        monkeypatch.setattr(sh, "send_batch", lambda batch: sent.append(batch))
        sh.main()
        return sent

    def test_session_start_event_sends_nothing(self, monkeypatch):
        sent = self._run_main(
            {"hook_event_name": "SessionStart", "session_id": "s1",
             "source": "startup", "model": "claude-opus-4-8", "cwd": "/tmp"},
            monkeypatch,
        )
        assert sent == []

    def test_stop_failure_event_sends_batch(self, monkeypatch, tmp_path):
        p = tmp_path / "t.jsonl"
        p.write_text("")
        sent = self._run_main(
            {"hook_event_name": "StopFailure", "session_id": "s1",
             "transcript_path": str(p)},
            monkeypatch,
        )
        assert len(sent) == 1
        assert "stop-failure" in sent[0][0]["body"]["tags"]


class TestExtractLastApiError:
    def _write_transcript(self, entries, tmp_path):
        p = tmp_path / "sess.jsonl"
        p.write_text("\n".join(json.dumps(e) for e in entries))
        return str(p)

    def test_empty_transcript(self, tmp_path):
        path = self._write_transcript([], tmp_path)
        assert sh.extract_last_api_error(path) == {}

    def test_single_error(self, tmp_path):
        entries = [
            {"type": "system", "subtype": "api_error", "timestamp": "2026-01-01T00:00:00Z",
             "error": {"status": 500, "message": "Internal"}},
        ]
        path = self._write_transcript(entries, tmp_path)
        result = sh.extract_last_api_error(path)
        assert result["last_error_status"] == 500
        assert result["last_error_message"] == "Internal"

    def test_missing_file(self):
        assert sh.extract_last_api_error("/nonexistent/path.jsonl") == {}


class TestSendBatchReportsRejections:
    """The StopFailure hook POSTs to the same 207 endpoint as the main hook.

    It advances no state, so a rejection here costs one stop-failure tag rather
    than a window of turns — but logging a 100-char prefix of the body hid
    rejections just the same, and `errors` trails `successes` in that body.
    """

    def _resp(self, body, status=207):
        class MockResp:
            def __init__(self):
                self.status = status

            def read(self):
                return body.encode()

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        return MockResp()

    def test_rejected_events_are_logged_as_an_error(self, monkeypatch):
        body = json.dumps({"successes": [],
                           "errors": [{"id": "evt-x", "status": 400, "message": "bad event"}]})
        monkeypatch.setattr(sh, "urlopen", lambda req, timeout=10: self._resp(body))
        logged = []
        monkeypatch.setattr(sh, "log", lambda m: logged.append(m))

        sh.send_batch([{"id": "evt-x"}])

        assert any("[ERROR]" in m for m in logged)
        assert any("evt-x" in m for m in logged)

    def test_accepted_events_do_not_log_an_error(self, monkeypatch):
        body = json.dumps({"successes": [{"id": "evt-x", "status": 201}], "errors": []})
        monkeypatch.setattr(sh, "urlopen", lambda req, timeout=10: self._resp(body))
        logged = []
        monkeypatch.setattr(sh, "log", lambda m: logged.append(m))

        sh.send_batch([{"id": "evt-x"}])

        assert not any("[ERROR]" in m for m in logged)
