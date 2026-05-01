"""Tests for session-start-hook.py (SessionStart + StopFailure handlers)."""
import importlib.util
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


class TestDeriveModelFamily:
    def test_opus(self):
        assert sh.derive_model_family("claude-opus-4-6") == "opus"

    def test_sonnet(self):
        assert sh.derive_model_family("claude-sonnet-4-6") == "sonnet"

    def test_haiku(self):
        assert sh.derive_model_family("claude-haiku-4-5") == "haiku"

    def test_unknown(self):
        assert sh.derive_model_family("unknown-model") == "unknown"

    def test_empty(self):
        assert sh.derive_model_family("") == "unknown"


class TestBuildSessionStartBatch:
    def test_basic_trace_created(self):
        batch = sh.build_session_start_batch(
            session_id="sess-abc123",
            source="startup",
            model="claude-sonnet-4-6",
            cwd="/home/user/project",
        )
        assert len(batch) == 1
        evt = batch[0]
        assert evt["type"] == "trace-create"
        body = evt["body"]
        assert body["id"] == "trace-sess-abc123"
        assert body["sessionId"] == "sess-abc123"
        assert "source:startup" in body["tags"]
        assert "sonnet" in body["tags"]
        assert "claude-code" in body["tags"]

    def test_resume_source_tag(self):
        batch = sh.build_session_start_batch("s1", "resume", "claude-opus-4-6", "/tmp")
        tags = batch[0]["body"]["tags"]
        assert "source:resume" in tags
        assert "opus" in tags

    def test_compact_source_tag(self):
        batch = sh.build_session_start_batch("s1", "compact", "", "/tmp")
        tags = batch[0]["body"]["tags"]
        assert "source:compact" in tags

    def test_model_empty_no_family_tag(self):
        batch = sh.build_session_start_batch("s1", "startup", "", "/tmp")
        tags = batch[0]["body"]["tags"]
        model_tags = [t for t in tags if t in ("opus", "sonnet", "haiku")]
        assert model_tags == []

    def test_repo_name_from_cwd(self):
        batch = sh.build_session_start_batch("s1", "startup", "claude-sonnet-4-6", "/home/user/myproject")
        tags = batch[0]["body"]["tags"]
        assert "myproject" in tags

    def test_metadata_fields(self):
        batch = sh.build_session_start_batch("s1", "resume", "claude-opus-4-6", "/tmp/proj")
        meta = batch[0]["body"]["metadata"]
        assert meta["session_source"] == "resume"
        assert meta["initial_model"] == "claude-opus-4-6"
        assert meta["cwd"] == "/tmp/proj"


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
