# tests/test_robustness.py
"""Failure handling around the edges of the hook: state writes, crashes, and
HTTP-level rejections.

The hook runs unattended on every Stop, so a failure it does not log is a
failure nobody sees. Each test here pins one way that used to go unnoticed.
"""
import importlib.util
import io
import json
import os
import sys
from urllib.error import HTTPError

import pytest

_dir = os.path.join(os.path.dirname(__file__), "..")
_spec = importlib.util.spec_from_file_location(
    "langfuse_hook", os.path.join(_dir, "langfuse-hook.py"))
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)
_sspec = importlib.util.spec_from_file_location(
    "session_start_hook", os.path.join(_dir, "session-start-hook.py"))
sh = importlib.util.module_from_spec(_sspec)
_sspec.loader.exec_module(sh)

import langfuse_common


# ---------------------------------------------------------------------------
# State files are replaced atomically
# ---------------------------------------------------------------------------

class TestAtomicWrite:
    def test_writes_content(self, tmp_path):
        target = tmp_path / "s.offset"
        langfuse_common.atomic_write_text(str(target), "hello")
        assert target.read_text() == "hello"

    def test_failed_write_keeps_the_old_file(self, tmp_path, monkeypatch):
        """A crash mid-write used to leave a truncated state file. load_state
        reads that as offset 0 and the next fire resends the whole session."""
        target = tmp_path / "s.offset"
        target.write_text('{"lines": 40, "turns": 4}')

        def boom(*a, **kw):
            raise OSError("disk full")
        monkeypatch.setattr(langfuse_common.os, "replace", boom)

        with pytest.raises(OSError):
            langfuse_common.atomic_write_text(str(target), '{"lines": 9')
        assert target.read_text() == '{"lines": 40, "turns": 4}'
        assert os.listdir(tmp_path) == ["s.offset"], "temp file left behind"

    def test_save_state_goes_through_atomic_write(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path))
        written = []
        monkeypatch.setattr(hook, "atomic_write_text",
                            lambda path, text: written.append(os.path.basename(path)))
        hook.save_state("sess", 10, 2)
        hook.save_subagent_state("sess", {"a": {}})
        assert written == ["sess.offset", "sess.subagents.json"]


# ---------------------------------------------------------------------------
# Unexpected exceptions are logged, not lost to stderr
# ---------------------------------------------------------------------------

class TestExceptionGuard:
    def _stdin(self, monkeypatch, payload):
        monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
        monkeypatch.setattr(sys, "argv", ["langfuse-hook.py"])

    def test_stop_hook_logs_a_crash_with_its_traceback(self, monkeypatch):
        self._stdin(monkeypatch, {"session_id": "s1", "transcript_path": "/x.jsonl"})
        monkeypatch.setattr(hook, "LANGFUSE_PUBLIC_KEY", "pk")
        monkeypatch.setattr(hook, "LANGFUSE_SECRET_KEY", "sk")

        def crash(*a, **kw):
            raise KeyError("usage")
        monkeypatch.setattr(hook, "process_session", crash)
        logged = []
        monkeypatch.setattr(hook, "log", logged.append)

        hook.main()  # must not raise

        assert any("[ERROR]" in m and "s1" in m and "KeyError" in m for m in logged)
        assert any("Traceback" in m for m in logged)

    def test_stdin_that_is_not_an_object_is_logged(self, monkeypatch):
        monkeypatch.setattr(sys, "stdin", io.StringIO("[1, 2]"))
        monkeypatch.setattr(sys, "argv", ["langfuse-hook.py"])
        logged = []
        monkeypatch.setattr(hook, "log", logged.append)
        hook.main()
        assert any("[ERROR]" in m for m in logged)

    def test_reprocess_survives_a_session_that_crashes(self, tmp_path, monkeypatch):
        """reprocess_all only caught I/O and JSON errors, so one malformed
        transcript raising TypeError aborted every session after it."""
        projects = tmp_path / ".claude" / "projects" / "p"
        projects.mkdir(parents=True)
        for sid in ("aaa", "bbb"):
            (projects / f"{sid}.jsonl").write_text(json.dumps({"type": "user"}) + "\n")
        monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))
        monkeypatch.setattr(hook, "LANGFUSE_PUBLIC_KEY", "pk")
        monkeypatch.setattr(hook, "LANGFUSE_SECRET_KEY", "sk")
        monkeypatch.setattr(hook, "log", lambda m: None)

        seen = []

        def process(session_id, *a, **kw):
            seen.append(session_id)
            if session_id == "aaa":
                raise TypeError("bad entry")
        monkeypatch.setattr(hook, "process_session", process)

        hook.reprocess_all()
        assert seen == ["aaa", "bbb"]

    def test_stop_failure_hook_logs_a_crash(self, monkeypatch):
        monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(
            {"hook_event_name": "StopFailure", "session_id": "s1",
             "transcript_path": "/x.jsonl"})))
        monkeypatch.setattr(sh, "LANGFUSE_PUBLIC_KEY", "pk")
        monkeypatch.setattr(sh, "LANGFUSE_SECRET_KEY", "sk")

        def crash(*a, **kw):
            raise ValueError("nope")
        monkeypatch.setattr(sh, "build_stop_failure_batch", crash)
        logged = []
        monkeypatch.setattr(sh, "log", logged.append)

        sh.main()  # must not raise
        assert any("[ERROR]" in m and "ValueError" in m for m in logged)


# ---------------------------------------------------------------------------
# HTTP-level rejections
# ---------------------------------------------------------------------------

def _raise_http(code):
    def opener(req, timeout=15):
        raise HTTPError(req.full_url, code, f"status {code}", {}, io.BytesIO(b"{}"))
    return opener


class TestHttpErrorStatus:
    @pytest.fixture(autouse=True)
    def _keys(self, monkeypatch):
        monkeypatch.setattr(langfuse_common, "LANGFUSE_PUBLIC_KEY", "pk")
        monkeypatch.setattr(langfuse_common, "LANGFUSE_SECRET_KEY", "sk")
        self.logged = []
        monkeypatch.setattr(hook, "log", self.logged.append)

    @pytest.mark.parametrize("code", [400, 413, 422])
    def test_request_the_server_can_never_accept_advances(self, code, monkeypatch):
        """HTTPError subclasses URLError, so a 413 (batch too large) used to be
        retried on every fire, forever, with an ever-growing batch."""
        monkeypatch.setattr(hook, "urlopen", _raise_http(code))
        assert hook.send_to_langfuse([{"id": "e1"}]) is True
        assert any("[ERROR]" in m and str(code) in m for m in self.logged)

    @pytest.mark.parametrize("code", [401, 403, 429, 500, 503])
    def test_fixable_or_transient_status_is_retried(self, code, monkeypatch):
        """Bad keys (401/403) get fixed by a human; dropping the events in
        the meantime would lose them for good. Keep them for the next fire."""
        monkeypatch.setattr(hook, "urlopen", _raise_http(code))
        assert hook.send_to_langfuse([{"id": "e1"}]) is False
