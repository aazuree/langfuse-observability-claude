# tests/test_delete_traces.py
"""`--delete-traces`: clear the way for a reprocess that renumbers turns.

Generation IDs are keyed on the turn index. When turn building changes how
turns are counted, `--reprocess` writes new generations beside the old ones
and the trace double-counts cost. Deleting the traces first is the only
clean migration; reprocess itself must not delete, because Langfuse deletes
asynchronously and a late cascade wipes freshly ingested observations.
"""
import importlib.util
import io
import json
import os
from urllib.error import HTTPError, URLError

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook", os.path.join(os.path.dirname(__file__), "..", "langfuse-hook.py"))
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)


def _projects(tmp_path, *session_ids):
    proj = tmp_path / ".claude" / "projects" / "p"
    proj.mkdir(parents=True)
    for sid in session_ids:
        (proj / f"{sid}.jsonl").write_text(json.dumps({"type": "user"}) + "\n")


class _Resp:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


def test_deletes_every_local_session_and_resets_its_state(tmp_path, monkeypatch):
    _projects(tmp_path, "s1", "s2")
    monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
    state = tmp_path / "state"
    monkeypatch.setattr(hook, "STATE_DIR", str(state))
    hook.save_state("s1", 10, 2)
    hook.save_subagent_state("s1", {"a": {}})
    monkeypatch.setattr(hook, "LANGFUSE_PUBLIC_KEY", "pk")
    monkeypatch.setattr(hook, "LANGFUSE_SECRET_KEY", "sk")
    deleted = []
    monkeypatch.setattr(hook, "delete_trace", lambda tid: deleted.append(tid) or True)

    assert hook.delete_all_traces() == 0
    assert sorted(deleted) == ["trace-s1", "trace-s2"]
    # The next fire must re-send the whole session, not just new lines.
    assert hook.load_state("s1") == (0, 0)
    assert hook.load_subagent_state("s1") == {}


def test_reports_failures(tmp_path, monkeypatch):
    _projects(tmp_path, "s1")
    monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
    monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setattr(hook, "LANGFUSE_PUBLIC_KEY", "pk")
    monkeypatch.setattr(hook, "LANGFUSE_SECRET_KEY", "sk")
    monkeypatch.setattr(hook, "delete_trace", lambda tid: False)
    assert hook.delete_all_traces() == 1


class TestDeleteTrace:
    def test_success_and_already_gone_both_count_as_deleted(self, monkeypatch):
        monkeypatch.setattr(hook, "urlopen", lambda req, timeout=15: _Resp())
        assert hook.delete_trace("trace-x") is True

        def gone(req, timeout=15):
            raise HTTPError(req.full_url, 404, "not found", {}, io.BytesIO(b""))
        monkeypatch.setattr(hook, "urlopen", gone)
        assert hook.delete_trace("trace-x") is True

    def test_failure_is_logged_not_swallowed(self, monkeypatch):
        logged = []
        monkeypatch.setattr(hook, "log", logged.append)

        def down(req, timeout=15):
            raise URLError("connection refused")
        monkeypatch.setattr(hook, "urlopen", down)
        assert hook.delete_trace("trace-x") is False
        assert any("trace-x" in m for m in logged)


def test_cli_flag_dispatches(monkeypatch):
    called = []
    monkeypatch.setattr(hook, "delete_all_traces", lambda: called.append(True) or 0)
    monkeypatch.setattr("sys.argv", ["langfuse-hook.py", "--delete-traces"])
    try:
        hook.main()
    except SystemExit as e:
        assert e.code == 0
    assert called
