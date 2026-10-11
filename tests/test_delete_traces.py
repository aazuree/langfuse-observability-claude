# tests/test_delete_traces.py
"""`--delete-traces`: clear a session out of Langfuse before re-sending it.

Langfuse v4 keeps every export of a span, so re-sending a session that is
still there doubles its cost. Deleting first is the only clean re-ingest;
reprocess itself must not delete, because Langfuse deletes asynchronously
and a late cascade wipes freshly ingested observations.

Each turn is its own trace, so a session's traces are one per turn, plus the
single `trace-<session>` trace written before that layout.
"""
import importlib.util
import json
import os
import uuid
from urllib.error import URLError

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook", os.path.join(os.path.dirname(__file__), "..", "langfuse-hook.py"))
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)

import langfuse_common


def _two_turns():
    rows = []
    for i in (1, 2):
        rows += [
            {"type": "user", "timestamp": f"2026-10-11T10:0{i}:00Z",
             "message": {"role": "user", "content": f"q{i}"}},
            {"type": "assistant", "timestamp": f"2026-10-11T10:0{i}:01Z",
             "message": {"id": f"m{i}", "role": "assistant", "stop_reason": "end_turn",
                         "content": [{"type": "text", "text": "a"}], "usage": {}}},
        ]
    return "\n".join(json.dumps(r) for r in rows) + "\n"


def _projects(tmp_path, *session_ids):
    proj = tmp_path / ".claude" / "projects" / "p"
    proj.mkdir(parents=True)
    for sid in session_ids:
        (proj / f"{sid}.jsonl").write_text(_two_turns())


class _Resp:
    status = 200

    def read(self):
        return b"{}"

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _turn_trace(sid, i):
    return uuid.uuid5(uuid.NAMESPACE_URL, f"{sid}:turn:{i}").hex


def test_session_trace_ids_cover_every_turn_and_the_old_session_trace(tmp_path):
    _projects(tmp_path, "s1")
    path = str(tmp_path / ".claude" / "projects" / "p" / "s1.jsonl")
    assert hook.session_trace_ids("s1", path) == [
        _turn_trace("s1", 0), _turn_trace("s1", 1), "trace-s1"]


def test_deletes_every_local_session_and_resets_its_state(tmp_path, monkeypatch):
    _projects(tmp_path, "s1", "s2")
    monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
    monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))
    hook.save_state("s1", 10, 2)
    hook.save_subagent_state("s1", {"a": {}})
    monkeypatch.setattr(hook, "LANGFUSE_PUBLIC_KEY", "pk")
    monkeypatch.setattr(hook, "LANGFUSE_SECRET_KEY", "sk")
    deleted = []
    monkeypatch.setattr(hook, "delete_traces", lambda ids: deleted.extend(ids) or True)
    scores = []
    monkeypatch.setattr(hook, "delete_scores", lambda ids: scores.extend(ids) or True)

    assert hook.delete_all_traces() == 0
    # Session scores are not part of any trace, so a trace delete leaves them.
    assert hook.session_score_id("s2", "session_cost_usd") in scores
    assert len(scores) == 2 * len(hook.SESSION_SCORE_NAMES)
    assert "trace-s1" in deleted and "trace-s2" in deleted
    assert _turn_trace("s2", 1) in deleted
    # The next fire must re-send the whole session, not just new lines.
    assert hook.load_state("s1") == (0, 0)
    assert hook.load_subagent_state("s1") == {}


def test_reports_failures(tmp_path, monkeypatch):
    _projects(tmp_path, "s1")
    monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
    monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setattr(hook, "LANGFUSE_PUBLIC_KEY", "pk")
    monkeypatch.setattr(hook, "LANGFUSE_SECRET_KEY", "sk")
    monkeypatch.setattr(hook, "delete_traces", lambda ids: False)
    monkeypatch.setattr(hook, "delete_scores", lambda ids: True)
    assert hook.delete_all_traces() == 1


def test_a_failed_score_delete_is_a_failure(tmp_path, monkeypatch):
    _projects(tmp_path, "s1")
    monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
    monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setattr(hook, "LANGFUSE_PUBLIC_KEY", "pk")
    monkeypatch.setattr(hook, "LANGFUSE_SECRET_KEY", "sk")
    monkeypatch.setattr(hook, "delete_traces", lambda ids: True)
    monkeypatch.setattr(hook, "delete_scores", lambda ids: False)
    assert hook.delete_all_traces() == 1


def test_score_events_use_the_ids_that_get_deleted(tmp_path):
    """The ids the delete targets are the ids the scores were sent with."""
    turns = [{"usage": {"cache_read": 5, "cache_create": 5}, "tool_calls": []}]
    events = hook.build_hook_score_events("s1", turns, 1.0, {})
    assert events
    for e in events:
        assert e["body"]["name"] in hook.SESSION_SCORE_NAMES
        assert e["body"]["id"] == hook.session_score_id("s1", e["body"]["name"])


class TestDeleteTraces:
    def test_batches_of_30_in_one_request_each(self, monkeypatch):
        monkeypatch.setattr(langfuse_common, "LANGFUSE_PUBLIC_KEY", "pk")
        monkeypatch.setattr(langfuse_common, "LANGFUSE_SECRET_KEY", "sk")
        requests = []
        monkeypatch.setattr(hook, "urlopen", lambda req, timeout=30: requests.append(req) or _Resp())
        ids = [f"t{i}" for i in range(31)]
        assert hook.delete_traces(ids) is True
        assert [r.get_method() for r in requests] == ["DELETE", "DELETE"]
        assert requests[0].full_url.endswith("/api/public/traces")
        assert json.loads(requests[0].data)["traceIds"] == ids[:30]
        assert json.loads(requests[1].data)["traceIds"] == ids[30:]

    def test_failure_is_logged_not_swallowed(self, monkeypatch):
        logged = []
        monkeypatch.setattr(hook, "log", logged.append)

        def down(req, timeout=30):
            raise URLError("connection refused")
        monkeypatch.setattr(hook, "urlopen", down)
        assert hook.delete_traces(["t1"]) is False
        assert any("[ERROR]" in m and "DELETE" in m for m in logged)


class TestDeleteScores:
    def test_one_delete_per_score(self, monkeypatch):
        monkeypatch.setattr(langfuse_common, "LANGFUSE_PUBLIC_KEY", "pk")
        monkeypatch.setattr(langfuse_common, "LANGFUSE_SECRET_KEY", "sk")
        requests = []
        monkeypatch.setattr(hook, "urlopen", lambda req, timeout=30: requests.append(req) or _Resp())
        assert hook.delete_scores(["score-a", "score-b"]) is True
        assert [r.get_method() for r in requests] == ["DELETE", "DELETE"]
        assert requests[0].full_url.endswith("/api/public/scores/score-a")
        assert requests[0].data is None

    def test_failure_returns_false(self, monkeypatch):
        monkeypatch.setattr(hook, "log", lambda m: None)

        def down(req, timeout=30):
            raise URLError("connection refused")
        monkeypatch.setattr(hook, "urlopen", down)
        assert hook.delete_scores(["score-a"]) is False


def test_cli_flag_dispatches(monkeypatch):
    called = []
    monkeypatch.setattr(hook, "delete_all_traces", lambda: called.append(True) or 0)
    monkeypatch.setattr("sys.argv", ["langfuse-hook.py", "--delete-traces"])
    try:
        hook.main()
    except SystemExit as e:
        assert e.code == 0
    assert called
