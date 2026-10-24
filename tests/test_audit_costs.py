"""Tests for tools/audit_costs.py.

The audit only earns trust if each check fires on the failure it exists for,
so every check has a case that must produce a finding and one that must not.
Transcripts are synthetic, in a temp projects dir; nothing touches the network.
"""
import importlib.util
import json
import os

import pytest

_spec = importlib.util.spec_from_file_location(
    "audit_costs",
    os.path.join(os.path.dirname(__file__), "..", "tools", "audit_costs.py"),
)
audit_costs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(audit_costs)

MODEL = "claude-opus-4-8"  # $5 / $25 per 1M
P = "11111111-1111-4111-8111-111111111111"
S = "22222222-2222-4222-8222-222222222222"


def _user(uid, sid, text, ts):
    return {"type": "user", "uuid": uid, "sessionId": sid, "timestamp": ts, "cwd": "/x/repo",
            "message": {"role": "user", "content": text}}


def _asst(uid, sid, mid, ts, out, stop_reason="end_turn", model=MODEL, content=None):
    return {"type": "assistant", "uuid": uid, "sessionId": sid, "timestamp": ts,
            "requestId": f"req-{mid}",
            "message": {"id": mid, "role": "assistant", "model": model,
                        "stop_reason": stop_reason,
                        "content": content or [{"type": "text", "text": "ok"}],
                        "usage": {"input_tokens": 1000, "output_tokens": out}}}


def _cost_state(by_model, total=1.0, start_ms=1792490400000):
    """A cost-state whose process started at the transcripts' first entry."""
    return {"type": "cost-state", "totalCostUSD": total, "startTime": start_ms,
            "modelUsage": {m: {"costUSD": 0.0, "inputTokens": i, "outputTokens": o,
                               "cacheReadInputTokens": 0, "cacheCreationInputTokens": 0}
                           for m, (i, o) in by_model.items()}}


def _write(path, entries):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(e) + "\n" for e in entries))


def _session(sid, n=2, out=100, t0="2026-10-20T10:00:00Z"):
    entries = []
    for i in range(n):
        entries.append(_user(f"{sid}-u{i}", sid, f"question {i}", f"2026-10-20T10:0{i}:00Z"))
        entries.append(_asst(f"{sid}-a{i}", sid, f"{sid}-m{i}", f"2026-10-20T10:0{i}:01Z", out))
    entries[0]["timestamp"] = t0
    return entries


@pytest.fixture
def projects(tmp_path):
    return tmp_path / "projects"


@pytest.fixture
def run(projects, monkeypatch):
    """Audit `projects`; `langfuse` stands in for the stored per-session costs."""
    def _run(langfuse=None):
        if langfuse is not None:
            monkeypatch.setattr(audit_costs, "langfuse_session_costs", lambda env, since: langfuse)
        return audit_costs.audit(projects, audit_costs.DEFAULT_HOOK,
                                 {"LANGFUSE_HOST": "x"} if langfuse is not None else None)
    return _run


@pytest.fixture(autouse=True)
def _old_mtimes(monkeypatch):
    """Treat every synthetic transcript as idle, not an active session."""
    monkeypatch.setattr(audit_costs, "ACTIVE_S", -1)


# ---------------------------------------------------------------------------
# Check 1: the recount
# ---------------------------------------------------------------------------

def test_clean_session_has_no_findings(projects, run):
    _write(projects / "p" / f"{P}.jsonl",
           _session(P) + [_cost_state({MODEL: (2000, 200)})])
    report = run()
    assert report["findings"] == []
    row = report["sessions"][0]
    # 2 x (1000 in @ $5 + 100 out @ $25) per 1M
    assert row["recount_usd"] == row["hook_usd"] == pytest.approx(0.015)


def test_recount_takes_each_messages_last_entry(projects):
    """A streamed message is several entries with a running output count."""
    entries = _session(P, n=1, out=10)
    entries.append(dict(entries[-1], uuid="later", message={**entries[-1]["message"],
                                                              "usage": {"input_tokens": 1000, "output_tokens": 900}}))
    _write(projects / "p" / f"{P}.jsonl", entries)
    hook = audit_costs.load_hook(audit_costs.DEFAULT_HOOK, str(projects.parent))
    tokens = audit_costs.recount(projects, hook)[P]["tokens"][MODEL]
    assert tokens["output_tokens"] == 900


def test_fork_copy_of_the_spawning_message_is_billed_once(projects, run):
    """A forked subagent's transcript opens with a later-timestamped copy of
    its spawner's Agent call (same message id)."""
    main = _session(P, n=1)
    spawn = main[-1]
    _write(projects / "p" / f"{P}.jsonl", main)
    _write(projects / "p" / P / "subagents" / "agent-fork.jsonl", [
        {"type": "fork-context-ref", "parentSessionId": P},
        dict(spawn, timestamp="2026-10-20T10:05:00Z", isSidechain=True),
        _user("f-u1", P, "fork directive", "2026-10-20T10:05:01Z"),
        _asst("f-a1", P, "fork-m1", "2026-10-20T10:05:02Z", 50),
    ])
    hook = audit_costs.load_hook(audit_costs.DEFAULT_HOOK, str(projects.parent))
    naive = audit_costs.recount(projects, hook)
    assert naive[P]["messages"] == 2  # the spawner's message once, the fork's own once


def test_continuation_copy_belongs_to_the_predecessor(projects):
    """The successor's copy keeps the predecessor's timestamps; the
    `continued-in` entry decides who owns it."""
    history = _session(P, n=1)
    _write(projects / "p" / f"{P}.jsonl", history + [
        {"type": "continued-in", "sessionId": P, "continuedInSessionId": S,
         "timestamp": "2026-10-20T10:00:02Z"}])
    copy = [dict(e, sessionId=S) for e in history]
    _write(projects / "p" / f"{S}.jsonl", copy + [
        _user("s-u1", S, "more", "2026-10-20T10:10:00Z"),
        _asst("s-a1", S, "s-m1", "2026-10-20T10:10:01Z", 100)])
    hook = audit_costs.load_hook(audit_costs.DEFAULT_HOOK, str(projects.parent))
    naive = audit_costs.recount(projects, hook)
    assert naive[P]["messages"] == 1
    assert naive[S]["messages"] == 1


def test_hook_double_count_is_a_finding(projects, run, monkeypatch):
    """If the hook ever bills a message twice, the recount disagrees."""
    _write(projects / "p" / f"{P}.jsonl", _session(P))
    real = audit_costs.run_hook

    def doubled(projects_dir, hook):
        scores = real(projects_dir, hook)
        for body in scores.values():
            body["value"] *= 2
        return scores

    monkeypatch.setattr(audit_costs, "run_hook", doubled)
    findings = run()["findings"]
    assert len(findings) == 1
    assert "recount: hook over recount" in findings[0]


# ---------------------------------------------------------------------------
# Check 2: Claude Code's cost-state
# ---------------------------------------------------------------------------

def test_transcript_above_the_clis_count_is_an_overcount(projects, run):
    # The transcript records 2,000 output tokens; the CLI counted 1,000 ($0.025 over).
    _write(projects / "p" / f"{P}.jsonl", _session(P, out=1000) + [_cost_state({MODEL: (2000, 1000)})])
    findings = run()["findings"]
    assert any("overcount" in f and MODEL in f for f in findings)


def test_overcount_within_rounding_is_not_a_finding(projects, run):
    # 10 output tokens over = $0.00025, below OVERCOUNT_TOLERANCE_USD.
    _write(projects / "p" / f"{P}.jsonl", _session(P) + [_cost_state({MODEL: (2000, 190)})])
    assert run()["findings"] == []


def test_output_the_transcript_never_recorded_is_an_undercount(projects, run):
    # No side calls, yet the CLI counted 5,000 more output tokens.
    _write(projects / "p" / f"{P}.jsonl", _session(P) + [_cost_state({MODEL: (2000, 5200)})])
    findings = run()["findings"]
    assert any("undercount" in f for f in findings)


def test_side_calls_explain_extra_cli_output(projects, run):
    # Two WebFetch calls: the CLI's extra output is the Haiku page summaries.
    entries = _session(P)
    entries[1]["message"]["content"] = [
        {"type": "tool_use", "id": f"t{i}", "name": "WebFetch", "input": {}} for i in range(2)]
    _write(projects / "p" / f"{P}.jsonl", entries + [_cost_state({MODEL: (2000, 5200)})])
    assert run()["findings"] == []


def test_partial_cost_state_is_not_compared(projects, run):
    # The CLI process started 10 minutes after the transcript: its totals
    # cover only part of the session.
    _write(projects / "p" / f"{P}.jsonl",
           _session(P) + [_cost_state({MODEL: (2000, 5200)}, start_ms=1792491000000)])
    assert run()["findings"] == []


# ---------------------------------------------------------------------------
# Check 3: Langfuse
# ---------------------------------------------------------------------------

def test_stored_cost_above_the_hooks_is_a_finding(projects, run):
    _write(projects / "p" / f"{P}.jsonl", _session(P))
    findings = run(langfuse={P: 0.030})["findings"]  # twice the real $0.015
    assert any("stored over hook" in f for f in findings)


def test_stored_cost_matching_the_hook_passes(projects, run):
    _write(projects / "p" / f"{P}.jsonl", _session(P))
    assert run(langfuse={P: 0.015})["findings"] == []


def test_session_missing_from_langfuse_is_a_finding(projects, run):
    _write(projects / "p" / f"{P}.jsonl", _session(P))
    findings = run(langfuse={})["findings"]
    assert any("session missing" in f for f in findings)


def test_exit_status(projects):
    _write(projects / "p" / f"{P}.jsonl", _session(P, out=1000) + [_cost_state({MODEL: (2000, 1000)})])
    assert audit_costs.main(["--projects", str(projects)]) == 1
    _write(projects / "p" / f"{P}.jsonl", _session(P) + [_cost_state({MODEL: (2000, 200)})])
    assert audit_costs.main(["--projects", str(projects)]) == 0
