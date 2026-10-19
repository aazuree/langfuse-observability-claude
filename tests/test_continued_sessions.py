"""A session continued in a new one is billed once.

When Claude Code continues a session in a new one, it writes `continued-in`
into the old transcript and starts the new transcript with a copy of the old
history: the same entry `uuid`s and message ids, `sessionId` rewritten. The
hook sent that copy again under the new session (11 local pairs, ~$9.9 billed
twice on 2026-10-18). The continuation can happen mid-turn, before the old
session ever fired a Stop, so the old session cannot be assumed to have sent
its part.

The old session (P) keeps the copy: the new one (S) flushes P once, then skips
the copied turns (indices unchanged), bills only its own messages in the turn
that straddles the cut, and computes its rollups from its own entries.
"""
import json
import uuid

import importlib.util
from pathlib import Path

from helpers import session_rollups

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook", Path(__file__).resolve().parent.parent / "langfuse-hook.py"
)
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)

P = "11111111-1111-4111-8111-111111111111"
S = "22222222-2222-4222-8222-222222222222"


def _user(uid, sid, text, ts, **extra):
    return {"type": "user", "uuid": uid, "sessionId": sid, "timestamp": ts, "cwd": "/x/repo",
            "message": {"role": "user", "content": text}, **extra}


def _asst(uid, sid, mid, ts, content, stop_reason, out):
    return {"type": "assistant", "uuid": uid, "sessionId": sid, "timestamp": ts,
            "requestId": f"req-{mid}",
            "message": {"id": mid, "role": "assistant", "model": "claude-opus-4-8",
                        "stop_reason": stop_reason, "content": content,
                        "usage": {"input_tokens": 10, "output_tokens": out}}}


def _cost_state(total, start=1790000000000):
    return {"type": "cost-state", "totalCostUSD": total, "startTime": start,
            "modelUsage": {}}


def _p_history(sid):
    """P's history as written in transcript `sid` (P itself, or S's copy)."""
    return [
        _user("p-u1", sid, "first question", "2026-10-18T10:00:00Z"),
        _asst("p-a1", sid, "m1", "2026-10-18T10:00:01Z",
              [{"type": "text", "text": "answer one"}], "end_turn", 100),
        _user("p-u2", sid, "second question", "2026-10-18T10:01:00Z"),
        _asst("p-a2", sid, "m2", "2026-10-18T10:01:01Z",
              [{"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "ls"}}],
              "tool_use", 200),
        _user("p-u3", sid, [{"type": "tool_result", "tool_use_id": "t1", "content": "a b"}],
              "2026-10-18T10:01:02Z"),
    ]


def _write_pair(tmp_path, s_extra=(), p_tail=None):
    proj = tmp_path / "proj"
    proj.mkdir()
    p_entries = _p_history(P) + (p_tail if p_tail is not None else [
        _cost_state(0.4),
        {"type": "continued-in", "timestamp": "2026-10-18T10:01:03Z", "sessionId": P,
         "continuedInSessionId": S},
    ])
    s_entries = [{"type": "ai-title", "sessionId": S, "aiTitle": "continued work"}]
    s_entries += [dict(e, sessionKind="fg") for e in _p_history(S)]
    s_entries += [
        # m3 finishes the turn that was in flight when P was continued.
        _asst("s-a3", S, "m3", "2026-10-18T10:01:10Z",
              [{"type": "text", "text": "listed"}], "end_turn", 30),
        _user("s-u4", S, "third question", "2026-10-18T10:02:00Z"),
        _asst("s-a4", S, "m4", "2026-10-18T10:02:01Z",
              [{"type": "text", "text": "answer three"}], "end_turn", 40),
        *s_extra,
    ]
    p_path, s_path = proj / f"{P}.jsonl", proj / f"{S}.jsonl"
    p_path.write_text("\n".join(json.dumps(e) for e in p_entries) + "\n")
    s_path.write_text("\n".join(json.dumps(e) for e in s_entries) + "\n")
    return str(p_path), str(s_path)


def _capture(monkeypatch, tmp_path):
    sent = []
    monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent.append(batch) or True)
    monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))
    return sent


def _session_of(batch):
    return next(e["body"]["sessionId"] for e in batch if e["type"] == "trace-create"
                or e["type"] == "score-create")


def _trace_ids(batch):
    return {e["body"]["id"] for e in batch if e["type"] == "trace-create"}


def _turn_trace(sid, index):
    return uuid.uuid5(uuid.NAMESPACE_URL, f"{sid}:turn:{index}").hex


def _opus48(out, inp=10):
    return inp * 5 / 1e6 + out * 25 / 1e6


# ------------------------------------------------------------ discovery

class TestFindPredecessor:
    def test_found_by_tail_scan_of_the_project_dir(self, tmp_path):
        p_path, s_path = _write_pair(tmp_path)
        assert hook.find_predecessor(S, s_path) == p_path

    def test_found_when_bookkeeping_lines_follow(self, tmp_path):
        tail = [{"type": "continued-in", "timestamp": "2026-10-18T10:01:03Z", "sessionId": P,
                 "continuedInSessionId": S},
                {"type": "last-prompt", "lastPrompt": "x" * 2000}, _cost_state(0.4)]
        p_path, s_path = _write_pair(tmp_path, p_tail=tail)
        assert hook.find_predecessor(S, s_path) == p_path

    def test_none_without_continued_in(self, tmp_path):
        _p, s_path = _write_pair(tmp_path, p_tail=[])
        assert hook.find_predecessor(S, s_path) == ""

    def test_a_mention_of_the_id_is_not_a_continuation(self, tmp_path):
        tail = [{"type": "user", "message": {"role": "user", "content": f"see {S}"}}]
        _p, s_path = _write_pair(tmp_path, p_tail=tail)
        assert hook.find_predecessor(S, s_path) == ""


# ------------------------------------------------------------ the successor

class TestSuccessor:
    def test_predecessor_is_flushed_first_and_keeps_the_copy(self, tmp_path, monkeypatch):
        sent = _capture(monkeypatch, tmp_path)
        _p, s_path = _write_pair(tmp_path)
        hook.process_session(S, s_path, "/x/repo")

        assert [_session_of(b) for b in sent] == [P, S]
        # P's in-flight turn is sent too: P is over, so its flush is final.
        assert _trace_ids(sent[0]) == {_turn_trace(P, 0), _turn_trace(P, 1)}

    def test_copied_turns_skipped_without_renumbering(self, tmp_path, monkeypatch):
        sent = _capture(monkeypatch, tmp_path)
        _p, s_path = _write_pair(tmp_path)
        hook.process_session(S, s_path, "/x/repo")
        assert _trace_ids(sent[1]) == {_turn_trace(S, 1), _turn_trace(S, 2)}

    def test_straddling_turn_bills_only_its_own_messages(self, tmp_path, monkeypatch):
        sent = _capture(monkeypatch, tmp_path)
        _p, s_path = _write_pair(tmp_path)
        hook.process_session(S, s_path, "/x/repo")
        gens = [e["body"] for e in sent[1] if e["type"] == "generation-create"]
        straddling = next(g for g in gens if g["traceId"] == _turn_trace(S, 1))
        assert straddling["usageDetails"]["output"] == 30           # m3, not m2
        assert straddling["metadata"]["tools_used"] == []           # t1 belongs to m2
        assert not any(e["type"] == "span-create" and e["body"]["traceId"] == _turn_trace(S, 1)
                       for e in sent[1])

    def test_rollups_cover_only_the_successors_own_part(self, tmp_path, monkeypatch):
        sent = _capture(monkeypatch, tmp_path)
        _p, s_path = _write_pair(tmp_path, s_extra=[_cost_state(1.0)])
        hook.process_session(S, s_path, "/x/repo")
        md = session_rollups(sent[1])
        own = _opus48(30) + _opus48(40)
        assert md["session_cost_usd"] == round(own, 6)
        assert md["turn_count"] == 2
        assert md["tool_calls_total"] == 0
        assert md["continued_from"] == {"session_id": P, "copied_turns": 1,
                                        "copied_messages": 2}
        scores = {e["body"]["name"]: e["body"]["value"] for e in sent[1]
                  if e["type"] == "score-create"}
        assert scores["session_cost_usd"] == round(own, 6)
        # Same CLI process (equal startTime): S's cost-state includes P's 0.4.
        assert scores["cli_cost_usd"] == 0.6
        assert md["cli_cost_state"]["predecessor_cost_usd"] == 0.4
        assert md["cli_cost_state"]["gap_usd"] == round(0.6 - own, 6)
        assert "continuation" in next(e for e in sent[1] if e["type"] == "trace-create")["body"]["tags"]

    def test_predecessor_rollups_keep_the_copy(self, tmp_path, monkeypatch):
        sent = _capture(monkeypatch, tmp_path)
        _p, s_path = _write_pair(tmp_path)
        hook.process_session(S, s_path, "/x/repo")
        assert session_rollups(sent[0])["session_cost_usd"] == round(_opus48(100) + _opus48(200), 6)

    def test_resolved_once_and_flushed_once(self, tmp_path, monkeypatch):
        sent = _capture(monkeypatch, tmp_path)
        _p, s_path = _write_pair(tmp_path)
        hook.process_session(S, s_path, "/x/repo")
        calls = []
        real = hook.find_predecessor
        monkeypatch.setattr(hook, "find_predecessor", lambda *a: calls.append(a) or real(*a))
        with open(s_path, "a") as f:
            f.write(json.dumps(_user("s-u5", S, "fourth", "2026-10-18T10:03:00Z")) + "\n")
            f.write(json.dumps(_asst("s-a5", S, "m5", "2026-10-18T10:03:01Z",
                                     [{"type": "text", "text": "four"}], "end_turn", 50)) + "\n")
        hook.process_session(S, s_path, "/x/repo")
        assert calls == []
        assert [_session_of(b) for b in sent] == [P, S, S]
        assert _trace_ids(sent[2]) == {_turn_trace(S, 3)}


class TestProcessingOrder:
    def test_predecessor_first(self, tmp_path, monkeypatch):
        sent = _capture(monkeypatch, tmp_path)
        p_path, s_path = _write_pair(tmp_path)
        hook.process_session(P, p_path, "/x/repo", final=True)
        hook.process_session(S, s_path, "/x/repo", final=True)
        assert [_session_of(b) for b in sent] == [P, S]
        assert _trace_ids(sent[1]) == {_turn_trace(S, 1), _turn_trace(S, 2)}

    def test_successor_first_then_predecessor_sends_nothing_new(self, tmp_path, monkeypatch):
        sent = _capture(monkeypatch, tmp_path)
        p_path, s_path = _write_pair(tmp_path)
        hook.process_session(S, s_path, "/x/repo", final=True)
        hook.process_session(P, p_path, "/x/repo", final=True)
        assert [_session_of(b) for b in sent] == [P, S, P]
        assert _trace_ids(sent[2]) == set()


class TestNotAContinuation:
    def test_plain_session_unchanged(self, tmp_path, monkeypatch):
        sent = _capture(monkeypatch, tmp_path)
        _p, s_path = _write_pair(tmp_path, p_tail=[])
        hook.process_session(S, s_path, "/x/repo")
        assert [_session_of(b) for b in sent] == [S]
        assert _trace_ids(sent[0]) == {_turn_trace(S, i) for i in range(3)}
        md = session_rollups(sent[0])
        assert "continued_from" not in md
        assert md["turn_count"] == 3


class TestContinuationCostState:
    def test_side_model_cost_subtracts_the_predecessors(self, tmp_path, monkeypatch):
        sent = _capture(monkeypatch, tmp_path)
        p_cs = dict(_cost_state(0.4), modelUsage={"claude-haiku-5-5": {"costUSD": 0.3}})
        s_cs = dict(_cost_state(1.0), modelUsage={"claude-haiku-5-5": {"costUSD": 0.5},
                                                  "claude-opus-4-8": {"costUSD": 0.5}})
        _p, s_path = _write_pair(tmp_path, s_extra=[s_cs], p_tail=[
            p_cs, {"type": "continued-in", "timestamp": "2026-10-18T10:01:03Z",
                   "sessionId": P, "continuedInSessionId": S}])
        hook.process_session(S, s_path, "/x/repo")
        cs = session_rollups(sent[1])["cli_cost_state"]
        # Haiku answered no message: S's own share is 0.5 - P's 0.3.
        assert cs["side_model_cost_usd"] == 0.2

    def test_no_flush_once_the_predecessor_transcript_is_gone(self, tmp_path, monkeypatch):
        sent = _capture(monkeypatch, tmp_path)
        p_path, s_path = _write_pair(tmp_path)
        hook.process_session(S, s_path, "/x/repo")
        # P's state falls behind (as after a failed send), then its transcript expires.
        Path(tmp_path / "state" / f"{P}.offset").unlink()
        Path(p_path).unlink()
        with open(s_path, "a") as f:
            f.write(json.dumps(_user("s-u5", S, "fourth", "2026-10-18T10:03:00Z")) + "\n")
            f.write(json.dumps(_asst("s-a5", S, "m5", "2026-10-18T10:03:01Z",
                                     [{"type": "text", "text": "four"}], "end_turn", 5)) + "\n")
        logged = []
        monkeypatch.setattr(hook, "log", logged.append)
        hook.process_session(S, s_path, "/x/repo")
        assert not any(P in m for m in logged)
        assert _session_of(sent[-1]) == S
