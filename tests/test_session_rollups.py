# tests/test_session_rollups.py
"""Session rollups describe the whole session; each turn is sent once.

Every turn is its own trace, exported once when it is finished — Langfuse v4
keeps every export of a span, so re-sending a turn would double it. The
session-wide rollups (turn_count, token totals, cost_by_model, stop_reasons,
...) are recomputed over the whole transcript on every fire and sent on the
session-level `session_cost_usd` score, which upserts by id. Computing them
from one fire's turns is the bug this guards against: traces with 7-19 turns
once showed turn_count=1 and $0.13 of a real $2.09.
"""
import builtins
import importlib.util
import json
import os

from helpers import session_rollups, turn_traces

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook", os.path.join(os.path.dirname(__file__), "..", "langfuse-hook.py"))
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)


def _turn(n, *, minute, tool_error=False, inp=100, out=50):
    """One user->assistant turn at 10:<minute>, optionally with a failed Bash call."""
    ts = f"2026-09-20T10:{minute:02d}:00+00:00"
    end = f"2026-09-20T10:{minute:02d}:05+00:00"
    content = [{"type": "text", "text": f"answer {n}"}]
    entries = [
        {"type": "user", "uuid": f"u{n}", "timestamp": ts, "cwd": "/w/proj",
         "message": {"role": "user", "content": f"question {n}"}},
    ]
    if tool_error:
        content.append({"type": "tool_use", "id": f"t{n}", "name": "Bash",
                        "input": {"command": "false"}})
    entries.append(
        {"type": "assistant", "uuid": f"a{n}", "timestamp": end,
         "message": {"id": f"m{n}", "role": "assistant", "model": "claude-sonnet-5",
                     "stop_reason": "tool_use" if tool_error else "end_turn",
                     "content": content,
                     "usage": {"input_tokens": inp, "output_tokens": out,
                               "cache_read_input_tokens": 1000,
                               "cache_creation_input_tokens": 100}}})
    if tool_error:
        entries.append(
            {"type": "user", "uuid": f"r{n}", "timestamp": end,
             "message": {"role": "user", "content": [
                 {"type": "tool_result", "tool_use_id": f"t{n}",
                  "is_error": True, "content": "exit 1"}]}})
    return entries


def _turn_duration(minute, second, ms):
    return {"type": "system", "subtype": "turn_duration",
            "timestamp": f"2026-09-20T10:{minute:02d}:{second:02d}+00:00",
            "durationMs": ms}


class _Fires:
    """Append entries to a transcript and run one Stop fire per append."""

    def __init__(self, tmp_path, monkeypatch, sid="sess"):
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))
        self.batches = []
        monkeypatch.setattr(hook, "send_to_langfuse",
                            lambda batch: self.batches.append(batch) or True)
        self.path = tmp_path / f"{sid}.jsonl"
        self.path.write_text("")
        self.sid = sid

    def fire(self, entries):
        with open(self.path, "a") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")
        hook.process_session(self.sid, str(self.path), "/w/proj")
        return self.batches[-1]

    @staticmethod
    def trace(batch):
        return next(e["body"] for e in batch if e["type"] == "trace-create")

    @staticmethod
    def gens(batch):
        return {e["body"]["id"]: e["body"] for e in batch
                if e["type"] == "generation-create"}

    @staticmethod
    def scores(batch):
        return {e["body"]["name"]: e["body"]["value"] for e in batch
                if e["type"] == "score-create"}


class TestTraceRollupsCoverTheSession:
    def test_counts_and_costs_include_earlier_fires(self, tmp_path, monkeypatch):
        fires = _Fires(tmp_path, monkeypatch)
        fires.fire(_turn(1, minute=0))
        fires.fire(_turn(2, minute=1))
        md = session_rollups(fires.fire(_turn(3, minute=2)))

        assert md["turn_count"] == 3
        assert md["total_input_tokens"] == 300
        assert md["total_output_tokens"] == 150
        assert md["cost_by_model"]["claude-sonnet-5"]["turns"] == 3
        assert md["stop_reasons"]["by_reason"] == {"end_turn": 3}

    def test_scores_cover_the_session(self, tmp_path, monkeypatch):
        """A clean last fire used to erase the session's tool errors: the
        score's ID is per session, but its value came from one fire."""
        fires = _Fires(tmp_path, monkeypatch)
        fires.fire(_turn(1, minute=0, tool_error=True))
        batch = fires.fire(_turn(2, minute=1))
        assert fires.scores(batch)["tool_error_rate"] == 1.0

    def test_session_cost_covers_every_turn_sent_so_far(self, tmp_path, monkeypatch):
        fires = _Fires(tmp_path, monkeypatch)
        fires.fire(_turn(1, minute=0))
        batch = fires.fire(_turn(2, minute=1))
        gens = [g for b in fires.batches for g in fires.gens(b).values()]
        whole = sum(g["costDetails"]["total"] for g in gens)
        assert session_rollups(batch)["session_cost_usd"] == round(whole, 6)
        assert fires.scores(batch)["session_cost_usd"] == round(whole, 6)


class TestNullMetadataIsNotSent:
    def test_absent_rollups_are_omitted_not_null(self, tmp_path, monkeypatch):
        fires = _Fires(tmp_path, monkeypatch)
        md = session_rollups(fires.fire(_turn(1, minute=0)))
        assert "api_errors" not in md
        assert None not in md.values()


class TestEachTurnIsSentOnce:
    def test_every_turn_is_its_own_trace_sent_exactly_once(self, tmp_path, monkeypatch):
        fires = _Fires(tmp_path, monkeypatch)
        fires.fire(_turn(1, minute=0) + _turn(2, minute=1))
        fires.fire(_turn(3, minute=2))
        traces = [t for b in fires.batches for t in turn_traces(b)]
        gens = [g for b in fires.batches for g in fires.gens(b)]
        assert [t["name"] for t in traces] == ["question 1", "question 2", "question 3"]
        assert len(set(t["id"] for t in traces)) == 3
        assert len(gens) == len(set(gens)) == 3
        assert {t["sessionId"] for t in traces} == {"sess"}

    def test_an_unfinished_last_turn_waits_for_a_later_fire(self, tmp_path, monkeypatch):
        """An async fire can read the transcript after a queued prompt opened
        the next turn. That turn is mid-flight and must not be sent yet."""
        fires = _Fires(tmp_path, monkeypatch)
        queued = [{"type": "user", "uuid": "u2", "timestamp": "2026-09-20T10:01:00+00:00",
                   "message": {"role": "user", "content": "question 2"}}]
        first = fires.fire(_turn(1, minute=0) + queued)
        assert [t["name"] for t in turn_traces(first)] == ["question 1"]

        reply = _turn(2, minute=1)[1:]  # the assistant reply to the queued prompt
        second = fires.fire(reply)
        assert [t["name"] for t in turn_traces(second)] == ["question 2"]

    def test_a_turn_still_calling_tools_is_held(self, tmp_path, monkeypatch):
        fires = _Fires(tmp_path, monkeypatch)
        mid = _turn(1, minute=0, tool_error=True)  # ends on stop_reason tool_use
        assert turn_traces(fires.fire(mid)) == []

    def test_session_end_sends_an_unfinished_last_turn(self, tmp_path, monkeypatch):
        fires = _Fires(tmp_path, monkeypatch)
        fires.fire(_turn(1, minute=0, tool_error=True))
        hook.process_session(fires.sid, str(fires.path), "/w/proj", final=True)
        assert [t["name"] for t in turn_traces(fires.batches[-1])] == ["question 1"]

    def test_turn_duration_is_kept_when_already_written(self, tmp_path, monkeypatch):
        """Claude Code writes system/turn_duration after the Stop hook, so a
        live fire usually misses it; a re-ingest picks it up."""
        fires = _Fires(tmp_path, monkeypatch)
        batch = fires.fire(_turn(1, minute=0) + [_turn_duration(0, 6, 5000)])
        (gen,) = fires.gens(batch).values()
        assert gen["metadata"]["duration_ms"] == 5000


class TestTurnDurationParsing:
    def test_duration_attaches_by_file_order_not_a_30s_window(self):
        """A slow Stop hook (p99 19s, max 23s observed) pushes turn_duration
        past the old 30s proximity window."""
        entries = _turn(1, minute=0) + [_turn_duration(0, 50, 5000)]
        turns = hook.build_turns(entries)
        assert turns[0]["duration_ms"] == 5000


class TestSingleRead:
    def test_transcript_is_opened_once_per_fire(self, tmp_path, monkeypatch):
        """23 extractors each re-read the full transcript (0.25-0.35s per
        fire on a 4 MB transcript, against 0.02s for one pass)."""
        fires = _Fires(tmp_path, monkeypatch)
        target = str(fires.path)
        opens = []
        real_open = builtins.open

        def counting_open(file, *a, **kw):
            if str(file) == target and "a" not in (a[0] if a else kw.get("mode", "r")):
                opens.append(file)
            return real_open(file, *a, **kw)

        monkeypatch.setattr(builtins, "open", counting_open)
        fires.fire(_turn(1, minute=0))
        assert len(opens) == 1
