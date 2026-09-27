# tests/test_session_rollups.py
"""Trace-level values must describe the whole session, not the latest fire.

Each Stop fire upserts the same trace. The hook used to compute every trace
rollup (turn_count, token totals, cost_by_model, stop_reasons, scores, ...)
from only the turns that were new in that fire, so each fire overwrote the
session's numbers with one turn's worth. On the live stack, traces with 7-19
turns showed turn_count=1 and a cost_by_model of $0.13 against a real $2.09.

Metadata keys sent as null made it worse: Langfuse merges trace metadata key
by key, and a null value erases what an earlier fire wrote.
"""
import builtins
import importlib.util
import json
import os

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
        md = fires.trace(fires.fire(_turn(3, minute=2)))["metadata"]

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

    def test_trace_input_and_timestamp_stay_on_the_first_turn(self, tmp_path, monkeypatch):
        fires = _Fires(tmp_path, monkeypatch)
        fires.fire(_turn(1, minute=0))
        trace = fires.trace(fires.fire(_turn(2, minute=1)))
        assert trace["input"] == "question 1"
        assert trace["timestamp"].startswith("2026-09-20T10:00:00")

    def test_parent_cost_includes_fires_before_the_first_subagent(self, tmp_path, monkeypatch):
        """sa_state['_parent'] only started accumulating once a subagent was
        known, so earlier fires' parent cost was missing from harness totals."""
        fires = _Fires(tmp_path, monkeypatch)
        fires.fire(_turn(1, minute=0))
        batch = fires.fire(_turn(2, minute=1))
        # The second fire re-sends turn 1, so dedupe generations by ID.
        gens = {gid: g for b in fires.batches for gid, g in fires.gens(b).items()}
        whole = sum(g["costDetails"]["total"] for g in gens.values())
        assert fires.trace(batch)["metadata"]["session_cost_usd"] == round(whole, 6)


class TestNullMetadataIsNotSent:
    def test_absent_rollups_are_omitted_not_null(self, tmp_path, monkeypatch):
        """Langfuse merges metadata key by key and a null erases the earlier
        value, so `api_errors: None` on a clean fire wiped a previous error."""
        fires = _Fires(tmp_path, monkeypatch)
        trace = fires.trace(fires.fire(_turn(1, minute=0)))
        assert "api_errors" not in trace["metadata"]
        assert None not in trace["metadata"].values()
        assert None not in trace.values()


class TestLateTurnDuration:
    def test_duration_written_after_the_stop_hook_reaches_its_turn(self, tmp_path, monkeypatch):
        """Claude Code writes system/turn_duration *after* the Stop hook runs
        (101 of 103 observed turns). The next fire must re-send the previous
        turn so the duration lands on it."""
        fires = _Fires(tmp_path, monkeypatch)
        first = fires.fire(_turn(1, minute=0))
        (gen1_id,) = fires.gens(first)

        second = fires.fire([_turn_duration(0, 6, 5000)] + _turn(2, minute=1))
        gens = fires.gens(second)
        assert gens[gen1_id]["metadata"]["duration_ms"] == 5000
        other = [g for gid, g in gens.items() if gid != gen1_id]
        assert len(other) == 1
        assert "duration_ms" not in other[0]["metadata"]

    def test_only_the_last_sent_turn_is_resent(self, tmp_path, monkeypatch):
        fires = _Fires(tmp_path, monkeypatch)
        fires.fire(_turn(1, minute=0) + _turn(2, minute=1) + _turn(3, minute=2))
        batch = fires.fire(_turn(4, minute=3))
        names = sorted(g["name"] for g in fires.gens(batch).values())
        assert names == ["Turn 3: question 3", "Turn 4: question 4"]

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
