# tests/test_subagent_resume.py
"""Subagents that keep working after the parent turn that launched them.

A background (async) Agent returns "Async agent launched" at once, so the
parent turn ends while the subagent is still writing its transcript. The hook
only looked at a subagent when the parent turn holding its Agent call was in
the batch, and parsed only the lines after its saved offset. Everything the
agent did afterwards was lost: 17 agents / 658 API calls / ~$19 in local
state. Resuming mid-turn from an offset also dropped the tail, because those
lines carry no user prompt for build_turns to start a turn from.
"""
import importlib.util
import json
import os

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook", os.path.join(os.path.dirname(__file__), "..", "langfuse-hook.py"))
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)

SID = "bg-sess"
AGENT = "a0b1c2d3e4f5a6b7"


def _assistant(mid, ts, *, text=None, tool=None, stop="tool_use", out=100):
    content = []
    if text:
        content.append({"type": "text", "text": text})
    if tool:
        content.append({"type": "tool_use", "id": tool, "name": "Read",
                        "input": {"file_path": "/x"}})
    return {"type": "assistant", "timestamp": ts,
            "message": {"id": mid, "role": "assistant", "model": "claude-sonnet-5",
                        "stop_reason": stop, "content": content,
                        "usage": {"input_tokens": 10, "output_tokens": out,
                                  "cache_read_input_tokens": 0,
                                  "cache_creation_input_tokens": 0}}}


def _result(tool, ts):
    return {"type": "user", "timestamp": ts,
            "message": {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": tool, "content": "ok"}]}}


def _parent_turn(n, minute, *, agent=False):
    ts = f"2026-09-20T10:{minute:02d}:00+00:00"
    end = f"2026-09-20T10:{minute:02d}:02+00:00"
    content = [{"type": "text", "text": f"reply {n}"}]
    if agent:
        content.append({"type": "tool_use", "id": "toolu_agent", "name": "Agent",
                        "input": {"description": "bg work", "subagent_type": "general-purpose",
                                  "prompt": "go", "run_in_background": True}})
    entries = [
        {"type": "user", "timestamp": ts, "cwd": "/w/p",
         "message": {"role": "user", "content": f"prompt {n}"}},
        {"type": "assistant", "timestamp": end,
         "message": {"id": f"pm{n}", "role": "assistant", "model": "claude-sonnet-5",
                     "stop_reason": "end_turn", "content": content,
                     "usage": {"input_tokens": 5, "output_tokens": 5,
                               "cache_read_input_tokens": 0,
                               "cache_creation_input_tokens": 0}}},
    ]
    if agent:
        entries.append({"type": "user", "timestamp": end, "message": {
            "role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_agent",
                                         "content": f"Async agent launched successfully.\nagentId: {AGENT}"}]}})
    return entries


class _Session:
    def __init__(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))
        self.batches = []
        monkeypatch.setattr(hook, "send_to_langfuse",
                            lambda b: self.batches.append(b) or True)
        proj = tmp_path / "proj"
        proj.mkdir()
        self.parent = proj / f"{SID}.jsonl"
        self.parent.write_text("")
        sub = proj / SID / "subagents"
        sub.mkdir(parents=True)
        self.agent = sub / f"agent-{AGENT}.jsonl"
        self.agent.write_text("")
        (sub / f"agent-{AGENT}.meta.json").write_text(json.dumps(
            {"agentType": "general-purpose", "description": "bg work",
             "toolUseId": "toolu_agent"}))

    def append(self, path, entries):
        with open(path, "a") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")

    def fire(self):
        hook.process_session(SID, str(self.parent), "/w/p")
        return self.batches[-1]

    def state(self):
        return hook.load_subagent_state(SID)[AGENT]


def _agent_gens(batch):
    return [e["body"] for e in batch
            if e["type"] == "generation-create" and e["body"]["id"].startswith("gen-subagent-")]


def test_background_agent_work_after_the_parent_turn_is_ingested(tmp_path, monkeypatch):
    s = _Session(tmp_path, monkeypatch)
    # Fire 1: parent launches the agent; the agent has only started.
    s.append(s.parent, _parent_turn(1, 0, agent=True))
    s.append(s.agent, [
        {"type": "user", "timestamp": "2026-09-20T10:00:01+00:00", "isSidechain": True,
         "message": {"role": "user", "content": "go"}},
        _assistant("sm1", "2026-09-20T10:00:03+00:00", tool="tr1"),
        _result("tr1", "2026-09-20T10:00:04+00:00"),
    ])
    s.fire()
    assert s.state()["status"] == "partial"

    # Fire 2: an unrelated parent turn; the agent has not moved.
    s.append(s.parent, _parent_turn(2, 1))
    s.fire()

    # Fire 3: the Agent's parent turn is no longer re-sent, but the agent
    # finished in the meantime — mid-turn, with no new user prompt.
    s.append(s.agent, [_assistant("sm2", "2026-09-20T10:02:30+00:00",
                                  text="all done", stop="end_turn", out=900)])
    s.append(s.parent, _parent_turn(3, 3))
    batch = s.fire()

    gens = _agent_gens(batch)
    assert gens, "background agent's later work was never sent"
    assert gens[-1]["output"] == "all done"
    assert gens[-1]["usageDetails"]["output"] == 100 + 900
    assert gens[-1]["parentObservationId"].startswith("span-")
    state = s.state()
    assert state["status"] == "complete"
    assert state["total_tokens"] == (10 + 100) + (10 + 900)


def test_totals_are_not_double_counted_when_a_turn_is_resent(tmp_path, monkeypatch):
    """The parent re-sends its last turn every fire, which re-reads its
    subagent. Totals must be the agent's whole cost, not a running sum."""
    s = _Session(tmp_path, monkeypatch)
    s.append(s.parent, _parent_turn(1, 0, agent=True))
    s.append(s.agent, [
        {"type": "user", "timestamp": "2026-09-20T10:00:01+00:00", "isSidechain": True,
         "message": {"role": "user", "content": "go"}},
        _assistant("sm1", "2026-09-20T10:00:03+00:00", tool="tr1"),
        _result("tr1", "2026-09-20T10:00:04+00:00"),
    ])
    s.fire()
    s.append(s.agent, [_assistant("sm2", "2026-09-20T10:00:30+00:00",
                                  text="done", stop="end_turn")])
    s.append(s.parent, [{"type": "system", "subtype": "turn_duration",
                         "timestamp": "2026-09-20T10:00:40+00:00", "durationMs": 40000}])
    s.fire()
    s.append(s.parent, _parent_turn(2, 1))
    s.fire()

    assert s.state()["total_tokens"] == (10 + 100) * 2
    trace = next(e["body"] for e in s.batches[-1] if e["type"] == "trace-create")
    agents = trace["metadata"]["subagent_costs"]["agents"]
    assert [a["total_tokens"] for a in agents] == [220]


def test_unchanged_agent_is_not_reread(tmp_path, monkeypatch):
    s = _Session(tmp_path, monkeypatch)
    s.append(s.parent, _parent_turn(1, 0, agent=True))
    s.append(s.agent, [
        {"type": "user", "timestamp": "2026-09-20T10:00:01+00:00", "isSidechain": True,
         "message": {"role": "user", "content": "go"}},
        _assistant("sm1", "2026-09-20T10:00:03+00:00", text="done", stop="end_turn"),
    ])
    s.fire()
    s.append(s.parent, _parent_turn(2, 1))
    s.fire()
    s.append(s.parent, _parent_turn(3, 2))

    reads = []
    real = hook.parse_transcript
    monkeypatch.setattr(hook, "parse_transcript",
                        lambda path, *a, **kw: reads.append(path) or real(path, *a, **kw))
    s.fire()
    assert str(s.agent) not in reads
