# tests/test_subagent_identity.py
"""Which agent each observation belongs to, and which agent an Agent span spawned.

`subagent_id` always names the agent that *owns* an observation (absent on the
main thread's). An Agent tool span names the agent it *spawned* with
`spawned_agent_*`, at every depth. Until 2026-10-26 the main thread's Agent span
carried the child as `subagent_id`, while a nested Agent span carried its
spawner there, so one key meant two things.
"""
import json

from helpers import session_rollups
from test_subagent_tracking import (
    _make_agent_spawning_jsonl,
    _make_full_subagent_jsonl,
    _make_meta_json,
    langfuse_hook,
)


def _write_meta(directory, agent_id, **meta):
    (directory / f"agent-{agent_id}.meta.json").write_text(json.dumps(meta))


def _main_transcript(path, tool_use_id):
    entries = [
        {"type": "user", "timestamp": "2026-06-10T10:00:00+00:00",
         "message": {"role": "user", "content": "dispatch"}},
        {"type": "assistant", "timestamp": "2026-06-10T10:00:01+00:00",
         "message": {"id": "mm1", "role": "assistant", "stop_reason": "end_turn",
                     "model": "claude-opus-4-8",
                     "content": [{"type": "tool_use", "id": tool_use_id, "name": "Agent",
                                  "input": {"description": "parent agent",
                                            "subagent_type": "general-purpose",
                                            "prompt": "go"}}],
                     "usage": {"input_tokens": 10, "output_tokens": 10,
                               "cache_read_input_tokens": 0,
                               "cache_creation_input_tokens": 0}}},
        {"type": "user", "timestamp": "2026-06-10T10:00:20+00:00",
         "message": {"role": "user", "content": [
             {"type": "tool_result", "tool_use_id": tool_use_id, "content": "done"}]}},
        {"type": "assistant", "timestamp": "2026-06-10T10:00:21+00:00",
         "message": {"id": "mm2", "role": "assistant", "stop_reason": "end_turn",
                     "model": "claude-opus-4-8",
                     "content": [{"type": "text", "text": "all done"}],
                     "usage": {"input_tokens": 5, "output_tokens": 5,
                               "cache_read_input_tokens": 0,
                               "cache_creation_input_tokens": 0}}},
    ]
    path.write_text("\n".join(json.dumps(e) for e in entries) + "\n")


def _nested_session(tmp_path, monkeypatch):
    """Main thread -> agent p1 (background) -> agent c1 (Explore)."""
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    monkeypatch.setattr(langfuse_hook, "STATE_DIR", str(state_dir))
    sent = []
    monkeypatch.setattr(langfuse_hook, "send_to_langfuse", lambda b: sent.append(b) or True)

    sa_dir = tmp_path / "sess-ids" / "subagents"
    sa_dir.mkdir(parents=True)
    _make_agent_spawning_jsonl(sa_dir, "p1", "toolu_C1", start_ts="2026-06-10T10:00:02+00:00")
    _write_meta(sa_dir, "p1", agentType="general-purpose", description="parent agent",
                toolUseId="toolu_P1", requestShape="background", spawnDepth=1)
    _make_full_subagent_jsonl(sa_dir, "c1", start_ts="2026-06-10T10:00:04+00:00")
    _write_meta(sa_dir, "c1", agentType="Explore", description="child probe",
                toolUseId="toolu_C1", requestShape="foreground", spawnDepth=2)
    transcript = tmp_path / "sess-ids.jsonl"
    _main_transcript(transcript, "toolu_P1")
    langfuse_hook.process_session("sess-ids", str(transcript), "/test")
    assert len(sent) == 1
    return sent[0]


def _bodies(batch, kind):
    return [e["body"] for e in batch if e["type"] == kind]


def test_agent_spans_name_the_agent_they_spawned_at_every_depth(tmp_path, monkeypatch):
    batch = _nested_session(tmp_path, monkeypatch)
    agent_spans = {b["metadata"]["tool_use_id"]: b for b in _bodies(batch, "span-create")
                   if b["name"] == "Agent"}

    main_span = agent_spans["toolu_P1"]["metadata"]
    assert main_span["spawned_agent_id"] == "p1"
    assert main_span["spawned_agent_type"] == "general-purpose"
    assert main_span["spawned_agent_description"] == "parent agent"
    assert main_span["spawned_agent_mode"] == "background"
    # The main thread owns this span: no subagent_id.
    assert "subagent_id" not in main_span

    nested_span = agent_spans["toolu_C1"]["metadata"]
    assert nested_span["spawned_agent_id"] == "c1"
    assert nested_span["spawned_agent_type"] == "Explore"
    assert nested_span["spawned_agent_description"] == "child probe"
    assert nested_span["spawned_agent_mode"] == "foreground"
    # Owned by p1, the agent that made the call.
    assert nested_span["subagent_id"] == "p1"


def test_subagent_generations_carry_identity_and_lineage(tmp_path, monkeypatch):
    batch = _nested_session(tmp_path, monkeypatch)
    gens = {b["metadata"]["subagent_id"]: b for b in _bodies(batch, "generation-create")
            if "subagent_id" in b["metadata"]}

    p1 = gens["p1"]
    assert p1["metadata"]["subagent_type"] == "general-purpose"
    assert p1["metadata"]["subagent_depth"] == 1
    assert p1["metadata"]["spawn_tool_use_id"] == "toolu_P1"
    assert p1["metadata"]["subagent_mode"] == "background"
    assert "parent_agent_id" not in p1["metadata"]
    assert "agent_id" not in p1["metadata"]  # was a duplicate of subagent_id
    assert p1["name"].startswith("Subagent (general-purpose) Turn 1: ")

    c1 = gens["c1"]
    assert c1["metadata"]["subagent_type"] == "Explore"
    assert c1["metadata"]["subagent_depth"] == 2
    assert c1["metadata"]["parent_agent_id"] == "p1"
    assert c1["metadata"]["spawn_tool_use_id"] == "toolu_C1"
    assert c1["name"].startswith("Subagent (Explore) Turn 1: ")


def test_cost_rollup_links_each_agent_to_its_spawning_call(tmp_path, monkeypatch):
    batch = _nested_session(tmp_path, monkeypatch)
    agents = {a["agent_id"]: a for a in session_rollups(batch)["subagent_costs"]["agents"]}
    assert agents["p1"]["tool_use_id"] == "toolu_P1"
    assert agents["p1"]["mode"] == "background"
    assert agents["c1"]["tool_use_id"] == "toolu_C1"
    assert agents["c1"]["mode"] == "foreground"


def test_meta_index_reads_mode_and_finished(tmp_path):
    sa_dir = tmp_path / "subagents"
    sa_dir.mkdir()
    _make_full_subagent_jsonl(sa_dir, "a1")
    _write_meta(sa_dir, "a1", agentType="web-fetch", description="d", toolUseId="toolu_1",
                requestShape="foreground", finished=True)
    _make_full_subagent_jsonl(sa_dir, "a2")
    _make_meta_json(sa_dir, "a2", tool_use_id="toolu_2")  # older CC: neither field
    idx = langfuse_hook.build_subagent_meta_index([str(sa_dir)])
    assert idx["toolu_1"]["mode"] == "foreground"
    assert idx["toolu_1"]["finished"] is True
    assert idx["toolu_1"]["tool_use_id"] == "toolu_1"
    assert idx["toolu_2"]["mode"] == ""
    assert idx["toolu_2"]["finished"] is False


def _unfinished_agent(directory, agent_id):
    """One finished turn, then a reply that stopped for a tool call."""
    entries = [
        {"type": "user", "timestamp": "2026-06-10T10:00:01+00:00", "agentId": agent_id,
         "message": {"role": "user", "content": "work"}},
        {"type": "assistant", "timestamp": "2026-06-10T10:00:02+00:00",
         "message": {"id": "m1", "role": "assistant", "stop_reason": "tool_use",
                     "model": "claude-opus-4-8",
                     "content": [{"type": "tool_use", "id": "t1", "name": "Read", "input": {}}],
                     "usage": {"input_tokens": 1, "output_tokens": 1,
                               "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0}}},
    ]
    path = directory / f"agent-{agent_id}.jsonl"
    path.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
    return str(path)


def test_meta_finished_marks_the_agent_complete(tmp_path):
    sa_dir = tmp_path / "subagents"
    sa_dir.mkdir()
    path = _unfinished_agent(sa_dir, "a1")
    common = dict(agent_id="a1", transcript_path=path, parent_span_id="s", trace_id="t",
                  session_id="sess", subagent_offset=0)
    *_, status = langfuse_hook.ingest_subagent(**common)
    assert status == "partial"
    *_, status = langfuse_hook.ingest_subagent(**common, meta={"finished": True})
    assert status == "complete"


def test_agents_recorded_before_the_link_get_it_without_growing(tmp_path, monkeypatch):
    """sa_state written by an older hook has no tool_use_id / mode; the next fire
    fills them in from the .meta.json even when the agent has nothing new."""
    _nested_session(tmp_path, monkeypatch)
    state = langfuse_hook.load_subagent_state("sess-ids")
    for entry in state.values():
        entry.pop("tool_use_id")
        entry.pop("mode")
    langfuse_hook.save_subagent_state("sess-ids", state)

    sent = []
    monkeypatch.setattr(langfuse_hook, "send_to_langfuse", lambda b: sent.append(b) or True)
    transcript = tmp_path / "sess-ids.jsonl"
    with open(transcript, "a") as f:
        f.write(json.dumps({"type": "user", "timestamp": "2026-06-10T10:01:00+00:00",
                            "message": {"role": "user", "content": "next"}}) + "\n")
    langfuse_hook.process_session("sess-ids", str(transcript), "/test")
    agents = {a["agent_id"]: a for a in session_rollups(sent[-1])["subagent_costs"]["agents"]}
    assert agents["p1"]["tool_use_id"] == "toolu_P1"
    assert agents["c1"]["tool_use_id"] == "toolu_C1"
    assert agents["c1"]["mode"] == "foreground"
