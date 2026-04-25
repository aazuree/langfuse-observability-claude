# tests/test_subagent_tracking.py
"""Tests for subagent cost tracking in langfuse-hook.py"""
import importlib.util
import json
import os
import sys
import tempfile

# Import the hook module (hyphenated filename requires importlib)
_spec = importlib.util.spec_from_file_location(
    "langfuse_hook",
    os.path.join(os.path.dirname(__file__), "..", "langfuse-hook.py"),
)
langfuse_hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(langfuse_hook)


def test_load_subagent_state_missing_file(tmp_path):
    """Returns empty dict when no state file exists."""
    orig = langfuse_hook.STATE_DIR
    langfuse_hook.STATE_DIR = str(tmp_path)
    try:
        result = langfuse_hook.load_subagent_state("nonexistent-session")
        assert result == {}
    finally:
        langfuse_hook.STATE_DIR = orig


def test_save_and_load_subagent_state(tmp_path):
    """Round-trips subagent state through save/load."""
    orig = langfuse_hook.STATE_DIR
    langfuse_hook.STATE_DIR = str(tmp_path)
    try:
        state = {
            "abc123": {"offset": 50, "turn_count": 5, "status": "complete"},
            "def456": {"offset": 10, "turn_count": 2, "status": "partial"},
        }
        langfuse_hook.save_subagent_state("test-session", state)
        loaded = langfuse_hook.load_subagent_state("test-session")
        assert loaded == state

        # Verify file is .subagents.json, not .offset
        assert (tmp_path / "test-session.subagents.json").exists()
        assert not (tmp_path / "test-session.offset").exists()
    finally:
        langfuse_hook.STATE_DIR = orig


def test_load_subagent_state_corrupt_json(tmp_path):
    """Returns empty dict on corrupt JSON."""
    orig = langfuse_hook.STATE_DIR
    langfuse_hook.STATE_DIR = str(tmp_path)
    try:
        (tmp_path / "bad-session.subagents.json").write_text("not json{{{")
        result = langfuse_hook.load_subagent_state("bad-session")
        assert result == {}
    finally:
        langfuse_hook.STATE_DIR = orig


# --- Helpers for discover/ingest tests ---

def _make_subagent_jsonl(directory, agent_id, timestamp, lines=3):
    """Helper: create a minimal subagent JSONL file."""
    jsonl_path = directory / f"agent-{agent_id}.jsonl"
    entries = []
    for i in range(lines):
        entries.append(json.dumps({
            "type": "user" if i % 2 == 0 else "assistant",
            "timestamp": timestamp,
            "isSidechain": True,
            "agentId": agent_id,
            "sessionId": "parent-session",
            "message": {
                "role": "user" if i % 2 == 0 else "assistant",
                "content": f"message {i}",
                "model": "claude-sonnet-4-6" if i % 2 == 1 else "",
            },
        }))
    jsonl_path.write_text("\n".join(entries) + "\n")
    return jsonl_path


def _make_meta_json(directory, agent_id, agent_type="general-purpose", description=None):
    """Helper: create a .meta.json file for a subagent."""
    meta = {"agentType": agent_type}
    if description is not None:
        meta["description"] = description
    meta_path = directory / f"agent-{agent_id}.meta.json"
    meta_path.write_text(json.dumps(meta))
    return meta_path


# --- discover_subagents tests ---

def test_discover_subagents_no_directory(tmp_path):
    """Returns empty list when no subagents directory exists."""
    transcript = tmp_path / "session123.jsonl"
    transcript.write_text("")
    tool_uses = [("desc", "general-purpose", "2026-03-29T10:00:00+00:00", 0)]
    result = langfuse_hook.discover_subagents(str(transcript), tool_uses)
    assert result == []


def test_discover_subagents_excludes_aside_question(tmp_path):
    """aside_question subagents are excluded."""
    transcript = tmp_path / "session123.jsonl"
    transcript.write_text("")
    subagents_dir = tmp_path / "session123" / "subagents"
    subagents_dir.mkdir(parents=True)
    _make_subagent_jsonl(subagents_dir, "aside_question-abc123", "2026-03-29T10:00:01+00:00")
    tool_uses = [("desc", "general-purpose", "2026-03-29T10:00:00+00:00", 0)]
    result = langfuse_hook.discover_subagents(str(transcript), tool_uses)
    assert result == []


def test_discover_subagents_single_match(tmp_path):
    """Matches a single subagent by timestamp proximity."""
    transcript = tmp_path / "session123.jsonl"
    transcript.write_text("")
    subagents_dir = tmp_path / "session123" / "subagents"
    subagents_dir.mkdir(parents=True)
    _make_subagent_jsonl(subagents_dir, "abc123", "2026-03-29T10:00:02+00:00")
    _make_meta_json(subagents_dir, "abc123", description="Explore codebase")
    tool_uses = [("Explore codebase", "Explore", "2026-03-29T10:00:00+00:00", 0)]
    result = langfuse_hook.discover_subagents(str(transcript), tool_uses)
    assert len(result) == 1
    assert result[0][0] == "abc123"
    assert result[0][2] == "Explore codebase"
    assert result[0][3] == "Explore"
    assert result[0][4] == 0  # original tool_use content_index


def test_discover_subagents_rejects_outside_window(tmp_path):
    """Rejects subagent if timestamp gap exceeds SUBAGENT_MATCH_WINDOW_S."""
    transcript = tmp_path / "session123.jsonl"
    transcript.write_text("")
    subagents_dir = tmp_path / "session123" / "subagents"
    subagents_dir.mkdir(parents=True)
    _make_subagent_jsonl(subagents_dir, "abc123", "2026-03-29T10:02:00+00:00")
    tool_uses = [("desc", "general-purpose", "2026-03-29T10:00:00+00:00", 0)]
    result = langfuse_hook.discover_subagents(str(transcript), tool_uses)
    assert result == []


def test_discover_subagents_multiple_ordered(tmp_path):
    """Multiple Agent tool_uses in same entry match by index order."""
    transcript = tmp_path / "session123.jsonl"
    transcript.write_text("")
    subagents_dir = tmp_path / "session123" / "subagents"
    subagents_dir.mkdir(parents=True)
    _make_subagent_jsonl(subagents_dir, "first", "2026-03-29T10:00:01+00:00")
    _make_subagent_jsonl(subagents_dir, "second", "2026-03-29T10:00:03+00:00")
    tool_uses = [
        ("Implement feature", "general-purpose", "2026-03-29T10:00:00+00:00", 0),
        ("Review spec", "general-purpose", "2026-03-29T10:00:00+00:00", 1),
    ]
    result = langfuse_hook.discover_subagents(str(transcript), tool_uses)
    assert len(result) == 2
    assert result[0][0] == "first"
    assert result[1][0] == "second"


def test_discover_subagents_no_meta_json(tmp_path):
    """Works without .meta.json — uses tool_use description/type."""
    transcript = tmp_path / "session123.jsonl"
    transcript.write_text("")
    subagents_dir = tmp_path / "session123" / "subagents"
    subagents_dir.mkdir(parents=True)
    _make_subagent_jsonl(subagents_dir, "abc123", "2026-03-29T10:00:01+00:00")
    tool_uses = [("Run tests", "general-purpose", "2026-03-29T10:00:00+00:00", 0)]
    result = langfuse_hook.discover_subagents(str(transcript), tool_uses)
    assert len(result) == 1
    assert result[0][0] == "abc123"
    assert result[0][2] == "Run tests"
    assert result[0][3] == "general-purpose"


# --- ingest_subagent tests ---

def _make_full_subagent_jsonl(directory, agent_id, start_ts="2026-03-29T10:00:01+00:00"):
    """Helper: create a realistic subagent JSONL with turns, tool calls, and usage."""
    entries = [
        {
            "type": "user",
            "timestamp": start_ts,
            "isSidechain": True,
            "agentId": agent_id,
            "sessionId": "parent-session",
            "message": {"role": "user", "content": "Implement the feature"},
        },
        {
            "type": "assistant",
            "timestamp": "2026-03-29T10:00:05+00:00",
            "message": {
                "id": f"msg-{agent_id}-1",
                "role": "assistant",
                "model": "claude-sonnet-4-6",
                "content": [
                    {"type": "text", "text": "I'll read the file first."},
                    {
                        "type": "tool_use",
                        "id": "tool-read-1",
                        "name": "Read",
                        "input": {"file_path": "/tmp/test.py"},
                    },
                ],
                "usage": {
                    "input_tokens": 100, "output_tokens": 50,
                    "cache_read_input_tokens": 5000, "cache_creation_input_tokens": 200,
                },
            },
        },
        {
            "type": "user",
            "timestamp": "2026-03-29T10:00:06+00:00",
            "message": {
                "role": "user",
                "content": [{"type": "tool_result", "tool_use_id": "tool-read-1",
                             "content": [{"type": "text", "text": "file contents here"}]}],
            },
        },
        {
            "type": "assistant",
            "timestamp": "2026-03-29T10:00:10+00:00",
            "stop_reason": "end_turn",
            "message": {
                "id": f"msg-{agent_id}-2",
                "role": "assistant",
                "model": "claude-sonnet-4-6",
                "content": [{"type": "text", "text": "Done implementing."}],
                "usage": {
                    "input_tokens": 200, "output_tokens": 80,
                    "cache_read_input_tokens": 6000, "cache_creation_input_tokens": 100,
                },
            },
        },
    ]
    jsonl_path = directory / f"agent-{agent_id}.jsonl"
    jsonl_path.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
    return jsonl_path, len(entries)


def test_ingest_subagent_returns_events_and_cost(tmp_path):
    """ingest_subagent parses JSONL and returns Langfuse events + cost summary."""
    subagents_dir = tmp_path / "subagents"
    subagents_dir.mkdir()
    jsonl_path, total_lines = _make_full_subagent_jsonl(subagents_dir, "test-agent")

    events, cost_summary, new_offset, new_tc, status = langfuse_hook.ingest_subagent(
        agent_id="test-agent",
        transcript_path=str(jsonl_path),
        parent_span_id="span-parent-0",
        trace_id="trace-parent-session",
        session_id="parent-session",
        subagent_offset=0,
    )

    assert len(events) > 0
    gen_events = [e for e in events if e["type"] == "generation-create"]
    span_events = [e for e in events if e["type"] == "span-create"]
    assert len(gen_events) >= 1
    assert len(span_events) >= 1

    # All events reference the parent trace
    for e in events:
        assert e["body"]["traceId"] == "trace-parent-session"

    # Generations are children of the parent span
    for e in gen_events:
        assert e["body"]["parentObservationId"] == "span-parent-0"

    # Tool spans are children of their generation
    for e in span_events:
        assert e["body"]["parentObservationId"].startswith("gen-subagent-test-agent-")

    # Cost summary
    assert cost_summary["agent_id"] == "test-agent"
    assert cost_summary["total_cost"] > 0
    assert cost_summary["total_tokens"] > 0
    assert cost_summary["cost_breakdown"]["cache_read"] > 0

    assert new_offset == total_lines
    assert status == "complete"


def test_ingest_subagent_partial(tmp_path):
    """Partial subagent (no end_turn) returns status 'partial'."""
    subagents_dir = tmp_path / "subagents"
    subagents_dir.mkdir()
    entries = [
        {
            "type": "user", "timestamp": "2026-03-29T10:00:01+00:00",
            "isSidechain": True, "agentId": "partial-agent",
            "message": {"role": "user", "content": "Do something"},
        },
        {
            "type": "assistant", "timestamp": "2026-03-29T10:00:05+00:00",
            "message": {
                "id": "msg-partial-1", "role": "assistant", "model": "claude-sonnet-4-6",
                "content": [{"type": "text", "text": "Working on it..."}],
                "usage": {"input_tokens": 50, "output_tokens": 20,
                          "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
            },
        },
    ]
    jsonl_path = subagents_dir / "agent-partial-agent.jsonl"
    jsonl_path.write_text("\n".join(json.dumps(e) for e in entries) + "\n")

    events, cost_summary, new_offset, new_tc, status = langfuse_hook.ingest_subagent(
        "partial-agent", str(jsonl_path), "span-parent-0", "trace-parent", "parent", 0,
    )
    assert status == "partial"
    assert cost_summary["status"] == "partial"


def test_ingest_subagent_deterministic_ids(tmp_path):
    """IDs are deterministic — same input produces same IDs."""
    subagents_dir = tmp_path / "subagents"
    subagents_dir.mkdir()
    jsonl_path, _ = _make_full_subagent_jsonl(subagents_dir, "det-agent")

    events1, _, _, _, _ = langfuse_hook.ingest_subagent(
        "det-agent", str(jsonl_path), "span-p-0", "trace-p", "p", 0)
    events2, _, _, _, _ = langfuse_hook.ingest_subagent(
        "det-agent", str(jsonl_path), "span-p-0", "trace-p", "p", 0)

    ids1 = [e["body"]["id"] for e in events1]
    ids2 = [e["body"]["id"] for e in events2]
    assert ids1 == ids2


def test_ingest_subagent_respects_offset(tmp_path):
    """Skips already-processed lines when subagent_offset > 0."""
    subagents_dir = tmp_path / "subagents"
    subagents_dir.mkdir()
    jsonl_path, total_lines = _make_full_subagent_jsonl(subagents_dir, "off-agent")

    events1, _, offset1, tc1, _ = langfuse_hook.ingest_subagent(
        "off-agent", str(jsonl_path), "span-p-0", "trace-p", "p", 0)
    assert len(events1) > 0
    assert offset1 == total_lines
    assert tc1 > 0  # turns were processed

    events2, _, offset2, tc2, _ = langfuse_hook.ingest_subagent(
        "off-agent", str(jsonl_path), "span-p-0", "trace-p", "p", total_lines, prior_turn_count=tc1)
    assert len(events2) == 0
    assert offset2 == total_lines


# --- Integration test ---

def test_process_session_with_subagent(tmp_path, monkeypatch):
    """Full integration: process_session discovers and ingests subagents."""
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    monkeypatch.setattr(langfuse_hook, "STATE_DIR", str(state_dir))

    sent_batches = []
    monkeypatch.setattr(langfuse_hook, "send_to_langfuse", lambda batch: sent_batches.append(batch))

    session_dir = tmp_path / "projects" / "test-project"
    session_dir.mkdir(parents=True)
    transcript_path = session_dir / "test-session.jsonl"

    parent_entries = [
        {
            "type": "user", "uuid": "u1", "timestamp": "2026-03-29T10:00:00+00:00",
            "sessionId": "test-session", "cwd": "/home/test/project",
            "version": "2.1.86", "gitBranch": "main", "entrypoint": "cli",
            "message": {"role": "user", "content": "Implement the feature"},
        },
        {
            "type": "assistant", "uuid": "a1", "timestamp": "2026-03-29T10:00:02+00:00",
            "message": {
                "id": "msg-parent-1", "role": "assistant", "model": "claude-opus-4-6",
                "content": [
                    {"type": "text", "text": "I'll dispatch a subagent."},
                    {
                        "type": "tool_use", "id": "tool-agent-1", "name": "Agent",
                        "input": {
                            "description": "Implement feature X",
                            "subagent_type": "general-purpose",
                            "prompt": "Do the thing",
                        },
                    },
                ],
                "usage": {
                    "input_tokens": 50, "output_tokens": 30,
                    "cache_read_input_tokens": 1000, "cache_creation_input_tokens": 50,
                },
            },
        },
        {
            "type": "user", "uuid": "u2", "timestamp": "2026-03-29T10:01:00+00:00",
            "message": {
                "role": "user",
                "content": [{"type": "tool_result", "tool_use_id": "tool-agent-1",
                             "content": [{"type": "text", "text": "Subagent completed."}]}],
            },
        },
    ]
    transcript_path.write_text("\n".join(json.dumps(e) for e in parent_entries) + "\n")

    subagent_dir = session_dir / "test-session" / "subagents"
    subagent_dir.mkdir(parents=True)
    _make_full_subagent_jsonl(subagent_dir, "impl-agent", "2026-03-29T10:00:03+00:00")

    langfuse_hook.process_session("test-session", str(transcript_path), "/home/test/project")

    assert len(sent_batches) == 1
    all_events = sent_batches[0]

    trace_events = [e for e in all_events if e["type"] == "trace-create"]
    gen_events = [e for e in all_events if e["type"] == "generation-create"]

    assert len(trace_events) == 1
    assert len(gen_events) >= 2  # parent turn + subagent turns

    trace_body = trace_events[0]["body"]
    assert "subagent_costs" in trace_body["metadata"]
    assert "has-subagents" in trace_body["tags"]

    costs = trace_body["metadata"]["subagent_costs"]
    assert costs["total_subagent_cost"] > 0
    assert costs["harness_total_cost"] > 0
    assert len(costs["agents"]) == 1
    assert costs["agents"][0]["agent_id"] == "impl-agent"

    sa_state = langfuse_hook.load_subagent_state("test-session")
    assert "impl-agent" in sa_state


def test_harness_cost_accumulates_across_hook_fires(tmp_path, monkeypatch):
    """harness_total_cost equals cumulative cost across multiple hook fires."""
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    monkeypatch.setattr(langfuse_hook, "STATE_DIR", str(state_dir))
    sent_batches = []
    monkeypatch.setattr(
        langfuse_hook, "send_to_langfuse", lambda batch: sent_batches.append(batch) or True
    )

    session_dir = tmp_path / "projects" / "test-project"
    session_dir.mkdir(parents=True)
    transcript_path = session_dir / "fire-session.jsonl"

    # Fire 1 entries: user → agent dispatch → tool result
    fire1_entries = [
        {
            "type": "user", "uuid": "u1", "timestamp": "2026-04-25T10:00:00+00:00",
            "sessionId": "fire-session", "cwd": "/home/test/project",
            "version": "2.1.86", "gitBranch": "main", "entrypoint": "cli",
            "message": {"role": "user", "content": "Do the thing"},
        },
        {
            "type": "assistant", "uuid": "a1", "timestamp": "2026-04-25T10:00:02+00:00",
            "message": {
                "id": "msg-fire-1", "role": "assistant", "model": "claude-sonnet-4-6",
                "content": [
                    {"type": "text", "text": "Dispatching subagent."},
                    {
                        "type": "tool_use", "id": "tool-agent-1", "name": "Agent",
                        "input": {
                            "description": "Run task", "subagent_type": "general-purpose",
                            "prompt": "do it",
                        },
                    },
                ],
                "usage": {
                    "input_tokens": 50, "output_tokens": 20,
                    "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0,
                },
            },
        },
        {
            "type": "user", "uuid": "u2", "timestamp": "2026-04-25T10:01:00+00:00",
            "message": {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "tool-agent-1",
                 "content": [{"type": "text", "text": "Done."}]}
            ]},
        },
    ]
    transcript_path.write_text("\n".join(json.dumps(e) for e in fire1_entries) + "\n")

    subagent_dir = session_dir / "fire-session" / "subagents"
    subagent_dir.mkdir(parents=True)
    _make_full_subagent_jsonl(subagent_dir, "run-agent", "2026-04-25T10:00:03+00:00")

    # First hook fire
    langfuse_hook.process_session("fire-session", str(transcript_path), "/home/test/project")
    assert len(sent_batches) == 1, "Expected exactly 1 batch after fire 1"

    fire1_trace = next(e for e in sent_batches[0] if e["type"] == "trace-create")
    fire1_costs = fire1_trace["body"]["metadata"]["subagent_costs"]
    fire1_harness = fire1_costs["harness_total_cost"]
    fire1_parent = fire1_costs["parent_cost"]
    fire1_subagent = fire1_costs["total_subagent_cost"]
    assert fire1_harness > 0
    assert fire1_parent > 0
    assert fire1_subagent > 0
    assert abs(fire1_harness - (fire1_parent + fire1_subagent)) < 1e-9

    # Append one more parent turn (no new subagent activity)
    with open(transcript_path, "a") as f:
        f.write(json.dumps({
            "type": "user", "uuid": "u3", "timestamp": "2026-04-25T10:02:00+00:00",
            "message": {"role": "user", "content": "One more question"},
        }) + "\n")
        f.write(json.dumps({
            "type": "assistant", "uuid": "a2", "timestamp": "2026-04-25T10:02:05+00:00",
            "message": {
                "id": "msg-fire-2", "role": "assistant", "model": "claude-sonnet-4-6",
                "content": [{"type": "text", "text": "Answer."}],
                "usage": {
                    "input_tokens": 60, "output_tokens": 30,
                    "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0,
                },
            },
        }) + "\n")

    # Second hook fire
    langfuse_hook.process_session("fire-session", str(transcript_path), "/home/test/project")
    assert len(sent_batches) == 2, "Expected exactly 2 batches after fire 2"

    fire2_trace = next(e for e in sent_batches[1] if e["type"] == "trace-create")
    fire2_costs = fire2_trace["body"]["metadata"]["subagent_costs"]
    fire2_harness = fire2_costs["harness_total_cost"]
    fire2_parent = fire2_costs["parent_cost"]
    fire2_subagent = fire2_costs["total_subagent_cost"]

    # Parent cost grows (new turn was added); subagent cost is unchanged
    assert fire2_parent > fire1_parent, "parent_cost must grow across fires"
    assert abs(fire2_subagent - fire1_subagent) < 1e-9, "subagent cost must not change in fire 2"
    # harness_total must be the running cumulative
    assert abs(fire2_harness - (fire2_parent + fire2_subagent)) < 1e-9
    assert fire2_harness > fire1_harness, "harness_total must grow across fires"
    # Agent must still appear in the agents list even with no new turns
    agents = fire2_costs["agents"]
    assert any(a["agent_id"] == "run-agent" for a in agents), \
        "run-agent must appear in agents list even in fire 2"


def test_parent_cost_stored_in_sa_state(tmp_path, monkeypatch):
    """After processing a session with subagents, sa_state stores _parent.total_cost."""
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    monkeypatch.setattr(langfuse_hook, "STATE_DIR", str(state_dir))
    monkeypatch.setattr(langfuse_hook, "send_to_langfuse", lambda batch: True)

    session_dir = tmp_path / "projects" / "test-project"
    session_dir.mkdir(parents=True)
    transcript_path = session_dir / "parent-cost-session.jsonl"

    entries = [
        {
            "type": "user", "uuid": "u1", "timestamp": "2026-04-25T10:00:00+00:00",
            "sessionId": "parent-cost-session", "cwd": "/home/test/project",
            "version": "2.1.86", "gitBranch": "main", "entrypoint": "cli",
            "message": {"role": "user", "content": "Do the thing"},
        },
        {
            "type": "assistant", "uuid": "a1", "timestamp": "2026-04-25T10:00:02+00:00",
            "message": {
                "id": "msg-pc-1", "role": "assistant", "model": "claude-sonnet-4-6",
                "content": [
                    {"type": "text", "text": "Dispatching."},
                    {
                        "type": "tool_use", "id": "tool-agent-1", "name": "Agent",
                        "input": {
                            "description": "Run task", "subagent_type": "general-purpose",
                            "prompt": "do it",
                        },
                    },
                ],
                "usage": {
                    "input_tokens": 50, "output_tokens": 20,
                    "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0,
                },
            },
        },
        {
            "type": "user", "uuid": "u2", "timestamp": "2026-04-25T10:01:00+00:00",
            "message": {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "tool-agent-1",
                 "content": [{"type": "text", "text": "Done."}]}
            ]},
        },
    ]
    transcript_path.write_text("\n".join(json.dumps(e) for e in entries) + "\n")

    subagent_dir = session_dir / "parent-cost-session" / "subagents"
    subagent_dir.mkdir(parents=True)
    _make_full_subagent_jsonl(subagent_dir, "pc-agent", "2026-04-25T10:00:03+00:00")

    langfuse_hook.process_session(
        "parent-cost-session", str(transcript_path), "/home/test/project"
    )

    sa_state = langfuse_hook.load_subagent_state("parent-cost-session")
    assert "_parent" in sa_state, "sa_state must contain _parent key after processing"
    assert sa_state["_parent"]["total_cost"] > 0, "_parent.total_cost must be positive"
