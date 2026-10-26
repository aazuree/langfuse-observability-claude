# tests/test_tag_cleanup.py
"""Tags carry filterable signal only (census of 97 local sessions, 2026-10-26).

Dropped: `claude-code` (every trace), the `opus`/`sonnet`/`haiku` family tags
(`model:<id>` says the same), `permission:<mode>` (only ever `auto`; the
`permission-bypass` alert stays), `agent-name:<title>` and `subagents:<n>`
(one value per session; both stay in metadata), `has-background-tasks` (live
fires only, so a reprocess lost it). `worktree:<name>` became `worktree`;
the bare repo and entrypoint tags got `repo:` / `entrypoint:` prefixes;
`session-kind:fg` is no longer given to SDK runs. `has-queued-prompts` counts
only typed prompts that waited, not harness messages or instant delivery.

Added back the same day: `title:<session_title>`, so the tag filter lists
sessions by title (one value per session, kept on purpose).
"""
import json

from helpers import session_rollups
from test_subagent_tracking import _make_full_subagent_jsonl, langfuse_hook as hook


def _run(tmp_path, monkeypatch, extra_entries=(), background_tasks=None, sid="sess-t",
         entrypoint="cli"):
    entries = [
        {"type": "user", "timestamp": "2026-06-30T00:00:00Z", "cwd": "/x/myrepo",
         "entrypoint": entrypoint, "sessionId": sid,
         "message": {"role": "user", "content": "do a thing"}},
        {"type": "assistant", "timestamp": "2026-06-30T00:00:02Z",
         "message": {"role": "assistant", "id": "m1", "model": "claude-opus-4-8",
                     "stop_reason": "end_turn",
                     "content": [{"type": "text", "text": "done"}],
                     "usage": {"input_tokens": 5, "output_tokens": 3}}},
        *extra_entries,
    ]
    transcript = tmp_path / f"{sid}.jsonl"
    transcript.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
    sent = []
    monkeypatch.setattr(hook, "send_to_langfuse", lambda b: sent.append(b) or True)
    monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))
    hook.process_session(sid, str(transcript), "/x/myrepo", background_tasks=background_tasks)
    batch = sent[-1]
    trace = next(e["body"] for e in batch if e["type"] == "trace-create")
    return trace["tags"], session_rollups(batch)


def test_noise_tags_are_gone(tmp_path, monkeypatch):
    tags, meta = _run(tmp_path, monkeypatch, [
        {"type": "permission-mode", "permissionMode": "auto"},
        {"type": "agent-name", "agentName": "Some long session title"},
        {"type": "worktree-state", "worktreeSession": {"worktreeName": "fix-x",
                                                      "worktreeBranch": "fix/x"}},
    ], background_tasks=[{"id": "t1"}])
    assert "claude-code" not in tags
    assert "opus" not in tags
    assert not [t for t in tags if t.startswith(("permission:", "agent-name:", "worktree:"))]
    assert "has-background-tasks" not in tags
    # Kept as signal / metadata
    assert "model:claude-opus-4-8" in tags
    assert "worktree" in tags
    assert meta["agent_name"] == "Some long session title"
    assert meta["permission_mode"] == "auto"
    assert meta["worktree"]["name"] == "fix-x"
    assert meta["background_tasks"] == [{"id": "t1"}]


def test_repo_and_entrypoint_are_prefixed(tmp_path, monkeypatch):
    tags, _ = _run(tmp_path, monkeypatch)
    assert "repo:myrepo" in tags
    assert "entrypoint:cli" in tags
    assert "myrepo" not in tags
    assert "cli" not in tags


def _title_tags(tags):
    return [t for t in tags if t.startswith("title:")]


def test_title_tag_prefers_ai_title(tmp_path, monkeypatch):
    tags, meta = _run(tmp_path, monkeypatch, [
        {"type": "agent-name", "agentName": "agent-slug"},
        {"type": "ai-title", "aiTitle": "Fix Langfuse tags"},
    ])
    assert _title_tags(tags) == ["title:Fix Langfuse tags"]
    assert meta["session_title"] == "Fix Langfuse tags"


def test_title_tag_falls_back_to_agent_name_then_prompt(tmp_path, monkeypatch):
    tags, _ = _run(tmp_path, monkeypatch, [
        {"type": "agent-name", "agentName": "agent-slug"},
    ], sid="t-agent")
    assert _title_tags(tags) == ["title:agent-slug"]
    tags, _ = _run(tmp_path, monkeypatch, sid="t-prompt")
    assert _title_tags(tags) == ["title:do a thing"]


def test_title_tag_is_one_line_and_capped(tmp_path, monkeypatch):
    tags, _ = _run(tmp_path, monkeypatch, [
        {"type": "ai-title", "aiTitle": "Two\n  lines " + "x" * 80},
    ])
    (tag,) = _title_tags(tags)
    assert "\n" not in tag
    assert tag.startswith("title:Two lines xxx")
    assert len(tag) == len("title:") + 60 + len("...")


def test_permission_bypass_alert_stays(tmp_path, monkeypatch):
    tags, _ = _run(tmp_path, monkeypatch, [
        {"type": "permission-mode", "permissionMode": "bypassPermissions"},
    ])
    assert "permission-bypass" in tags


def test_session_kind_is_never_guessed(tmp_path, monkeypatch):
    # Claude Code writes only sessionKind "bg": an interactive CLI session has none.
    tags, meta = _run(tmp_path, monkeypatch, sid="cli-fg")
    assert "session-kind:fg" in tags
    assert meta["session_kind"] == "fg"
    # An SDK run is neither.
    tags, meta = _run(tmp_path, monkeypatch, sid="sdk", entrypoint="sdk-cli")
    assert not [t for t in tags if t.startswith("session-kind:")]
    assert "session_kind" not in meta
    tags, meta = _run(tmp_path, monkeypatch, [
        {"type": "user", "timestamp": "2026-06-30T00:00:03Z", "sessionKind": "bg",
         "message": {"role": "user", "content": "more"}},
    ], sid="bg-kind")
    assert "session-kind:bg" in tags
    assert meta["session_kind"] == "bg"


def test_subagent_count_tag_is_gone(tmp_path, monkeypatch):
    sa_dir = tmp_path / "sess-sa" / "subagents"
    sa_dir.mkdir(parents=True)
    _make_full_subagent_jsonl(sa_dir, "a1", tool_use_id="toolu_A",
                              start_ts="2026-06-30T00:00:01Z")
    entries = [
        {"type": "user", "timestamp": "2026-06-30T00:00:00Z",
         "message": {"role": "user", "content": "go"}},
        {"type": "assistant", "timestamp": "2026-06-30T00:00:01Z",
         "message": {"role": "assistant", "id": "m1", "model": "claude-opus-4-8",
                     "stop_reason": "end_turn",
                     "content": [{"type": "tool_use", "id": "toolu_A", "name": "Agent",
                                  "input": {"prompt": "x"}}],
                     "usage": {"input_tokens": 5, "output_tokens": 3}}},
        {"type": "user", "timestamp": "2026-06-30T00:00:09Z",
         "message": {"role": "user", "content": [
             {"type": "tool_result", "tool_use_id": "toolu_A", "content": "ok"}]}},
        {"type": "assistant", "timestamp": "2026-06-30T00:00:10Z",
         "message": {"role": "assistant", "id": "m2", "model": "claude-opus-4-8",
                     "stop_reason": "end_turn", "content": [{"type": "text", "text": "ok"}],
                     "usage": {"input_tokens": 5, "output_tokens": 3}}},
    ]
    transcript = tmp_path / "sess-sa.jsonl"
    transcript.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
    sent = []
    monkeypatch.setattr(hook, "send_to_langfuse", lambda b: sent.append(b) or True)
    monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))
    hook.process_session("sess-sa", str(transcript), "/x")
    tags = next(e["body"] for e in sent[0] if e["type"] == "trace-create")["tags"]
    assert "has-subagents" in tags
    assert not [t for t in tags if t.startswith("subagents:")]
    assert len(session_rollups(sent[0])["subagent_costs"]["agents"]) == 1


# --- queued prompts: only a typed prompt that had to wait ---

def _q(op, ts, content=None, **extra):
    e = {"type": "queue-operation", "operation": op, "timestamp": f"2026-06-30T10:00:{ts:02d}.000Z"}
    if content is not None:
        e["content"] = content
    return {**e, **extra}


def _queue(tmp_path, entries):
    path = tmp_path / "q.jsonl"
    path.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
    return hook.extract_queue_operations(str(path))


def test_harness_messages_are_not_queued_prompts(tmp_path):
    out = _queue(tmp_path, [
        _q("enqueue", 0, "<task-notification>\n<task-id>x</task-id>"),
        _q("remove", 9, "<task-notification>\n<task-id>x</task-id>", reason="absorbed_mid_turn"),
        _q("enqueue", 10, '<agent-message from="a1">hi</agent-message>'),
        _q("dequeue", 20),
    ])
    assert out["queued"] == 0
    assert out["harness_messages"] == 2
    assert out["total_wait_ms"] == 0


def test_instant_delivery_is_not_queueing(tmp_path):
    # SDK / bg sessions deliver their first prompt through the queue.
    out = _queue(tmp_path, [_q("enqueue", 0, "first prompt"), _q("dequeue", 0)])
    assert out["queued"] == 0


def test_absorbed_mid_turn_is_a_queued_prompt_not_a_cancel(tmp_path):
    out = _queue(tmp_path, [
        _q("enqueue", 0, "also check X"),
        _q("remove", 7, "also check X", reason="absorbed_mid_turn"),
    ])
    assert out["queued"] == 1
    assert out["absorbed"] == 1
    assert out["removed"] == 0
    assert out["total_wait_ms"] == 7000


def test_remove_matches_its_own_prompt(tmp_path):
    # A harness message queued first must not be paired with the typed prompt's removal.
    out = _queue(tmp_path, [
        _q("enqueue", 0, "<task-notification>t</task-notification>"),
        _q("enqueue", 1, "typed"),
        _q("remove", 5, "typed", reason="absorbed_mid_turn"),
        _q("dequeue", 6),
    ])
    assert out["queued"] == 1
    assert out["max_wait_ms"] == 4000
    assert out["harness_messages"] == 1


def test_only_waiting_typed_prompts_tag_the_session(tmp_path, monkeypatch):
    tags, meta = _run(tmp_path, monkeypatch, [
        _q("enqueue", 0, "<task-notification>t</task-notification>"),
        _q("dequeue", 9),
    ], sid="q-machine")
    assert "has-queued-prompts" not in tags
    assert meta["queue_operations"]["queued"] == 0
    tags, _ = _run(tmp_path, monkeypatch, [
        _q("enqueue", 0, "follow up"), _q("dequeue", 4),
    ], sid="q-typed")
    assert "has-queued-prompts" in tags
