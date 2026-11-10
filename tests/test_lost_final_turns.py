# tests/test_lost_final_turns.py
"""A session's final turn must reach Langfuse.

Claude Code can run the async Stop hook before it has flushed the reply that
ended the turn. Read then, the turn looks unfinished and is held; the async
SessionEnd fire is cut off when the CLI exits, so nothing sends it later.
Three local sessions lost $10.90 this way (2026-10-26 .. 11-04): each fire's
read stopped one or two lines before the final `end_turn` entries.

Two fixes: a Stop fire waits for the final reply (wait_for_final_reply), and
`--catch-up` sends ended sessions whose saved offset is behind the transcript.
"""
import importlib.util
import json
import os
import threading
import time
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook", os.path.join(os.path.dirname(__file__), "..", "langfuse-hook.py"))
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)

_USAGE = {"input_tokens": 10, "output_tokens": 5,
          "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0}


def _assistant(mid, ts, stop_reason, content):
    return {"type": "assistant", "timestamp": ts, "uuid": f"u-{mid}",
            "message": {"id": mid, "role": "assistant", "model": "claude-sonnet-5",
                        "stop_reason": stop_reason, "content": content, "usage": _USAGE}}


# One turn: a tool call, its result, then the reply that ends the turn.
HEAD = [
    {"type": "user", "timestamp": "2026-11-01T10:00:00Z", "uuid": "u0",
     "message": {"role": "user", "content": "list files"}},
    _assistant("m1", "2026-11-01T10:00:01Z", "tool_use",
               [{"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "ls"}}]),
    {"type": "user", "timestamp": "2026-11-01T10:00:02Z", "uuid": "u1",
     "message": {"role": "user", "content": [
         {"type": "tool_result", "tool_use_id": "t1", "content": "a b"}]}},
]
FINAL = [_assistant("m2", "2026-11-01T10:00:03Z", "end_turn",
                    [{"type": "text", "text": "Two files: a and b."}])]


def _write(path, entries):
    path.write_text("".join(json.dumps(e) + "\n" for e in entries))


def _append(path, entries):
    with open(path, "a") as f:
        f.write("".join(json.dumps(e) + "\n" for e in entries))


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setattr(hook, "LANGFUSE_PUBLIC_KEY", "pk")
    monkeypatch.setattr(hook, "LANGFUSE_SECRET_KEY", "sk")
    sent = []
    monkeypatch.setattr(hook, "send_to_langfuse", lambda b: sent.append(b) or True)
    logged = []
    monkeypatch.setattr(hook, "log", logged.append)
    monkeypatch.setenv("HOME", str(tmp_path))
    return {"sent": sent, "logged": logged, "tmp": tmp_path}


def _stop(path, event="Stop"):
    hook.run_stop_hook({"session_id": "s1", "transcript_path": str(path),
                        "cwd": "/w", "hook_event_name": event})


# --- Stop waits for the flush --------------------------------------------

def test_stop_fire_waits_for_the_final_reply(env, monkeypatch):
    path = env["tmp"] / "s1.jsonl"
    _write(path, HEAD)
    writer = threading.Timer(0.3, _append, args=(path, FINAL))
    writer.start()
    _stop(path)
    writer.join()
    assert hook.load_state("s1") == (4, 1), "final turn held: read before the flush"


def test_without_the_wait_the_turn_is_held(env, monkeypatch):
    """The bug: a fire that reads before the flush holds the turn."""
    monkeypatch.setattr(hook, "STOP_FLUSH_WAIT_S", 0.0)
    path = env["tmp"] / "s1.jsonl"
    _write(path, HEAD)
    _stop(path)
    assert hook.load_state("s1") == (3, 0)
    assert any("not terminal" in m for m in env["logged"])


def test_already_flushed_reply_does_not_wait(env, monkeypatch):
    path = env["tmp"] / "s1.jsonl"
    _write(path, HEAD + FINAL)
    start = time.monotonic()
    _stop(path)
    assert time.monotonic() - start < 1.0
    assert hook.load_state("s1") == (4, 1)


def test_stop_failure_does_not_wait(env):
    """StopFailure's last reply is never terminal; waiting would only delay."""
    path = env["tmp"] / "s1.jsonl"
    _write(path, HEAD)
    start = time.monotonic()
    _stop(path, event="StopFailure")
    assert time.monotonic() - start < 1.0


def test_tail_skips_partial_line_and_sidechain(tmp_path):
    path = tmp_path / "t.jsonl"
    side = _assistant("m9", "2026-11-01T10:00:04Z", "tool_use", [])
    side["isSidechain"] = True
    _write(path, HEAD + FINAL + [side])
    with open(path, "a") as f:
        f.write('{"type": "assistant", "message": {"stop_re')  # being written
    assert hook._last_reply_stop_reason(str(path)) == "end_turn"


# --- --catch-up ------------------------------------------------------------

def _project(env, sid, entries, age_s, offset=None):
    proj = env["tmp"] / ".claude" / "projects" / "-w"
    proj.mkdir(parents=True, exist_ok=True)
    path = proj / f"{sid}.jsonl"
    _write(path, entries)
    old = time.time() - age_s
    os.utime(path, (old, old))
    if offset is not None:
        hook.save_state(sid, offset, 0)
    return path


def test_catch_up_finds_ended_sessions_with_unread_lines(env):
    hour = hook.REPROCESS_IDLE_S
    _project(env, "held", HEAD + FINAL, 2 * hour, offset=3)
    _project(env, "done", HEAD + FINAL, 2 * hour, offset=4)
    _project(env, "never", HEAD + FINAL, 3 * hour)
    _project(env, "active", HEAD, 60, offset=1)
    busy = _project(env, "busy", HEAD + FINAL, 2 * hour, offset=3)
    sub = busy.parent / "busy" / "subagents"
    sub.mkdir(parents=True)
    (sub / "agent-a1.jsonl").write_text("{}\n")  # a subagent still writing

    due = [p.stem for p in hook.catch_up_sessions()]
    assert due == ["never", "held"], "oldest first; active, busy and done skipped"


def test_catch_up_sends_the_held_turn_and_settles(env):
    path = _project(env, "held", HEAD + FINAL, 2 * hook.REPROCESS_IDLE_S, offset=3)
    assert hook.catch_up() == 0
    assert hook.load_state("held") == (4, 1)
    assert len(env["sent"]) == 1
    assert hook.catch_up_sessions() == [], "a second run has nothing to do"
    assert path.exists()


def test_catch_up_without_keys_fails(env, monkeypatch):
    monkeypatch.setattr(hook, "LANGFUSE_PUBLIC_KEY", "")
    assert hook.catch_up() == 1
