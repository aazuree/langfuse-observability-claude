"""A message whose entries span two turns is counted once.

Claude Code writes one transcript entry per content block of an assistant
message. When a message calls Skill (or reads an image) and then calls another
tool, the skill body (or the "[Image: ...]" note) lands as an `isMeta` user
entry *between* that message's entries. The entry opens a new turn, so the
message's id ends up in both turns, and each turn added its usage: 6 of 101
local transcripts (2026-10-18), e.g. a turn billed $0.486 that Claude Code's
own cost-state put at $0.337.
"""
import importlib.util
import os

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook", os.path.join(os.path.dirname(__file__), "..", "langfuse-hook.py"))
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)


def _user(content, ts, **extra):
    return {"type": "user", "timestamp": f"2026-10-18T15:47:{ts}Z",
            "message": {"role": "user", "content": content}, **extra}


def _asst(mid, ts, block, stop_reason=None, out=100):
    return {"type": "assistant", "timestamp": f"2026-10-18T15:47:{ts}Z",
            "message": {"id": mid, "role": "assistant", "model": "claude-opus-5-5",
                        "stop_reason": stop_reason, "content": [block],
                        "usage": {"input_tokens": 10, "output_tokens": out,
                                  "cache_read_input_tokens": 1000,
                                  "cache_creation_input_tokens": 50}}}


def _tool_use(tid, name):
    return {"type": "tool_use", "id": tid, "name": name, "input": {}}


def _tool_result(tid):
    return [{"type": "tool_result", "tool_use_id": tid, "content": "ok"}]


def _split_session():
    # m1 calls Skill, the skill body arrives, then m1 goes on to call Bash.
    return [
        _user("why is it missing?", "34"),
        _asst("m1", "35", {"type": "thinking", "thinking": ""}),
        _asst("m1", "36", _tool_use("t1", "Skill")),
        _user(_tool_result("t1"), "37"),
        _user([{"type": "text", "text": "Base directory for this skill: /s\n\nbody"}], "38",
              isMeta=True, sourceToolUseID="t1"),
        _asst("m1", "39", _tool_use("t2", "Bash"), stop_reason="tool_use"),
        _user(_tool_result("t2"), "40"),
        _asst("m2", "41", {"type": "text", "text": "found it"}, stop_reason="end_turn", out=20),
    ]


def test_message_spanning_two_turns_is_counted_once():
    turns = hook.build_turns(_split_session())
    assert sum(t["usage"]["output"] for t in turns) == 100 + 20
    assert sum(t["usage"]["input"] for t in turns) == 10 + 10
    assert sum(t["usage"]["cache_read"] for t in turns) == 1000 + 1000
    assert sum(t["usage"]["cache_creation"] for t in turns) == 50 + 50


def test_usage_stays_with_the_turn_the_message_started_in():
    turns = hook.build_turns(_split_session())
    assert turns[0]["usage"]["output"] == 100
    assert turns[-1]["usage"]["output"] == 20


def test_session_cost_counts_the_message_once():
    turns = hook.build_turns(_split_session())
    total = sum(hook.calculate_turn_cost(t["usage"], t["model"],
                                         t["cache_ephemeral_5m"], t["cache_ephemeral_1h"])[0]
                for t in turns)
    once = 2 * hook.calculate_turn_cost(
        {"input": 10, "output": 0, "total": 10, "cache_read": 1000, "cache_creation": 50},
        "claude-opus-5-5")[0]
    once += hook.calculate_turn_cost(
        {"input": 0, "output": 120, "total": 120, "cache_read": 0, "cache_creation": 0},
        "claude-opus-5-5")[0]
    assert abs(total - once) < 1e-12


def test_unsplit_messages_unchanged():
    turns = hook.build_turns([
        _user("a", "00"),
        _asst("m1", "01", {"type": "text", "text": "x"}, stop_reason="end_turn"),
        _user("b", "02"),
        _asst("m2", "03", {"type": "text", "text": "y"}, stop_reason="end_turn"),
    ])
    assert [t["usage"]["output"] for t in turns] == [100, 100]
