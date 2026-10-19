"""A forked subagent's copy of its spawning message is billed once, by the spawner.

A fork's transcript starts with a `fork-context-ref` entry and then a copy of
the spawner's assistant message that made the Agent call (same `message.id`,
same usage, new timestamp), before any user text. Its first user-text entry is
the tool_result for that call plus the fork's directive. The copy is the
spawner's API call, already counted in the spawner's turns; counting it in the
fork again would bill it once more per fork (census 2026-10-18: 6 of 6 fork
transcripts, three forks of one message each carrying all 7,740 of its output
tokens). build_turns ignores assistant entries before the first user text, so
the copy is not counted. These tests keep it that way.
"""
import importlib.util
import json
import os

from helpers import session_score

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook", os.path.join(os.path.dirname(__file__), "..", "langfuse-hook.py"))
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)

MODEL = "claude-opus-5-5"


def _asst(mid, ts, block, stop_reason, out, cache_read=0, **extra):
    return {"type": "assistant", "timestamp": f"2026-10-18T10:00:{ts}Z",
            "message": {"id": mid, "role": "assistant", "model": MODEL,
                        "stop_reason": stop_reason, "content": [block],
                        "usage": {"input_tokens": 10, "output_tokens": out,
                                  "cache_read_input_tokens": cache_read,
                                  "cache_creation_input_tokens": 0}},
            **extra}


def _user(content, ts, **extra):
    return {"type": "user", "timestamp": f"2026-10-18T10:00:{ts}Z",
            "message": {"role": "user", "content": content}, **extra}


def _tool_result(tid, text="ok"):
    return {"type": "tool_result", "tool_use_id": tid, "content": text}


AGENT_CALL = {"type": "tool_use", "id": "toolu_fork", "name": "Agent",
              "input": {"subagent_type": "fork", "prompt": "do the thing"}}


def _parent():
    return [
        _user("split the work", "00"),
        _asst("msg_P", "01", AGENT_CALL, "tool_use", out=7740, cache_read=1000),
        _user([_tool_result("toolu_fork", "Fork started")], "02"),
        _asst("msg_Q", "03", {"type": "text", "text": "started"}, "end_turn", out=50),
    ]


def _fork(with_copy=True):
    copy = [_asst("msg_P", "05", AGENT_CALL, "tool_use", out=7740, cache_read=1000,
                  isSidechain=True)]
    return [
        {"type": "fork-context-ref", "agentId": "afork", "parentSessionId": "sess-fork",
         "parentLastUuid": "u-parent"},
        *(copy if with_copy else []),
        _user([_tool_result("toolu_fork", "Fork started"),
               {"type": "text", "text": "<fork-boilerplate>directive</fork-boilerplate>"}],
              "06", isSidechain=True),
        _asst("msg_F1", "08", {"type": "tool_use", "id": "toolu_b", "name": "Bash",
                               "input": {"command": "ls"}}, "tool_use", out=300,
              isSidechain=True),
        _user([_tool_result("toolu_b")], "09", isSidechain=True),
        _asst("msg_F2", "10", {"type": "text", "text": "report"}, "end_turn", out=200,
              isSidechain=True),
    ]


def _cost(out, cache_read=0):
    usage = {"input": 10, "output": out, "total": 10 + out,
             "cache_read": cache_read, "cache_creation": 0}
    return hook.calculate_turn_cost(usage, MODEL)[0]


def test_fork_turn_excludes_the_copied_spawning_message():
    turns = hook.build_turns(_fork())
    assert len(turns) == 1
    assert "msg_P" not in turns[0]["api_call_ids"]
    assert turns[0]["usage"]["output"] == 300 + 200
    # Timing starts at the directive, not at the copy.
    assert turns[0]["start_time"] == "2026-10-18T10:00:06Z"
    assert turns[0]["first_token_time"] == "2026-10-18T10:00:08Z"


def test_the_copy_changes_nothing():
    with_copy, without = hook.build_turns(_fork()), hook.build_turns(_fork(with_copy=False))
    for key in ("usage", "start_time", "end_time", "first_token_time", "api_call_ids",
                "user_input", "assistant_output", "stop_reason"):
        assert with_copy[0][key] == without[0][key], key


def test_session_bills_the_spawning_message_once(tmp_path, monkeypatch):
    transcript = tmp_path / "sess-fork.jsonl"
    transcript.write_text("\n".join(json.dumps(e) for e in _parent()) + "\n")
    sub_dir = tmp_path / "sess-fork" / "subagents"
    sub_dir.mkdir(parents=True)
    (sub_dir / "agent-afork.jsonl").write_text(
        "\n".join(json.dumps(e) for e in _fork()) + "\n")
    (sub_dir / "agent-afork.meta.json").write_text(json.dumps(
        {"agentType": "fork", "description": "fork task", "toolUseId": "toolu_fork"}))

    captured = {}
    monkeypatch.setattr(hook, "send_to_langfuse",
                        lambda batch: captured.setdefault("batch", batch) or True)
    monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setattr(hook, "PROJECTS_DIR", str(tmp_path / "projects"))

    hook.process_session("sess-fork", str(transcript), "/x/repo", final=True)
    batch = captured["batch"]

    expected = _cost(7740, cache_read=1000) + _cost(50) + _cost(300) + _cost(200)
    assert abs(session_score(batch)["value"] - round(expected, 6)) < 1e-6

    fork_gens = [e["body"] for e in batch if e["type"] == "generation-create"
                 and e["body"]["id"].startswith("gen-subagent-afork-")]
    # One fork turn, at index 0: the copy does not take a turn index.
    assert [g["id"] for g in fork_gens] == ["gen-subagent-afork-0"]
    assert fork_gens[0]["usageDetails"]["output"] == 500
