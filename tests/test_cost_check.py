"""The hook checks its own cost against Claude Code's count on every session.

Each check must fire on the failure it exists for and stay quiet otherwise:
an overcount (the hook bills more tokens than the CLI counted), an undercount
the CLI's unrecorded side calls cannot explain, and a message billed twice
across the parent and its subagents.
"""
import importlib.util
import json
from pathlib import Path

import pytest

from helpers import session_rollups, session_score

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook", Path(__file__).resolve().parent.parent / "langfuse-hook.py"
)
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)

MODEL = "claude-opus-4-8"  # $5 / $25 per 1M
SID = "33333333-3333-4333-8333-333333333333"
NO_CALLS = dict.fromkeys(hook.SIDE_CALL_OUTPUT_ALLOWANCE, 0)


def _tokens(inp=2000, out=200, cr=0, cc=0):
    return {"input_tokens": inp, "output_tokens": out, "cache_read_tokens": cr, "cache_create_tokens": cc}


# ---------------------------------------------------------------------------
# check_cost
# ---------------------------------------------------------------------------

def test_matching_counts_are_ok():
    check = hook.check_cost({MODEL: _tokens()}, {MODEL: _tokens()}, NO_CALLS, 0)
    assert check["status"] == "ok"
    assert check["overcount"] is None


def test_billing_more_than_the_cli_counted_is_an_overcount():
    check = hook.check_cost({MODEL: _tokens(out=2200)}, {MODEL: _tokens(out=200)}, NO_CALLS, 0)
    assert check["status"] == "overcount"
    assert check["overcount"] == {MODEL: {"output_tokens": 2000}}
    assert check["overcount_usd"] == pytest.approx(2000 * 25 / 1e6)


def test_a_model_the_cli_never_saw_is_an_overcount():
    check = hook.check_cost({MODEL: _tokens(out=2000)}, {}, NO_CALLS, 0)
    assert check["status"] == "overcount"


def test_overcount_within_rounding_is_ok():
    check = hook.check_cost({MODEL: _tokens(out=210)}, {MODEL: _tokens(out=200)}, NO_CALLS, 0)
    assert check["status"] == "ok"


def test_cli_output_nothing_explains_is_unreconciled():
    check = hook.check_cost({MODEL: _tokens(out=200)}, {MODEL: _tokens(out=5200)}, NO_CALLS, 0)
    assert check["status"] == "unreconciled"
    assert check["output_excess_tokens"] == 5000
    assert check["output_allowance"] == hook.OUTPUT_SLACK_TOKENS


def test_side_calls_explain_extra_cli_output():
    calls = {**NO_CALLS, "web_tool_calls": 2}
    check = hook.check_cost({MODEL: _tokens(out=200)}, {MODEL: _tokens(out=5200)}, calls, 0)
    assert check["status"] == "ok"


def test_partial_subagent_output_is_a_lower_bound_not_a_finding():
    check = hook.check_cost({MODEL: _tokens(out=200)}, {MODEL: _tokens(out=50_000)}, NO_CALLS, 3)
    assert check["status"] == "output-lower-bound"


def test_overcount_wins_over_a_lower_bound():
    check = hook.check_cost({MODEL: _tokens(out=9000)}, {MODEL: _tokens(out=200)}, NO_CALLS, 3)
    assert check["status"] == "overcount"


# ---------------------------------------------------------------------------
# build_cost_check: duplicates and when the comparison runs
# ---------------------------------------------------------------------------

def _turn(*mids):
    return {"api_call_ids": list(mids), "tool_calls": [], "model": MODEL,
            "usage": {"input": 0, "output": 0, "total": 0, "cache_read": 0, "cache_creation": 0}}


def test_a_message_billed_by_parent_and_subagent_is_a_duplicate():
    agents = {"a1": {"message_ids": ["m2", "m3"], "tokens_by_model": {}}}
    check = hook.build_cost_check([_turn("m1", "m2")], agents, None, None, None, [], 0, frozenset())
    assert check["duplicate_messages"] == 1
    assert check["tags"] == ["duplicate-billing"]


def test_a_message_split_across_two_parent_turns_is_not_a_duplicate():
    """build_turns bills it once, in the turn it started in."""
    check = hook.build_cost_check([_turn("m1"), _turn("m1", "m2")], {}, None, None, None, [], 0,
                                  frozenset())
    assert check["duplicate_messages"] == 0
    assert check["tags"] == []


def test_a_continuations_copied_messages_are_not_duplicates():
    agents = {"a1": {"message_ids": ["m1"], "tokens_by_model": {}}}
    check = hook.build_cost_check([_turn("m1")], agents, None, None, None, [], 0, frozenset({"m1"}))
    assert check["duplicate_messages"] == 0


@pytest.mark.parametrize("cost_state, cli_cost_state, agents, status", [
    (None, None, {}, "unchecked: no cost-state yet"),
    ({"partial": True}, {}, {}, "unchecked: cost-state covers part of the session"),
    ({"by_model": {}}, {"predecessor_cost_usd": 1.0}, {}, "unchecked: continuation"),
    ({"by_model": {}}, {}, {"a1": {"message_ids": []}}, "unchecked: subagent state predates the check"),
])
def test_comparison_needs_a_whole_session_cost_state(cost_state, cli_cost_state, agents, status):
    check = hook.build_cost_check([_turn("m1")], agents, cost_state, cli_cost_state, None, [], 0,
                                  frozenset())
    assert check["status"] == status


# ---------------------------------------------------------------------------
# End to end: tags, rollup, score, log
# ---------------------------------------------------------------------------

def _write_session(tmp_path, cli_output_tokens):
    entries = [
        {"type": "user", "uuid": "u1", "sessionId": SID, "timestamp": "2026-10-20T10:00:00Z",
         "cwd": "/x/repo", "message": {"role": "user", "content": "hi"}},
        {"type": "assistant", "uuid": "a1", "sessionId": SID, "timestamp": "2026-10-20T10:00:01Z",
         "message": {"id": "m1", "role": "assistant", "model": MODEL, "stop_reason": "end_turn",
                     "content": [{"type": "text", "text": "hello"}],
                     "usage": {"input_tokens": 2000, "output_tokens": 2200}}},
        {"type": "cost-state", "totalCostUSD": 0.015, "startTime": 1792490400000,
         "modelUsage": {MODEL: {"costUSD": 0.015, "inputTokens": 2000,
                                "outputTokens": cli_output_tokens,
                                "cacheReadInputTokens": 0, "cacheCreationInputTokens": 0}}},
    ]
    path = tmp_path / "proj" / f"{SID}.jsonl"
    path.parent.mkdir()
    path.write_text("".join(json.dumps(e) + "\n" for e in entries))
    return str(path)


def _run(monkeypatch, tmp_path, cli_output_tokens):
    sent = []
    monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent.append(batch) or True)
    monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))
    hook.process_session(SID, _write_session(tmp_path, cli_output_tokens), "/x/repo", final=True)
    return sent[-1]


def test_an_overcount_is_tagged_logged_and_scored(monkeypatch, tmp_path):
    batch = _run(monkeypatch, tmp_path, cli_output_tokens=200)
    rollups = session_rollups(batch)
    assert rollups["cost_check"]["status"] == "overcount"
    assert "cost-overcount" in rollups["tags"]
    residual = next(e["body"]["value"] for e in batch
                    if e["type"] == "score-create" and e["body"]["name"] == "cost_residual_usd")
    assert residual == pytest.approx(0.015 - session_score(batch)["value"])
    assert residual < 0
    assert "[WARN] cost-overcount in session" in Path(hook.LOG_FILE).read_text()


def test_a_matching_session_raises_nothing(monkeypatch, tmp_path):
    batch = _run(monkeypatch, tmp_path, cli_output_tokens=2200)
    rollups = session_rollups(batch)
    assert rollups["cost_check"]["status"] == "ok"
    assert not {"cost-overcount", "cost-unreconciled", "duplicate-billing"} & set(rollups["tags"])
