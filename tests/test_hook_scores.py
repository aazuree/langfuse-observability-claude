# tests/test_hook_scores.py
"""Tests for hook-level score classifiers in langfuse-hook.py"""
import importlib.util
import os
import sys

# Import the hook module (hyphenated filename requires importlib)
_spec = importlib.util.spec_from_file_location(
    "langfuse_hook",
    os.path.join(os.path.dirname(__file__), "..", "langfuse-hook.py"),
)
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)


# --- compute_cache_hit_rate ---

def test_cache_hit_rate_no_cache():
    """Zero cache activity returns None (distinct from cache-miss-only sessions)."""
    turns = [{"usage": {"cache_read": 0, "cache_creation": 0}}]
    assert hook.compute_cache_hit_rate(turns) is None


def test_cache_hit_rate_all_reads():
    """All cache reads, no creation → 1.0."""
    turns = [{"usage": {"cache_read": 1000, "cache_creation": 0}}]
    assert hook.compute_cache_hit_rate(turns) == 1.0


def test_cache_hit_rate_all_creates():
    """All cache creation, no reads → 0.0."""
    turns = [{"usage": {"cache_read": 0, "cache_creation": 1000}}]
    assert hook.compute_cache_hit_rate(turns) == 0.0


def test_cache_hit_rate_mixed():
    """Half and half → 0.5."""
    turns = [{"usage": {"cache_read": 1000, "cache_creation": 1000}}]
    assert hook.compute_cache_hit_rate(turns) == 0.5


def test_cache_hit_rate_multi_turn():
    """Aggregates across turns."""
    turns = [
        {"usage": {"cache_read": 100, "cache_creation": 100}},
        {"usage": {"cache_read": 300, "cache_creation": 200}},
    ]
    # total read: 400, total create: 300, ratio: 400/700 ≈ 0.5714
    assert hook.compute_cache_hit_rate(turns) == 0.5714


def test_cache_hit_rate_empty_turns():
    """Empty turns list returns None — nothing to measure."""
    assert hook.compute_cache_hit_rate([]) is None


# --- build_hook_score_events ---

def _turn(cache_read=0, cache_creation=0, tool_calls=()):
    return {"usage": {"input": 100, "output": 200, "total": 300,
                      "cache_read": cache_read, "cache_creation": cache_creation},
            "assistant_output": "Done.", "tool_calls": list(tool_calls)}


def _by_name(events):
    return {e["body"]["name"]: e["body"] for e in events}


def test_session_cost_is_always_sent_with_the_rollups():
    """No cache activity and no tool calls -> only the session cost score."""
    events = hook.build_hook_score_events(
        session_id="abc", turns=[_turn()], session_cost=0.05, rollups={"turn_count": 1})
    scores = _by_name(events)
    assert set(scores) == {"session_cost_usd"}
    assert scores["session_cost_usd"]["value"] == 0.05
    assert scores["session_cost_usd"]["metadata"] == {"turn_count": 1}


def test_cache_hit_rate_when_cache_was_used():
    events = hook.build_hook_score_events(
        session_id="abc", turns=[_turn(500, 500)], session_cost=0.05)
    assert set(_by_name(events)) == {"session_cost_usd", "cache_hit_rate"}


def test_all_scores_attach_to_the_session():
    events = hook.build_hook_score_events(
        session_id="xyz",
        turns=[_turn(2000, 3000, [{"output": "[ERROR] x"}, {"output": "ok"}])],
        session_cost=0.50,
    )
    scores = _by_name(events)
    assert set(scores) == {"session_cost_usd", "cache_hit_rate", "tool_error_rate", "tool_denial_rate"}
    for body in scores.values():
        assert body["sessionId"] == "xyz"
        assert "traceId" not in body
        assert body["dataType"] == "NUMERIC"
    assert scores["cache_hit_rate"]["value"] == 0.4  # 2000 / (2000 + 3000)
    assert scores["tool_error_rate"]["value"] == 0.5  # 1 error / 2 calls
    # Calls ran and none were denied: a genuine 0.0, distinct from absent.
    assert scores["tool_denial_rate"]["value"] == 0.0


def test_score_ids_are_stable_so_every_fire_upserts():
    """Keyed on session + name only: a later fire overwrites the same score."""
    a = hook.build_hook_score_events(session_id="abc", turns=[_turn(1, 1)], session_cost=0.01)
    b = hook.build_hook_score_events(session_id="abc", turns=[_turn(1, 1)] * 3, session_cost=0.09)
    assert [e["body"]["id"] for e in a] == [e["body"]["id"] for e in b]


# --- calculate_tool_error_rate ---

def test_tool_error_rate_no_tools():
    turns = [{"tool_calls": []}]
    assert hook.calculate_tool_error_rate(turns) is None


def test_tool_error_rate_empty_turns():
    assert hook.calculate_tool_error_rate([]) is None


def test_tool_error_rate_all_clean():
    turns = [{"tool_calls": [{"output": "ok"}, {"output": "done"}]}]
    assert hook.calculate_tool_error_rate(turns) == 0.0


def test_tool_error_rate_mixed():
    turns = [{"tool_calls": [
        {"output": "[ERROR] boom"},
        {"output": "ok"},
        {"output": "ok"},
        {"output": "ok"},
    ]}]
    assert hook.calculate_tool_error_rate(turns) == 0.25


def test_tool_error_rate_multi_turn_rounding():
    turns = [
        {"tool_calls": [{"output": "[ERROR] x"}, {"output": "ok"}]},
        {"tool_calls": [{"output": "ok"}]},
    ]
    # 1 error / 3 calls = 0.3333
    assert hook.calculate_tool_error_rate(turns) == 0.3333
