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

def test_build_hook_score_events_omits_both_when_no_activity():
    """No cache activity and no tool calls -> zero score events."""
    events = hook.build_hook_score_events(
        trace_id="trace-abc",
        session_id="abc",
        first_user_input="fix the login bug",
        turns=[{"usage": {"input": 100, "output": 200, "total": 300,
                          "cache_read": 0, "cache_creation": 0},
                "assistant_output": "Done.",
                "tool_calls": []}],
        total_cost=0.05,
    )
    assert events == []


def test_build_hook_score_events_cache_hit_only():
    """Cache activity but no tool calls -> only cache_hit_rate."""
    events = hook.build_hook_score_events(
        trace_id="trace-abc",
        session_id="abc",
        first_user_input="fix the login bug",
        turns=[{"usage": {"input": 100, "output": 200, "total": 300,
                          "cache_read": 500, "cache_creation": 500},
                "assistant_output": "Done.",
                "tool_calls": []}],
        total_cost=0.05,
    )
    names = {e["body"]["name"] for e in events}
    assert names == {"cache_hit_rate"}


def test_build_hook_score_events_both_scores():
    """Cache activity + tool calls -> both scores, correct types/values."""
    events = hook.build_hook_score_events(
        trace_id="trace-xyz",
        session_id="xyz",
        first_user_input="explain the auth module",
        turns=[{"usage": {"input": 500, "output": 500, "total": 1000,
                          "cache_read": 2000, "cache_creation": 3000},
                "assistant_output": "The auth module works by...",
                "tool_calls": [{"output": "[ERROR] x"}, {"output": "ok"}]}],
        total_cost=0.50,
    )
    by_name = {e["body"]["name"]: e for e in events}
    assert set(by_name) == {"cache_hit_rate", "tool_error_rate"}

    chr = by_name["cache_hit_rate"]
    assert chr["type"] == "score-create"
    assert chr["body"]["dataType"] == "NUMERIC"
    assert chr["body"]["value"] == 0.4  # 2000 / (2000 + 3000)
    assert chr["body"]["traceId"] == "trace-xyz"

    ter = by_name["tool_error_rate"]
    assert ter["body"]["dataType"] == "NUMERIC"
    assert ter["body"]["value"] == 0.5  # 1 error / 2 calls


def test_build_hook_score_events_deterministic_ids():
    """Same inputs should produce same event IDs (idempotent re-ingestion)."""
    args = dict(
        trace_id="trace-abc", session_id="abc",         first_user_input="hello",
        turns=[{"usage": {"input": 0, "output": 100, "total": 100,
                           "cache_read": 0, "cache_creation": 0},
                "assistant_output": "Hi!",
                "tool_calls": []}],
        total_cost=0.01,
    )
    events_a = hook.build_hook_score_events(**args)
    events_b = hook.build_hook_score_events(**args)
    ids_a = [e["id"] for e in events_a]
    ids_b = [e["id"] for e in events_b]
    assert ids_a == ids_b


def test_build_hook_score_events_stable_ids_across_turns():
    """Score IDs must be the same regardless of how many turns have been processed.
    Previously the ID included prev_offset, creating duplicate scores per turn."""
    base = dict(
        trace_id="trace-abc", session_id="abc",
        first_user_input="hello",
        turns=[{"usage": {"input": 0, "output": 100, "total": 100,
                           "cache_read": 0, "cache_creation": 0},
                "assistant_output": "Hi!", "tool_calls": []}],
        total_cost=0.01,
    )
    events_turn1 = hook.build_hook_score_events(**base)
    events_turn3 = hook.build_hook_score_events(**base)
    assert [e["id"] for e in events_turn1] == [e["id"] for e in events_turn3]


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
