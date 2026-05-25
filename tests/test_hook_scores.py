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


# --- classify_session_type ---

def test_session_type_bug_fix():
    assert hook.classify_session_type("fix the login bug") == "bug-fix"
    assert hook.classify_session_type("there's an error in the API") == "bug-fix"
    assert hook.classify_session_type("debug why tests fail") == "bug-fix"
    assert hook.classify_session_type("this is broken, can you fix it?") == "bug-fix"
    assert hook.classify_session_type("troubleshoot the connection issue") == "bug-fix"


def test_session_type_feature():
    assert hook.classify_session_type("add a new endpoint for users") == "feature"
    assert hook.classify_session_type("create a login page") == "feature"
    assert hook.classify_session_type("implement the scoring system") == "feature"
    assert hook.classify_session_type("build a dashboard component") == "feature"
    assert hook.classify_session_type("write a script to process data") == "feature"


def test_session_type_refactor():
    assert hook.classify_session_type("refactor the auth module") == "refactor"
    assert hook.classify_session_type("clean up the utils file") == "refactor"
    assert hook.classify_session_type("rename getUserData to fetchUser") == "refactor"
    assert hook.classify_session_type("reorganize the project structure") == "refactor"
    assert hook.classify_session_type("migrate from callbacks to async/await") == "refactor"


def test_session_type_research():
    assert hook.classify_session_type("explain how the auth middleware works") == "research"
    assert hook.classify_session_type("what does this function do?") == "research"
    assert hook.classify_session_type("how does the caching layer work?") == "research"
    assert hook.classify_session_type("read the config file and summarize it") == "research"
    assert hook.classify_session_type("find where the API key is used") == "research"


def test_session_type_exploratory():
    assert hook.classify_session_type("hello") == "exploratory"
    assert hook.classify_session_type("let's work on the project") == "exploratory"
    assert hook.classify_session_type("") == "exploratory"
    assert hook.classify_session_type("hmm, not sure what to do") == "exploratory"


def test_session_type_case_insensitive():
    assert hook.classify_session_type("FIX the BUG") == "bug-fix"
    assert hook.classify_session_type("REFACTOR the code") == "refactor"


def test_session_type_priority_bugfix_over_feature():
    """Bug-fix keywords should win when mixed with feature keywords."""
    assert hook.classify_session_type("add error handling to fix the crash") == "bug-fix"


# --- calculate_token_efficiency ---

def test_token_efficiency_balanced():
    """50/50 input/output should give ~0.5."""
    turns = [{"usage": {"input": 500, "output": 500, "total": 1000,
                         "cache_read": 0, "cache_creation": 0}}]
    result = hook.calculate_token_efficiency(turns)
    assert result == 0.5


def test_token_efficiency_heavy_cache():
    """Sessions dominated by cache reads should score low."""
    turns = [{"usage": {"input": 100, "output": 500, "total": 600,
                         "cache_read": 50000, "cache_creation": 10000}}]
    result = hook.calculate_token_efficiency(turns)
    assert 0.0 < result < 0.02  # 500 / (500 + 100 + 50000 + 10000) ≈ 0.008


def test_token_efficiency_output_only():
    """All output tokens = max efficiency (1.0)."""
    turns = [{"usage": {"input": 0, "output": 1000, "total": 1000,
                         "cache_read": 0, "cache_creation": 0}}]
    result = hook.calculate_token_efficiency(turns)
    assert result == 1.0


def test_token_efficiency_zero_tokens():
    """No tokens at all should return 0.0 (avoid division by zero)."""
    turns = [{"usage": {"input": 0, "output": 0, "total": 0,
                         "cache_read": 0, "cache_creation": 0}}]
    result = hook.calculate_token_efficiency(turns)
    assert result == 0.0


def test_token_efficiency_empty_turns():
    """Empty turn list should return 0.0."""
    result = hook.calculate_token_efficiency([])
    assert result == 0.0


def test_token_efficiency_multiple_turns():
    """Should aggregate across all turns."""
    turns = [
        {"usage": {"input": 100, "output": 200, "total": 300,
                    "cache_read": 0, "cache_creation": 0}},
        {"usage": {"input": 100, "output": 300, "total": 400,
                    "cache_read": 0, "cache_creation": 0}},
    ]
    result = hook.calculate_token_efficiency(turns)
    # (200 + 300) / (200 + 300 + 100 + 100) = 500 / 700 = 0.714...
    assert round(result, 2) == 0.71


def test_token_efficiency_rounds_to_4_decimals():
    """Result should be rounded to 4 decimal places."""
    turns = [{"usage": {"input": 333, "output": 777, "total": 1110,
                         "cache_read": 111, "cache_creation": 0}}]
    result = hook.calculate_token_efficiency(turns)
    assert result == round(result, 4)


# --- classify_task_completed ---

def _make_turns(last_output, tool_calls=None):
    """Helper to build a minimal turn list for classify_task_completed."""
    return [{
        "user_input": "do something",
        "assistant_output": last_output,
        "tool_calls": tool_calls or [],
        "usage": {"input": 0, "output": 100, "total": 100,
                  "cache_read": 0, "cache_creation": 0},
    }]


def test_task_completed_clean_finish():
    turns = _make_turns("Done! I've updated the file.")
    assert hook.classify_task_completed(turns) is True


def test_task_completed_with_error_output():
    turns = _make_turns("I encountered an error and couldn't complete the task.")
    assert hook.classify_task_completed(turns) is False


def test_task_completed_tool_error():
    turns = _make_turns("Here are the results.", [
        {"name": "Bash", "output": "[ERROR] command not found", "input": {}},
    ])
    assert hook.classify_task_completed(turns) is False


def test_task_completed_empty_turns():
    assert hook.classify_task_completed([]) is True


def test_task_completed_asks_question():
    """If the last output is asking a clarifying question, task is not completed."""
    turns = _make_turns("Could you clarify what you mean by 'fix the layout'?")
    assert hook.classify_task_completed(turns) is False


def test_task_completed_single_turn_success():
    turns = _make_turns("The function has been refactored to use async/await.")
    assert hook.classify_task_completed(turns) is True


def test_task_completed_tool_error_not_last():
    """Error in non-last tool call but clean final output is still success."""
    turns = [
        {
            "user_input": "fix it",
            "assistant_output": "Had an error but recovered.",
            "tool_calls": [{"name": "Bash", "output": "[ERROR] oops", "input": {}}],
            "usage": {"input": 0, "output": 100, "total": 100,
                      "cache_read": 0, "cache_creation": 0},
        },
        {
            "user_input": "",
            "assistant_output": "Fixed! The tests pass now.",
            "tool_calls": [{"name": "Bash", "output": "All 5 tests passed", "input": {}}],
            "usage": {"input": 0, "output": 100, "total": 100,
                      "cache_read": 0, "cache_creation": 0},
        },
    ]
    assert hook.classify_task_completed(turns) is True


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


# --- classify_cost_tier ---

def test_cost_tier_cheap():
    """Cost < $0.10 → 'cheap'."""
    assert hook.classify_cost_tier(0.00) == "cheap"
    assert hook.classify_cost_tier(0.05) == "cheap"
    assert hook.classify_cost_tier(0.099) == "cheap"


def test_cost_tier_moderate():
    """$0.10 ≤ cost < $1.00 → 'moderate'."""
    assert hook.classify_cost_tier(0.10) == "moderate"
    assert hook.classify_cost_tier(0.50) == "moderate"
    assert hook.classify_cost_tier(0.999) == "moderate"


def test_cost_tier_expensive():
    """Cost ≥ $1.00 → 'expensive'."""
    assert hook.classify_cost_tier(1.00) == "expensive"
    assert hook.classify_cost_tier(5.00) == "expensive"
    assert hook.classify_cost_tier(100.00) == "expensive"


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
