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
