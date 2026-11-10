"""Model IDs with a dotted version (claude-opus-4.8, as LLM gateways spell them)
bill exactly like the dashed ID.

Before the fix, claude-opus-5.5 parsed as Opus 5 ($5/$25 instead of $4/$20),
claude-sonnet-5.5 as Sonnet 5 (cache reads $0.20 instead of $0.10), and
claude-opus-4.8 / claude-haiku-5.5 were unrecognised ($0 plus a [WARN]).
"""
import importlib.util
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook", Path(__file__).resolve().parent.parent / "langfuse-hook.py"
)
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)


def _usage(inp=0, out=0, cache_read=0, cache_creation=0):
    return {
        "input": inp, "output": out, "total": inp + out,
        "cache_read": cache_read, "cache_creation": cache_creation,
    }


# One million of each billed kind, so each line of costDetails is the rate.
_ALL_KINDS = _usage(inp=1_000_000, out=1_000_000, cache_read=1_000_000,
                    cache_creation=2_000_000)

PAIRS = [
    ("claude-opus-5-5", "claude-opus-5.5"),
    ("claude-opus-4-8", "claude-opus-4.8"),
    ("claude-opus-4-1", "claude-opus-4.1"),
    ("claude-sonnet-5-5", "claude-sonnet-5.5"),
    ("claude-sonnet-4-5", "claude-sonnet-4.5"),
    ("claude-haiku-5-5", "claude-haiku-5.5"),
    ("claude-haiku-4-5", "claude-haiku-4.5"),
    ("claude-fable-5-1", "claude-fable-5.1"),
    ("claude-3-5-sonnet-20241022", "claude-3.5-sonnet-20241022"),
    ("anthropic.claude-opus-5-5", "anthropic.claude-opus-5.5"),
    ("us.anthropic.claude-opus-4-8", "us.anthropic.claude-opus-4.8"),
    ("anthropic/claude-opus-4-8", "anthropic/claude-opus-4.8"),
]


@pytest.mark.parametrize("dashed,dotted", PAIRS)
def test_dotted_id_bills_like_dashed(dashed, dotted):
    kwargs = dict(cache_5m=1_000_000, cache_1h=1_000_000)
    want = hook.calculate_turn_cost(_ALL_KINDS, dashed, **kwargs)
    got = hook.calculate_turn_cost(_ALL_KINDS, dotted, **kwargs)
    assert want[0] > 0, f"{dashed} should be a priced model"
    assert got == want


@pytest.mark.parametrize("dashed,dotted", [
    ("claude-opus-5-5", "claude-opus-5.5"),
    ("claude-opus-4-8", "claude-opus-4.8"),
])
def test_fast_mode_premium_applies_to_dotted_id(dashed, dotted):
    want = hook.calculate_turn_cost(_usage(inp=1_000_000), dashed, speed="fast")
    got = hook.calculate_turn_cost(_usage(inp=1_000_000), dotted, speed="fast")
    assert got == want


def test_opus_5_5_dotted_is_not_opus_5():
    *_, details = hook.calculate_turn_cost(
        _usage(inp=1_000_000, out=1_000_000, cache_read=1_000_000), "claude-opus-5.5"
    )
    assert abs(details["input"] - 4.0) < 1e-6
    assert abs(details["output"] - 20.0) < 1e-6
    assert abs(details["cache_read_input_tokens"] - 0.20) < 1e-6


def test_haiku_5_5_dotted_has_long_context_tier():
    assert hook._has_long_context_tier("claude-haiku-5.5")
    long_usage = _usage(inp=1_000_000)
    want = hook.calculate_turn_cost(_usage(), "claude-haiku-5-5", long_context_usage=long_usage)
    got = hook.calculate_turn_cost(_usage(), "claude-haiku-5.5", long_context_usage=long_usage)
    assert want[0] > 0 and got == want


def test_unknown_dotted_minor_still_warns(monkeypatch):
    """claude-opus-5.6 is a new model: $0 + [WARN], not Opus 5's rate."""
    logs = []
    monkeypatch.setattr(hook, "log", logs.append)
    cost, *_ = hook.calculate_turn_cost(_usage(inp=1_000_000), "claude-opus-5.6")
    assert cost == 0.0
    assert any("[WARN]" in m and "claude-opus-5.6" in m for m in logs)


@pytest.mark.parametrize("model,want", [
    ("claude-opus-5.5", "claude-opus-5-5"),
    ("CLAUDE-OPUS-4.8", "claude-opus-4-8"),
    ("us.anthropic.claude-opus-4.8", "us.anthropic.claude-opus-4-8"),
    ("anthropic.claude-3-5-sonnet-20241022-v2:0", "anthropic.claude-3-5-sonnet-20241022-v2:0"),
])
def test_normalise_model_id(model, want):
    assert hook._normalise_model_id(model) == want


def test_opus_5_5_savings_counts_dotted_id():
    turn = {
        "model": "claude-opus-5.5", "usage": _usage(inp=1_000_000),
        "cache_ephemeral_5m": 0, "cache_ephemeral_1h": 0,
        "speed": "", "inference_geo": "", "web_search_requests": 0,
        "start_time": "2026-11-01T10:00:00Z",
    }
    savings = hook.build_opus_5_5_savings([turn])
    assert savings and savings["turns"] == 1
