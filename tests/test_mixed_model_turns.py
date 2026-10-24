"""A turn answered by more than one model prices each model's share at its own rates.

Some Opus turns carry a few Haiku messages. The turn's `model` is the last
one that answered, and the whole turn used to be billed at it: Haiku tokens at
Opus rates (2 local sessions, $0.36 over, found by tools/audit_costs.py on
2026-10-24).
"""
import importlib.util
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook", Path(__file__).resolve().parent.parent / "langfuse-hook.py"
)
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)

OPUS = "claude-opus-4-8"      # $5 / $25 per 1M
HAIKU = "claude-haiku-4-5"    # $1 / $5 per 1M
MTOK = 1_000_000


def _user(uid, text, ts):
    return {"type": "user", "uuid": uid, "timestamp": ts,
            "message": {"role": "user", "content": text}}


def _asst(uid, mid, model, ts, inp, out, stop_reason="tool_use", cc_1h=0):
    usage = {"input_tokens": inp, "output_tokens": out}
    if cc_1h:
        usage["cache_creation_input_tokens"] = cc_1h
        usage["cache_creation"] = {"ephemeral_5m_input_tokens": 0, "ephemeral_1h_input_tokens": cc_1h}
    return {"type": "assistant", "uuid": uid, "timestamp": ts,
            "message": {"id": mid, "role": "assistant", "model": model, "stop_reason": stop_reason,
                        "content": [{"type": "text", "text": "x"}], "usage": usage}}


def _mixed_turn():
    return [
        _user("u1", "do it", "2026-10-20T10:00:00Z"),
        _asst("a1", "m1", HAIKU, "2026-10-20T10:00:01Z", 1000, 100),
        _asst("a2", "m2", OPUS, "2026-10-20T10:00:02Z", 2000, 200, "end_turn", cc_1h=1000),
    ]


def test_each_models_share_is_priced_at_its_own_rates():
    (turn,) = hook.build_turns(_mixed_turn())
    assert turn["model"] == OPUS
    haiku = (1000 * 1 + 100 * 5) / MTOK
    opus = (2000 * 5 + 200 * 25 + 1000 * 10) / MTOK
    cost, inp, out, details = hook.price_turn(turn)
    assert cost == pytest.approx(haiku + opus)
    assert inp + out == pytest.approx(cost)
    assert details["total"] == pytest.approx(cost)
    assert details["cache_creation_input_tokens"] == pytest.approx(1000 * 10 / MTOK)


def test_turn_totals_are_unchanged():
    (turn,) = hook.build_turns(_mixed_turn())
    assert turn["usage"]["input"] == 3000
    assert turn["usage"]["output"] == 300
    assert set(turn["usage_by_model"]) == {OPUS, HAIKU}


def test_single_model_turn_has_no_shares():
    entries = [e for e in _mixed_turn() if e.get("message", {}).get("model") != HAIKU]
    (turn,) = hook.build_turns(entries)
    assert "usage_by_model" not in turn
    assert hook.price_turn(turn) == hook.calculate_turn_cost(
        turn["usage"], OPUS, 0, 1000, speed="", inference_geo="", web_search_requests=0,
        turn_start_time=turn["start_time"], long_context_usage=None)


def test_cost_by_model_splits_a_mixed_turn():
    (turn,) = hook.build_turns(_mixed_turn())
    breakdown = hook.build_model_cost_breakdown([turn])
    assert breakdown[HAIKU]["output_tokens"] == 100
    assert breakdown[HAIKU]["cost_usd"] == pytest.approx((1000 * 1 + 100 * 5) / MTOK)
    assert breakdown[OPUS]["output_tokens"] == 200
    assert breakdown[HAIKU]["turns"] == breakdown[OPUS]["turns"] == 1


def test_counterfactual_reprices_only_the_turns_own_model():
    (turn,) = hook.build_turns(_mixed_turn())
    as_haiku = hook._turn_cost_usd(turn, model=HAIKU)
    everything_haiku = (3000 * 1 + 300 * 5 + 1000 * 2) / MTOK
    assert as_haiku == pytest.approx(everything_haiku)
