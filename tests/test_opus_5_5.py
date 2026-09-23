"""Opus 5.5 pricing plus the per-model / per-effort / refusal rollups that
make an Opus 5 -> Opus 5.5 switch legible in Langfuse."""
import importlib.util
import json
from pathlib import Path

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


def _turn(model, *, inp=1_000_000, out=0, effort="", stop_reason="end_turn",
          refusal_category=None, start_time="2026-09-20T10:00:00Z"):
    return {
        "model": model,
        "usage": _usage(inp=inp, out=out),
        "effort": effort,
        "stop_reason": stop_reason,
        "refusal_category": refusal_category,
        "start_time": start_time,
        "cache_ephemeral_5m": 0,
        "cache_ephemeral_1h": 0,
        "speed": "",
        "inference_geo": "",
        "web_search_requests": 0,
    }


# ---------------------------------------------------------------- pricing

class TestOpus55Pricing:
    def test_base_rates(self):
        _c, _inp, out, d = hook.calculate_turn_cost(
            _usage(inp=1_000_000, out=1_000_000, cache_read=1_000_000),
            "claude-opus-5-5",
        )
        assert abs(d["input"] - 4.0) < 1e-6
        assert abs(out - 20.0) < 1e-6
        assert abs(d["cache_read_input_tokens"] - 0.20) < 1e-6

    def test_cache_write_tiers(self):
        u = _usage(cache_creation=2_000_000)
        _c, _i, _o, d = hook.calculate_turn_cost(
            u, "claude-opus-5-5", cache_5m=1_000_000, cache_1h=1_000_000
        )
        assert abs(d["cache_creation_input_tokens"] - (5.0 + 8.0)) < 1e-6

    def test_fast_mode_is_2x(self):
        _c, inp, out, _d = hook.calculate_turn_cost(
            _usage(inp=1_000_000, out=1_000_000), "claude-opus-5-5", speed="fast"
        )
        assert abs(inp - 8.0) < 1e-6
        assert abs(out - 40.0) < 1e-6

    def test_us_geo(self):
        _c, inp, _o, _d = hook.calculate_turn_cost(
            _usage(inp=1_000_000), "claude-opus-5-5", inference_geo="us"
        )
        assert abs(inp - 4.4) < 1e-6

    def test_opus_5_unchanged(self):
        # Guard against the fix moving Opus 5 itself.
        _c, inp, out, _d = hook.calculate_turn_cost(
            _usage(inp=1_000_000, out=1_000_000), "claude-opus-5"
        )
        assert abs(inp - 5.0) < 1e-6
        assert abs(out - 25.0) < 1e-6

    def test_dated_opus_5_still_opus_5(self):
        _c, inp, _o, _d = hook.calculate_turn_cost(
            _usage(inp=1_000_000), "claude-opus-5-20260401"
        )
        assert abs(inp - 5.0) < 1e-6

    def test_dated_opus_5_5_is_opus_5_5(self):
        _c, inp, _o, _d = hook.calculate_turn_cost(
            _usage(inp=1_000_000), "claude-opus-5-5-20260915"
        )
        assert abs(inp - 4.0) < 1e-6

    def test_bedrock_id(self):
        _c, inp, _o, _d = hook.calculate_turn_cost(
            _usage(inp=1_000_000), "anthropic.claude-opus-5-5"
        )
        assert abs(inp - 4.0) < 1e-6

    def test_unknown_opus_5_minor_warns_not_opus_5_rate(self, monkeypatch):
        """A future claude-opus-5-6 must not silently inherit Opus 5 pricing —
        exactly the bug that billed Opus 5.5 at $5/$25."""
        logs = []
        monkeypatch.setattr(hook, "log", logs.append)
        cost, *_ = hook.calculate_turn_cost(_usage(inp=1_000_000), "claude-opus-5-6")
        assert cost == 0.0
        assert any("[WARN]" in m and "claude-opus-5-6" in m for m in logs)


# ---------------------------------------------------------------- rollups

class TestModelCostBreakdown:
    def test_none_when_no_turns(self):
        assert hook.build_model_cost_breakdown([]) is None

    def test_splits_by_model_and_effort(self):
        turns = [
            _turn("claude-opus-5", effort="high"),
            _turn("claude-opus-5-5", effort="medium"),
            _turn("claude-opus-5-5", effort="medium"),
            _turn("claude-opus-5-5", effort=""),
        ]
        b = hook.build_model_cost_breakdown(turns)
        assert set(b) == {"claude-opus-5", "claude-opus-5-5"}
        o5 = b["claude-opus-5"]
        assert o5["turns"] == 1
        assert abs(o5["cost_usd"] - 5.0) < 1e-6
        o55 = b["claude-opus-5-5"]
        assert o55["turns"] == 3
        assert o55["input_tokens"] == 3_000_000
        assert abs(o55["cost_usd"] - 12.0) < 1e-6
        assert o55["by_effort"]["medium"] == {"turns": 2, "cost_usd": 8.0}
        assert o55["by_effort"]["unset"] == {"turns": 1, "cost_usd": 4.0}

    def test_missing_model_bucket(self):
        b = hook.build_model_cost_breakdown([_turn("", inp=10)])
        assert "_missing" in b


class TestOpus55Savings:
    def test_none_without_opus_5_5_turns(self):
        assert hook.build_opus_5_5_savings([_turn("claude-opus-5")]) is None

    def test_counterfactual_against_opus_5(self):
        turns = [
            _turn("claude-opus-5-5", inp=1_000_000, out=1_000_000),
            _turn("claude-opus-5"),  # ignored: not an Opus 5.5 turn
        ]
        s = hook.build_opus_5_5_savings(turns)
        assert s["turns"] == 1
        assert abs(s["actual_cost_usd"] - 24.0) < 1e-6
        assert abs(s["opus_5_equivalent_cost_usd"] - 30.0) < 1e-6
        assert abs(s["saved_usd"] - 6.0) < 1e-6


class TestRefusalCategories:
    def test_rollup_counts_categories(self):
        turns = [
            _turn("claude-opus-5-5", stop_reason="refusal", refusal_category="bio"),
            _turn("claude-opus-5-5", stop_reason="refusal", refusal_category=None),
            _turn("claude-opus-5-5"),
        ]
        s = hook.build_stop_reasons_summary(turns)
        assert s["refusal_categories"] == {"bio": 1, "uncategorized": 1}

    def test_absent_without_refusals(self):
        s = hook.build_stop_reasons_summary([_turn("claude-opus-5-5")])
        assert "refusal_categories" not in s


# ---------------------------------------------------------------- end to end

def test_process_session_emits_model_tags_and_rollups(tmp_path, monkeypatch):
    transcript = tmp_path / "sess.jsonl"
    lines = [
        {"type": "user", "timestamp": "2026-09-20T10:00:00Z", "cwd": "/x/repo",
         "version": "2.1.280", "message": {"role": "user", "content": "first"}},
        {"type": "assistant", "timestamp": "2026-09-20T10:00:01Z", "effort": "high",
         "message": {"role": "assistant", "id": "m1", "model": "claude-opus-5",
                     "stop_reason": "end_turn",
                     "content": [{"type": "text", "text": "a"}],
                     "usage": {"input_tokens": 1000, "output_tokens": 100}}},
        {"type": "user", "timestamp": "2026-09-20T10:01:00Z",
         "message": {"role": "user", "content": "second"}},
        {"type": "assistant", "timestamp": "2026-09-20T10:01:01Z", "effort": "medium",
         "message": {"role": "assistant", "id": "m2", "model": "claude-opus-5-5",
                     "stop_reason": "refusal",
                     "stop_details": {"type": "refusal", "category": "reasoning_extraction",
                                      "explanation": "x"},
                     "content": [{"type": "text", "text": "b"}],
                     "usage": {"input_tokens": 1000, "output_tokens": 100}}},
    ]
    transcript.write_text("\n".join(json.dumps(l) for l in lines) + "\n")

    captured = {}
    monkeypatch.setattr(hook, "send_to_langfuse",
                        lambda batch: captured.setdefault("batch", batch) or True)
    monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))

    hook.process_session("sess", str(transcript), "/x/repo")

    trace = next(e for e in captured["batch"] if e["type"] == "trace-create")
    tags = trace["body"]["tags"]
    meta = trace["body"]["metadata"]
    assert "model:claude-opus-5" in tags
    assert "model:claude-opus-5-5" in tags
    assert "refusal:reasoning_extraction" in tags
    assert set(meta["cost_by_model"]) == {"claude-opus-5", "claude-opus-5-5"}
    assert meta["opus_5_5_savings"]["turns"] == 1
    assert meta["stop_reasons"]["refusal_categories"] == {"reasoning_extraction": 1}

    gens = [e for e in captured["batch"] if e["type"] == "generation-create"]
    refused = next(g for g in gens if g["body"]["model"] == "claude-opus-5-5")
    assert refused["body"]["metadata"]["refusal_category"] == "reasoning_extraction"
