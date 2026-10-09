"""Haiku 5.5 prompt-length pricing tiers, and the per-turn transcript signals
first seen in Claude Code 2.1.252-2.1.295 (requestedModel, thinkingDurationMs,
truncatedAfterOutput, isAbortedMidStream, advisorModel, turnOrigin)."""
import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook", Path(__file__).resolve().parent.parent / "langfuse-hook.py"
)
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)

M = 1_000_000


def _usage(inp=0, out=0, cache_read=0, cache_creation=0):
    return {
        "input": inp, "output": out, "total": inp + out,
        "cache_read": cache_read, "cache_creation": cache_creation,
    }


def _long(inp=0, out=0, cache_read=0, cache_creation=0, cache_5m=0, cache_1h=0):
    return {
        "input": inp, "output": out, "cache_read": cache_read,
        "cache_creation": cache_creation, "cache_5m": cache_5m, "cache_1h": cache_1h,
    }


def _user(text, ts="2026-10-09T10:00:00Z", **extra):
    return {"type": "user", "timestamp": ts,
            "message": {"role": "user", "content": text}, **extra}


def _asst(mid, *, model="claude-haiku-5-5", inp=10, out=5, cache_read=0,
          cache_creation=0, ts="2026-10-09T10:00:01Z", text="ok", **extra):
    return {
        "type": "assistant", "timestamp": ts, "requestId": f"req-{mid}",
        "message": {
            "id": mid, "role": "assistant", "model": model,
            "content": [{"type": "text", "text": text}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": inp, "output_tokens": out,
                      "cache_read_input_tokens": cache_read,
                      "cache_creation_input_tokens": cache_creation},
        },
        **extra,
    }


# ---------------------------------------------------------------- pricing

class TestHaiku55Pricing:
    def test_short_prompt_rates(self):
        _c, _i, out, d = hook.calculate_turn_cost(
            _usage(inp=M, out=M, cache_read=M), "claude-haiku-5-5")
        assert abs(d["input"] - 0.10) < 1e-9
        assert abs(out - 0.50) < 1e-9
        assert abs(d["cache_read_input_tokens"] - 0.01) < 1e-9

    def test_short_prompt_cache_write_tiers(self):
        _c, _i, _o, d = hook.calculate_turn_cost(
            _usage(cache_creation=2 * M), "claude-haiku-5-5", cache_5m=M, cache_1h=M)
        assert abs(d["cache_creation_input_tokens"] - (0.125 + 0.20)) < 1e-9

    def test_long_prompt_rates(self):
        u = _usage(inp=M, out=M, cache_read=M, cache_creation=2 * M)
        _c, _i, out, d = hook.calculate_turn_cost(
            u, "claude-haiku-5-5", cache_5m=M, cache_1h=M,
            long_context_usage=_long(inp=M, out=M, cache_read=M,
                                     cache_creation=2 * M, cache_5m=M, cache_1h=M))
        assert abs(d["input"] - 0.50) < 1e-9
        assert abs(out - 2.50) < 1e-9
        assert abs(d["cache_read_input_tokens"] - 0.05) < 1e-9
        assert abs(d["cache_creation_input_tokens"] - (0.625 + 1.00)) < 1e-9

    def test_mixed_turn_splits_by_request(self):
        # 2M input in the turn, 1M of it from a request over 100K.
        cost, _i, _o, d = hook.calculate_turn_cost(
            _usage(inp=2 * M), "claude-haiku-5-5",
            long_context_usage=_long(inp=M))
        assert abs(d["input"] - (0.10 + 0.50)) < 1e-9
        assert abs(cost - 0.60) < 1e-9

    def test_long_cache_creation_without_tier_split_bills_5m(self):
        _c, _i, _o, d = hook.calculate_turn_cost(
            _usage(cache_creation=M), "claude-haiku-5-5",
            long_context_usage=_long(cache_creation=M))
        assert abs(d["cache_creation_input_tokens"] - 0.625) < 1e-9

    def test_us_geo_applies_to_both_tiers(self):
        cost, _i, _o, _d = hook.calculate_turn_cost(
            _usage(inp=2 * M), "claude-haiku-5-5", inference_geo="us",
            long_context_usage=_long(inp=M))
        assert abs(cost - 0.60 * 1.1) < 1e-9

    def test_no_fast_mode(self):
        c_fast, *_ = hook.calculate_turn_cost(_usage(inp=M), "claude-haiku-5-5", speed="fast")
        c_std, *_ = hook.calculate_turn_cost(_usage(inp=M), "claude-haiku-5-5")
        assert c_fast == c_std

    def test_bedrock_id(self):
        cost, *_ = hook.calculate_turn_cost(_usage(inp=M), "us.anthropic.claude-haiku-5-5")
        assert abs(cost - 0.10) < 1e-9

    def test_long_bucket_ignored_without_long_tier(self):
        # Only Haiku 5.5 is priced by prompt length; other models keep one rate.
        base, *_ = hook.calculate_turn_cost(_usage(inp=M), "claude-sonnet-5-5")
        with_long, *_ = hook.calculate_turn_cost(
            _usage(inp=M), "claude-sonnet-5-5", long_context_usage=_long(inp=M))
        assert base == with_long

    def test_haiku_4_5_unchanged(self):
        cost, *_ = hook.calculate_turn_cost(_usage(inp=M, out=M), "claude-haiku-4-5-20251001")
        assert abs(cost - 6.0) < 1e-9


class TestLongContextBucket:
    def test_only_requests_over_threshold_go_to_long_bucket(self):
        turns = hook.build_turns([
            _user("hi"),
            _asst("m1", inp=1_000, cache_read=49_000, out=10),
            _asst("m2", inp=1_000, cache_read=149_000, cache_creation=500, out=20,
                  ts="2026-10-09T10:00:02Z"),
        ])
        assert len(turns) == 1
        t = turns[0]
        assert t["usage_long"] == _long(inp=1_000, out=20, cache_read=149_000,
                                         cache_creation=500)
        assert t["usage"]["input"] == 2_000

    def test_exactly_threshold_is_short(self):
        turns = hook.build_turns([
            _user("hi"), _asst("m1", inp=1_000, cache_read=99_000)])
        assert turns[0]["usage_long"] is None

    def test_end_to_end_cost(self):
        turns = hook.build_turns([
            _user("hi"),
            _asst("m1", inp=M, out=0),           # prompt 1M > 100K: long tier
        ])
        t = turns[0]
        cost, *_ = hook.calculate_turn_cost(
            t["usage"], t["model"], long_context_usage=t["usage_long"])
        assert abs(cost - 0.50) < 1e-9


# ---------------------------------------------------------- turn signals

class TestTurnSignals:
    def test_fallback_when_requested_model_differs(self):
        t = hook.build_turns([
            _user("hi"),
            _asst("m1", model="claude-sonnet-5", requestedModel="claude-sonnet-5-5"),
        ])[0]
        assert t["requested_model"] == "claude-sonnet-5-5"
        md = hook.gen_metadata_turn_signals(t)
        assert md["model_fallback"] is True
        assert md["requested_model"] == "claude-sonnet-5-5"

    def test_no_fallback_when_models_match(self):
        t = hook.build_turns([
            _user("hi"),
            _asst("m1", model="claude-opus-5-5", requestedModel="claude-opus-5-5"),
        ])[0]
        md = hook.gen_metadata_turn_signals(t)
        assert "model_fallback" not in md
        assert "requested_model" not in md

    def test_thinking_duration_max_per_message_summed_across_messages(self):
        t = hook.build_turns([
            _user("hi"),
            _asst("m1", thinkingDurationMs=1442),
            _asst("m1", thinkingDurationMs=1),     # same message, later block
            _asst("m2", thinkingDurationMs=500, ts="2026-10-09T10:00:05Z"),
        ])[0]
        assert t["thinking_duration_ms"] == 1942
        assert hook.gen_metadata_turn_signals(t)["thinking_duration_ms"] == 1942

    def test_truncated_and_aborted_flags(self):
        t = hook.build_turns([
            _user("hi"),
            _asst("m1", truncatedAfterOutput=True, isAbortedMidStream=True),
        ])[0]
        md = hook.gen_metadata_turn_signals(t)
        assert md["truncated_after_output"] is True
        assert md["aborted_mid_stream"] is True

    def test_advisor_model_and_turn_origin(self):
        t = hook.build_turns([
            _user("hi", turnOrigin="task_notification"),
            _asst("m1", advisorModel="claude-opus-5-5"),
        ])[0]
        md = hook.gen_metadata_turn_signals(t)
        assert md["advisor_model"] == "claude-opus-5-5"
        assert md["turn_origin"] == "task_notification"

    def test_plain_turn_emits_nothing(self):
        t = hook.build_turns([_user("hi"), _asst("m1")])[0]
        assert hook.gen_metadata_turn_signals(t) == {}

    def test_long_context_request_count_in_metadata(self):
        t = hook.build_turns([_user("hi"), _asst("m1", inp=200_000)])[0]
        assert hook.gen_metadata_turn_signals(t)["long_context_requests"] == 1

    def test_long_context_count_omitted_for_models_without_tier(self):
        t = hook.build_turns([_user("hi"), _asst("m1", model="claude-sonnet-5-5",
                                                 inp=200_000)])[0]
        assert "long_context_requests" not in hook.gen_metadata_turn_signals(t)


class TestTurnSignalSummary:
    def test_none_when_no_signals(self):
        t = hook.build_turns([_user("hi"), _asst("m1")])
        assert hook.build_turn_signal_summary(t) is None

    def test_rollup(self):
        turns = hook.build_turns([
            _user("a", turnOrigin="human"),
            _asst("m1", model="claude-sonnet-5", requestedModel="claude-sonnet-5-5",
                  thinkingDurationMs=100),
            _user("b", ts="2026-10-09T10:01:00Z", turnOrigin="sdk"),
            _asst("m2", ts="2026-10-09T10:01:01Z", truncatedAfterOutput=True,
                  thinkingDurationMs=50),
            _user("c", ts="2026-10-09T10:02:00Z", turnOrigin="human"),
            _asst("m3", ts="2026-10-09T10:02:01Z"),
        ])
        s = hook.build_turn_signal_summary(turns)
        assert s["turn_origins"] == {"human": 2, "sdk": 1}
        assert s["model_fallback_turns"] == 1
        assert s["truncated_turns"] == 1
        assert s["aborted_turns"] == 0
        assert s["thinking_duration_ms"] == 150
