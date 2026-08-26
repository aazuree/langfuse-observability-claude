# tests/test_thinking_tokens.py
"""Tests for capturing usage.output_tokens_details.thinking_tokens (CC 2.1.2xx).

Thinking tokens are a breakdown *inside* output_tokens — they are billed at the
output rate, so this is an analytics capture, not a cost-model change. It is the
only signal that shows what an effort level actually buys.
"""
import importlib.util
import os

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook",
    os.path.join(os.path.dirname(__file__), "..", "langfuse-hook.py"),
)
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)


def _assistant(msg_id, output_tokens, thinking_tokens, ts="2026-08-26T10:00:01Z"):
    usage = {
        "input_tokens": 10,
        "output_tokens": output_tokens,
        "cache_read_input_tokens": 0,
        "cache_creation_input_tokens": 0,
    }
    if thinking_tokens is not None:
        usage["output_tokens_details"] = {"thinking_tokens": thinking_tokens}
    return {
        "type": "assistant",
        "timestamp": ts,
        "message": {
            "id": msg_id,
            "model": "claude-opus-5",
            "usage": usage,
            "content": [{"type": "text", "text": "hi"}],
        },
    }


def _user(text="do a thing", ts="2026-08-26T10:00:00Z"):
    return {
        "type": "user",
        "timestamp": ts,
        "message": {"role": "user", "content": text},
    }


class TestBuildTurnsThinkingTokens:
    def test_turn_captures_thinking_tokens(self):
        turns = hook.build_turns([_user(), _assistant("m1", 500, 120)])

        assert turns[0]["thinking_tokens"] == 120

    def test_thinking_tokens_summed_across_api_calls_in_a_turn(self):
        entries = [
            _user(),
            _assistant("m1", 500, 120, ts="2026-08-26T10:00:01Z"),
            _assistant("m2", 300, 80, ts="2026-08-26T10:00:02Z"),
        ]

        turns = hook.build_turns(entries)

        assert turns[0]["thinking_tokens"] == 200

    def test_absent_details_yields_zero_not_crash(self):
        turns = hook.build_turns([_user(), _assistant("m1", 500, None)])

        assert turns[0]["thinking_tokens"] == 0

    def test_zero_thinking_tokens_recorded_as_zero(self):
        turns = hook.build_turns([_user(), _assistant("m1", 500, 0)])

        assert turns[0]["thinking_tokens"] == 0

    def test_malformed_details_does_not_crash(self):
        entry = _assistant("m1", 500, None)
        entry["message"]["usage"]["output_tokens_details"] = "not-a-dict"

        turns = hook.build_turns([_user(), entry])

        assert turns[0]["thinking_tokens"] == 0

    def test_streaming_duplicates_not_double_counted(self):
        """Streaming entries share a message.id and carry identical usage since
        CC 2.1.97, so a turn must count that usage once, not once per entry."""
        entries = [
            _user(),
            _assistant("m1", 500, 120, ts="2026-08-26T10:00:01Z"),
            _assistant("m1", 500, 120, ts="2026-08-26T10:00:02Z"),
        ]

        turns = hook.build_turns(entries)

        assert turns[0]["thinking_tokens"] == 120


class TestThinkingSummary:
    def test_summary_totals_and_share_of_output(self):
        turns = [
            {"thinking_tokens": 120, "usage": {"output": 500}},
            {"thinking_tokens": 80, "usage": {"output": 300}},
        ]

        summary = hook.build_thinking_summary(turns)

        assert summary["total_thinking_tokens"] == 200
        assert summary["total_output_tokens"] == 800
        assert summary["share_of_output"] == 0.25

    def test_summary_counts_turns_with_thinking(self):
        turns = [
            {"thinking_tokens": 120, "usage": {"output": 500}},
            {"thinking_tokens": 0, "usage": {"output": 300}},
        ]

        summary = hook.build_thinking_summary(turns)

        assert summary["turns_with_thinking"] == 1

    def test_summary_is_none_when_no_thinking_anywhere(self):
        turns = [
            {"thinking_tokens": 0, "usage": {"output": 300}},
            {"thinking_tokens": 0, "usage": {"output": 200}},
        ]

        assert hook.build_thinking_summary(turns) is None

    def test_summary_is_none_for_empty_turns(self):
        assert hook.build_thinking_summary([]) is None

    def test_zero_output_does_not_divide_by_zero(self):
        turns = [{"thinking_tokens": 50, "usage": {"output": 0}}]

        summary = hook.build_thinking_summary(turns)

        assert summary["share_of_output"] is None
