# tests/test_tool_denials.py
"""Tests for permission-denial capture (CC 2.1.2xx).

A `user` entry carrying `toolDenialKind` is a tool call that was *refused*,
not one that ran and failed: the user rejected it, or auto-mode's classifier
blocked it or was unavailable. Claude Code marks the tool_result block
`is_error: true` either way, so without this distinction every denial is
counted as a broken tool by `tool_error_rate` — conflating "the command is
wrong" with "the call was never allowed to run".
"""
import importlib.util
import json
import os

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook",
    os.path.join(os.path.dirname(__file__), "..", "langfuse-hook.py"),
)
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)


def _write(tmp_path, entries):
    p = tmp_path / "session.jsonl"
    p.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
    return str(p)


def _denial(kind, tool_use_id="toolu_1", text="Permission denied"):
    return {
        "type": "user",
        "toolDenialKind": kind,
        "message": {
            "role": "user",
            "content": [{
                "type": "tool_result",
                "tool_use_id": tool_use_id,
                "content": text,
                "is_error": True,
            }],
        },
    }


class TestExtractToolResultsDenialMarking:
    def test_denied_result_gets_denied_prefix_not_error(self):
        content = [{
            "type": "tool_result", "tool_use_id": "toolu_1",
            "content": "blocked by classifier", "is_error": True,
        }]
        out = hook.extract_tool_results(content, denial_kind="automode-blocked")
        assert out["toolu_1"].startswith("[DENIED:automode-blocked]")
        assert not out["toolu_1"].startswith("[ERROR]")

    def test_genuine_error_still_gets_error_prefix(self):
        content = [{
            "type": "tool_result", "tool_use_id": "toolu_1",
            "content": "command not found", "is_error": True,
        }]
        out = hook.extract_tool_results(content)
        assert out["toolu_1"].startswith("[ERROR]")

    def test_denial_kind_ignored_when_result_is_not_an_error(self):
        content = [{
            "type": "tool_result", "tool_use_id": "toolu_1",
            "content": "ok", "is_error": False,
        }]
        out = hook.extract_tool_results(content, denial_kind="user-rejected")
        assert out["toolu_1"] == "ok"


class TestToolErrorRateExcludesDenials:
    def test_denials_excluded_from_numerator_and_denominator(self):
        turns = [{"tool_calls": [
            {"output": "fine"},
            {"output": "[ERROR] real failure"},
            {"output": "[DENIED:user-rejected] nope"},
            {"output": "[DENIED:automode-blocked] nope"},
        ]}]
        # 1 error out of 2 executed calls; the 2 denials are not outcomes.
        assert hook.calculate_tool_error_rate(turns) == 0.5

    def test_returns_none_when_every_call_was_denied(self):
        turns = [{"tool_calls": [{"output": "[DENIED:user-rejected] nope"}]}]
        assert hook.calculate_tool_error_rate(turns) is None


class TestCalculateToolDenialRate:
    def test_fraction_of_attempted_calls_that_were_denied(self):
        turns = [{"tool_calls": [
            {"output": "fine"},
            {"output": "[ERROR] real failure"},
            {"output": "[DENIED:user-rejected] nope"},
        ]}]
        assert hook.calculate_tool_denial_rate(turns) == round(1 / 3, 4)

    def test_returns_none_when_no_tool_calls(self):
        assert hook.calculate_tool_denial_rate([{"tool_calls": []}]) is None

    def test_zero_when_calls_ran_but_none_denied(self):
        turns = [{"tool_calls": [{"output": "fine"}]}]
        assert hook.calculate_tool_denial_rate(turns) == 0.0


class TestDenialRateScoreEmission:
    def _scores(self, turns):
        events = hook.build_hook_score_events(
            "trace-1", "sess-1", "hi", turns, 0.0,
        )
        return {e["body"]["name"]: e["body"]["value"] for e in events}

    def test_emits_denial_rate_alongside_error_rate(self):
        turns = [{"tool_calls": [
            {"output": "fine"},
            {"output": "[ERROR] real failure"},
            {"output": "[DENIED:user-rejected] nope"},
        ]}]
        scores = self._scores(turns)
        assert scores["tool_denial_rate"] == round(1 / 3, 4)
        # The denial is out of the error rate: 1 error over 2 executed calls.
        assert scores["tool_error_rate"] == 0.5

    def test_omits_denial_rate_when_no_tool_calls(self):
        assert "tool_denial_rate" not in self._scores([{"tool_calls": []}])


class TestExtractToolDenials:
    def test_counts_denials_by_kind(self, tmp_path):
        path = _write(tmp_path, [
            _denial("user-rejected"),
            _denial("user-rejected"),
            _denial("automode-blocked"),
            {"type": "user", "message": {"role": "user", "content": "hi"}},
        ])
        assert hook.extract_tool_denials(path) == {
            "denial_count": 3,
            "by_kind": {"user-rejected": 2, "automode-blocked": 1},
        }

    def test_returns_empty_when_no_denials(self, tmp_path):
        path = _write(tmp_path, [
            {"type": "user", "message": {"role": "user", "content": "hi"}},
        ])
        assert hook.extract_tool_denials(path) == {}

    def test_missing_transcript_returns_empty(self):
        assert hook.extract_tool_denials("/nonexistent/session.jsonl") == {}
