# tests/test_prompt_provenance.py
"""Tests for prompt-provenance capture (CC 2.1.2xx).

`user` entries in the transcript are not all user prompts: most are tool
results, some are background task notifications, subagent reports injected
back into the parent, or system-reminder companions. `origin.kind`,
`promptSource` and `turnCompanion` are what separate a real human prompt from
the rest — without them a turn count is inflated by machine-authored entries.

Prompt *text* is never captured here: `origin.kind == "peer"` carries the full
subagent report in `body`, the same PII reasoning that keeps `content` out of
the queue-operation rollup.
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


def _user_entry(**kwargs):
    entry = {"type": "user", "message": {"role": "user", "content": "hello"}}
    entry.update(kwargs)
    return entry


class TestExtractPromptProvenance:
    def test_counts_origin_kinds(self, tmp_path):
        path = _write(tmp_path, [
            _user_entry(origin={"kind": "human"}),
            _user_entry(origin={"kind": "human"}),
            _user_entry(origin={"kind": "task-notification"}),
        ])

        result = hook.extract_prompt_provenance(path)

        assert result["by_origin"] == {"human": 2, "task-notification": 1}

    def test_counts_prompt_sources(self, tmp_path):
        path = _write(tmp_path, [
            _user_entry(origin={"kind": "human"}, promptSource="typed"),
            _user_entry(origin={"kind": "human"}, promptSource="queued"),
            _user_entry(origin={"kind": "human"}, promptSource="typed"),
        ])

        result = hook.extract_prompt_provenance(path)

        assert result["by_source"] == {"typed": 2, "queued": 1}

    def test_human_prompts_counted_separately(self, tmp_path):
        path = _write(tmp_path, [
            _user_entry(origin={"kind": "human"}),
            _user_entry(origin={"kind": "task-notification"}),
            _user_entry(origin={"kind": "peer", "from": "general-purpose"}),
        ])

        result = hook.extract_prompt_provenance(path)

        assert result["human_prompts"] == 1

    def test_counts_turn_companions(self, tmp_path):
        path = _write(tmp_path, [
            _user_entry(origin={"kind": "human"}),
            _user_entry(turnCompanion=True),
            _user_entry(turnCompanion=True),
        ])

        result = hook.extract_prompt_provenance(path)

        assert result["companion_messages"] == 2

    def test_tool_result_entries_without_origin_are_ignored(self, tmp_path):
        """The vast majority of `user` entries are tool results with no origin."""
        tool_result = _user_entry(message={"role": "user", "content": [{
            "type": "tool_result", "tool_use_id": "toolu_1", "content": "ok",
        }]})
        path = _write(tmp_path, [
            _user_entry(origin={"kind": "human"}),
            tool_result,
            tool_result,
        ])

        result = hook.extract_prompt_provenance(path)

        assert result["by_origin"] == {"human": 1}
        assert result["prompt_entries"] == 1

    def test_peer_origin_records_sender_agent_type_not_body(self, tmp_path):
        """A peer report's `body` is subagent output — count it, never store it."""
        path = _write(tmp_path, [
            _user_entry(origin={
                "kind": "peer",
                "from": "general-purpose",
                "senderTaskId": "a2800460c8115856a",
                "body": "SECRET REPORT CONTENTS",
            }),
        ])

        result = hook.extract_prompt_provenance(path)

        assert result["by_peer_agent"] == {"general-purpose": 1}
        assert "SECRET REPORT CONTENTS" not in json.dumps(result)

    def test_returns_empty_dict_when_nothing_is_a_prompt(self, tmp_path):
        """Only tool results: no prompts to classify, tagged or untagged."""
        tool_result = _user_entry(message={"role": "user", "content": [{
            "type": "tool_result", "tool_use_id": "toolu_1", "content": "ok",
        }]})
        path = _write(tmp_path, [tool_result, tool_result])

        assert hook.extract_prompt_provenance(path) == {}

    def test_untagged_prompts_are_classified_not_dropped(self, tmp_path):
        """Entries with no origin/promptSource fall back to shape-based rules.

        See test_prompt_provenance_fallback.py for the full rule set; this
        pins the interaction with the tagged path.
        """
        path = _write(tmp_path, [_user_entry(), _user_entry()])

        result = hook.extract_prompt_provenance(path)

        assert result["human_prompts"] == 2
        assert result["by_source"] == {"untagged": 2}

    def test_assistant_entries_are_ignored(self, tmp_path):
        path = _write(tmp_path, [
            {"type": "assistant", "origin": {"kind": "human"}},
            _user_entry(origin={"kind": "human"}),
        ])

        result = hook.extract_prompt_provenance(path)

        assert result["by_origin"] == {"human": 1}

    def test_malformed_origin_does_not_crash(self, tmp_path):
        path = _write(tmp_path, [
            _user_entry(origin="not-a-dict"),
            _user_entry(origin={"kind": "human"}),
        ])

        result = hook.extract_prompt_provenance(path)

        # The malformed one falls through to the untagged fallback rather than
        # raising: it is still a `user` entry carrying prompt text.
        assert result["by_origin"] == {"human": 2}
        assert result["by_source"] == {"untagged": 1}

    def test_missing_file_returns_empty(self, tmp_path):
        assert hook.extract_prompt_provenance(str(tmp_path / "nope.jsonl")) == {}
