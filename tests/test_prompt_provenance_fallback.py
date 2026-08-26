# tests/test_prompt_provenance_fallback.py
"""Tests for classifying `user` entries that carry no provenance fields.

`origin.kind` / `promptSource` are present on only a minority of prompt
entries — a census of 159 local transcripts found them on 159 of 599
non-tool-result `user` entries. The other 440 are not unclassifiable, they
are just untagged, and they split cleanly:

  * `isMeta: true` — injected context (system reminders, hook output). Not a
    prompt at all.
  * `[Request interrupted by user]` — an interruption marker. Not a prompt.
  * `<local-command-stdout>` — output of a local command. Not a prompt.
  * `<command-name>` — a slash-command invocation. A *human* prompt, and the
    single largest untagged group (75 of 440); counting it as nothing is what
    made `human_prompts` undercount.
  * a sidechain root (`isSidechain: true` with no `parentUuid`) — the task
    prompt the parent wrote to dispatch a subagent. Machine-authored, and all
    66 such entries in the census matched this shape exactly.

Text is inspected only to classify; it is never stored, same rule as the
tagged path.
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


def _user(content, **kwargs):
    entry = {"type": "user", "message": {"role": "user", "content": content}}
    entry.update(kwargs)
    return entry


class TestUntaggedHumanPrompts:
    def test_slash_command_counts_as_a_human_prompt(self, tmp_path):
        path = _write(tmp_path, [_user(
            "<command-message>kb-query</command-message>"
            "<command-name>/kb-query</command-name>"
        )])
        out = hook.extract_prompt_provenance(path)
        assert out["human_prompts"] == 1
        assert out["by_origin"]["human"] == 1
        assert out["by_source"]["slash_command"] == 1

    def test_plain_untagged_text_counts_as_a_human_prompt(self, tmp_path):
        path = _write(tmp_path, [_user("push the branch and open a PR")])
        out = hook.extract_prompt_provenance(path)
        assert out["human_prompts"] == 1
        assert out["by_source"]["untagged"] == 1

    def test_list_content_is_classified_like_string_content(self, tmp_path):
        path = _write(tmp_path, [_user(
            [{"type": "text", "text": "<command-name>/kb-lint</command-name>"}]
        )])
        out = hook.extract_prompt_provenance(path)
        assert out["by_source"]["slash_command"] == 1


class TestNonPromptEntriesExcluded:
    def test_meta_entry_is_not_a_prompt(self, tmp_path):
        path = _write(tmp_path, [_user("<system-reminder>note</system-reminder>",
                                       isMeta=True)])
        out = hook.extract_prompt_provenance(path)
        assert out["meta_entries"] == 1
        assert out["prompt_entries"] == 0
        assert out["human_prompts"] == 0

    def test_interruption_marker_is_not_a_prompt(self, tmp_path):
        path = _write(tmp_path, [_user(
            [{"type": "text", "text": "[Request interrupted by user]"}]
        )])
        out = hook.extract_prompt_provenance(path)
        assert out["interruptions"] == 1
        assert out["prompt_entries"] == 0

    def test_local_command_output_is_not_a_prompt(self, tmp_path):
        # Nothing else in the session, so it rolls up to nothing at all.
        path = _write(tmp_path, [_user(
            "<local-command-stdout>branch is clean</local-command-stdout>"
        )])
        assert hook.extract_prompt_provenance(path) == {}

    def test_local_command_output_does_not_inflate_a_real_session(self, tmp_path):
        path = _write(tmp_path, [
            _user("do the thing"),
            _user("<local-command-stdout>branch is clean</local-command-stdout>"),
        ])
        out = hook.extract_prompt_provenance(path)
        assert out["prompt_entries"] == 1
        assert out["human_prompts"] == 1

    def test_empty_content_is_not_a_prompt(self, tmp_path):
        path = _write(tmp_path, [_user("   ")])
        assert hook.extract_prompt_provenance(path) == {}

    def test_tool_results_are_still_ignored_entirely(self, tmp_path):
        path = _write(tmp_path, [_user([{
            "type": "tool_result", "tool_use_id": "toolu_1", "content": "ok",
        }])])
        assert hook.extract_prompt_provenance(path) == {}


class TestSubagentDispatch:
    def test_sidechain_root_is_an_agent_dispatch_not_a_human_prompt(self, tmp_path):
        path = _write(tmp_path, [_user(
            "You are doing price research ONLY — do not write any files.",
            isSidechain=True, parentUuid=None,
        )])
        out = hook.extract_prompt_provenance(path)
        assert out["by_origin"]["agent-dispatch"] == 1
        assert out["human_prompts"] == 0
        assert out["prompt_entries"] == 1

    def test_sidechain_entry_with_a_parent_is_an_ordinary_prompt(self, tmp_path):
        path = _write(tmp_path, [_user(
            "continue", isSidechain=True, parentUuid="abc-123",
        )])
        out = hook.extract_prompt_provenance(path)
        assert "agent-dispatch" not in out["by_origin"]
        assert out["human_prompts"] == 1


class TestTaggedEntriesUnchanged:
    def test_origin_kind_still_wins_over_the_fallback(self, tmp_path):
        path = _write(tmp_path, [_user(
            "<command-name>/kb-query</command-name>",
            origin={"kind": "task-notification"},
        )])
        out = hook.extract_prompt_provenance(path)
        assert out["by_origin"] == {"task-notification": 1}
        assert out["human_prompts"] == 0
        assert "slash_command" not in out["by_source"]

    def test_empty_transcript_still_returns_empty(self, tmp_path):
        path = _write(tmp_path, [{"type": "assistant", "message": {"role": "assistant"}}])
        assert hook.extract_prompt_provenance(path) == {}


class TestCombinedSessionShape:
    def test_mixed_session_totals(self, tmp_path):
        path = _write(tmp_path, [
            _user("hello there"),                                   # untagged human
            _user("<command-name>/kb-lint</command-name>"),         # slash human
            _user("reminder", isMeta=True),                         # meta
            _user([{"type": "text", "text": "[Request interrupted by user]"}]),
            _user("dispatch", isSidechain=True),                    # agent-dispatch
            _user("tagged", origin={"kind": "human"}, promptSource="typed"),
        ])
        out = hook.extract_prompt_provenance(path)
        assert out["human_prompts"] == 3          # untagged + slash + tagged
        assert out["by_origin"]["agent-dispatch"] == 1
        assert out["meta_entries"] == 1
        assert out["interruptions"] == 1
        assert out["prompt_entries"] == 4         # 3 human + 1 dispatch
