# tests/test_turn_merging.py
"""Consecutive user entries before any assistant reply form one turn.

Claude Code writes several user entries for one thing the user did: a slash
command is a `<command-name>` entry followed by an `isMeta` entry carrying the
expanded skill text, and a prompt with an image is the typed text followed by
an `isMeta` "[Image: source: ...]" entry. build_turns started a new turn on
every user text entry, so each of these became an empty $0 turn followed by a
turn whose input was the skill body or the image placeholder — about 108
empty generations across the last 150 local sessions.
"""
import importlib.util
import json
import os

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook", os.path.join(os.path.dirname(__file__), "..", "langfuse-hook.py"))
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)


def _user(text, ts, **extra):
    return {"type": "user", "timestamp": f"2026-09-20T10:{ts}+00:00",
            "message": {"role": "user", "content": text}, **extra}


def _reply(mid, ts, text="done"):
    return {"type": "assistant", "timestamp": f"2026-09-20T10:{ts}+00:00",
            "message": {"id": mid, "role": "assistant", "model": "claude-sonnet-5",
                        "stop_reason": "end_turn",
                        "content": [{"type": "text", "text": text}],
                        "usage": {"input_tokens": 10, "output_tokens": 5,
                                  "cache_read_input_tokens": 0,
                                  "cache_creation_input_tokens": 0}}}


SLASH = ("<command-message>superpowers:brainstorming</command-message>\n"
         "<command-name>/superpowers:brainstorming</command-name>")


class TestBuildTurns:
    def test_slash_command_and_its_expansion_are_one_turn(self):
        turns = hook.build_turns([
            _user(SLASH, "00:00"),
            _user("Base directory for this skill: ...", "00:00", isMeta=True),
            _reply("m1", "00:04"),
        ])
        assert len(turns) == 1
        assert turns[0]["api_call_ids"] == ["m1"]
        assert "brainstorming" in turns[0]["user_input"]
        assert "Base directory" in turns[0]["user_input"]

    def test_prompt_with_image_is_one_turn_named_by_the_text(self):
        turns = hook.build_turns([
            _user("why is this red?", "00:00"),
            _user("[Image: source: /tmp/img/1.png]", "00:00", isMeta=True),
            _reply("m1", "00:03"),
        ])
        assert len(turns) == 1
        assert turns[0]["display_prompt"] == "why is this red?"

    def test_turn_starts_at_the_last_input_before_the_reply(self):
        """A /model switch at 10:00 and a prompt at 10:05 are one turn, but its
        time-to-first-token is measured from the prompt, not five minutes early."""
        turns = hook.build_turns([
            _user("<command-name>/model</command-name>", "00:00"),
            _user("<local-command-stdout>Set model</local-command-stdout>", "00:00"),
            _user("fix the bug", "05:00"),
            _reply("m1", "05:02"),
        ])
        assert len(turns) == 1
        assert turns[0]["start_time"] == "2026-09-20T10:05:00+00:00"
        assert turns[0]["display_prompt"] == "fix the bug"

    def test_a_prompt_after_a_reply_still_starts_a_new_turn(self):
        turns = hook.build_turns([
            _user("first", "00:00"), _reply("m1", "00:02"),
            _user("second", "01:00"), _reply("m2", "01:02"),
        ])
        assert [t["user_input"] for t in turns] == ["first", "second"]

    def test_interrupt_marker_is_not_the_display_prompt(self):
        turns = hook.build_turns([
            _user("first", "00:00"), _reply("m1", "00:02"),
            _user("[Request interrupted by user]", "00:03"),
            _user("try again differently", "00:10"), _reply("m2", "00:12"),
        ])
        assert len(turns) == 2
        assert turns[1]["display_prompt"] == "try again differently"


class TestProcessSessionNaming:
    def test_generation_and_trace_are_named_by_the_human_prompt(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))
        sent = []
        monkeypatch.setattr(hook, "send_to_langfuse", lambda b: sent.append(b) or True)
        path = tmp_path / "s.jsonl"
        path.write_text("\n".join(json.dumps(e) for e in [
            _user("<command-name>/model</command-name>", "00:00"),
            _user("look at this", "00:10"),
            _user("[Image: source: /tmp/img/1.png]", "00:10", isMeta=True),
            _reply("m1", "00:12"),
        ]) + "\n")
        hook.process_session("s", str(path), "/w/p")

        gens = [e["body"] for e in sent[0] if e["type"] == "generation-create"]
        trace = next(e["body"] for e in sent[0] if e["type"] == "trace-create")
        assert [g["name"] for g in gens] == ["Turn 1: look at this"]
        assert trace["name"] == "look at this"
