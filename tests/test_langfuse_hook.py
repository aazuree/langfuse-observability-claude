# tests/test_langfuse_hook.py
"""Comprehensive tests for langfuse-hook.py core functions."""
import importlib.util
import json
import os
import tempfile
import unittest.mock as mock
from pathlib import Path
from datetime import datetime, timezone
import pytest

# Import the hook module (hyphenated filename requires importlib)
_spec = importlib.util.spec_from_file_location(
    "langfuse_hook",
    os.path.join(os.path.dirname(__file__), "..", "langfuse-hook.py"),
)
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)


# ---------------------------------------------------------------------------
# sanitize_id
# ---------------------------------------------------------------------------

class TestSanitizeId:
    def test_valid_alphanumeric(self):
        assert hook.sanitize_id("abc123") == "abc123"

    def test_valid_with_hyphens_underscores(self):
        assert hook.sanitize_id("session-abc_123") == "session-abc_123"

    def test_path_traversal_attack(self):
        result = hook.sanitize_id("../../etc/passwd")
        assert "/" not in result
        assert ".." not in result
        assert len(result) == 32  # sha256 hex prefix

    def test_special_characters_hashed(self):
        result = hook.sanitize_id("session id with spaces")
        assert " " not in result
        assert len(result) == 32

    def test_deterministic(self):
        assert hook.sanitize_id("../bad") == hook.sanitize_id("../bad")

    def test_empty_string_hashed(self):
        result = hook.sanitize_id("")
        # empty string doesn't match SAFE_ID_RE
        assert len(result) == 32


# ---------------------------------------------------------------------------
# redact_secrets
# ---------------------------------------------------------------------------

class TestRedactSecrets:
    def test_api_key_assignment(self):
        text = 'api_key = "sk-abc123456789012345"'
        result = hook.redact_secrets(text)
        assert "sk-abc123456789012345" not in result
        assert "[REDACTED]" in result

    def test_bearer_token(self):
        text = "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.abcdefgh"
        result = hook.redact_secrets(text)
        assert "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" not in result

    def test_github_token(self):
        text = "token: ghp_1234567890abcdefghijklmnopqrstuv"
        result = hook.redact_secrets(text)
        assert "ghp_1234567890abcdefghijklmnopqrstuv" not in result

    def test_aws_access_key(self):
        text = "AKIAIOSFODNN7EXAMPLE1234"
        result = hook.redact_secrets(text)
        assert "AKIAIOSFODNN7EXAMPLE1234" not in result

    def test_private_key_block(self):
        # Construct key markers dynamically to avoid pre-commit hook detection
        begin = "-" * 5 + "BEGIN RSA " + "PRIVATE KEY" + "-" * 5
        end = "-" * 5 + "END RSA " + "PRIVATE KEY" + "-" * 5
        key_body = "FAKEKEYDATANOTREAL"
        text = f"{begin}\n{key_body}\n{end}"
        result = hook.redact_secrets(text)
        assert key_body not in result

    def test_no_secrets_unchanged(self):
        text = "This is a normal message with no secrets"
        assert hook.redact_secrets(text) == text

    def test_password_in_config(self):
        text = 'password: "mysecretpassword123"'
        result = hook.redact_secrets(text)
        assert "mysecretpassword123" not in result

    def test_slack_token(self):
        # xoxb with sufficient length after prefix triggers the pattern
        text = "xoxb-1234567890abcdef1234567890abcdef"
        result = hook.redact_secrets(text)
        assert "xoxb-1234567890abcdef1234567890abcdef" not in result


# ---------------------------------------------------------------------------
# make_auth_header
# ---------------------------------------------------------------------------

class TestMakeAuthHeader:
    def test_basic_auth_format(self, monkeypatch):
        monkeypatch.setattr(hook, "LANGFUSE_PUBLIC_KEY", "pk-test")
        monkeypatch.setattr(hook, "LANGFUSE_SECRET_KEY", "sk-test")
        result = hook.make_auth_header()
        assert result.startswith("Basic ")
        import base64
        decoded = base64.b64decode(result.split(" ")[1]).decode()
        assert decoded == "pk-test:sk-test"


# ---------------------------------------------------------------------------
# truncate
# ---------------------------------------------------------------------------

class TestTruncate:
    def test_short_string_unchanged(self):
        assert hook.truncate("hello", 10) == "hello"

    def test_exact_limit_unchanged(self):
        assert hook.truncate("hello", 5) == "hello"

    def test_over_limit_truncated(self):
        assert hook.truncate("hello world", 5) == "hello..."

    def test_empty_string(self):
        assert hook.truncate("", 10) == ""


# ---------------------------------------------------------------------------
# extract_text_blocks
# ---------------------------------------------------------------------------

class TestExtractTextBlocks:
    def test_string_input(self):
        assert hook.extract_text_blocks("hello") == "hello"

    def test_list_of_text_blocks(self):
        content = [
            {"type": "text", "text": "Hello"},
            {"type": "text", "text": "World"},
        ]
        assert hook.extract_text_blocks(content) == "Hello\nWorld"

    def test_mixed_content_ignores_non_text(self):
        content = [
            {"type": "text", "text": "Hello"},
            {"type": "tool_use", "id": "t1", "name": "Bash", "input": {}},
        ]
        assert hook.extract_text_blocks(content) == "Hello"

    def test_list_of_strings(self):
        content = ["hello", "world"]
        assert hook.extract_text_blocks(content) == "hello\nworld"

    def test_empty_list(self):
        assert hook.extract_text_blocks([]) == ""

    def test_non_string_non_list(self):
        assert hook.extract_text_blocks(123) == "123"


# ---------------------------------------------------------------------------
# extract_tool_uses
# ---------------------------------------------------------------------------

class TestExtractToolUses:
    def test_extracts_tool_use_blocks(self):
        content = [
            {"type": "text", "text": "I'll run this"},
            {"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "ls"}},
        ]
        result = hook.extract_tool_uses(content)
        assert len(result) == 1
        assert result[0]["name"] == "Bash"

    def test_string_input_returns_empty(self):
        assert hook.extract_tool_uses("not a list") == []

    def test_no_tool_uses(self):
        content = [{"type": "text", "text": "hello"}]
        assert hook.extract_tool_uses(content) == []

    def test_multiple_tool_uses(self):
        content = [
            {"type": "tool_use", "id": "t1", "name": "Read", "input": {}},
            {"type": "tool_use", "id": "t2", "name": "Write", "input": {}},
        ]
        result = hook.extract_tool_uses(content)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# extract_thinking_blocks / format_thinking_output
# ---------------------------------------------------------------------------

class TestExtractThinkingBlocks:
    def test_plain_thinking_block(self):
        content = [
            {"type": "thinking", "thinking": "Let me reason about this..."},
            {"type": "text", "text": "The answer is 4."},
        ]
        assert hook.extract_thinking_blocks(content) == "Let me reason about this..."

    def test_multiple_thinking_blocks_joined(self):
        content = [
            {"type": "thinking", "thinking": "Step 1"},
            {"type": "thinking", "thinking": "Step 2"},
            {"type": "text", "text": "Done"},
        ]
        assert hook.extract_thinking_blocks(content) == "Step 1\nStep 2"

    def test_redacted_thinking_shows_marker(self):
        content = [
            {"type": "redacted_thinking", "data": "opaque-encrypted-payload"},
            {"type": "text", "text": "Response"},
        ]
        result = hook.extract_thinking_blocks(content)
        assert "[redacted thinking block]" in result
        # Never leak the encrypted payload
        assert "opaque-encrypted-payload" not in result

    def test_mixed_thinking_and_redacted(self):
        content = [
            {"type": "thinking", "thinking": "Plain cot"},
            {"type": "redacted_thinking", "data": "xxx"},
            {"type": "thinking", "thinking": "More cot"},
        ]
        result = hook.extract_thinking_blocks(content)
        assert result == "Plain cot\n[redacted thinking block]\nMore cot"

    def test_no_thinking_returns_empty(self):
        content = [{"type": "text", "text": "just a response"}]
        assert hook.extract_thinking_blocks(content) == ""

    def test_string_input_returns_empty(self):
        assert hook.extract_thinking_blocks("plain string") == ""

    def test_empty_thinking_skipped(self):
        content = [{"type": "thinking", "thinking": ""}]
        assert hook.extract_thinking_blocks(content) == ""


class TestFormatThinkingOutput:
    def test_no_thinking_returns_output_unchanged(self):
        assert hook.format_thinking_output("", "response") == "response"

    def test_prepends_thinking_to_output(self):
        result = hook.format_thinking_output("reasoning", "answer")
        assert result == "<thinking>\nreasoning\n</thinking>\n\nanswer"

    def test_thinking_only_no_output(self):
        # Turns can be tool-use-only with no final text; still surface thinking
        result = hook.format_thinking_output("reasoning", "")
        assert result == "<thinking>\nreasoning\n</thinking>"

    def test_both_empty(self):
        assert hook.format_thinking_output("", "") == ""


class TestBuildTurnsWithThinking:
    """Integration: verify thinking flows through build_turns to turn['thinking']."""

    def test_single_turn_captures_thinking(self):
        entries = [
            {"type": "user", "timestamp": "2026-04-17T10:00:00+00:00", "uuid": "u1",
             "message": {"role": "user", "content": "Solve this"}},
            {"type": "assistant", "timestamp": "2026-04-17T10:00:02+00:00",
             "message": {
                 "id": "msg-1", "role": "assistant", "model": "claude-opus-4-7",
                 "content": [
                     {"type": "thinking", "thinking": "I should think step by step"},
                     {"type": "text", "text": "The answer is 42."},
                 ],
                 "usage": {"input_tokens": 10, "output_tokens": 5,
                           "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
             }},
        ]
        turns = hook.build_turns(entries)
        assert len(turns) == 1
        assert turns[0]["thinking"] == "I should think step by step"
        assert turns[0]["assistant_output"] == "The answer is 42."

    def test_multi_step_turn_concatenates_thinking_across_api_calls(self):
        """A turn that does think -> tool -> think -> reply should preserve both thinking segments."""
        entries = [
            {"type": "user", "timestamp": "2026-04-17T10:00:00+00:00", "uuid": "u1",
             "message": {"role": "user", "content": "Do a thing"}},
            # First assistant API call: think + tool_use
            {"type": "assistant", "timestamp": "2026-04-17T10:00:01+00:00",
             "message": {
                 "id": "msg-1", "role": "assistant", "model": "claude-opus-4-7",
                 "content": [
                     {"type": "thinking", "thinking": "First I need to read the file"},
                     {"type": "tool_use", "id": "t1", "name": "Read", "input": {"path": "x"}},
                 ],
                 "usage": {"input_tokens": 10, "output_tokens": 5,
                           "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
             }},
            # Tool result (user message with tool_result, no user text)
            {"type": "user", "timestamp": "2026-04-17T10:00:02+00:00", "uuid": "u2",
             "message": {"role": "user", "content": [
                 {"type": "tool_result", "tool_use_id": "t1", "content": "file contents"},
             ]}},
            # Second assistant API call: think + final text
            {"type": "assistant", "timestamp": "2026-04-17T10:00:03+00:00",
             "message": {
                 "id": "msg-2", "role": "assistant", "model": "claude-opus-4-7",
                 "content": [
                     {"type": "thinking", "thinking": "Now I can answer"},
                     {"type": "text", "text": "Here is the result."},
                 ],
                 "usage": {"input_tokens": 15, "output_tokens": 8,
                           "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
             }},
        ]
        turns = hook.build_turns(entries)
        assert len(turns) == 1
        # Both thinking segments preserved in chronological order
        assert "First I need to read the file" in turns[0]["thinking"]
        assert "Now I can answer" in turns[0]["thinking"]
        assert turns[0]["thinking"].index("First") < turns[0]["thinking"].index("Now")
        assert turns[0]["assistant_output"] == "Here is the result."

    def test_streaming_dedupe_keeps_last_thinking_per_message_id(self):
        """Streaming updates share a message_id; only the final (complete) thinking should be kept."""
        entries = [
            {"type": "user", "timestamp": "2026-04-17T10:00:00+00:00", "uuid": "u1",
             "message": {"role": "user", "content": "Go"}},
            # Partial streaming update
            {"type": "assistant", "timestamp": "2026-04-17T10:00:01+00:00",
             "message": {
                 "id": "msg-1", "role": "assistant", "model": "claude-opus-4-7",
                 "content": [{"type": "thinking", "thinking": "Partial..."}],
                 "usage": {"input_tokens": 10, "output_tokens": 1,
                           "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
             }},
            # Final streaming update (same msg_id, complete thinking)
            {"type": "assistant", "timestamp": "2026-04-17T10:00:02+00:00",
             "message": {
                 "id": "msg-1", "role": "assistant", "model": "claude-opus-4-7",
                 "content": [
                     {"type": "thinking", "thinking": "Partial but complete now"},
                     {"type": "text", "text": "Done."},
                 ],
                 "usage": {"input_tokens": 10, "output_tokens": 5,
                           "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
             }},
        ]
        turns = hook.build_turns(entries)
        assert turns[0]["thinking"] == "Partial but complete now"

    def test_no_thinking_produces_empty_field(self):
        entries = [
            {"type": "user", "timestamp": "2026-04-17T10:00:00+00:00", "uuid": "u1",
             "message": {"role": "user", "content": "Hi"}},
            {"type": "assistant", "timestamp": "2026-04-17T10:00:01+00:00",
             "message": {
                 "id": "msg-1", "role": "assistant", "model": "claude-opus-4-7",
                 "content": [{"type": "text", "text": "Hello."}],
                 "usage": {"input_tokens": 5, "output_tokens": 2,
                           "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
             }},
        ]
        turns = hook.build_turns(entries)
        assert turns[0]["thinking"] == ""


# ---------------------------------------------------------------------------
# extract_tool_results
# ---------------------------------------------------------------------------

class TestExtractToolResults:
    def test_basic_tool_result(self):
        content = [
            {
                "type": "tool_result",
                "tool_use_id": "t1",
                "content": [{"type": "text", "text": "file contents"}],
            }
        ]
        result = hook.extract_tool_results(content)
        assert result["t1"] == "file contents"

    def test_string_content(self):
        content = [
            {"type": "tool_result", "tool_use_id": "t1", "content": "output text"}
        ]
        result = hook.extract_tool_results(content)
        assert result["t1"] == "output text"

    def test_error_result_prefixed(self):
        content = [
            {
                "type": "tool_result",
                "tool_use_id": "t1",
                "content": "command not found",
                "is_error": True,
            }
        ]
        result = hook.extract_tool_results(content)
        assert result["t1"] == "[ERROR] command not found"

    def test_non_list_returns_empty(self):
        assert hook.extract_tool_results("not a list") == {}

    def test_multiple_results(self):
        content = [
            {"type": "tool_result", "tool_use_id": "t1", "content": "out1"},
            {"type": "tool_result", "tool_use_id": "t2", "content": "out2"},
        ]
        result = hook.extract_tool_results(content)
        assert result["t1"] == "out1"
        assert result["t2"] == "out2"

    def test_mixed_text_parts_in_content(self):
        content = [
            {
                "type": "tool_result",
                "tool_use_id": "t1",
                "content": [
                    {"type": "text", "text": "line1"},
                    "line2",
                ],
            }
        ]
        result = hook.extract_tool_results(content)
        assert result["t1"] == "line1\nline2"


# ---------------------------------------------------------------------------
# parse_ts
# ---------------------------------------------------------------------------

class TestParseTs:
    def test_iso_with_timezone(self):
        result = hook.parse_ts("2026-03-29T10:00:00+00:00")
        assert result is not None
        assert result.year == 2026
        assert result.month == 3
        assert result.day == 29

    def test_iso_with_z_suffix(self):
        result = hook.parse_ts("2026-03-29T10:00:00Z")
        assert result is not None
        assert result.hour == 10

    def test_empty_string_returns_none(self):
        assert hook.parse_ts("") is None

    def test_invalid_string_returns_none(self):
        assert hook.parse_ts("not-a-date") is None

    def test_none_like_empty(self):
        # parse_ts checks `if not ts_str` so empty string returns None
        assert hook.parse_ts("") is None


# ---------------------------------------------------------------------------
# parse_transcript
# ---------------------------------------------------------------------------

class TestParseTranscript:
    def test_basic_parsing(self, tmp_path):
        f = tmp_path / "test.jsonl"
        entries = [
            {"type": "user", "message": {"role": "user", "content": "hello"}},
            {"type": "assistant", "message": {"role": "assistant", "content": "hi"}},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        result, total = hook.parse_transcript(str(f))
        assert total == 2
        assert len(result) == 2
        assert result[0]["type"] == "user"

    def test_skip_lines(self, tmp_path):
        f = tmp_path / "test.jsonl"
        entries = [
            {"type": "user", "message": {"content": "first"}},
            {"type": "user", "message": {"content": "second"}},
            {"type": "assistant", "message": {"content": "third"}},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        result, total = hook.parse_transcript(str(f), skip_lines=2)
        assert total == 3
        assert len(result) == 1
        assert result[0]["type"] == "assistant"

    def test_invalid_json_skipped(self, tmp_path):
        f = tmp_path / "test.jsonl"
        f.write_text('{"type": "user"}\nnot json\n{"type": "assistant"}\n')
        result, total = hook.parse_transcript(str(f))
        assert total == 3
        assert len(result) == 2  # invalid line skipped

    def test_empty_lines_skipped(self, tmp_path):
        f = tmp_path / "test.jsonl"
        f.write_text('{"type": "user"}\n\n{"type": "assistant"}\n')
        result, total = hook.parse_transcript(str(f))
        assert total == 3
        assert len(result) == 2

    def test_nonexistent_file(self):
        result, total = hook.parse_transcript("/nonexistent/path.jsonl")
        assert result == []
        assert total == 0

    def test_skip_all_lines(self, tmp_path):
        f = tmp_path / "test.jsonl"
        f.write_text('{"type": "user"}\n{"type": "assistant"}\n')
        result, total = hook.parse_transcript(str(f), skip_lines=2)
        assert total == 2
        assert len(result) == 0


# ---------------------------------------------------------------------------
# load_state / save_state
# ---------------------------------------------------------------------------

class TestState:
    def test_load_missing_state_returns_zero(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path))
        assert hook.load_state("nonexistent") == 0

    def test_save_and_load_roundtrip(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path))
        hook.save_state("test-session", 42)
        assert hook.load_state("test-session") == 42

    def test_state_file_is_offset(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path))
        hook.save_state("my-session", 100)
        assert (tmp_path / "my-session.offset").exists()
        assert (tmp_path / "my-session.offset").read_text().strip() == "100"

    def test_corrupt_state_returns_zero(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path))
        (tmp_path / "bad.offset").write_text("not a number")
        assert hook.load_state("bad") == 0


# ---------------------------------------------------------------------------
# extract_session_metadata
# ---------------------------------------------------------------------------

class TestExtractSessionMetadata:
    def test_extracts_first_user_entry_fields(self, tmp_path):
        f = tmp_path / "test.jsonl"
        entries = [
            {
                "type": "user",
                "cwd": "/home/test/project",
                "gitBranch": "main",
                "version": "2.1.86",
                "entrypoint": "cli",
                "message": {"role": "user", "content": "hello"},
            }
        ]
        f.write_text(json.dumps(entries[0]) + "\n")
        meta = hook.extract_session_metadata(str(f))
        assert meta["cwd"] == "/home/test/project"
        assert meta["gitBranch"] == "main"
        assert meta["version"] == "2.1.86"
        assert meta["entrypoint"] == "cli"

    def test_missing_fields_return_empty(self, tmp_path):
        f = tmp_path / "test.jsonl"
        f.write_text(json.dumps({"type": "user", "message": {"role": "user"}}) + "\n")
        meta = hook.extract_session_metadata(str(f))
        assert meta["cwd"] == ""
        assert meta["gitBranch"] == ""

    def test_nonexistent_file(self):
        meta = hook.extract_session_metadata("/nonexistent.jsonl")
        assert meta["cwd"] == ""

    def test_skips_non_user_entries(self, tmp_path):
        f = tmp_path / "test.jsonl"
        entries = [
            {"type": "system", "cwd": "/wrong"},
            {"type": "user", "cwd": "/correct", "message": {"role": "user"}},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        meta = hook.extract_session_metadata(str(f))
        assert meta["cwd"] == "/correct"


# ---------------------------------------------------------------------------
# extract_cwd
# ---------------------------------------------------------------------------

class TestExtractCwd:
    def test_returns_first_cwd(self, tmp_path):
        f = tmp_path / "test.jsonl"
        entries = [
            {"type": "user", "cwd": "/home/test"},
            {"type": "assistant", "cwd": "/other"},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        assert hook.extract_cwd(str(f)) == "/home/test"

    def test_missing_cwd(self, tmp_path):
        f = tmp_path / "test.jsonl"
        f.write_text(json.dumps({"type": "user"}) + "\n")
        assert hook.extract_cwd(str(f)) == ""

    def test_nonexistent_file(self):
        assert hook.extract_cwd("/nonexistent.jsonl") == ""


# ---------------------------------------------------------------------------
# extract_slug
# ---------------------------------------------------------------------------

class TestExtractSlug:
    def test_returns_first_slug(self, tmp_path):
        f = tmp_path / "test.jsonl"
        entries = [
            {"type": "user", "slug": "fix-login-bug"},
            {"type": "assistant"},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        assert hook.extract_slug(str(f)) == "fix-login-bug"

    def test_no_slug(self, tmp_path):
        f = tmp_path / "test.jsonl"
        f.write_text(json.dumps({"type": "user"}) + "\n")
        assert hook.extract_slug(str(f)) == ""

    def test_nonexistent_file(self):
        assert hook.extract_slug("/nonexistent.jsonl") == ""

    def test_skips_empty_slug(self, tmp_path):
        f = tmp_path / "test.jsonl"
        entries = [
            {"type": "user", "slug": ""},
            {"type": "user", "slug": "real-slug"},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        assert hook.extract_slug(str(f)) == "real-slug"


# ---------------------------------------------------------------------------
# extract_custom_title (v2.1.110+ schema)
# ---------------------------------------------------------------------------

class TestExtractCustomTitle:
    def test_returns_first_custom_title(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [
            {"type": "user"},
            {"type": "custom-title", "customTitle": "fix-pricing-bug"},
            {"type": "custom-title", "customTitle": "later-renamed"},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        assert hook.extract_custom_title(str(f)) == "fix-pricing-bug"

    def test_no_custom_title(self, tmp_path):
        f = tmp_path / "t.jsonl"
        f.write_text(json.dumps({"type": "user"}) + "\n")
        assert hook.extract_custom_title(str(f)) == ""

    def test_skips_empty(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [
            {"type": "custom-title", "customTitle": ""},
            {"type": "custom-title", "customTitle": "real-title"},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        assert hook.extract_custom_title(str(f)) == "real-title"

    def test_nonexistent_file(self):
        assert hook.extract_custom_title("/nonexistent.jsonl") == ""


# ---------------------------------------------------------------------------
# extract_permission_mode
# ---------------------------------------------------------------------------

class TestExtractPermissionMode:
    def test_returns_last_mode(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [
            {"type": "permission-mode", "permissionMode": "default"},
            {"type": "user"},
            {"type": "permission-mode", "permissionMode": "acceptEdits"},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        assert hook.extract_permission_mode(str(f)) == "acceptEdits"

    def test_no_mode(self, tmp_path):
        f = tmp_path / "t.jsonl"
        f.write_text(json.dumps({"type": "user"}) + "\n")
        assert hook.extract_permission_mode(str(f)) == ""

    def test_nonexistent_file(self):
        assert hook.extract_permission_mode("/nonexistent.jsonl") == ""


# ---------------------------------------------------------------------------
# extract_pr_links
# ---------------------------------------------------------------------------

class TestExtractPrLinks:
    def test_collects_in_order(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [
            {"type": "pr-link", "prNumber": 9, "prUrl": "https://x/9",
             "prRepository": "a/b", "timestamp": "2026-04-15T13:18:20.953Z"},
            {"type": "user"},
            {"type": "pr-link", "prNumber": 10, "prUrl": "https://x/10",
             "prRepository": "a/b", "timestamp": "2026-04-15T14:00:00.000Z"},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        out = hook.extract_pr_links(str(f))
        assert len(out) == 2
        assert out[0]["number"] == 9 and out[0]["url"] == "https://x/9"
        assert out[1]["number"] == 10

    def test_empty_when_none(self, tmp_path):
        f = tmp_path / "t.jsonl"
        f.write_text(json.dumps({"type": "user"}) + "\n")
        assert hook.extract_pr_links(str(f)) == []


# ---------------------------------------------------------------------------
# extract_away_summaries
# ---------------------------------------------------------------------------

class TestExtractAwaySummaries:
    def test_collects_summaries(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [
            {"type": "system", "subtype": "away_summary",
             "content": "Goal: design routine X", "timestamp": "2026-04-17T08:00:00Z"},
            {"type": "system", "subtype": "stop_hook_summary"},
            {"type": "system", "subtype": "away_summary",
             "content": "Continued planning", "timestamp": "2026-04-17T08:10:00Z"},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        out = hook.extract_away_summaries(str(f))
        assert len(out) == 2
        assert out[0]["content"] == "Goal: design routine X"
        assert out[1]["content"] == "Continued planning"

    def test_skips_empty_content(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [
            {"type": "system", "subtype": "away_summary", "content": ""},
            {"type": "system", "subtype": "away_summary", "content": "real"},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        out = hook.extract_away_summaries(str(f))
        assert len(out) == 1
        assert out[0]["content"] == "real"

    def test_empty_when_none(self, tmp_path):
        f = tmp_path / "t.jsonl"
        f.write_text(json.dumps({"type": "user"}) + "\n")
        assert hook.extract_away_summaries(str(f)) == []


# ---------------------------------------------------------------------------
# calculate_turn_cost
# ---------------------------------------------------------------------------

class TestCalculateTurnCost:
    def _usage(self, inp=0, out=0, cache_read=0, cache_creation=0):
        return {
            "input": inp, "output": out, "total": inp + out,
            "cache_read": cache_read, "cache_creation": cache_creation,
        }

    def test_sonnet_pricing(self):
        usage = self._usage(inp=1_000_000, out=1_000_000)
        cost, inp_cost, out_cost, details = hook.calculate_turn_cost(usage, "claude-sonnet-4-6")
        assert abs(inp_cost - 3.0) < 0.001  # $3/1M input
        assert abs(out_cost - 15.0) < 0.001  # $15/1M output

    def test_opus_pricing_new(self):
        # Opus 4.5 / 4.6: $5 input, $25 output
        usage = self._usage(inp=1_000_000, out=1_000_000)
        cost, inp_cost, out_cost, details = hook.calculate_turn_cost(usage, "claude-opus-4-6")
        assert abs(inp_cost - 5.0) < 0.001   # $5/1M input
        assert abs(out_cost - 25.0) < 0.001  # $25/1M output

    def test_opus_pricing_legacy(self):
        # Opus 4.1 / 4.0: $15 input, $75 output
        usage = self._usage(inp=1_000_000, out=1_000_000)
        cost, inp_cost, out_cost, details = hook.calculate_turn_cost(usage, "claude-opus-4-20250514")
        assert abs(inp_cost - 15.0) < 0.001  # $15/1M input
        assert abs(out_cost - 75.0) < 0.001  # $75/1M output

    def test_haiku_4_5_pricing(self):
        # Haiku 4.5: $1 input, $5 output
        usage = self._usage(inp=1_000_000, out=1_000_000)
        cost, inp_cost, out_cost, details = hook.calculate_turn_cost(usage, "claude-haiku-4-5-20251001")
        assert abs(inp_cost - 1.00) < 0.001  # $1/1M input
        assert abs(out_cost - 5.00) < 0.001  # $5/1M output

    def test_haiku_3_5_pricing(self):
        # Haiku 3.5: $0.80 input, $4 output
        usage = self._usage(inp=1_000_000, out=1_000_000)
        cost, inp_cost, out_cost, details = hook.calculate_turn_cost(usage, "claude-3-5-haiku-20241022")
        assert abs(inp_cost - 0.80) < 0.001  # $0.80/1M input
        assert abs(out_cost - 4.00) < 0.001  # $4/1M output

    def test_cache_read_cost(self):
        usage = self._usage(cache_read=1_000_000)
        cost, inp_cost, out_cost, details = hook.calculate_turn_cost(usage, "claude-sonnet-4-6")
        assert abs(details["cache_read_input_tokens"] - 0.30) < 0.001

    def test_cache_creation_cost_5m(self):
        # All cache_creation treated as 5m when no per-tier breakdown given
        usage = self._usage(cache_creation=1_000_000)
        cost, inp_cost, out_cost, details = hook.calculate_turn_cost(usage, "claude-sonnet-4-6")
        assert abs(details["cache_creation_input_tokens"] - 3.75) < 0.001

    def test_cache_creation_cost_tiered(self):
        # 500k at 5m ($3.75) + 500k at 1h ($6.00) = $1.875 + $3.00 = $4.875
        usage = self._usage(cache_creation=1_000_000)
        cost, inp_cost, out_cost, details = hook.calculate_turn_cost(
            usage, "claude-sonnet-4-6", cache_5m=500_000, cache_1h=500_000
        )
        expected = (500_000 * 3.75 + 500_000 * 6.00) / 1_000_000
        assert abs(details["cache_creation_input_tokens"] - expected) < 0.001

    def test_opus_4_6_cache_write(self):
        # Opus 4.6 5m cache write = $6.25/MTok
        usage = self._usage(cache_creation=1_000_000)
        cost, inp_cost, out_cost, details = hook.calculate_turn_cost(usage, "claude-opus-4-6")
        assert abs(details["cache_creation_input_tokens"] - 6.25) < 0.001

    def test_zero_usage(self):
        usage = self._usage()
        cost, inp_cost, out_cost, details = hook.calculate_turn_cost(usage, "claude-sonnet-4-6")
        assert cost == 0.0

    def test_cost_disabled(self, monkeypatch):
        monkeypatch.setattr(hook, "REPORT_API_EQUIVALENT_COST", False)
        usage = self._usage(inp=1000, out=1000)
        cost, inp_cost, out_cost, details = hook.calculate_turn_cost(usage, "claude-sonnet-4-6")
        assert cost == 0.0
        assert details == {}

    def test_unknown_model_returns_zero_and_warns(self, monkeypatch):
        warnings = []
        monkeypatch.setattr(hook, "log", lambda msg: warnings.append(msg))
        usage = self._usage(inp=1_000_000, out=1_000_000)
        cost, inp_cost, out_cost, details = hook.calculate_turn_cost(usage, "some-unknown-model")
        # Returns $0 so the dashboard shows an obvious gap rather than a wrong number
        assert cost == 0.0
        assert inp_cost == 0.0
        assert details == {}
        # Must have logged a warning naming the model
        assert any("[WARN]" in w and "some-unknown-model" in w for w in warnings)

    @pytest.mark.parametrize("model,expected_input,expected_output", [
        # Current models
        ("claude-opus-4-7",              5.0,   25.0),
        ("claude-opus-4-7-20260415",     5.0,   25.0),
        ("claude-opus-4-6",              5.0,   25.0),
        ("claude-opus-4-5-20251101",     5.0,   25.0),
        ("claude-sonnet-4-6",            3.0,   15.0),
        ("claude-sonnet-4-5-20250929",   3.0,   15.0),
        ("claude-sonnet-4-20250514",     3.0,   15.0),
        ("claude-haiku-4-5-20251001",    1.0,    5.0),
        # Legacy models
        ("claude-opus-4-1-20250805",    15.0,   75.0),
        ("claude-opus-4-20250514",      15.0,   75.0),
        ("claude-3-5-haiku-20241022",    0.80,   4.0),
        ("claude-3-haiku-20240307",      0.25,   1.25),
    ])
    def test_all_known_models_have_explicit_pricing(self, model, expected_input, expected_output):
        """Ensures every known model ID hits an explicit branch, not the unknown-model fallback."""
        logged = []
        with mock.patch.object(hook, "log", side_effect=lambda msg: logged.append(msg)):
            usage = self._usage(inp=1_000_000, out=1_000_000)
            cost, inp_cost, out_cost, _ = hook.calculate_turn_cost(usage, model)
        assert not any("[WARN]" in w for w in logged), \
            f"Model '{model}' triggered unknown-model fallback — update calculate_turn_cost()"
        assert abs(inp_cost - expected_input) < 0.001, f"{model} input cost wrong"
        assert abs(out_cost - expected_output) < 0.001, f"{model} output cost wrong"

    def test_total_equals_input_plus_output(self):
        usage = self._usage(inp=500_000, out=200_000, cache_read=100_000)
        cost, inp_cost, out_cost, details = hook.calculate_turn_cost(usage, "claude-sonnet-4-6")
        assert abs(cost - (inp_cost + out_cost)) < 0.0001
        assert abs(details["total"] - cost) < 0.0001


# ---------------------------------------------------------------------------
# build_turns
# ---------------------------------------------------------------------------

class TestBuildTurns:
    def _make_entries(self):
        """Create a minimal set of transcript entries forming one turn."""
        return [
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:00+00:00",
                "uuid": "u1",
                "message": {"role": "user", "content": "What is 2+2?"},
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:02+00:00",
                "message": {
                    "id": "msg-1",
                    "role": "assistant",
                    "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "The answer is 4."}],
                    "usage": {"input_tokens": 100, "output_tokens": 50,
                              "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
                },
            },
        ]

    def test_single_turn(self):
        turns = hook.build_turns(self._make_entries())
        assert len(turns) == 1
        assert turns[0]["user_input"] == "What is 2+2?"
        assert turns[0]["assistant_output"] == "The answer is 4."
        assert turns[0]["model"] == "claude-sonnet-4-6"

    def test_usage_aggregation(self):
        turns = hook.build_turns(self._make_entries())
        usage = turns[0]["usage"]
        assert usage["input"] == 100
        assert usage["output"] == 50
        assert usage["total"] == 150

    def test_empty_entries(self):
        assert hook.build_turns([]) == []

    def test_tool_call_extraction(self):
        entries = [
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "List files"},
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:01+00:00",
                "message": {
                    "id": "msg-1",
                    "role": "assistant",
                    "model": "claude-sonnet-4-6",
                    "content": [
                        {"type": "text", "text": "I'll list the files."},
                        {"type": "tool_use", "id": "t1", "name": "Bash",
                         "input": {"command": "ls"}},
                    ],
                    "usage": {"input_tokens": 50, "output_tokens": 30,
                              "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
                },
            },
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:02+00:00",
                "message": {
                    "role": "user",
                    "content": [
                        {"type": "tool_result", "tool_use_id": "t1",
                         "content": [{"type": "text", "text": "file1.py\nfile2.py"}]},
                    ],
                },
            },
        ]
        turns = hook.build_turns(entries)
        assert len(turns) == 1
        assert len(turns[0]["tool_calls"]) == 1
        assert turns[0]["tool_calls"][0]["name"] == "Bash"
        assert turns[0]["tool_calls"][0]["output"] == "file1.py\nfile2.py"

    def test_multiple_turns(self):
        entries = [
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "First question"},
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:01+00:00",
                "message": {
                    "id": "msg-1", "role": "assistant", "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "First answer"}],
                    "usage": {"input_tokens": 10, "output_tokens": 10,
                              "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
                },
            },
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:05+00:00",
                "message": {"role": "user", "content": "Second question"},
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:06+00:00",
                "message": {
                    "id": "msg-2", "role": "assistant", "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Second answer"}],
                    "usage": {"input_tokens": 20, "output_tokens": 20,
                              "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
                },
            },
        ]
        turns = hook.build_turns(entries)
        assert len(turns) == 2
        assert turns[0]["user_input"] == "First question"
        assert turns[1]["user_input"] == "Second question"

    def test_streaming_deduplication(self):
        """Multiple assistant entries with same message_id: last usage wins."""
        entries = [
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "Hello"},
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:01+00:00",
                "message": {
                    "id": "msg-stream",
                    "role": "assistant",
                    "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "H"}],
                    "usage": {"input_tokens": 10, "output_tokens": 1,
                              "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
                },
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:02+00:00",
                "message": {
                    "id": "msg-stream",
                    "role": "assistant",
                    "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Hello there!"}],
                    "usage": {"input_tokens": 10, "output_tokens": 5,
                              "cache_read_input_tokens": 100, "cache_creation_input_tokens": 0},
                },
            },
        ]
        turns = hook.build_turns(entries)
        assert len(turns) == 1
        # Last usage wins for deduplicated message
        assert turns[0]["usage"]["output"] == 5
        assert turns[0]["usage"]["cache_read"] == 100
        # Last text content wins
        assert turns[0]["assistant_output"] == "Hello there!"

    def test_synthetic_model_ignored(self):
        """<synthetic> model should not override real model."""
        entries = [
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "Hello"},
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:01+00:00",
                "message": {
                    "id": "msg-1", "role": "assistant", "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Hi"}],
                    "usage": {"input_tokens": 10, "output_tokens": 5,
                              "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
                },
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:02+00:00",
                "message": {
                    "id": "msg-2", "role": "assistant", "model": "<synthetic>",
                    "content": [{"type": "text", "text": "Synthetic"}],
                    "usage": {},
                },
            },
        ]
        turns = hook.build_turns(entries)
        assert turns[0]["model"] == "claude-sonnet-4-6"

    def test_first_token_time_tracked(self):
        entries = self._make_entries()
        turns = hook.build_turns(entries)
        assert turns[0]["first_token_time"] == "2026-03-29T10:00:02+00:00"

    def test_cache_tokens_aggregated(self):
        entries = [
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "Hello"},
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:01+00:00",
                "message": {
                    "id": "msg-1", "role": "assistant", "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Hi"}],
                    "usage": {"input_tokens": 100, "output_tokens": 50,
                              "cache_read_input_tokens": 5000, "cache_creation_input_tokens": 200},
                },
            },
        ]
        turns = hook.build_turns(entries)
        assert turns[0]["usage"]["cache_read"] == 5000
        assert turns[0]["usage"]["cache_creation"] == 200

    def test_turn_duration_matching(self):
        entries = [
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "Hello"},
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:05+00:00",
                "message": {
                    "id": "msg-1", "role": "assistant", "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Hi"}],
                    "usage": {"input_tokens": 10, "output_tokens": 5,
                              "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
                },
            },
            {
                "type": "system",
                "subtype": "turn_duration",
                "timestamp": "2026-03-29T10:00:05+00:00",
                "durationMs": 5000,
            },
        ]
        turns = hook.build_turns(entries)
        assert turns[0].get("duration_ms") == 5000

    def test_error_tool_result_prefixed(self):
        entries = [
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "Run something"},
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:01+00:00",
                "message": {
                    "id": "msg-1", "role": "assistant", "model": "claude-sonnet-4-6",
                    "content": [
                        {"type": "tool_use", "id": "t1", "name": "Bash",
                         "input": {"command": "bad_cmd"}},
                    ],
                    "usage": {"input_tokens": 10, "output_tokens": 5,
                              "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
                },
            },
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:02+00:00",
                "message": {
                    "role": "user",
                    "content": [
                        {"type": "tool_result", "tool_use_id": "t1",
                         "content": "command not found", "is_error": True},
                    ],
                },
            },
        ]
        turns = hook.build_turns(entries)
        assert turns[0]["tool_calls"][0]["output"] == "[ERROR] command not found"


# ---------------------------------------------------------------------------
# TestBuildTurnsNewFields
# ---------------------------------------------------------------------------

class TestBuildTurnsNewFields:
    """Tests for new per-generation fields extracted in build_turns()."""

    def _make_entries(self, assistant_usage, request_id=None, extra_assistants=None):
        """Helper: one user + one assistant entry."""
        assistant_entry = {
            "type": "assistant",
            "timestamp": "2026-03-29T10:00:01+00:00",
            "message": {
                "id": "msg-1",
                "role": "assistant",
                "model": "claude-sonnet-4-6",
                "content": [{"type": "text", "text": "Answer"}],
                "usage": assistant_usage,
            },
        }
        if request_id is not None:
            assistant_entry["requestId"] = request_id

        entries = [
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "Hello"},
            },
            assistant_entry,
        ]
        if extra_assistants:
            entries.extend(extra_assistants)
        return entries

    def test_speed_extracted(self):
        entries = self._make_entries({"input_tokens": 10, "output_tokens": 5, "speed": "fast"})
        turns = hook.build_turns(entries)
        assert turns[0]["speed"] == "fast"

    def test_speed_defaults_empty(self):
        entries = self._make_entries({"input_tokens": 10, "output_tokens": 5})
        turns = hook.build_turns(entries)
        assert turns[0]["speed"] == ""

    def test_service_tier_extracted(self):
        entries = self._make_entries({"input_tokens": 10, "output_tokens": 5, "service_tier": "standard"})
        turns = hook.build_turns(entries)
        assert turns[0]["service_tier"] == "standard"

    def test_inference_geo_extracted(self):
        entries = self._make_entries({"input_tokens": 10, "output_tokens": 5, "inference_geo": "us-east-1"})
        turns = hook.build_turns(entries)
        assert turns[0]["inference_geo"] == "us-east-1"

    def test_inference_geo_skips_empty_string(self):
        entries = self._make_entries({"input_tokens": 10, "output_tokens": 5, "inference_geo": ""})
        turns = hook.build_turns(entries)
        assert turns[0]["inference_geo"] == ""

    def test_server_tool_use_summed(self):
        entries = [
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "Hello"},
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:01+00:00",
                "message": {
                    "id": "msg-1",
                    "role": "assistant",
                    "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Answer 1"}],
                    "usage": {
                        "input_tokens": 10,
                        "output_tokens": 5,
                        "server_tool_use": {"web_search_requests": 2, "web_fetch_requests": 1},
                    },
                },
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:02+00:00",
                "message": {
                    "id": "msg-2",
                    "role": "assistant",
                    "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Answer 2"}],
                    "usage": {
                        "input_tokens": 10,
                        "output_tokens": 5,
                        "server_tool_use": {"web_search_requests": 1, "web_fetch_requests": 3},
                    },
                },
            },
        ]
        turns = hook.build_turns(entries)
        assert turns[0]["web_search_requests"] == 3
        assert turns[0]["web_fetch_requests"] == 4

    def test_cache_ephemeral_summed(self):
        entries = [
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "Hello"},
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:01+00:00",
                "message": {
                    "id": "msg-1",
                    "role": "assistant",
                    "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Answer 1"}],
                    "usage": {
                        "input_tokens": 10,
                        "output_tokens": 5,
                        "cache_creation": {"ephemeral_5m_input_tokens": 100, "ephemeral_1h_input_tokens": 200},
                    },
                },
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:02+00:00",
                "message": {
                    "id": "msg-2",
                    "role": "assistant",
                    "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Answer 2"}],
                    "usage": {
                        "input_tokens": 10,
                        "output_tokens": 5,
                        "cache_creation": {"ephemeral_5m_input_tokens": 50, "ephemeral_1h_input_tokens": 150},
                    },
                },
            },
        ]
        turns = hook.build_turns(entries)
        assert turns[0]["cache_ephemeral_5m"] == 150
        assert turns[0]["cache_ephemeral_1h"] == 350

    def test_request_ids_collected(self):
        entries = [
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "Hello"},
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:01+00:00",
                "requestId": "req-aaa",
                "message": {
                    "id": "msg-1",
                    "role": "assistant",
                    "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Answer 1"}],
                    "usage": {"input_tokens": 10, "output_tokens": 5},
                },
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:02+00:00",
                "requestId": "req-bbb",
                "message": {
                    "id": "msg-2",
                    "role": "assistant",
                    "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Answer 2"}],
                    "usage": {"input_tokens": 10, "output_tokens": 5},
                },
            },
        ]
        turns = hook.build_turns(entries)
        assert sorted(turns[0]["request_ids"]) == ["req-aaa", "req-bbb"]

    def test_request_ids_empty_when_absent(self):
        entries = self._make_entries({"input_tokens": 10, "output_tokens": 5})
        turns = hook.build_turns(entries)
        assert turns[0]["request_ids"] == []

    def test_multiple_api_calls_last_wins_for_scalar_fields(self):
        entries = [
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "Hello"},
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:01+00:00",
                "message": {
                    "id": "msg-1",
                    "role": "assistant",
                    "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Answer 1"}],
                    "usage": {
                        "input_tokens": 10,
                        "output_tokens": 5,
                        "speed": "fast",
                        "service_tier": "standard",
                        "inference_geo": "us-east-1",
                    },
                },
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:02+00:00",
                "message": {
                    "id": "msg-2",
                    "role": "assistant",
                    "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Answer 2"}],
                    "usage": {
                        "input_tokens": 10,
                        "output_tokens": 5,
                        "speed": "turbo",
                        "service_tier": "premium",
                        "inference_geo": "eu-west-1",
                    },
                },
            },
        ]
        turns = hook.build_turns(entries)
        # Last non-empty value wins — but order of iteration over a set is non-deterministic,
        # so we just verify one of the two valid values is present.
        assert turns[0]["speed"] in ("fast", "turbo")
        assert turns[0]["service_tier"] in ("standard", "premium")
        assert turns[0]["inference_geo"] in ("us-east-1", "eu-west-1")


# ---------------------------------------------------------------------------
# log (rotation)
# ---------------------------------------------------------------------------

class TestLog:
    def test_writes_to_log_file(self, tmp_path, monkeypatch):
        log_file = str(tmp_path / "test.log")
        monkeypatch.setattr(hook, "LOG_FILE", log_file)
        hook.log("test message")
        content = Path(log_file).read_text()
        assert "test message" in content

    def test_log_rotation(self, tmp_path, monkeypatch):
        log_file = str(tmp_path / "test.log")
        monkeypatch.setattr(hook, "LOG_FILE", log_file)
        monkeypatch.setattr(hook, "MAX_LOG_BYTES", 50)  # very small limit

        # Write enough to trigger rotation
        hook.log("x" * 60)
        assert os.path.exists(log_file)

        # Next write should rotate
        hook.log("after rotation")
        assert os.path.exists(log_file)
        assert os.path.exists(log_file + ".1")

    def test_log_does_not_raise(self, monkeypatch):
        monkeypatch.setattr(hook, "LOG_FILE", "/nonexistent/dir/log.txt")
        # Should silently fail, not raise
        hook.log("should not crash")


# ---------------------------------------------------------------------------
# send_to_langfuse (batching)
# ---------------------------------------------------------------------------

class TestSendToLangfuse:
    def test_batches_in_chunks_of_50(self, monkeypatch):
        """Verify that large batches are split into chunks of 50."""
        sent_payloads = []

        def mock_urlopen(req, timeout=15):
            body = json.loads(req.data.decode())
            sent_payloads.append(body)

            class MockResp:
                status = 200
                def read(self):
                    return b'{"ok": true}'
                def __enter__(self):
                    return self
                def __exit__(self, *args):
                    pass

            return MockResp()

        monkeypatch.setattr(hook, "urlopen", mock_urlopen)
        monkeypatch.setattr(hook, "LANGFUSE_PUBLIC_KEY", "pk-test")
        monkeypatch.setattr(hook, "LANGFUSE_SECRET_KEY", "sk-test")

        batch = [{"id": f"evt-{i}", "type": "test"} for i in range(120)]
        hook.send_to_langfuse(batch)

        assert len(sent_payloads) == 3  # 50 + 50 + 20
        assert len(sent_payloads[0]["batch"]) == 50
        assert len(sent_payloads[1]["batch"]) == 50
        assert len(sent_payloads[2]["batch"]) == 20


# ---------------------------------------------------------------------------
# process_session (integration)
# ---------------------------------------------------------------------------

class TestProcessSession:
    def _make_transcript(self, tmp_path, entries):
        f = tmp_path / "session.jsonl"
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        return str(f)

    def test_basic_session_processing(self, tmp_path, monkeypatch):
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        monkeypatch.setattr(hook, "STATE_DIR", str(state_dir))

        sent_batches = []
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch))

        entries = [
            {
                "type": "user", "uuid": "u1",
                "timestamp": "2026-03-29T10:00:00+00:00",
                "cwd": "/home/test/project",
                "version": "2.1.86", "entrypoint": "cli",
                "message": {"role": "user", "content": "Fix the login bug"},
            },
            {
                "type": "assistant", "uuid": "a1",
                "timestamp": "2026-03-29T10:00:02+00:00",
                "message": {
                    "id": "msg-1", "role": "assistant", "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "I've fixed the login bug."}],
                    "usage": {"input_tokens": 100, "output_tokens": 50,
                              "cache_read_input_tokens": 5000, "cache_creation_input_tokens": 200},
                },
            },
        ]
        path = self._make_transcript(tmp_path, entries)
        hook.process_session("test-session", path, "/home/test/project")

        assert len(sent_batches) == 1
        batch = sent_batches[0]

        # Should have: 1 trace + 1 generation + 5 scores = 7 events
        trace_events = [e for e in batch if e["type"] == "trace-create"]
        gen_events = [e for e in batch if e["type"] == "generation-create"]
        score_events = [e for e in batch if e["type"] == "score-create"]
        assert len(trace_events) == 1
        assert len(gen_events) == 1
        assert len(score_events) == 5

        # Trace metadata
        trace = trace_events[0]["body"]
        assert trace["id"] == "trace-test-session"
        assert "claude-code" in trace["tags"]
        assert trace["metadata"]["turn_count"] == 1

        # Score: session_type should be bug-fix
        score_by_name = {e["body"]["name"]: e["body"] for e in score_events}
        assert score_by_name["session_type"]["value"] == "bug-fix"

    def test_no_new_entries_skips_send(self, tmp_path, monkeypatch):
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        monkeypatch.setattr(hook, "STATE_DIR", str(state_dir))

        sent_batches = []
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch))

        entries = [
            {
                "type": "user", "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "Hello"},
            },
            {
                "type": "assistant", "timestamp": "2026-03-29T10:00:01+00:00",
                "message": {
                    "id": "msg-1", "role": "assistant", "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Hi"}],
                    "usage": {"input_tokens": 10, "output_tokens": 5,
                              "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
                },
            },
        ]
        path = self._make_transcript(tmp_path, entries)

        # First call processes normally
        hook.process_session("sess", path, "/test")
        assert len(sent_batches) == 1

        # Second call: no new entries, no send
        hook.process_session("sess", path, "/test")
        assert len(sent_batches) == 1

    def test_incremental_processing(self, tmp_path, monkeypatch):
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        monkeypatch.setattr(hook, "STATE_DIR", str(state_dir))

        sent_batches = []
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch))

        f = tmp_path / "session.jsonl"

        # First batch of entries
        entries1 = [
            {
                "type": "user", "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "First question"},
            },
            {
                "type": "assistant", "timestamp": "2026-03-29T10:00:01+00:00",
                "message": {
                    "id": "msg-1", "role": "assistant", "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "First answer"}],
                    "usage": {"input_tokens": 10, "output_tokens": 5,
                              "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
                },
            },
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries1) + "\n")
        hook.process_session("inc-sess", str(f), "/test")
        assert len(sent_batches) == 1

        # Append more entries
        entries2 = [
            {
                "type": "user", "timestamp": "2026-03-29T10:01:00+00:00",
                "message": {"role": "user", "content": "Second question"},
            },
            {
                "type": "assistant", "timestamp": "2026-03-29T10:01:01+00:00",
                "message": {
                    "id": "msg-2", "role": "assistant", "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Second answer"}],
                    "usage": {"input_tokens": 20, "output_tokens": 10,
                              "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
                },
            },
        ]
        with open(str(f), "a") as fh:
            for e in entries2:
                fh.write(json.dumps(e) + "\n")

        hook.process_session("inc-sess", str(f), "/test")
        assert len(sent_batches) == 2

    def test_tool_spans_created(self, tmp_path, monkeypatch):
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        monkeypatch.setattr(hook, "STATE_DIR", str(state_dir))

        sent_batches = []
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch))

        entries = [
            {
                "type": "user", "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "List files"},
            },
            {
                "type": "assistant", "timestamp": "2026-03-29T10:00:01+00:00",
                "message": {
                    "id": "msg-1", "role": "assistant", "model": "claude-sonnet-4-6",
                    "content": [
                        {"type": "text", "text": "I'll list the files."},
                        {"type": "tool_use", "id": "t1", "name": "Bash",
                         "input": {"command": "ls"}},
                    ],
                    "usage": {"input_tokens": 50, "output_tokens": 30,
                              "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
                },
            },
            {
                "type": "user", "timestamp": "2026-03-29T10:00:02+00:00",
                "message": {
                    "role": "user",
                    "content": [
                        {"type": "tool_result", "tool_use_id": "t1",
                         "content": [{"type": "text", "text": "file1.py"}]},
                    ],
                },
            },
        ]
        path = self._make_transcript(tmp_path, entries)
        hook.process_session("tool-sess", path, "/test")

        batch = sent_batches[0]
        span_events = [e for e in batch if e["type"] == "span-create"]
        assert len(span_events) == 1
        assert span_events[0]["body"]["name"] == "Bash"

    def test_secret_redaction_in_output(self, tmp_path, monkeypatch):
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        monkeypatch.setattr(hook, "STATE_DIR", str(state_dir))

        sent_batches = []
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch))

        entries = [
            {
                "type": "user", "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "api_key = 'sk-abc123456789012345678'"},
            },
            {
                "type": "assistant", "timestamp": "2026-03-29T10:00:01+00:00",
                "message": {
                    "id": "msg-1", "role": "assistant", "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Found the key."}],
                    "usage": {"input_tokens": 10, "output_tokens": 5,
                              "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
                },
            },
        ]
        path = self._make_transcript(tmp_path, entries)
        hook.process_session("secret-sess", path, "/test")

        batch = sent_batches[0]
        trace = [e for e in batch if e["type"] == "trace-create"][0]
        assert "sk-abc123456789012345678" not in json.dumps(trace)

    def test_model_family_tags(self, tmp_path, monkeypatch):
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        monkeypatch.setattr(hook, "STATE_DIR", str(state_dir))

        sent_batches = []
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch))

        entries = [
            {
                "type": "user", "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "Hello"},
            },
            {
                "type": "assistant", "timestamp": "2026-03-29T10:00:01+00:00",
                "message": {
                    "id": "msg-1", "role": "assistant", "model": "claude-opus-4-6",
                    "content": [{"type": "text", "text": "Hi"}],
                    "usage": {"input_tokens": 10, "output_tokens": 5,
                              "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
                },
            },
        ]
        path = self._make_transcript(tmp_path, entries)
        hook.process_session("tag-sess", path, "/test")

        trace = [e for e in sent_batches[0] if e["type"] == "trace-create"][0]
        assert "opus" in trace["body"]["tags"]


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

class TestMain:
    def test_reprocess_flag(self, monkeypatch):
        called = []
        monkeypatch.setattr(hook, "reprocess_all", lambda: called.append(True))
        monkeypatch.setattr("sys.argv", ["langfuse-hook.py", "--reprocess"])
        hook.main()
        assert called

    def test_missing_keys_logs_and_returns(self, monkeypatch):
        monkeypatch.setattr(hook, "LANGFUSE_PUBLIC_KEY", "")
        monkeypatch.setattr(hook, "LANGFUSE_SECRET_KEY", "")
        monkeypatch.setattr("sys.argv", ["langfuse-hook.py"])

        import io
        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps({
            "session_id": "test", "transcript_path": "/tmp/test.jsonl",
        })))

        logged = []
        monkeypatch.setattr(hook, "log", lambda msg: logged.append(msg))

        hook.main()
        assert any("must be set" in m for m in logged)

    def test_stop_hook_active_returns_early(self, monkeypatch):
        monkeypatch.setattr(hook, "LANGFUSE_PUBLIC_KEY", "pk-test")
        monkeypatch.setattr(hook, "LANGFUSE_SECRET_KEY", "sk-test")
        monkeypatch.setattr("sys.argv", ["langfuse-hook.py"])

        import io
        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps({
            "session_id": "test",
            "transcript_path": "/tmp/test.jsonl",
            "stop_hook_active": True,
        })))

        called = []
        monkeypatch.setattr(hook, "process_session", lambda *a: called.append(True))

        hook.main()
        assert not called

    def test_missing_transcript_path(self, monkeypatch):
        monkeypatch.setattr(hook, "LANGFUSE_PUBLIC_KEY", "pk-test")
        monkeypatch.setattr(hook, "LANGFUSE_SECRET_KEY", "sk-test")
        monkeypatch.setattr("sys.argv", ["langfuse-hook.py"])

        import io
        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps({
            "session_id": "test",
        })))

        logged = []
        monkeypatch.setattr(hook, "log", lambda msg: logged.append(msg))

        hook.main()
        assert any("transcript_path" in m for m in logged)


# ---------------------------------------------------------------------------
# TestProcessSessionNewFields
# ---------------------------------------------------------------------------

class TestProcessSessionNewFields:
    def _make_transcript(self, tmp_path, entries):
        f = tmp_path / "session.jsonl"
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        return str(f)

    def test_generation_metadata_contains_new_fields(self, tmp_path, monkeypatch):
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        monkeypatch.setattr(hook, "STATE_DIR", str(state_dir))

        sent_batches = []
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch))

        entries = [
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "Hello"},
            },
            {
                "type": "assistant",
                "requestId": "req-abc",
                "timestamp": "2026-03-29T10:00:01+00:00",
                "message": {
                    "id": "msg-1",
                    "role": "assistant",
                    "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Answer"}],
                    "usage": {
                        "input_tokens": 10,
                        "output_tokens": 5,
                        "speed": "fast",
                        "service_tier": "standard",
                        "inference_geo": "us-east-1",
                        "server_tool_use": {"web_search_requests": 3, "web_fetch_requests": 1},
                        "cache_creation": {"ephemeral_5m_input_tokens": 100, "ephemeral_1h_input_tokens": 200},
                    },
                },
            },
        ]
        path = self._make_transcript(tmp_path, entries)
        hook.process_session("new-fields-session", path, "/test/project")

        assert len(sent_batches) == 1
        batch = sent_batches[0]

        gen_events = [e for e in batch if e["type"] == "generation-create"]
        assert len(gen_events) == 1

        gen_body = gen_events[0]["body"]
        metadata = gen_body["metadata"]

        # Verify new metadata fields
        assert metadata["speed"] == "fast"
        assert metadata["service_tier"] == "standard"
        assert metadata["inference_geo"] == "us-east-1"
        assert metadata["request_ids"] == ["req-abc"]
        assert metadata["web_search_requests"] == 3
        assert metadata["web_fetch_requests"] == 1

        # Verify usageDetails contains cache ephemeral fields
        usage_details = gen_body["usageDetails"]
        assert usage_details["cache_5m"] == 100
        assert usage_details["cache_1h"] == 200

    def test_fast_tag_added(self, tmp_path, monkeypatch):
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        monkeypatch.setattr(hook, "STATE_DIR", str(state_dir))

        sent_batches = []
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch))

        entries = [
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "Hello"},
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:01+00:00",
                "message": {
                    "id": "msg-1",
                    "role": "assistant",
                    "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Answer"}],
                    "usage": {
                        "input_tokens": 10,
                        "output_tokens": 5,
                        "speed": "fast",
                    },
                },
            },
        ]
        path = self._make_transcript(tmp_path, entries)
        hook.process_session("fast-tag-session", path, "/test/project")

        assert len(sent_batches) == 1
        batch = sent_batches[0]

        trace_events = [e for e in batch if e["type"] == "trace-create"]
        assert len(trace_events) == 1

        tags = trace_events[0]["body"]["tags"]
        assert "fast" in tags


# ---------------------------------------------------------------------------
# extract_api_errors
# ---------------------------------------------------------------------------

class TestExtractApiErrors:
    def test_no_errors(self):
        entries = [
            {"type": "user", "message": {"role": "user", "content": "hello"}},
            {"type": "assistant", "message": {"role": "assistant", "content": "hi"}},
        ]
        result = hook.extract_api_errors(entries)
        assert result["total_count"] == 0
        assert result["by_status"] == {}

    def test_single_error(self):
        entries = [
            {
                "type": "system",
                "subtype": "api_error",
                "level": "error",
                "timestamp": "2026-03-29T10:00:05+00:00",
                "error": {"status": 529},
            }
        ]
        result = hook.extract_api_errors(entries)
        assert result["total_count"] == 1
        assert result["by_status"] == {"529": 1}
        assert result["first_error_at"] == "2026-03-29T10:00:05+00:00"
        assert result["last_error_at"] == "2026-03-29T10:00:05+00:00"

    def test_multiple_errors(self):
        entries = [
            {
                "type": "system",
                "subtype": "api_error",
                "level": "error",
                "timestamp": "2026-03-29T10:00:01+00:00",
                "error": {"status": 529},
            },
            {
                "type": "system",
                "subtype": "api_error",
                "level": "error",
                "timestamp": "2026-03-29T10:00:03+00:00",
                "error": {"status": 500},
            },
            {
                "type": "system",
                "subtype": "api_error",
                "level": "error",
                "timestamp": "2026-03-29T10:00:05+00:00",
                "error": {"status": 529},
            },
        ]
        result = hook.extract_api_errors(entries)
        assert result["total_count"] == 3
        assert result["by_status"] == {"529": 2, "500": 1}
        assert result["first_error_at"] == "2026-03-29T10:00:01+00:00"
        assert result["last_error_at"] == "2026-03-29T10:00:05+00:00"

    def test_missing_error_status(self):
        entries = [
            {
                "type": "system",
                "subtype": "api_error",
                "level": "error",
                "timestamp": "2026-03-29T10:00:00+00:00",
                "error": {},
            }
        ]
        result = hook.extract_api_errors(entries)
        assert result["total_count"] == 1
        assert result["by_status"] == {"unknown": 1}

    def test_non_error_system_entries_ignored(self):
        entries = [
            {
                "type": "system",
                "subtype": "turn_duration",
                "timestamp": "2026-03-29T10:00:01+00:00",
                "durationMs": 1234,
            },
            {
                "type": "system",
                "subtype": "local_command",
                "timestamp": "2026-03-29T10:00:02+00:00",
            },
        ]
        result = hook.extract_api_errors(entries)
        assert result["total_count"] == 0


# ---------------------------------------------------------------------------
# TestProcessSessionApiErrors
# ---------------------------------------------------------------------------

class TestProcessSessionApiErrors:
    def _make_transcript(self, tmp_path, entries):
        f = tmp_path / "session.jsonl"
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        return str(f)

    def test_api_errors_in_trace_metadata(self, tmp_path, monkeypatch):
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        monkeypatch.setattr(hook, "STATE_DIR", str(state_dir))

        sent_batches = []
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch))

        entries = [
            {
                "type": "system",
                "subtype": "api_error",
                "level": "error",
                "timestamp": "2026-03-29T10:00:00+00:00",
                "error": {"status": 529},
            },
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:01+00:00",
                "message": {"role": "user", "content": "Hello"},
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:02+00:00",
                "message": {
                    "id": "msg-1",
                    "role": "assistant",
                    "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Answer"}],
                    "usage": {"input_tokens": 10, "output_tokens": 5},
                },
            },
        ]
        path = self._make_transcript(tmp_path, entries)
        hook.process_session("api-errors-session", path, "/test/project")

        assert len(sent_batches) == 1
        batch = sent_batches[0]

        trace_events = [e for e in batch if e["type"] == "trace-create"]
        assert len(trace_events) == 1

        trace_body = trace_events[0]["body"]
        metadata = trace_body["metadata"]
        tags = trace_body["tags"]

        assert "api_errors" in metadata
        api_errors = metadata["api_errors"]
        assert api_errors["total_count"] == 1
        assert api_errors["by_status"] == {"529": 1}

        assert "has-errors" in tags

    def test_no_errors_no_tag(self, tmp_path, monkeypatch):
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        monkeypatch.setattr(hook, "STATE_DIR", str(state_dir))

        sent_batches = []
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch))

        entries = [
            {
                "type": "user",
                "timestamp": "2026-03-29T10:00:00+00:00",
                "message": {"role": "user", "content": "Hello"},
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-29T10:00:01+00:00",
                "message": {
                    "id": "msg-1",
                    "role": "assistant",
                    "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Answer"}],
                    "usage": {"input_tokens": 10, "output_tokens": 5},
                },
            },
        ]
        path = self._make_transcript(tmp_path, entries)
        hook.process_session("no-errors-session", path, "/test/project")

        assert len(sent_batches) == 1
        batch = sent_batches[0]

        trace_events = [e for e in batch if e["type"] == "trace-create"]
        assert len(trace_events) == 1

        tags = trace_events[0]["body"]["tags"]
        assert "has-errors" not in tags


# ---------------------------------------------------------------------------
# TestSubagentNewFields
# ---------------------------------------------------------------------------

class TestSubagentNewFields:
    def test_subagent_generations_have_new_metadata(self, tmp_path):
        entries = [
            {"type": "user", "timestamp": "2026-04-03T10:00:00+00:00",
             "message": {"role": "user", "content": "Do research"}},
            {"type": "assistant", "timestamp": "2026-04-03T10:00:02+00:00",
             "requestId": "req_sub001",
             "message": {
                 "id": "msg-s1", "role": "assistant", "model": "claude-sonnet-4-6",
                 "content": [{"type": "text", "text": "Found it."}],
                 "usage": {
                     "input_tokens": 50, "output_tokens": 20,
                     "cache_read_input_tokens": 1000, "cache_creation_input_tokens": 100,
                     "speed": "standard",
                     "service_tier": "standard",
                     "inference_geo": "eu-west-1",
                     "server_tool_use": {"web_search_requests": 1, "web_fetch_requests": 0},
                     "cache_creation": {"ephemeral_5m_input_tokens": 0, "ephemeral_1h_input_tokens": 100},
                 },
             }},
        ]
        f = tmp_path / "subagent.jsonl"
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")

        events, cost, offset, tc, status = hook.ingest_subagent(
            agent_id="test-sa", transcript_path=str(f),
            parent_span_id="span-parent", trace_id="trace-parent",
            session_id="sess-parent", subagent_offset=0,
        )

        gen_events = [e for e in events if e["type"] == "generation-create"]
        assert len(gen_events) == 1
        meta = gen_events[0]["body"]["metadata"]
        assert meta["speed"] == "standard"
        assert meta["service_tier"] == "standard"
        assert meta["inference_geo"] == "eu-west-1"
        assert meta["request_ids"] == ["req_sub001"]
        assert meta["web_search_requests"] == 1
        assert meta["web_fetch_requests"] == 0

        usage_details = gen_events[0]["body"]["usageDetails"]
        assert usage_details["cache_5m"] == 0
        assert usage_details["cache_1h"] == 100
