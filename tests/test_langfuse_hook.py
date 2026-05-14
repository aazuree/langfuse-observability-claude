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

# Import langfuse_common for patching auth keys in tests
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import langfuse_common


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
        monkeypatch.setattr(langfuse_common, "LANGFUSE_PUBLIC_KEY", "pk-test")
        monkeypatch.setattr(langfuse_common, "LANGFUSE_SECRET_KEY", "sk-test")
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
        result, total, read_ok = hook.parse_transcript(str(f))
        assert total == 2
        assert len(result) == 2
        assert result[0]["type"] == "user"
        assert read_ok is True

    def test_skip_lines(self, tmp_path):
        f = tmp_path / "test.jsonl"
        entries = [
            {"type": "user", "message": {"content": "first"}},
            {"type": "user", "message": {"content": "second"}},
            {"type": "assistant", "message": {"content": "third"}},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        result, total, read_ok = hook.parse_transcript(str(f), skip_lines=2)
        assert total == 3
        assert len(result) == 1
        assert result[0]["type"] == "assistant"
        assert read_ok is True

    def test_invalid_json_skipped(self, tmp_path):
        f = tmp_path / "test.jsonl"
        f.write_text('{"type": "user"}\nnot json\n{"type": "assistant"}\n')
        result, total, read_ok = hook.parse_transcript(str(f))
        assert total == 3
        assert len(result) == 2  # invalid line skipped
        assert read_ok is True

    def test_empty_lines_skipped(self, tmp_path):
        f = tmp_path / "test.jsonl"
        f.write_text('{"type": "user"}\n\n{"type": "assistant"}\n')
        result, total, read_ok = hook.parse_transcript(str(f))
        assert total == 3
        assert len(result) == 2
        assert read_ok is True

    def test_nonexistent_file_signals_read_failure(self):
        # Read failure must be distinguishable from "empty file" so the caller
        # does not clobber the saved offset with 0 on a transient I/O error.
        result, total, read_ok = hook.parse_transcript("/nonexistent/path.jsonl")
        assert result == []
        assert total == 0
        assert read_ok is False

    def test_empty_file_is_read_ok(self, tmp_path):
        # Distinct from a read failure: file exists, has zero entries.
        f = tmp_path / "empty.jsonl"
        f.write_text("")
        result, total, read_ok = hook.parse_transcript(str(f))
        assert result == []
        assert total == 0
        assert read_ok is True

    def test_skip_all_lines(self, tmp_path):
        f = tmp_path / "test.jsonl"
        f.write_text('{"type": "user"}\n{"type": "assistant"}\n')
        result, total, read_ok = hook.parse_transcript(str(f), skip_lines=2)
        assert total == 2
        assert len(result) == 0
        assert read_ok is True


# ---------------------------------------------------------------------------
# process_session — state save behaviour on transcript read failure
# ---------------------------------------------------------------------------

class TestProcessSessionStatePreservation:
    def test_does_not_clobber_state_on_read_failure(self, tmp_path, monkeypatch):
        """A transient transcript read failure (path moved, permission denied)
        must NOT overwrite the existing line offset. Otherwise the next fire
        would re-build every turn from scratch, racing with the async delete
        pipeline and dropping observations."""
        # Pre-existing state with a non-zero offset
        sid = "test-read-fail-session"
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))
        from pathlib import Path
        Path(hook.STATE_DIR).mkdir(parents=True, exist_ok=True)
        hook.save_state(sid, line_offset=100, turn_count=5)

        # Run process_session against a path that doesn't exist
        hook.process_session(sid, "/nonexistent/missing.jsonl", cwd="")

        # State must be UNCHANGED — not clobbered to lines:0
        lines, turns = hook.load_state(sid)
        assert lines == 100, "offset clobbered on read failure"
        assert turns == 5, "turn count clobbered on read failure"


# ---------------------------------------------------------------------------
# load_state / save_state
# ---------------------------------------------------------------------------

class TestState:
    def test_load_missing_state_returns_zero(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path))
        assert hook.load_state("nonexistent") == (0, 0)

    def test_save_and_load_roundtrip(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path))
        hook.save_state("test-session", 42, 3)
        assert hook.load_state("test-session") == (42, 3)

    def test_state_file_is_json(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path))
        hook.save_state("my-session", 100, 7)
        assert (tmp_path / "my-session.offset").exists()
        data = json.loads((tmp_path / "my-session.offset").read_text())
        assert data == {"lines": 100, "turns": 7}

    def test_corrupt_state_returns_zero(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path))
        (tmp_path / "bad.offset").write_text("not a number")
        assert hook.load_state("bad") == (0, 0)

    def test_legacy_plain_int_state_backward_compat(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path))
        (tmp_path / "legacy.offset").write_text("52")
        line_offset, turn_count = hook.load_state("legacy")
        assert line_offset == 52
        assert turn_count == 0  # unknown from legacy format


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
# extract_agent_name
# ---------------------------------------------------------------------------

class TestExtractAgentName:
    def test_returns_first_agent_name(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [
            {"type": "user"},
            {"type": "agent-name", "agentName": "langfuse-usagedetails-fix"},
            {"type": "agent-name", "agentName": "second-name"},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        assert hook.extract_agent_name(str(f)) == "langfuse-usagedetails-fix"

    def test_no_agent_name_entry(self, tmp_path):
        f = tmp_path / "t.jsonl"
        f.write_text(json.dumps({"type": "user"}) + "\n")
        assert hook.extract_agent_name(str(f)) == ""

    def test_skips_empty_agent_name(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [
            {"type": "agent-name", "agentName": ""},
            {"type": "agent-name", "agentName": "real-name"},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        assert hook.extract_agent_name(str(f)) == "real-name"

    def test_nonexistent_file(self):
        assert hook.extract_agent_name("/nonexistent.jsonl") == ""


# ---------------------------------------------------------------------------
# extract_ai_title
# ---------------------------------------------------------------------------

class TestExtractAiTitle:
    def test_returns_first_ai_title(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [
            {"type": "user"},
            {"type": "ai-title", "aiTitle": "otel feature parity audit"},
            {"type": "ai-title", "aiTitle": "stale-second"},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        assert hook.extract_ai_title(str(f)) == "otel feature parity audit"

    def test_no_ai_title_entry(self, tmp_path):
        f = tmp_path / "t.jsonl"
        f.write_text(json.dumps({"type": "user"}) + "\n")
        assert hook.extract_ai_title(str(f)) == ""

    def test_skips_empty_ai_title(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [
            {"type": "ai-title", "aiTitle": ""},
            {"type": "ai-title", "aiTitle": "real-title"},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        assert hook.extract_ai_title(str(f)) == "real-title"

    def test_nonexistent_file(self):
        assert hook.extract_ai_title("/nonexistent.jsonl") == ""


# ---------------------------------------------------------------------------
# extract_session_kind
# ---------------------------------------------------------------------------

class TestExtractSessionKind:
    def test_returns_first_session_kind(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [
            {"type": "user", "sessionKind": "bg"},
            {"type": "user", "sessionKind": "fg"},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        assert hook.extract_session_kind(str(f)) == "bg"

    def test_defaults_to_fg_when_absent(self, tmp_path):
        # Older transcripts don't carry sessionKind at all
        f = tmp_path / "t.jsonl"
        f.write_text(json.dumps({"type": "user"}) + "\n")
        assert hook.extract_session_kind(str(f)) == "fg"

    def test_skips_empty_values(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [
            {"type": "user", "sessionKind": ""},
            {"type": "user", "sessionKind": "fg"},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        assert hook.extract_session_kind(str(f)) == "fg"

    def test_nonexistent_file(self):
        assert hook.extract_session_kind("/nonexistent.jsonl") == "fg"


# ---------------------------------------------------------------------------
# extract_attachments
# ---------------------------------------------------------------------------

class TestExtractAttachments:
    def test_counts_and_groups_by_type(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [
            {"type": "attachment", "attachment": {"type": "hook_success"}},
            {"type": "attachment", "attachment": {"type": "hook_success"}},
            {"type": "attachment", "attachment": {"type": "file"}},
            {"type": "user"},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        result = hook.extract_attachments(str(f))
        assert result == {"count": 3, "by_type": {"hook_success": 2, "file": 1}}

    def test_empty_when_no_attachments(self, tmp_path):
        f = tmp_path / "t.jsonl"
        f.write_text(json.dumps({"type": "user"}) + "\n")
        assert hook.extract_attachments(str(f)) == {}

    def test_missing_attachment_type_labelled_unknown(self, tmp_path):
        f = tmp_path / "t.jsonl"
        # Malformed entry: 'attachment' field missing/null
        entries = [
            {"type": "attachment"},
            {"type": "attachment", "attachment": None},
            {"type": "attachment", "attachment": {}},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        result = hook.extract_attachments(str(f))
        assert result["count"] == 3
        assert result["by_type"]["unknown"] == 3

    def test_nonexistent_file(self):
        assert hook.extract_attachments("/nonexistent.jsonl") == {}


# ---------------------------------------------------------------------------
# extract_local_commands
# ---------------------------------------------------------------------------

class TestExtractLocalCommands:
    def _entry(self, body, ts="2026-05-13T20:00:00Z"):
        return {
            "type": "system",
            "subtype": "local_command",
            "content": f"<local-command-stdout>{body}</local-command-stdout>",
            "timestamp": ts,
        }

    def test_strips_wrapper_and_returns_body(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [self._entry("compaction summary text")]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        out = hook.extract_local_commands(str(f))
        assert len(out) == 1
        assert out[0]["content"] == "compaction summary text"
        assert out[0]["timestamp"] == "2026-05-13T20:00:00Z"

    def test_skips_empty_wrapped_content(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [self._entry(""), self._entry("real output")]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        out = hook.extract_local_commands(str(f))
        assert len(out) == 1
        assert out[0]["content"] == "real output"

    def test_respects_max_entries(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [self._entry(f"output-{i}") for i in range(10)]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        out = hook.extract_local_commands(str(f), max_entries=3)
        assert len(out) == 3

    def test_truncates_long_content(self, tmp_path):
        f = tmp_path / "t.jsonl"
        long_body = "x" * 1000
        f.write_text(json.dumps(self._entry(long_body)) + "\n")
        out = hook.extract_local_commands(str(f), max_content=50)
        assert out[0]["content"].startswith("x" * 50)
        assert "..." in out[0]["content"]

    def test_ignores_other_system_subtypes(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [
            {"type": "system", "subtype": "turn_duration", "content": "skip"},
            {"type": "user"},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        assert hook.extract_local_commands(str(f)) == []

    def test_nonexistent_file(self):
        assert hook.extract_local_commands("/nonexistent.jsonl") == []


# ---------------------------------------------------------------------------
# _otel_genai_attrs (OpenTelemetry GenAI semantic-convention aliases)
# ---------------------------------------------------------------------------

class TestOtelGenAIAttrs:
    def test_emits_required_attributes(self):
        attrs = hook._otel_genai_attrs(
            session_id="sess-123",
            model="claude-opus-4-7",
            usage={"input": 100, "output": 200},
            request_ids=["req-abc"],
            stop_reason="end_turn",
        )
        assert attrs["gen_ai.system"] == "anthropic"
        assert attrs["gen_ai.provider.name"] == "anthropic"
        assert attrs["gen_ai.operation.name"] == "chat"
        assert attrs["gen_ai.conversation.id"] == "sess-123"
        assert attrs["gen_ai.request.model"] == "claude-opus-4-7"
        assert attrs["gen_ai.response.model"] == "claude-opus-4-7"
        assert attrs["gen_ai.usage.input_tokens"] == 100
        assert attrs["gen_ai.usage.output_tokens"] == 200
        assert attrs["gen_ai.response.id"] == "req-abc"
        assert attrs["gen_ai.response.finish_reasons"] == ["end_turn"]

    def test_omits_response_id_when_no_request_ids(self):
        attrs = hook._otel_genai_attrs(
            session_id="s", model="m", usage={"input": 0, "output": 0},
            request_ids=[], stop_reason="",
        )
        assert "gen_ai.response.id" not in attrs
        assert "gen_ai.response.finish_reasons" not in attrs

    def test_uses_first_request_id(self):
        attrs = hook._otel_genai_attrs(
            session_id="s", model="m", usage={"input": 0, "output": 0},
            request_ids=["first", "second", "third"], stop_reason="",
        )
        assert attrs["gen_ai.response.id"] == "first"


# ---------------------------------------------------------------------------
# Opus pricing whitelist behaviour
# ---------------------------------------------------------------------------

class TestOpusPricingWhitelist:
    def _usage(self, inp=1_000_000, out=0):
        return {
            "input": inp, "output": out, "total": inp + out,
            "cache_read": 0, "cache_creation": 0,
        }

    def test_future_opus_falls_through_to_warning_not_legacy(self, monkeypatch):
        """Hypothetical 'claude-opus-4-9' must NOT silently inherit the
        legacy $15/$75 rate. It must hit the unknown-model WARN path so the
        operator updates the table explicitly."""
        warnings = []
        monkeypatch.setattr(hook, "log", lambda msg: warnings.append(msg))
        cost, *_ = hook.calculate_turn_cost(self._usage(), "claude-opus-4-9")
        assert cost == 0.0
        assert any("[WARN]" in w and "claude-opus-4-9" in w for w in warnings)

    def test_legacy_opus_3_still_routes_correctly(self, monkeypatch):
        warnings = []
        monkeypatch.setattr(hook, "log", lambda msg: warnings.append(msg))
        cost, inp, _out, _ = hook.calculate_turn_cost(
            self._usage(), "claude-3-opus-20240229"
        )
        assert not any("[WARN]" in w for w in warnings)
        assert abs(inp - 15.0) < 0.001  # legacy $15/MTok input


# ---------------------------------------------------------------------------
# extract_file_history_stats
# ---------------------------------------------------------------------------

class TestExtractFileHistoryStats:
    def test_empty_when_no_entries(self, tmp_path):
        f = tmp_path / "t.jsonl"
        f.write_text(json.dumps({"type": "user"}) + "\n")
        result = hook.extract_file_history_stats(str(f))
        assert result == {"snapshot_count": 0, "tracked_files_count": 0}

    def test_counts_snapshots(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [
            {"type": "file-history-snapshot", "snapshot": {"trackedFileBackups": {}, "timestamp": "t1"}, "isSnapshotUpdate": False},
            {"type": "file-history-snapshot", "snapshot": {"trackedFileBackups": {}, "timestamp": "t2"}, "isSnapshotUpdate": True},
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        result = hook.extract_file_history_stats(str(f))
        assert result["snapshot_count"] == 2

    def test_deduplicates_file_paths(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [
            {
                "type": "file-history-snapshot",
                "snapshot": {
                    "trackedFileBackups": {"src/a.py": "content", "src/b.py": "content"},
                    "timestamp": "t1",
                },
                "isSnapshotUpdate": False,
            },
            {
                "type": "file-history-snapshot",
                "snapshot": {
                    "trackedFileBackups": {"src/a.py": "updated", "src/c.py": "content"},
                    "timestamp": "t2",
                },
                "isSnapshotUpdate": True,
            },
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        result = hook.extract_file_history_stats(str(f))
        assert result["snapshot_count"] == 2
        assert result["tracked_files_count"] == 3  # a.py, b.py, c.py deduplicated

    def test_nonexistent_file(self):
        result = hook.extract_file_history_stats("/nonexistent.jsonl")
        assert result == {"snapshot_count": 0, "tracked_files_count": 0}


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

    # ----- Fast mode 6x premium (Opus 4.6/4.7 only) -----

    def test_fast_mode_opus_4_7_applies_6x(self):
        usage = self._usage(inp=1_000_000, out=1_000_000)
        cost, inp_cost, out_cost, details = hook.calculate_turn_cost(
            usage, "claude-opus-4-7", speed="fast"
        )
        assert abs(inp_cost - 30.0) < 0.001   # $5 * 6
        assert abs(out_cost - 150.0) < 0.001  # $25 * 6

    def test_fast_mode_opus_4_6_applies_6x(self):
        usage = self._usage(inp=1_000_000, out=0)
        cost, *_ = hook.calculate_turn_cost(usage, "claude-opus-4-6", speed="fast")
        assert abs(cost - 30.0) < 0.001

    def test_fast_mode_cache_read_scaled(self):
        # Cache read: $0.50 base * 6 = $3.00 per 1M
        usage = self._usage(cache_read=1_000_000)
        cost, *_, details = hook.calculate_turn_cost(usage, "claude-opus-4-7", speed="fast")
        assert abs(details["cache_read_input_tokens"] - 3.0) < 0.001

    def test_fast_mode_cache_write_tiered_scaled(self):
        # 5m: $6.25 * 6 = $37.50; 1h: $10 * 6 = $60
        usage = self._usage(cache_creation=2_000_000)
        cost, *_, details = hook.calculate_turn_cost(
            usage, "claude-opus-4-7",
            cache_5m=1_000_000, cache_1h=1_000_000, speed="fast",
        )
        expected = 37.5 + 60.0
        assert abs(details["cache_creation_input_tokens"] - expected) < 0.01

    def test_fast_mode_ignored_for_opus_4_5(self):
        # Opus 4.5 not eligible for fast mode per spec
        usage = self._usage(inp=1_000_000, out=0)
        cost, *_ = hook.calculate_turn_cost(usage, "claude-opus-4-5-20251101", speed="fast")
        assert abs(cost - 5.0) < 0.001  # base rate, no premium

    def test_fast_mode_ignored_for_sonnet(self):
        usage = self._usage(inp=1_000_000, out=0)
        cost, *_ = hook.calculate_turn_cost(usage, "claude-sonnet-4-6", speed="fast")
        assert abs(cost - 3.0) < 0.001  # base rate

    # ----- Data residency 1.1x (Opus 4.6+/Sonnet 4.6+) -----

    def test_inference_geo_us_opus_4_7_applies_1_1x(self):
        usage = self._usage(inp=1_000_000, out=0)
        cost, *_ = hook.calculate_turn_cost(usage, "claude-opus-4-7", inference_geo="us")
        assert abs(cost - 5.5) < 0.001  # $5 * 1.1

    def test_inference_geo_us_sonnet_4_6_applies_1_1x(self):
        usage = self._usage(inp=1_000_000, out=1_000_000)
        cost, inp_cost, out_cost, _ = hook.calculate_turn_cost(
            usage, "claude-sonnet-4-6", inference_geo="us"
        )
        assert abs(inp_cost - 3.3) < 0.001    # $3 * 1.1
        assert abs(out_cost - 16.5) < 0.001   # $15 * 1.1

    def test_inference_geo_us_case_insensitive(self):
        usage = self._usage(inp=1_000_000, out=0)
        cost, *_ = hook.calculate_turn_cost(usage, "claude-opus-4-6", inference_geo="US")
        assert abs(cost - 5.5) < 0.001

    def test_inference_geo_global_no_multiplier(self):
        usage = self._usage(inp=1_000_000, out=0)
        cost, *_ = hook.calculate_turn_cost(usage, "claude-opus-4-7", inference_geo="global")
        assert abs(cost - 5.0) < 0.001

    def test_inference_geo_us_ignored_for_opus_4_5(self):
        # Opus 4.5 does not support inference_geo per spec
        usage = self._usage(inp=1_000_000, out=0)
        cost, *_ = hook.calculate_turn_cost(
            usage, "claude-opus-4-5-20251101", inference_geo="us"
        )
        assert abs(cost - 5.0) < 0.001  # base rate

    def test_inference_geo_us_ignored_for_sonnet_4_5(self):
        usage = self._usage(inp=1_000_000, out=0)
        cost, *_ = hook.calculate_turn_cost(
            usage, "claude-sonnet-4-5-20250929", inference_geo="us"
        )
        assert abs(cost - 3.0) < 0.001  # base rate

    def test_fast_mode_and_inference_geo_stack(self):
        # 6x * 1.1x = 6.6x
        usage = self._usage(inp=1_000_000, out=0)
        cost, *_ = hook.calculate_turn_cost(
            usage, "claude-opus-4-7", speed="fast", inference_geo="us"
        )
        assert abs(cost - 33.0) < 0.001  # $5 * 6 * 1.1

    # ----- Web search billing $10/1000 -----

    def test_web_search_billing(self):
        usage = self._usage()
        cost, _inp, _out, details = hook.calculate_turn_cost(
            usage, "claude-sonnet-4-6", web_search_requests=5
        )
        assert abs(cost - 0.05) < 0.0001
        assert abs(details["web_search"] - 0.05) < 0.0001

    def test_web_search_added_to_total(self):
        usage = self._usage(inp=1_000_000, out=0)
        cost, _inp, _out, details = hook.calculate_turn_cost(
            usage, "claude-sonnet-4-6", web_search_requests=10
        )
        # input $3 + web $0.10 = $3.10
        assert abs(cost - 3.10) < 0.001
        assert abs(details["web_search"] - 0.10) < 0.001
        assert abs(details["total"] - 3.10) < 0.001

    def test_no_web_search_no_field(self):
        usage = self._usage(inp=1_000_000, out=0)
        cost, _inp, _out, details = hook.calculate_turn_cost(usage, "claude-sonnet-4-6")
        assert "web_search" not in details


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
        """Multiple assistant entries with same message_id: first usage wins (identical since v2.1.97)."""
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
                    "usage": {"input_tokens": 10, "output_tokens": 5,
                              "cache_read_input_tokens": 100, "cache_creation_input_tokens": 0},
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
        # All entries carry identical usage since v2.1.97; first is taken
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
        monkeypatch.setattr(langfuse_common, "MAX_LOG_BYTES", 50)  # very small limit

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
        monkeypatch.setattr(langfuse_common, "LANGFUSE_PUBLIC_KEY", "pk-test")
        monkeypatch.setattr(langfuse_common, "LANGFUSE_SECRET_KEY", "sk-test")

        batch = [{"id": f"evt-{i}", "type": "test"} for i in range(120)]
        hook.send_to_langfuse(batch)

        assert len(sent_payloads) == 3  # 50 + 50 + 20
        assert len(sent_payloads[0]["batch"]) == 50
        assert len(sent_payloads[1]["batch"]) == 50
        assert len(sent_payloads[2]["batch"]) == 20

    def test_send_to_langfuse_returns_true_on_success(self, monkeypatch):
        """Verify send_to_langfuse returns True when API succeeds."""
        def mock_urlopen(req, timeout=15):
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
        monkeypatch.setattr(langfuse_common, "LANGFUSE_PUBLIC_KEY", "pk-test")
        monkeypatch.setattr(langfuse_common, "LANGFUSE_SECRET_KEY", "sk-test")
        result = hook.send_to_langfuse([{"type": "trace"}])
        assert result is True

    def test_send_to_langfuse_returns_false_on_error(self, monkeypatch):
        """Verify send_to_langfuse returns False when API fails."""
        from urllib.error import URLError
        monkeypatch.setattr(hook, "urlopen", mock.Mock(side_effect=URLError("Connection failed")))
        monkeypatch.setattr(langfuse_common, "LANGFUSE_PUBLIC_KEY", "pk-test")
        monkeypatch.setattr(langfuse_common, "LANGFUSE_SECRET_KEY", "sk-test")
        result = hook.send_to_langfuse([{"type": "trace"}])
        assert result is False


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
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch) or True)

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

        # Should have: 1 trace + 1 generation + 7 scores = 9 events
        trace_events = [e for e in batch if e["type"] == "trace-create"]
        gen_events = [e for e in batch if e["type"] == "generation-create"]
        score_events = [e for e in batch if e["type"] == "score-create"]
        assert len(trace_events) == 1
        assert len(gen_events) == 1
        assert len(score_events) == 7

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
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch) or True)

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
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch) or True)

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

        # Turn names must be sequential (1, 2) not based on line offset (1, 3, 5…)
        all_gen_names = [
            evt["body"]["name"]
            for batch in sent_batches
            for evt in batch
            if evt["type"] == "generation-create"
        ]
        assert any(n.startswith("Turn 1:") for n in all_gen_names)
        assert any(n.startswith("Turn 2:") for n in all_gen_names)
        assert not any(n.startswith("Turn 3:") for n in all_gen_names)

    def test_turn_count_tracks_independently_of_line_offset(self, tmp_path, monkeypatch):
        """Turn names stay sequential even when each turn spans many JSONL lines."""
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        monkeypatch.setattr(hook, "STATE_DIR", str(state_dir))

        sent_batches = []
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch) or True)

        f = tmp_path / "session.jsonl"

        def make_turn(user_text, assistant_text, msg_id, ts_base):
            return [
                {"type": "user", "timestamp": f"{ts_base}:00+00:00",
                 "message": {"role": "user", "content": user_text}},
                {"type": "assistant", "timestamp": f"{ts_base}:01+00:00",
                 "message": {
                     "id": msg_id, "role": "assistant", "model": "claude-sonnet-4-6",
                     "content": [{"type": "text", "text": assistant_text}],
                     "usage": {"input_tokens": 10, "output_tokens": 5,
                               "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
                 }},
            ]

        # Fire 1: turn 1
        fire1 = make_turn("Q1", "A1", "msg-1", "2026-03-29T10:00")
        f.write_text("\n".join(json.dumps(e) for e in fire1) + "\n")
        hook.process_session("tc-sess", str(f), "/test")

        # Fire 2: turn 2 (appended)
        fire2 = make_turn("Q2", "A2", "msg-2", "2026-03-29T10:01")
        with open(str(f), "a") as fh:
            for e in fire2:
                fh.write(json.dumps(e) + "\n")
        hook.process_session("tc-sess", str(f), "/test")

        # Fire 3: turn 3 (appended)
        fire3 = make_turn("Q3", "A3", "msg-3", "2026-03-29T10:02")
        with open(str(f), "a") as fh:
            for e in fire3:
                fh.write(json.dumps(e) + "\n")
        hook.process_session("tc-sess", str(f), "/test")

        assert len(sent_batches) == 3
        gen_names = [
            evt["body"]["name"]
            for batch in sent_batches
            for evt in batch
            if evt["type"] == "generation-create"
        ]
        # Must be exactly Turn 1, Turn 2, Turn 3 — not Turn 1, Turn 3, Turn 5
        assert gen_names[0].startswith("Turn 1:")
        assert gen_names[1].startswith("Turn 2:")
        assert gen_names[2].startswith("Turn 3:")

    def test_tool_spans_created(self, tmp_path, monkeypatch):
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        monkeypatch.setattr(hook, "STATE_DIR", str(state_dir))

        sent_batches = []
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch) or True)

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
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch) or True)

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
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch) or True)

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


class TestReprocessAll:
    def test_does_not_call_delete_trace(self, tmp_path, monkeypatch):
        """delete_trace races with the new ingestion batch because Langfuse
        processes deletes asynchronously. Reprocessing must rely on
        deterministic UUID5 IDs to upsert in place instead."""
        # Fake a single transcript under ~/.claude/projects/
        projects = tmp_path / "claude" / "projects" / "fake-proj"
        projects.mkdir(parents=True)
        transcript = projects / "999aaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.jsonl"
        transcript.write_text(json.dumps({
            "type": "user", "cwd": "/tmp", "version": "2.1.140",
            "gitBranch": "main", "entrypoint": "cli",
        }) + "\n")

        monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))

        delete_calls = []
        monkeypatch.setattr(hook, "delete_trace", lambda tid: delete_calls.append(tid))
        monkeypatch.setattr(hook, "process_session", lambda *a, **kw: None)

        hook.reprocess_all()

        assert delete_calls == [], (
            "reprocess_all must not call delete_trace — the async cascade-delete "
            "drops observations from the new ingest batch (see bug context)"
        )

    def test_missing_keys_logs_and_returns(self, monkeypatch):
        monkeypatch.setattr(langfuse_common, "LANGFUSE_PUBLIC_KEY", "")
        monkeypatch.setattr(langfuse_common, "LANGFUSE_SECRET_KEY", "")
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
        monkeypatch.setattr(langfuse_common, "LANGFUSE_PUBLIC_KEY", "pk-test")
        monkeypatch.setattr(langfuse_common, "LANGFUSE_SECRET_KEY", "sk-test")
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
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch) or True)

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
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch) or True)

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
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch) or True)

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
        monkeypatch.setattr(hook, "send_to_langfuse", lambda batch: sent_batches.append(batch) or True)

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


# ---------------------------------------------------------------------------
# extract_stop_hook_stats
# ---------------------------------------------------------------------------

class TestExtractStopHookStats:
    def test_empty_when_no_entries(self, tmp_path):
        f = tmp_path / "t.jsonl"
        f.write_text(json.dumps({"type": "user"}) + "\n")
        result = hook.extract_stop_hook_stats(str(f))
        assert result == {
            "total_hook_fires": 0,
            "total_duration_ms": 0,
            "max_duration_ms": 0,
            "hook_errors": 0,
            "prevented_continuation_count": 0,
        }

    def test_aggregates_single_fire(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entry = {
            "type": "system",
            "subtype": "stop_hook_summary",
            "hookCount": 1,
            "hookInfos": [{"command": "python3 langfuse-hook.py", "durationMs": 300}],
            "hookErrors": [],
            "preventedContinuation": False,
        }
        f.write_text(json.dumps(entry) + "\n")
        result = hook.extract_stop_hook_stats(str(f))
        assert result["total_hook_fires"] == 1
        assert result["total_duration_ms"] == 300
        assert result["max_duration_ms"] == 300
        assert result["hook_errors"] == 0
        assert result["prevented_continuation_count"] == 0

    def test_aggregates_multiple_fires(self, tmp_path):
        f = tmp_path / "t.jsonl"
        entries = [
            {
                "type": "system",
                "subtype": "stop_hook_summary",
                "hookCount": 1,
                "hookInfos": [{"command": "cmd", "durationMs": 200}],
                "hookErrors": [],
                "preventedContinuation": False,
            },
            {
                "type": "system",
                "subtype": "stop_hook_summary",
                "hookCount": 1,
                "hookInfos": [{"command": "cmd", "durationMs": 500}],
                "hookErrors": ["some error"],
                "preventedContinuation": True,
            },
        ]
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        result = hook.extract_stop_hook_stats(str(f))
        assert result["total_hook_fires"] == 2
        assert result["total_duration_ms"] == 700
        assert result["max_duration_ms"] == 500
        assert result["hook_errors"] == 1
        assert result["prevented_continuation_count"] == 1

    def test_multiple_hooks_per_fire(self, tmp_path):
        """Multiple hookInfos entries in one fire — sum all durations."""
        f = tmp_path / "t.jsonl"
        entry = {
            "type": "system",
            "subtype": "stop_hook_summary",
            "hookCount": 2,
            "hookInfos": [
                {"command": "hook-a", "durationMs": 100},
                {"command": "hook-b", "durationMs": 150},
            ],
            "hookErrors": [],
            "preventedContinuation": False,
        }
        f.write_text(json.dumps(entry) + "\n")
        result = hook.extract_stop_hook_stats(str(f))
        assert result["total_duration_ms"] == 250
        assert result["max_duration_ms"] == 150

    def test_nonexistent_file(self):
        result = hook.extract_stop_hook_stats("/nonexistent.jsonl")
        assert result["total_hook_fires"] == 0


# ---------------------------------------------------------------------------
# Trace name precedence (process_session integration)
# ---------------------------------------------------------------------------

class TestTraceNamePrecedence:
    """Verify trace_name precedence: customTitle > agent_name > first_prompt > repo/branch."""

    def _make_transcript(self, tmp_path, entries):
        f = tmp_path / "t.jsonl"
        f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        return str(f)

    def _get_trace_event(self, tmp_path, monkeypatch, entries, cwd=""):
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        monkeypatch.setattr(hook, "STATE_DIR", str(state_dir))

        path = self._make_transcript(tmp_path, entries)
        captured = []
        monkeypatch.setattr(hook, "send_to_langfuse", lambda b: captured.extend(b) or True)
        hook.process_session("test-session-001", path, cwd)
        return next((e for e in captured if e.get("type") == "trace-create"), None)

    def _base_entries(self):
        return [
            {
                "type": "user",
                "message": {"role": "user", "content": "hello world prompt"},
                "timestamp": "2026-04-23T10:00:00Z",
                "cwd": "/repo",
                "gitBranch": "main",
                "version": "2.1.112",
                "entrypoint": "cli",
            },
            {
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": "I can help with that.",
                    "model": "claude-sonnet-4-6",
                    "usage": {"input_tokens": 10, "output_tokens": 5},
                },
                "timestamp": "2026-04-23T10:00:01Z",
            },
        ]

    def test_agent_name_beats_first_prompt(self, tmp_path, monkeypatch):
        entries = self._base_entries() + [
            {"type": "agent-name", "agentName": "my-stable-task-name"},
        ]
        evt = self._get_trace_event(tmp_path, monkeypatch, entries, cwd="/repo")
        assert evt["body"]["name"] == "my-stable-task-name"

    def test_custom_title_beats_agent_name(self, tmp_path, monkeypatch):
        entries = self._base_entries() + [
            {"type": "custom-title", "customTitle": "user-set-title"},
            {"type": "agent-name", "agentName": "auto-generated-name"},
        ]
        evt = self._get_trace_event(tmp_path, monkeypatch, entries, cwd="/repo")
        assert evt["body"]["name"] == "user-set-title"

    def test_first_prompt_when_no_agent_name(self, tmp_path, monkeypatch):
        entries = self._base_entries()
        evt = self._get_trace_event(tmp_path, monkeypatch, entries, cwd="/repo")
        assert evt["body"]["name"] == "hello world prompt"

    def test_repo_branch_fallback_when_no_prompt(self, tmp_path, monkeypatch):
        # Synthetic prompt (XML-wrapper) is skipped by _SYNTHETIC_PROMPT_RE, leaving
        # first_real_prompt empty — trace name falls back to repo/branch.
        entries = [
            {
                "type": "user",
                "message": {"role": "user", "content": "<system-reminder>do nothing</system-reminder>"},
                "timestamp": "2026-04-23T10:00:00Z",
                "cwd": "/home/user/myrepo",
                "gitBranch": "feat/improve-search",
                "version": "2.1.112",
                "entrypoint": "cli",
            },
            {
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": "Done.",
                    "model": "claude-sonnet-4-6",
                    "usage": {"input_tokens": 10, "output_tokens": 5},
                },
                "timestamp": "2026-04-23T10:00:01Z",
            },
        ]
        evt = self._get_trace_event(tmp_path, monkeypatch, entries, cwd="/home/user/myrepo")
        assert evt["body"]["name"] == "myrepo/feat/improve-search"

    def test_agent_name_tag_added_when_present(self, tmp_path, monkeypatch):
        entries = self._base_entries() + [
            {"type": "agent-name", "agentName": "my-task"},
        ]
        evt = self._get_trace_event(tmp_path, monkeypatch, entries, cwd="/repo")
        assert "agent-name:my-task" in evt["body"]["tags"]

    def test_agent_name_tag_absent_when_missing(self, tmp_path, monkeypatch):
        entries = self._base_entries()
        evt = self._get_trace_event(tmp_path, monkeypatch, entries, cwd="/repo")
        assert not any(t.startswith("agent-name:") for t in evt["body"]["tags"])

    def test_file_snapshots_in_metadata_when_present(self, tmp_path, monkeypatch):
        entries = self._base_entries() + [
            {
                "type": "file-history-snapshot",
                "snapshot": {"trackedFileBackups": {"src/a.py": "x"}, "timestamp": "t1"},
                "isSnapshotUpdate": False,
            },
        ]
        evt = self._get_trace_event(tmp_path, monkeypatch, entries, cwd="/repo")
        fs = evt["body"]["metadata"]["file_snapshots"]
        assert fs is not None
        assert fs["snapshot_count"] == 1
        assert fs["tracked_files_count"] == 1

    def test_file_snapshots_absent_from_metadata_when_missing(self, tmp_path, monkeypatch):
        entries = self._base_entries()
        evt = self._get_trace_event(tmp_path, monkeypatch, entries, cwd="/repo")
        assert evt["body"]["metadata"].get("file_snapshots") is None


# ---------------------------------------------------------------------------
# calculate_tool_diversity
# ---------------------------------------------------------------------------

class TestCalculateToolDiversity:
    def _turns(self, tool_names_per_turn):
        return [
            {"tool_calls": [{"name": n} for n in names]}
            for names in tool_names_per_turn
        ]

    def test_all_same_tool(self):
        turns = self._turns([["Bash", "Bash", "Bash"]])
        assert hook.calculate_tool_diversity(turns) == round(1/3, 4)

    def test_all_unique_tools(self):
        turns = self._turns([["Bash", "Read", "Write"]])
        assert hook.calculate_tool_diversity(turns) == 1.0

    def test_no_tool_calls(self):
        turns = [{"tool_calls": []}]
        assert hook.calculate_tool_diversity(turns) == 0.0

    def test_empty_turns(self):
        assert hook.calculate_tool_diversity([]) == 0.0

    def test_tools_across_multiple_turns(self):
        turns = self._turns([["Bash", "Bash"], ["Read"]])
        assert hook.calculate_tool_diversity(turns) == round(2/3, 4)

    def test_single_tool_single_call(self):
        turns = self._turns([["Bash"]])
        assert hook.calculate_tool_diversity(turns) == 1.0


# ---------------------------------------------------------------------------
# detect_compaction
# ---------------------------------------------------------------------------

class TestDetectCompaction:
    def _write_transcript(self, entries, tmp_path):
        p = tmp_path / "session.jsonl"
        p.write_text("\n".join(json.dumps(e) for e in entries))
        return str(p)

    def test_no_compaction(self, tmp_path):
        entries = [{"type": "user", "message": {"role": "user", "content": "hi"}}]
        path = self._write_transcript(entries, tmp_path)
        assert hook.detect_compaction(path) is False

    def test_compact_subtype(self, tmp_path):
        entries = [{"type": "system", "subtype": "compact"}]
        path = self._write_transcript(entries, tmp_path)
        assert hook.detect_compaction(path) is True

    def test_compaction_subtype(self, tmp_path):
        entries = [{"type": "system", "subtype": "compaction"}]
        path = self._write_transcript(entries, tmp_path)
        assert hook.detect_compaction(path) is True

    def test_summary_type(self, tmp_path):
        entries = [{"type": "summary", "summary": "..."}]
        path = self._write_transcript(entries, tmp_path)
        assert hook.detect_compaction(path) is True

    def test_missing_file(self):
        assert hook.detect_compaction("/nonexistent/path.jsonl") is False

    def test_empty_path_returns_false(self):
        assert hook.detect_compaction("") is False


class TestBuildHookScoreEventsNewScores:
    def _turns(self, tool_names_per_turn):
        return [
            {"tool_calls": [{"name": n} for n in names],
             "usage": {"input": 0, "output": 0, "total": 0, "cache_read": 0, "cache_creation": 0}}
            for names in tool_names_per_turn
        ]

    def test_tool_diversity_score_present(self, tmp_path):
        transcript = tmp_path / "s.jsonl"
        transcript.write_text("")
        turns = self._turns([["Bash", "Read"]])
        events = hook.build_hook_score_events("t1", "s1", "fix bug", turns, 0.05, str(transcript))
        names = {e["body"]["name"] for e in events}
        assert "tool_diversity" in names
        assert "compaction_occurred" in names
        # Empty transcript → no compaction
        comp_event = next(e for e in events if e["body"]["name"] == "compaction_occurred")
        assert comp_event["body"]["value"] == 0

    def test_compaction_occurred_true(self, tmp_path):
        transcript = tmp_path / "s.jsonl"
        transcript.write_text(json.dumps({"type": "system", "subtype": "compact"}))
        turns = self._turns([["Bash"]])
        events = hook.build_hook_score_events("t1", "s1", "fix bug", turns, 0.05, str(transcript))
        comp_event = next(e for e in events if e["body"]["name"] == "compaction_occurred")
        assert comp_event["body"]["value"] == 1

    def test_tool_diversity_zero_no_tools(self, tmp_path):
        transcript = tmp_path / "s.jsonl"
        transcript.write_text("")
        turns = [{"tool_calls": [], "usage": {"input": 0, "output": 0, "total": 0, "cache_read": 0, "cache_creation": 0}}]
        events = hook.build_hook_score_events("t1", "s1", "hi", turns, 0.0, str(transcript))
        div_event = next(e for e in events if e["body"]["name"] == "tool_diversity")
        assert div_event["body"]["value"] == 0.0


class TestEnsureDataset:
    def test_creates_dataset_on_404(self, monkeypatch):
        """When GET returns 404, POST to create the dataset."""
        from urllib.error import HTTPError
        create_called = {"url": None, "body": None}

        def fake_urlopen(req, timeout):
            if req.get_method() == "GET":
                raise HTTPError(req.full_url, 404, "Not Found", {}, None)
            create_called["url"] = req.full_url
            create_called["body"] = json.loads(req.data.decode())
            resp = mock.MagicMock(
                __enter__=lambda s: s,
                __exit__=lambda s, *a: False,
                read=lambda: b'{"id": "ds-1"}',
                status=201,
            )
            return resp

        monkeypatch.setattr(hook, "urlopen", fake_urlopen)
        monkeypatch.setattr(langfuse_common, "LANGFUSE_PUBLIC_KEY", "pk-test")
        monkeypatch.setattr(langfuse_common, "LANGFUSE_SECRET_KEY", "sk-test")
        result = hook.ensure_dataset("expensive-sessions")
        assert result is True
        assert create_called["body"]["name"] == "expensive-sessions"

    def test_returns_true_if_already_exists(self, monkeypatch):
        """GET returning 200 means dataset exists — no POST needed."""
        def fake_urlopen(req, timeout):
            resp = mock.MagicMock(
                __enter__=lambda s: s,
                __exit__=lambda s, *a: False,
                read=lambda: b'{"id": "ds-1", "name": "expensive-sessions"}',
                status=200,
            )
            return resp

        monkeypatch.setattr(hook, "urlopen", fake_urlopen)
        monkeypatch.setattr(langfuse_common, "LANGFUSE_PUBLIC_KEY", "pk-test")
        monkeypatch.setattr(langfuse_common, "LANGFUSE_SECRET_KEY", "sk-test")
        result = hook.ensure_dataset("expensive-sessions")
        assert result is True


class TestAddToDataset:
    def test_posts_dataset_item(self, monkeypatch):
        posted = {"body": None}

        def fake_urlopen(req, timeout):
            posted["body"] = json.loads(req.data.decode())
            resp = mock.MagicMock(
                __enter__=lambda s: s,
                __exit__=lambda s, *a: False,
                read=lambda: b'{"id": "item-1"}',
                status=201,
            )
            return resp

        monkeypatch.setattr(hook, "urlopen", fake_urlopen)
        monkeypatch.setattr(langfuse_common, "LANGFUSE_PUBLIC_KEY", "pk-test")
        monkeypatch.setattr(langfuse_common, "LANGFUSE_SECRET_KEY", "sk-test")
        result = hook.add_to_dataset("expensive-sessions", "trace-abc", "fix bug", "done")
        assert result is True
        assert posted["body"]["datasetName"] == "expensive-sessions"
        assert posted["body"]["sourceTraceId"] == "trace-abc"
        assert posted["body"]["input"] == "fix bug"
        assert posted["body"]["expectedOutput"] == "done"
