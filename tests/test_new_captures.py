"""Tests for v2.1.168 new-capture extractors: compaction, bridge, permission timeline."""
import importlib.util
import json
import os

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook",
    os.path.join(os.path.dirname(__file__), "..", "langfuse-hook.py"),
)
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)


def _write(entries, tmp_path):
    f = tmp_path / "session.jsonl"
    f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
    return str(f)


class TestExtractCompaction:
    def test_none_when_no_compaction(self, tmp_path):
        path = _write([{"type": "user"}], tmp_path)
        assert hook.extract_compaction(path) is None

    def test_nonexistent_file(self):
        assert hook.extract_compaction("/nonexistent.jsonl") is None

    def test_single_manual(self, tmp_path):
        entries = [{
            "type": "system", "subtype": "compact_boundary",
            "timestamp": "2026-06-01T13:02:31.453Z",
            "compactMetadata": {"trigger": "manual", "preTokens": 102632,
                                "postTokens": 10136, "durationMs": 668061},
        }]
        out = hook.extract_compaction(_write(entries, tmp_path))
        assert out["count"] == 1
        assert out["triggers"] == {"manual": 1}
        assert out["total_tokens_reclaimed"] == 92496
        assert out["total_pre_tokens"] == 102632
        assert out["total_post_tokens"] == 10136
        assert out["total_duration_ms"] == 668061
        assert out["events"][0] == {
            "trigger": "manual", "pre_tokens": 102632, "post_tokens": 10136,
            "tokens_reclaimed": 92496, "duration_ms": 668061,
            "timestamp": "2026-06-01T13:02:31.453Z",
        }

    def test_mixed_manual_and_auto(self, tmp_path):
        entries = [
            {"type": "system", "subtype": "compact_boundary",
             "compactMetadata": {"trigger": "manual", "preTokens": 100,
                                 "postTokens": 10, "durationMs": 5}},
            {"type": "system", "subtype": "compact_boundary",
             "compactMetadata": {"trigger": "auto", "preTokens": 200,
                                 "postTokens": 20, "durationMs": 7}},
        ]
        out = hook.extract_compaction(_write(entries, tmp_path))
        assert out["count"] == 2
        assert out["triggers"] == {"manual": 1, "auto": 1}
        assert out["total_tokens_reclaimed"] == 270
        assert out["total_duration_ms"] == 12

    def test_legacy_summary_only(self, tmp_path):
        entries = [{"type": "summary", "summary": "old", "timestamp": "2026-01-01T00:00:00Z"}]
        out = hook.extract_compaction(_write(entries, tmp_path))
        assert out["count"] == 1
        assert out["triggers"] == {}
        assert out["total_tokens_reclaimed"] == 0
        assert out["events"][0] == {"trigger": "legacy", "timestamp": "2026-01-01T00:00:00Z"}

    def test_missing_post_tokens_omits_reclaimed(self, tmp_path):
        entries = [{"type": "system", "subtype": "compact_boundary",
                    "compactMetadata": {"trigger": "auto", "preTokens": 100, "durationMs": 3}}]
        out = hook.extract_compaction(_write(entries, tmp_path))
        assert "tokens_reclaimed" not in out["events"][0]
        assert out["total_tokens_reclaimed"] == 0


class TestExtractBridge:
    def test_none_when_absent(self, tmp_path):
        assert hook.extract_bridge(_write([{"type": "user"}], tmp_path)) is None

    def test_nonexistent_file(self):
        assert hook.extract_bridge("/nonexistent.jsonl") is None

    def test_both_fields(self, tmp_path):
        entries = [
            {"type": "bridge-session", "bridgeSessionId": "cse_019X", "lastSequenceNum": 0},
            {"type": "system", "subtype": "bridge_status",
             "content": "/remote-control is active",
             "url": "https://claude.ai/code/session_019X"},
        ]
        out = hook.extract_bridge(_write(entries, tmp_path))
        assert out == {"bridge_session_id": "cse_019X",
                       "url": "https://claude.ai/code/session_019X"}

    def test_bridge_session_only(self, tmp_path):
        entries = [{"type": "bridge-session", "bridgeSessionId": "cse_abc"}]
        out = hook.extract_bridge(_write(entries, tmp_path))
        assert out == {"bridge_session_id": "cse_abc"}

    def test_bridge_status_only(self, tmp_path):
        entries = [{"type": "system", "subtype": "bridge_status",
                    "url": "https://claude.ai/code/session_z"}]
        out = hook.extract_bridge(_write(entries, tmp_path))
        assert out == {"url": "https://claude.ai/code/session_z"}


class TestExtractPermissionTimeline:
    def test_none_when_no_entries(self, tmp_path):
        assert hook.extract_permission_timeline(_write([{"type": "user"}], tmp_path)) is None

    def test_nonexistent_file(self):
        assert hook.extract_permission_timeline("/nonexistent.jsonl") is None

    def test_single_mode_zero_transitions(self, tmp_path):
        entries = [{"type": "permission-mode", "permissionMode": "default"}]
        out = hook.extract_permission_timeline(_write(entries, tmp_path))
        assert out["modes_used"] == ["default"]
        assert out["sequence"] == ["default"]
        assert out["transition_count"] == 0
        assert out["ever_bypass"] is False
        assert out["ever_accept_edits"] is False

    def test_collapses_consecutive_dups(self, tmp_path):
        entries = [
            {"type": "permission-mode", "permissionMode": "default"},
            {"type": "permission-mode", "permissionMode": "default"},
            {"type": "permission-mode", "permissionMode": "acceptEdits"},
            {"type": "permission-mode", "permissionMode": "default"},
        ]
        out = hook.extract_permission_timeline(_write(entries, tmp_path))
        assert out["sequence"] == ["default", "acceptEdits", "default"]
        assert out["transition_count"] == 2
        assert out["modes_used"] == ["acceptEdits", "default"]
        assert out["ever_accept_edits"] is True

    def test_bypass_flag(self, tmp_path):
        entries = [
            {"type": "permission-mode", "permissionMode": "default"},
            {"type": "permission-mode", "permissionMode": "bypassPermissions"},
        ]
        out = hook.extract_permission_timeline(_write(entries, tmp_path))
        assert out["ever_bypass"] is True
