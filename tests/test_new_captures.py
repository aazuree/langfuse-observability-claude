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


class TestExtractInterrupts:
    def test_none_when_no_interrupts(self, tmp_path):
        assert hook.extract_interrupts(_write([{"type": "user"}], tmp_path)) is None

    def test_nonexistent_file(self):
        assert hook.extract_interrupts("/nonexistent.jsonl") is None

    def test_counts_user_interrupts(self, tmp_path):
        entries = [
            {"type": "user", "interruptedMessageId": "msg_01A",
             "message": {"role": "user", "content": "stop"}},
            {"type": "user", "message": {"role": "user", "content": "normal"}},
            {"type": "user", "interruptedMessageId": "msg_01B",
             "message": {"role": "user", "content": "stop again"}},
        ]
        out = hook.extract_interrupts(_write(entries, tmp_path))
        assert out == {"count": 2}

    def test_ignores_empty_interrupted_id(self, tmp_path):
        entries = [{"type": "user", "interruptedMessageId": "",
                    "message": {"role": "user", "content": "x"}}]
        assert hook.extract_interrupts(_write(entries, tmp_path)) is None


class TestNewCapturesWiring:
    def test_metadata_and_tags_present(self, tmp_path, monkeypatch):
        entries = [
            {"type": "user", "timestamp": "2026-06-07T00:00:00Z", "cwd": "/x/repo",
             "version": "2.1.168", "message": {"role": "user", "content": "hi"}},
            {"type": "permission-mode", "permissionMode": "default"},
            {"type": "permission-mode", "permissionMode": "acceptEdits"},
            {"type": "bridge-session", "bridgeSessionId": "cse_019X"},
            {"type": "system", "subtype": "bridge_status",
             "url": "https://claude.ai/code/session_019X"},
            {"type": "system", "subtype": "compact_boundary",
             "timestamp": "2026-06-07T00:01:00Z",
             "compactMetadata": {"trigger": "manual", "preTokens": 100,
                                 "postTokens": 10, "durationMs": 5}},
            {"type": "assistant", "timestamp": "2026-06-07T00:00:05Z",
             "message": {"role": "assistant", "id": "m1", "model": "claude-opus-4-8",
                         "stop_reason": "end_turn",
                         "content": [{"type": "text", "text": "hello"}],
                         "usage": {"input_tokens": 5, "output_tokens": 3}}},
        ]
        transcript = tmp_path / "sess.jsonl"
        transcript.write_text("\n".join(json.dumps(e) for e in entries) + "\n")

        captured = {}
        monkeypatch.setattr(hook, "send_to_langfuse",
                            lambda batch: captured.setdefault("batch", batch) or True)
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))

        hook.process_session("sess", str(transcript), "/x/repo")

        trace = next(e for e in captured["batch"] if e["type"] == "trace-create")
        md = trace["body"]["metadata"]
        tags = trace["body"]["tags"]

        assert md["compaction"]["count"] == 1
        assert md["compaction"]["total_tokens_reclaimed"] == 90
        assert md["remote_control"] == {"bridge_session_id": "cse_019X",
                                        "url": "https://claude.ai/code/session_019X"}
        assert md["permission_timeline"]["transition_count"] == 1
        # backward-compat fields unchanged
        assert md["compaction_occurred"] is True
        assert md["permission_mode"] == "acceptEdits"

        assert "compacted" in tags
        assert "compact-trigger:manual" in tags
        assert "remote-control" in tags
        assert "permission:acceptEdits" in tags

    def test_interrupts_metadata_and_tag(self, tmp_path, monkeypatch):
        entries = [
            {"type": "user", "timestamp": "2026-06-18T00:00:00Z", "cwd": "/x/repo",
             "message": {"role": "user", "content": "do a thing"}},
            {"type": "assistant", "timestamp": "2026-06-18T00:00:02Z",
             "message": {"role": "assistant", "id": "m1", "model": "claude-opus-4-8",
                         "stop_reason": "end_turn",
                         "content": [{"type": "text", "text": "working"}],
                         "usage": {"input_tokens": 5, "output_tokens": 3}}},
            {"type": "user", "timestamp": "2026-06-18T00:00:03Z",
             "interruptedMessageId": "msg_01A",
             "message": {"role": "user", "content": "stop"}},
        ]
        transcript = tmp_path / "sess.jsonl"
        transcript.write_text("\n".join(json.dumps(e) for e in entries) + "\n")

        captured = {}
        monkeypatch.setattr(hook, "send_to_langfuse",
                            lambda batch: captured.setdefault("batch", batch) or True)
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))

        hook.process_session("sess-int", str(transcript), "/x/repo")

        trace = next(e for e in captured["batch"] if e["type"] == "trace-create")
        assert trace["body"]["metadata"]["interrupts"] == {"count": 1}
        assert "has-interrupts" in trace["body"]["tags"]
