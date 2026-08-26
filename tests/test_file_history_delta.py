# tests/test_file_history_delta.py
"""Tests for file-history-delta capture (CC 2.1.2xx).

`file-history-snapshot` (already captured) is the periodic full picture;
`file-history-delta` is the per-edit record that names the single file touched
and its backup version. Together they give edit depth per session.

File paths are counted, never stored — same rule as extract_file_history_stats.
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


def _delta(path, version=1):
    return {
        "type": "file-history-delta",
        "messageId": "m-1",
        "snapshotMessageId": "s-1",
        "trackingPath": path,
        "backup": {
            "backupFileName": "abc@v%d" % version,
            "version": version,
            "backupTime": "2026-08-26T07:48:28.455Z",
            "realParentDir": os.path.dirname(path),
        },
        "timestamp": "2026-08-26T07:48:28.455Z",
    }


class TestFileHistoryDelta:
    def test_counts_deltas(self, tmp_path):
        path = _write(tmp_path, [
            _delta("/repo/a.py"),
            _delta("/repo/b.py"),
        ])

        stats = hook.extract_file_history_stats(path)

        assert stats["delta_count"] == 2

    def test_counts_unique_edited_files_not_total_edits(self, tmp_path):
        path = _write(tmp_path, [
            _delta("/repo/a.py", version=1),
            _delta("/repo/a.py", version=2),
            _delta("/repo/b.py", version=1),
        ])

        stats = hook.extract_file_history_stats(path)

        assert stats["edited_files_count"] == 2

    def test_reports_deepest_backup_version(self, tmp_path):
        path = _write(tmp_path, [
            _delta("/repo/a.py", version=1),
            _delta("/repo/a.py", version=7),
        ])

        stats = hook.extract_file_history_stats(path)

        assert stats["max_backup_version"] == 7

    def test_file_paths_are_never_stored(self, tmp_path):
        path = _write(tmp_path, [_delta("/repo/secret-project/creds.py")])

        stats = hook.extract_file_history_stats(path)

        assert "secret-project" not in json.dumps(stats)

    def test_zero_deltas_when_none_present(self, tmp_path):
        path = _write(tmp_path, [{"type": "user", "message": {"content": "hi"}}])

        stats = hook.extract_file_history_stats(path)

        assert stats["delta_count"] == 0
        assert stats["edited_files_count"] == 0
        assert stats["max_backup_version"] == 0

    def test_malformed_backup_does_not_crash(self, tmp_path):
        bad = _delta("/repo/a.py")
        bad["backup"] = "not-a-dict"
        path = _write(tmp_path, [bad])

        stats = hook.extract_file_history_stats(path)

        assert stats["delta_count"] == 1
        assert stats["max_backup_version"] == 0

    def test_delta_without_tracking_path_still_counted(self, tmp_path):
        bad = _delta("/repo/a.py")
        del bad["trackingPath"]
        path = _write(tmp_path, [bad])

        stats = hook.extract_file_history_stats(path)

        assert stats["delta_count"] == 1
        assert stats["edited_files_count"] == 0

    def test_snapshot_fields_still_reported(self, tmp_path):
        """Existing snapshot behaviour must not regress."""
        path = _write(tmp_path, [
            {
                "type": "file-history-snapshot",
                "snapshot": {"trackedFileBackups": {"/repo/a.py": {}, "/repo/b.py": {}}},
            },
            _delta("/repo/c.py"),
        ])

        stats = hook.extract_file_history_stats(path)

        assert stats["snapshot_count"] == 1
        assert stats["tracked_files_count"] == 2
