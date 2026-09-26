# tests/test_session_lock.py
"""Two fires for one session must not interleave.

With `"async": true` on the Stop hook, a fire can start while the previous
one for the same session is still sending. Both would read the same saved
offset, send overlapping batches, and race on the state files.
"""
import fcntl
import importlib.util
import json
import os
import threading
import time

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook", os.path.join(os.path.dirname(__file__), "..", "langfuse-hook.py"))
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)


def _transcript(tmp_path):
    path = tmp_path / "s.jsonl"
    path.write_text("\n".join(json.dumps(e) for e in [
        {"type": "user", "timestamp": "2026-09-20T10:00:00+00:00",
         "message": {"role": "user", "content": "hi"}},
        {"type": "assistant", "timestamp": "2026-09-20T10:00:01+00:00",
         "message": {"id": "m1", "role": "assistant", "model": "claude-sonnet-5",
                     "content": [{"type": "text", "text": "hello"}],
                     "usage": {"input_tokens": 1, "output_tokens": 1,
                               "cache_read_input_tokens": 0,
                               "cache_creation_input_tokens": 0}}},
    ]) + "\n")
    return str(path)


def _hold_lock(state_dir, sid):
    os.makedirs(state_dir, exist_ok=True)
    fd = os.open(os.path.join(state_dir, f"{sid}.lock"), os.O_CREAT | os.O_RDWR)
    fcntl.flock(fd, fcntl.LOCK_EX)
    return fd


def test_waits_for_a_fire_already_running(tmp_path, monkeypatch):
    state_dir = str(tmp_path / "state")
    monkeypatch.setattr(hook, "STATE_DIR", state_dir)
    sent = []
    monkeypatch.setattr(hook, "send_to_langfuse", lambda b: sent.append(b) or True)
    path = _transcript(tmp_path)

    fd = _hold_lock(state_dir, "s")
    worker = threading.Thread(target=hook.process_session, args=("s", path, "/w"))
    worker.start()
    time.sleep(0.3)
    assert sent == [], "processed while another fire held the session lock"

    os.close(fd)  # releases the lock
    worker.join(timeout=5)
    assert len(sent) == 1


def test_gives_up_after_the_timeout_and_logs(tmp_path, monkeypatch):
    state_dir = str(tmp_path / "state")
    monkeypatch.setattr(hook, "STATE_DIR", state_dir)
    monkeypatch.setattr(hook, "SESSION_LOCK_TIMEOUT_S", 0.2)
    sent = []
    monkeypatch.setattr(hook, "send_to_langfuse", lambda b: sent.append(b) or True)
    logged = []
    monkeypatch.setattr(hook, "log", logged.append)

    fd = _hold_lock(state_dir, "s")
    try:
        hook.process_session("s", _transcript(tmp_path), "/w")
    finally:
        os.close(fd)
    assert sent == []
    assert any("[WARN]" in m and "lock" in m for m in logged)
