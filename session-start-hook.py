#!/usr/bin/env python3
"""Claude Code SessionStart + StopFailure hooks -> Langfuse.

SessionStart: Creates an early trace with source/model tags so the trace exists
              before the first Stop hook fires. Stop hook upserts the same trace ID.
StopFailure:  Updates the trace with a stop-failure tag and last API error metadata
              when a turn ends due to an API error.

Environment variables:
  LANGFUSE_PUBLIC_KEY  - Langfuse project public key
  LANGFUSE_SECRET_KEY  - Langfuse project secret key
  LANGFUSE_HOST        - Langfuse base URL (default: http://localhost:3100)
"""

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from urllib.error import URLError
from urllib.request import Request, urlopen

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from langfuse_common import log as common_log, make_auth_header

LANGFUSE_HOST = os.environ.get("LANGFUSE_HOST", "http://localhost:3100")
LANGFUSE_PUBLIC_KEY = os.environ.get("LANGFUSE_PUBLIC_KEY", "")
LANGFUSE_SECRET_KEY = os.environ.get("LANGFUSE_SECRET_KEY", "")
INGESTION_URL = f"{LANGFUSE_HOST}/api/public/ingestion"

LOG_FILE = os.path.expanduser("~/.claude/langfuse-hook.log")


def log(msg: str) -> None:
    common_log(LOG_FILE, msg)


def derive_model_family(model: str) -> str:
    """Return 'opus', 'sonnet', 'haiku', or 'unknown' from a model ID string."""
    m = model.lower()
    if not m:
        return "unknown"
    if "opus" in m:
        return "opus"
    if "haiku" in m:
        return "haiku"
    if "sonnet" in m:
        return "sonnet"
    return "unknown"


def _iter_transcript(transcript_path: str):
    """Yield parsed JSONL entries from transcript file."""
    try:
        with open(transcript_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue
    except (IOError, OSError):
        return


def extract_last_api_error(transcript_path: str) -> dict:
    """Return the last api_error entry's error fields, or empty dict."""
    last = {}
    for entry in _iter_transcript(transcript_path):
        if entry.get("type") == "system" and entry.get("subtype") == "api_error":
            err = entry.get("error", {})
            last = {
                "last_error_status": err.get("status"),
                "last_error_message": err.get("message", ""),
                "timestamp": entry.get("timestamp", ""),
            }
    return last


def build_session_start_batch(
    session_id: str,
    source: str,
    model: str,
    cwd: str,
) -> list[dict]:
    """Build a trace-create batch for SessionStart."""
    now = datetime.now(timezone.utc).isoformat()
    trace_id = f"trace-{session_id}"
    family = derive_model_family(model)

    tags = [t for t in [
        "claude-code",
        f"source:{source}" if source else None,
        family if family != "unknown" else None,
    ] if t]

    repo_name = os.path.basename(cwd.rstrip("/")) if cwd else ""
    if repo_name:
        tags.append(repo_name)

    return [{
        "id": f"evt-session-start-{session_id}",
        "timestamp": now,
        "type": "trace-create",
        "body": {
            "id": trace_id,
            "timestamp": now,
            "sessionId": session_id,
            "userId": os.environ.get("USER", "unknown"),
            "tags": tags,
            "metadata": {
                "session_source": source,
                "initial_model": model,
                "cwd": cwd,
            },
        },
    }]


def build_stop_failure_batch(
    session_id: str,
    transcript_path: str,
) -> list[dict]:
    """Build a trace-update batch for StopFailure."""
    now = datetime.now(timezone.utc).isoformat()
    trace_id = f"trace-{session_id}"

    error_info = extract_last_api_error(transcript_path)

    body = {
        "id": trace_id,
        "tags": ["stop-failure"],
    }
    if error_info:
        body["metadata"] = {"stop_failure": error_info}

    return [{
        "id": f"evt-stop-failure-{uuid.uuid4()}",
        "timestamp": now,
        "type": "trace-update",
        "body": body,
    }]


def send_batch(batch: list[dict]) -> None:
    """POST a batch to Langfuse ingestion endpoint."""
    payload = json.dumps({"batch": batch}).encode()
    req = Request(
        INGESTION_URL,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": make_auth_header(),
        },
        method="POST",
    )
    try:
        with urlopen(req, timeout=10) as resp:
            log(f"Langfuse response ({resp.status}): {resp.read().decode()[:100]}")
    except URLError as e:
        log(f"Failed to send to Langfuse: {e}")


def main() -> None:
    try:
        hook_input = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        log(f"Failed to parse stdin: {e}")
        return

    if not LANGFUSE_PUBLIC_KEY or not LANGFUSE_SECRET_KEY:
        log("LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY must be set")
        return

    event_name = hook_input.get("hook_event_name", "")
    session_id = hook_input.get("session_id", "unknown")
    transcript_path = hook_input.get("transcript_path", "")
    cwd = hook_input.get("cwd", "")

    if event_name == "SessionStart":
        source = hook_input.get("source", "")
        model = hook_input.get("model", "")
        batch = build_session_start_batch(session_id, source, model, cwd)
        send_batch(batch)
        log(f"SessionStart: source={source} model={model} session={session_id}")

    elif event_name == "StopFailure":
        if not transcript_path:
            log("StopFailure: no transcript_path")
            return
        batch = build_stop_failure_batch(session_id, transcript_path)
        send_batch(batch)
        log(f"StopFailure: session={session_id}")

    else:
        log(f"Unexpected event: {event_name}")


if __name__ == "__main__":
    main()
