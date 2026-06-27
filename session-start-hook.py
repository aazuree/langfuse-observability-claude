#!/usr/bin/env python3
"""Claude Code StopFailure hook -> Langfuse.

StopFailure: Updates the session trace with a stop-failure tag and last API error
             metadata when a turn ends due to an API error. The trace itself is
             created by the Stop hook (langfuse-hook.py) from the transcript; this
             hook only enriches it. (SessionStart skeleton-trace creation was
             removed — it produced empty orphan traces for abandoned sessions.)

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
from langfuse_common import iter_transcript, log as common_log, make_auth_header

LANGFUSE_HOST = os.environ.get("LANGFUSE_HOST", "http://localhost:3100")
LANGFUSE_PUBLIC_KEY = os.environ.get("LANGFUSE_PUBLIC_KEY", "")
LANGFUSE_SECRET_KEY = os.environ.get("LANGFUSE_SECRET_KEY", "")
INGESTION_URL = f"{LANGFUSE_HOST}/api/public/ingestion"

LOG_FILE = os.path.expanduser("~/.claude/langfuse-hook.log")


def log(msg: str) -> None:
    common_log(LOG_FILE, msg)


def extract_last_api_error(transcript_path: str) -> dict:
    """Return the last api_error entry's error fields, or empty dict."""
    last = {}
    for entry in iter_transcript(transcript_path):
        if entry.get("type") == "system" and entry.get("subtype") == "api_error":
            err = entry.get("error", {})
            last = {
                "last_error_status": err.get("status"),
                "last_error_message": err.get("message", ""),
                "timestamp": entry.get("timestamp", ""),
            }
    return last


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

    if event_name == "StopFailure":
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
