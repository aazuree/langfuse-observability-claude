#!/usr/bin/env python3
"""Shared utilities for Langfuse hooks.

Centralizes secret redaction, logging, and authentication to prevent duplication.
"""

import base64
import json
import os
import re
from datetime import datetime, timezone
from typing import Iterator

LANGFUSE_PUBLIC_KEY = os.environ.get("LANGFUSE_PUBLIC_KEY", "")
LANGFUSE_SECRET_KEY = os.environ.get("LANGFUSE_SECRET_KEY", "")

MAX_LOG_BYTES = 10 * 1024 * 1024  # 10 MB

# Patterns to redact from text before sending to Langfuse
SECRET_PATTERNS = [
    re.compile(r"(?i)(api[_-]?key|apikey|secret[_-]?key|access[_-]?key|token|password|passwd|credential|auth)[\s]*[=:]\s*['\"]?([^\s'\"]{8,})['\"]?"),
    re.compile(r"(?i)(sk|pk|api|key|token|secret|password|bearer|ghp|gho|ghu|ghs|ghr|glpat|xox[bposatr]|AKIA|AGPA|AIDA|AROA|AIPA|ANPA|ANVA|ASIA)[-_]?[a-zA-Z0-9/+=]{16,}"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"(?i)Bearer\s+[a-zA-Z0-9._\-/+=]{20,}"),
]


def iter_transcript(transcript_path: str) -> Iterator[dict]:
    """Yield parsed JSONL entries from a Claude Code transcript file."""
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


def log(log_file: str, msg: str) -> None:
    """Append a timestamped message to a log file with 10 MB rotation.

    Args:
        log_file: Path to log file (e.g., ~/.claude/langfuse-hook.log)
        msg: Message to log
    """
    try:
        if os.path.exists(log_file) and os.path.getsize(log_file) > MAX_LOG_BYTES:
            rotated = log_file + ".1"
            if os.path.exists(rotated):
                os.remove(rotated)
            os.rename(log_file, rotated)
        with open(log_file, "a") as f:
            f.write(f"{datetime.now(timezone.utc).isoformat()} {msg}\n")
    except Exception:
        pass


# Per-event failures worth retrying: the server was unwell, the payload wasn't
# wrong. Everything else is a validation/auth verdict that will never change.
TRANSIENT_INGESTION_STATUSES = frozenset({408, 429})


def classify_ingestion_errors(body: str) -> tuple[int, int, list[str]]:
    """Split an ingestion response body into (transient, permanent, details).

    The endpoint answers 207 Multi-Status: the HTTP status only says the request
    was accepted, while the body reports each event's fate in `errors`. Reading
    just the status is what let rejected events pass as delivered.

    A body with no `errors` key is a success — not every 2xx response carries
    the successes/errors shape. An unparseable body is counted transient: an
    HTML error page from a proxy is not evidence the events were rejected, so
    retrying is the safe reading. An error entry we cannot read a status from is
    counted permanent, because retrying something we do not understand forever
    is worse than dropping it loudly.
    """
    try:
        parsed = json.loads(body)
    except (ValueError, TypeError):
        return 1, 0, [f"unparseable response body: {body[:120]}"]

    if not isinstance(parsed, dict):
        return 1, 0, [f"unexpected response shape: {body[:120]}"]

    errors = parsed.get("errors") or []
    if not isinstance(errors, list):
        return 1, 0, [f"unexpected errors field: {str(errors)[:120]}"]

    transient = 0
    permanent = 0
    details = []
    for err in errors:
        if not isinstance(err, dict):
            permanent += 1
            details.append(f"malformed error entry: {str(err)[:120]}")
            continue
        status = err.get("status")
        if isinstance(status, int) and (status >= 500 or status in TRANSIENT_INGESTION_STATUSES):
            transient += 1
        else:
            permanent += 1
        details.append(
            f"{err.get('id', '<no id>')} status={status} {str(err.get('message', ''))[:160]}".strip()
        )
    return transient, permanent, details


def make_auth_header() -> str:
    """Create HTTP Basic auth header for Langfuse API.

    Returns:
        Base64-encoded Basic auth header value
    """
    creds = base64.b64encode(
        f"{LANGFUSE_PUBLIC_KEY}:{LANGFUSE_SECRET_KEY}".encode()
    ).decode()
    return f"Basic {creds}"


def redact_secrets(text: str) -> str:
    """Redact known secret patterns from text before logging/sending.

    Args:
        text: Raw text potentially containing secrets

    Returns:
        Text with secrets replaced by [REDACTED]
    """
    for pattern in SECRET_PATTERNS:
        text = pattern.sub("[REDACTED]", text)
    return text
