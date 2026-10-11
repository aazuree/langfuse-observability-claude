#!/usr/bin/env python3
"""Shared utilities for Langfuse hooks.

Centralizes secret redaction, logging, and authentication to prevent duplication.
"""

import base64
import json
import os
import re
import tempfile
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


def atomic_write_text(path: str, text: str) -> None:
    """Replace `path` with `text` so a reader sees either the old file or the
    new one, never a truncated mix.

    Writes a sibling temp file, fsyncs it, and os.replace()s it into place.
    On any failure the temp file is removed and the original is untouched.
    """
    directory = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=os.path.basename(path))
    try:
        with os.fdopen(fd, "w") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


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
