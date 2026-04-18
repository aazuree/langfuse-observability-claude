#!/usr/bin/env python3
"""Shared utilities for Langfuse hooks (langfuse-hook.py and eval-hook.py).

Centralizes secret redaction, logging, and authentication to prevent duplication.
"""

import base64
import os
import re
from datetime import datetime, timezone

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
