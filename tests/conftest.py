# tests/conftest.py
"""Suite-wide fixtures.

The hook logs to ~/.claude/langfuse-hook.log by default. Without the redirect
below, running the suite appends fixture sessions (sess-nested, abc123, ...)
to the production log, so its tail can no longer be trusted when diagnosing
live ingestion. Set before any test module imports the hook, since LOG_FILE is
a module-level constant evaluated at import time.
"""
import os
import tempfile

_LOG_DIR = tempfile.mkdtemp(prefix="langfuse-hook-tests-")
os.environ["LANGFUSE_HOOK_LOG"] = os.path.join(_LOG_DIR, "langfuse-hook.log")
