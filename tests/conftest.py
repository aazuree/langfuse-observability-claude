# tests/conftest.py
"""Suite-wide fixtures.

The hook logs to ~/.claude/langfuse-hook.log by default. Without the redirect
below, running the suite appends fixture sessions (sess-nested, abc123, ...)
to the production log, so its tail can no longer be trusted when diagnosing
live ingestion. Set before any test module imports the hook, since LOG_FILE is
a module-level constant evaluated at import time.
"""
import os
import sys
import tempfile

import pytest

_LOG_DIR = tempfile.mkdtemp(prefix="langfuse-hook-tests-")
os.environ["LANGFUSE_HOOK_LOG"] = os.path.join(_LOG_DIR, "langfuse-hook.log")


@pytest.fixture(autouse=True)
def _reset_warn_once():
    """calculate_turn_cost warns once per model per process; each test is its
    own "process" for that purpose, so a WARN one test expects is not
    swallowed because an earlier test already triggered it."""
    yield
    for module in list(sys.modules.values()):
        warned = getattr(module, "_WARNED_MODELS", None)
        if isinstance(warned, set):
            warned.clear()
