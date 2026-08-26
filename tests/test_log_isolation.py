# tests/test_log_isolation.py
"""Tests that the hook's log destination is overridable, so a test run never
writes into the production ~/.claude/langfuse-hook.log."""
import importlib.util
import os

PROD_LOG = os.path.expanduser("~/.claude/langfuse-hook.log")
HOOK_PATH = os.path.join(os.path.dirname(__file__), "..", "langfuse-hook.py")


def _load_hook():
    """Load langfuse-hook.py fresh, so module-level constants re-evaluate."""
    spec = importlib.util.spec_from_file_location("langfuse_hook_isolated", HOOK_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestLogFileOverride:
    def test_log_file_honours_env_override(self, monkeypatch, tmp_path):
        custom = tmp_path / "hook.log"
        monkeypatch.setenv("LANGFUSE_HOOK_LOG", str(custom))

        module = _load_hook()

        assert module.LOG_FILE == str(custom)

    def test_log_writes_to_override_not_production(self, monkeypatch, tmp_path):
        custom = tmp_path / "hook.log"
        monkeypatch.setenv("LANGFUSE_HOOK_LOG", str(custom))
        module = _load_hook()

        module.log("marker-from-test")

        assert "marker-from-test" in custom.read_text()

    def test_defaults_to_production_path_when_unset(self, monkeypatch):
        monkeypatch.delenv("LANGFUSE_HOOK_LOG", raising=False)

        module = _load_hook()

        assert module.LOG_FILE == PROD_LOG


class TestSuiteDoesNotTouchProductionLog:
    def test_conftest_redirects_log_for_the_whole_suite(self):
        """The suite-wide conftest must point every test at a temp log."""
        assert os.environ.get("LANGFUSE_HOOK_LOG"), (
            "conftest.py must set LANGFUSE_HOOK_LOG so pytest never writes "
            "into the production hook log"
        )
        assert os.environ["LANGFUSE_HOOK_LOG"] != PROD_LOG
