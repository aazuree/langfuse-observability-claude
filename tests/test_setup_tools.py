# tests/test_setup_tools.py
"""setup.sh helpers: rendering .env and registering the hooks.

Both used to be inline in setup.sh. The .env placeholders were filled with
`sed "s|__X__|$VALUE|g"`, so a password containing `&`, `|` or `\\` came out
corrupted (`&` is "the matched text" in a sed replacement). The hook merge
interpolated shell variables into Python source, and on re-run it rewrote the
Stop command without LANGFUSE_HOST, silently re-pointing a hook that targets
a remote stack at localhost.
"""
import importlib.util
import json
import os

_tools = os.path.join(os.path.dirname(__file__), "..", "tools")


def _load(name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(_tools, f"{name}.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


render_env = _load("render_env")
configure_hooks = _load("configure_hooks")


class TestRenderEnv:
    def test_values_with_sed_metacharacters_survive(self):
        out = render_env.render(
            "LANGFUSE_INIT_USER_PASSWORD=__ADMIN_PASSWORD__\nNAME=__ADMIN_NAME__\n",
            {"ADMIN_PASSWORD": r"a&b|c\d", "ADMIN_NAME": "Ann & Bo"},
        )
        assert out == "LANGFUSE_INIT_USER_PASSWORD=a&b|c\\d\nNAME=Ann & Bo\n"

    def test_dollar_is_escaped_for_docker_compose(self):
        out = render_env.render("P=__ADMIN_PASSWORD__\n", {"ADMIN_PASSWORD": "pa$$word"})
        assert out == "P=pa$$$$word\n"

    def test_unfilled_placeholder_is_an_error(self):
        try:
            render_env.render("P=__MISSING__\n", {})
        except KeyError as e:
            assert "MISSING" in str(e)
        else:
            raise AssertionError("a placeholder with no value must not pass through")


CMD = "LANGFUSE_HOST=http://localhost:3100 LANGFUSE_PUBLIC_KEY=pk LANGFUSE_SECRET_KEY=sk python3 /r/langfuse-hook.py"
EVENTS = ("Stop", "StopFailure", "SessionEnd")


def _hooks(settings, event):
    return [h for g in settings.get("hooks", {}).get(event, []) for h in g.get("hooks", [])]


class TestConfigureHooks:
    def test_registers_the_hook_on_every_event(self):
        """An existing settings.json without the hook used to get only a
        printed "add this manually" snippet."""
        settings, notes = configure_hooks.configure({"theme": "dark"}, CMD)
        assert settings["theme"] == "dark"
        for event in EVENTS:
            assert _hooks(settings, event) == [{"type": "command", "command": CMD, "async": True}]

    def test_updates_keys_on_the_same_host(self):
        old = CMD.replace("pk", "pk-old")
        settings, _ = configure_hooks.configure(
            {"hooks": {"Stop": [{"hooks": [{"type": "command", "command": old}]}]}}, CMD)
        assert [h["command"] for h in _hooks(settings, "Stop")] == [CMD]

    def test_leaves_a_hook_pointed_at_another_host_alone(self):
        remote = CMD.replace("localhost", "10.9.0.1")
        settings, notes = configure_hooks.configure(
            {"hooks": {"Stop": [{"hooks": [{"type": "command", "command": remote}]}]}}, CMD)
        assert [h["command"] for h in _hooks(settings, "Stop")] == [remote]
        assert any("10.9.0.1" in n for n in notes)

    def test_a_command_without_a_host_counts_as_localhost(self):
        bare = "LANGFUSE_PUBLIC_KEY=pk-old LANGFUSE_SECRET_KEY=sk python3 /r/langfuse-hook.py"
        settings, _ = configure_hooks.configure(
            {"hooks": {"Stop": [{"hooks": [{"type": "command", "command": bare}]}]}}, CMD)
        assert [h["command"] for h in _hooks(settings, "Stop")] == [CMD]

    def test_other_hooks_are_untouched_and_retired_entries_removed(self):
        other = {"type": "command", "command": "notify-send done"}
        retired_failure = {"type": "command", "async": True,
                           "command": "LANGFUSE_HOST=http://10.9.0.1:3100 python3 /r/session-start-hook.py"}
        settings, _ = configure_hooks.configure({"hooks": {
            "Stop": [{"hooks": [other, {"type": "command", "command": "python3 /r/eval-hook.py"}]}],
            "SessionStart": [{"hooks": [{"type": "command",
                                         "command": "python3 /r/session-start-hook.py"}]}],
            "StopFailure": [{"hooks": [retired_failure]}],
        }}, CMD)
        assert other in _hooks(settings, "Stop")
        assert not any("eval-hook" in h["command"] for h in _hooks(settings, "Stop"))
        assert "SessionStart" not in settings["hooks"]
        assert _hooks(settings, "StopFailure") == [{"type": "command", "command": CMD, "async": True}]

    def test_main_backs_up_and_writes(self, tmp_path, monkeypatch):
        path = tmp_path / "settings.json"
        path.write_text(json.dumps({"theme": "dark"}))
        monkeypatch.setenv("HOOK_CMD", CMD)
        assert configure_hooks.main([str(path)]) == 0
        assert _hooks(json.loads(path.read_text()), "SessionEnd")[0]["command"] == CMD
        backups = [p for p in os.listdir(tmp_path) if p.startswith("settings.json.backup.")]
        assert len(backups) == 1
