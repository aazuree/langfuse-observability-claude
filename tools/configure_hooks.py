#!/usr/bin/env python3
"""Register the Stop and StopFailure hooks in a Claude Code settings.json.

Used by setup.sh; takes the commands from STOP_HOOK_CMD and
STOP_FAILURE_HOOK_CMD so no value is ever spliced into source code.

    STOP_HOOK_CMD=... STOP_FAILURE_HOOK_CMD=... python3 tools/configure_hooks.py ~/.claude/settings.json

- Adds both hooks when missing, and updates an existing entry's keys.
- Never re-points a hook at a different Langfuse: an entry whose
  LANGFUSE_HOST differs from the new command's (e.g. a remote Pi stack) is
  left alone and reported. A command without LANGFUSE_HOST means localhost.
- Registers both hooks with "async": true so they never block Claude Code.
  langfuse-hook.py takes a per-session lock, so overlapping fires are safe.
- Removes entries from retired features: eval-hook.py on Stop and the old
  SessionStart registration of session-start-hook.py.
- Backs the file up to <path>.backup.<epoch> before writing.
"""
import json
import os
import re
import shutil
import sys
import time

DEFAULT_HOST = "http://localhost:3100"
_HOST_RE = re.compile(r"(?:^|\s)LANGFUSE_HOST=(\S+)")


def _host(command: str) -> str:
    match = _HOST_RE.search(command)
    return (match.group(1) if match else DEFAULT_HOST).rstrip("/")


def _upsert(groups: list, marker: str, command: str, notes: list) -> None:
    """Point every `marker` hook in `groups` at `command`, or add one."""
    found = False
    for group in groups:
        for hook in group.get("hooks", []):
            if marker not in hook.get("command", ""):
                continue
            found = True
            if _host(hook["command"]) != _host(command):
                notes.append(
                    f"left {marker} alone: it sends to {_host(hook['command'])}, "
                    f"not {_host(command)} — edit it by hand if you meant to switch")
                continue
            hook["command"] = command
            hook["async"] = True
    if not found:
        groups.append({"hooks": [{"type": "command", "command": command, "async": True}]})


def configure(settings: dict, stop_cmd: str, failure_cmd: str) -> tuple:
    """Return (settings, notes) with both hooks registered."""
    notes = []
    hooks = settings.setdefault("hooks", {})

    stop = hooks.setdefault("Stop", [])
    for group in stop:
        group["hooks"] = [h for h in group.get("hooks", [])
                          if "eval-hook.py" not in h.get("command", "")]
    hooks["Stop"] = [g for g in stop if g.get("hooks")]
    _upsert(hooks["Stop"], "langfuse-hook.py", stop_cmd, notes)

    if "SessionStart" in hooks:
        for group in hooks["SessionStart"]:
            group["hooks"] = [h for h in group.get("hooks", [])
                              if "session-start-hook.py" not in h.get("command", "")]
        hooks["SessionStart"] = [g for g in hooks["SessionStart"] if g.get("hooks")]
        if not hooks["SessionStart"]:
            del hooks["SessionStart"]

    _upsert(hooks.setdefault("StopFailure", []), "session-start-hook.py", failure_cmd, notes)
    return settings, notes


def main(argv: list) -> int:
    (path,) = argv
    stop_cmd = os.environ["STOP_HOOK_CMD"]
    failure_cmd = os.environ["STOP_FAILURE_HOOK_CMD"]

    settings = {}
    if os.path.exists(path):
        with open(path) as f:
            settings = json.load(f)
        backup = f"{path}.backup.{int(time.time())}"
        shutil.copy2(path, backup)
        print(f"[hook] Backed up {path} to {backup}")

    settings, notes = configure(settings, stop_cmd, failure_cmd)
    for note in notes:
        print(f"[hook] NOTE: {note}")

    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(settings, f, indent=2)
        f.write("\n")
    os.replace(tmp, path)
    print(f"[hook] Registered Stop + StopFailure hooks in {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
