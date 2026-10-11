#!/usr/bin/env python3
"""Register langfuse-hook.py in a Claude Code settings.json.

Used by setup.sh; takes the command from HOOK_CMD so no value is ever
spliced into source code.

    HOOK_CMD=... python3 tools/configure_hooks.py ~/.claude/settings.json

- Registers the same command on Stop, StopFailure and SessionEnd. Stop sends
  each finished turn; StopFailure fires instead of Stop when a turn ends in
  an API error and sends the turns before it (the failed turn follows with
  the next turn or at SessionEnd); SessionEnd sends a last turn that never
  finished.
- Updates an existing entry's keys, but never re-points a hook at a
  different Langfuse: an entry whose LANGFUSE_HOST differs from the new
  command's (e.g. a remote Pi stack) is left alone and reported, and the
  events it does not cover yet get that same entry: the three events share
  one state file per session, so they must all send to one Langfuse. A
  command without LANGFUSE_HOST means localhost.
- Registers with "async": true so the hook never blocks Claude Code.
  langfuse-hook.py takes a per-session lock, so overlapping fires are safe.
- Removes entries from retired features: eval-hook.py and
  session-start-hook.py, wherever they are registered.
- Backs the file up to <path>.backup.<epoch> before writing.
"""
import json
import os
import re
import shutil
import sys
import time

DEFAULT_HOST = "http://localhost:3100"
EVENTS = ("Stop", "StopFailure", "SessionEnd")
RETIRED = ("eval-hook.py", "session-start-hook.py")
_HOST_RE = re.compile(r"(?:^|\s)LANGFUSE_HOST=(\S+)")


def _host(command: str) -> str:
    match = _HOST_RE.search(command)
    return (match.group(1) if match else DEFAULT_HOST).rstrip("/")


def _upsert(groups: list, command: str, notes: list, missing: str) -> None:
    """Point every langfuse-hook.py entry in `groups` at `command`, or add
    `missing` when the event has none."""
    found = False
    for group in groups:
        for hook in group.get("hooks", []):
            if "langfuse-hook.py" not in hook.get("command", ""):
                continue
            found = True
            if _host(hook["command"]) != _host(command):
                notes.append(
                    f"left langfuse-hook.py alone: it sends to {_host(hook['command'])}, "
                    f"not {_host(command)} — edit it by hand if you meant to switch")
                continue
            hook["command"] = command
            hook["async"] = True
    if not found:
        groups.append({"hooks": [{"type": "command", "command": missing, "async": True}]})


def configure(settings: dict, command: str) -> tuple:
    """Return (settings, notes) with the hook registered on every event."""
    notes = []
    hooks = settings.setdefault("hooks", {})

    for event in list(hooks):
        for group in hooks[event]:
            group["hooks"] = [h for h in group.get("hooks", [])
                              if not any(r in h.get("command", "") for r in RETIRED)]
        hooks[event] = [g for g in hooks[event] if g.get("hooks")]
        if not hooks[event]:
            del hooks[event]

    # A hook already sending elsewhere decides where the missing events send.
    missing = command
    for event in EVENTS:
        for group in hooks.get(event, []):
            for hook in group.get("hooks", []):
                existing = hook.get("command", "")
                if "langfuse-hook.py" in existing and _host(existing) != _host(command):
                    missing = existing
    if missing != command:
        notes.append(f"registered the missing events with the existing hook for "
                     f"{_host(missing)}, so a session never splits across two Langfuse hosts")

    for event in EVENTS:
        _upsert(hooks.setdefault(event, []), command, notes, missing)
    return settings, notes


def main(argv: list) -> int:
    (path,) = argv
    command = os.environ["HOOK_CMD"]

    settings = {}
    if os.path.exists(path):
        with open(path) as f:
            settings = json.load(f)
        backup = f"{path}.backup.{int(time.time())}"
        shutil.copy2(path, backup)
        print(f"[hook] Backed up {path} to {backup}")

    settings, notes = configure(settings, command)
    for note in notes:
        print(f"[hook] NOTE: {note}")

    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(settings, f, indent=2)
        f.write("\n")
    os.replace(tmp, path)
    print(f"[hook] Registered {' + '.join(EVENTS)} hooks in {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
