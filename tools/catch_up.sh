#!/bin/sh
# Run `langfuse-hook.py --catch-up` with the host and keys of the Stop hook
# registered in ~/.claude/settings.json (the hook command carries them as env
# assignments). Used by the hourly langfuse-catch-up systemd user timer.
# Exit: 0 ok (a failed send is logged and retried next run), 1 a session
# crashed (see the hook log), 2 no hook registered.
set -eu
HOOK=$(jq -r '[.hooks.Stop[]?.hooks[]? | .command // empty | select(test("langfuse-hook\\.py"))][0] // empty' \
  "$HOME/.claude/settings.json")
if [ -z "$HOOK" ]; then
  echo "catch_up.sh: no langfuse-hook.py Stop hook in ~/.claude/settings.json" >&2
  exit 2
fi
exec sh -c "$HOOK --catch-up"
