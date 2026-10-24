#!/usr/bin/env python3
"""Compare what two versions of the hook bill for the same local transcripts.

Run before merging any change that touches usage or cost: both hooks run in
dry-run mode (nothing is sent, hook state is a temp dir) over every
transcript under ~/.claude/projects, and every session whose cost or
generation count differs is listed. Each delta must be one the change
intends; paste the summary into the PR.

    python3 tools/cost_diff.py                    # origin/main vs this checkout
    python3 tools/cost_diff.py --old main~3       # any git ref
    python3 tools/cost_diff.py --old-path /path/to/old/langfuse-hook.py --json

Exit status: 0 = identical, 1 = differences (listed), 2 = could not run.
"""
import argparse
import importlib.util
import json
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path

_spec = importlib.util.spec_from_file_location("audit_costs", Path(__file__).with_name("audit_costs.py"))
audit_costs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(audit_costs)

REPO_ROOT = audit_costs.REPO_ROOT
# Everything the hook imports from the repo.
HOOK_FILES = ("langfuse-hook.py", "langfuse_common.py", "langfuse_otlp.py")


def checkout_hook(ref: str, dest: Path) -> Path:
    """Write the hook files as of git `ref` into `dest`."""
    for name in HOOK_FILES:
        out = subprocess.run(["git", "-C", str(REPO_ROOT), "show", f"{ref}:{name}"],
                             capture_output=True, check=True)
        (dest / name).write_bytes(out.stdout)
    return dest / "langfuse-hook.py"


def bill(hook_path: Path, projects: Path) -> dict:
    """{session_id: {"cost": usd, "generations": n}} as `hook_path` bills it."""
    generations: Counter = Counter()

    def count(batch):
        # One batch is one session's fire; its session score names it (turn
        # generations carry the session on their trace, not themselves).
        sid = next((e["body"].get("sessionId") for e in batch if e.get("type") == "score-create"), None)
        generations[sid] += sum(1 for e in batch if e.get("type") == "generation-create")

    with tempfile.TemporaryDirectory(prefix="cost-diff-") as tmp:
        hook = audit_costs.load_hook(hook_path, tmp)
        scores = audit_costs.run_hook(projects, hook, on_batch=count)
    return {sid: {"cost": round(float(body.get("value", 0.0)), 6), "generations": generations[sid]}
            for sid, body in scores.items()}


def diff(old: dict, new: dict) -> dict:
    rows = []
    for sid in sorted(set(old) | set(new)):
        o = old.get(sid, {"cost": 0.0, "generations": 0})
        n = new.get(sid, {"cost": 0.0, "generations": 0})
        delta = round(n["cost"] - o["cost"], 6)
        if abs(delta) > audit_costs.COST_TOLERANCE_USD or o["generations"] != n["generations"]:
            rows.append({"session": sid, "old_usd": o["cost"], "new_usd": n["cost"], "delta_usd": delta,
                         "old_generations": o["generations"], "new_generations": n["generations"]})
    return {
        "sessions": len(set(old) | set(new)),
        "changed": len(rows),
        "old_usd": round(sum(v["cost"] for v in old.values()), 4),
        "new_usd": round(sum(v["cost"] for v in new.values()), 4),
        "old_generations": sum(v["generations"] for v in old.values()),
        "new_generations": sum(v["generations"] for v in new.values()),
        "rows": sorted(rows, key=lambda r: -abs(r["delta_usd"])),
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    old = ap.add_mutually_exclusive_group()
    old.add_argument("--old", default="origin/main", help="git ref of the baseline hook (default: origin/main)")
    old.add_argument("--old-path", type=Path, help="baseline langfuse-hook.py on disk instead of a ref")
    ap.add_argument("--new-path", type=Path, default=audit_costs.DEFAULT_HOOK,
                    help="hook under test (default: this checkout's)")
    ap.add_argument("--projects", type=Path, default=audit_costs.DEFAULT_PROJECTS, help="transcript root")
    ap.add_argument("--json", action="store_true", help="machine-readable report")
    args = ap.parse_args(argv)

    try:
        with tempfile.TemporaryDirectory(prefix="cost-diff-old-") as tmp:
            old_hook = args.old_path or checkout_hook(args.old, Path(tmp))
            report = diff(bill(old_hook, args.projects), bill(args.new_path, args.projects))
    except (OSError, subprocess.CalledProcessError) as e:
        print(f"cost_diff: could not run: {type(e).__name__}: {e}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        r = report
        print(f"{r['sessions']} sessions, {r['changed']} changed\n"
              f"total ${r['old_usd']:.2f} -> ${r['new_usd']:.2f} ({r['new_usd'] - r['old_usd']:+.2f})\n"
              f"generations {r['old_generations']} -> {r['new_generations']}")
        for row in r["rows"]:
            print(f"  {row['session']}  ${row['old_usd']:.4f} -> ${row['new_usd']:.4f} "
                  f"({row['delta_usd']:+.4f})  generations {row['old_generations']} -> {row['new_generations']}")
    return 1 if report["changed"] else 0


if __name__ == "__main__":
    sys.exit(main())
