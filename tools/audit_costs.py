#!/usr/bin/env python3
"""Audit the hook's cost numbers against independent sources.

Three checks, each catching a different way cost goes wrong:

1. **Recount** — a deliberately naive second implementation bills every API
   message exactly once across *all* local transcripts (main, subagent, fork,
   continuation): one usage per `message.id`, from its last entry, owned by
   the transcript that wrote it first. The hook is then run in dry-run mode
   (nothing is sent, hook state is a temp dir) and its per-session cost must
   match. A double count (a message billed in two turns, two sessions or by a
   fork) or a dropped message shows up as a per-session delta. Both sides use
   the hook's own rate table on purpose: rates are checked by
   `check_pricing_drift.py`, this checks counting.

2. **Claude Code's own totals** — the `cost-state` entry the CLI writes on
   exit, compared per model with the recount's tokens by the hook's own
   `check_cost` (the live hook runs the same rule on its own tokens, so the
   two can't drift apart). The transcript can never legitimately exceed
   the CLI's count: that is an **overcount**. CLI output beyond what its
   unrecorded side calls explain (auto-mode classifier, compaction, away
   summaries, the Haiku behind WebFetch / WebSearch) is an **undercount**.
   Sessions with partially recorded subagent messages are not checked for
   undercount: their output is a known lower bound.

3. **What Langfuse holds** (`--langfuse`) — the stored per-session cost must
   equal what a fresh run of the hook computes. More means spans exported
   twice (Langfuse v4 keeps every export); less means turns never sent.

Exit status: 0 = all checks pass, 1 = a finding, 2 = the audit could not run.

    python3 tools/audit_costs.py                       # checks 1 + 2
    python3 tools/audit_costs.py --langfuse            # + 3, host/keys from env
    python3 tools/audit_costs.py --langfuse --from-settings  # host/keys from the Stop hook
    python3 tools/audit_costs.py --hook /path/to/langfuse-hook.py --json
"""
import argparse
import base64
import contextlib
import importlib.util
import io
import json
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_HOOK = REPO_ROOT / "langfuse-hook.py"
DEFAULT_PROJECTS = Path.home() / ".claude" / "projects"

# Dollar differences below this are float rounding, not a finding.
COST_TOLERANCE_USD = 0.005
# Sessions written to within this many seconds may still be running.
ACTIVE_S = 3600

TOKEN_KINDS = ("input_tokens", "output_tokens", "cache_read_tokens", "cache_create_tokens")


def load_hook(path: Path, log_dir: str):
    """Import the hook from `path` with its log and state in `log_dir`."""
    # LOG_FILE is read at import time; restore the variable afterwards so the
    # caller's own logging is untouched.
    saved = os.environ.get("LANGFUSE_HOOK_LOG")
    os.environ["LANGFUSE_HOOK_LOG"] = os.path.join(log_dir, "hook.log")
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location(f"hook_{abs(hash(str(path)))}", path)
        hook = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(hook)
    finally:
        sys.path.remove(str(path.parent))
        if saved is None:
            os.environ.pop("LANGFUSE_HOOK_LOG", None)
        else:
            os.environ["LANGFUSE_HOOK_LOG"] = saved
    hook.STATE_DIR = os.path.join(log_dir, "state")
    return hook


# ---------------------------------------------------------------------------
# Transcripts
# ---------------------------------------------------------------------------

def transcripts(projects: Path):
    """(session_id, path, is_subagent) for every main and subagent transcript."""
    for main in sorted(projects.glob("*/*.jsonl")):
        yield main.stem, main, False
        for sub in sorted((main.parent / main.stem / "subagents").glob("*.jsonl")):
            yield main.stem, sub, True


def read_entries(path: Path):
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(entry, dict):
                yield entry


def first_timestamp(path: Path) -> datetime | None:
    for entry in read_entries(path):
        ts = entry.get("timestamp")
        if ts:
            try:
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                return None
    return None


# ---------------------------------------------------------------------------
# Check 1: the naive recount
# ---------------------------------------------------------------------------

def recount(projects: Path, hook) -> dict:
    """Bill every API message once across all transcripts.

    Returns {session_id: {"cost": usd, "messages": n, "web_tool_calls": n,
    "tokens": {model: {kind: n}}}}.
    """
    # message id -> (owner key, session, model, last usage)
    messages: dict[str, tuple] = {}
    web_tool_calls: dict[str, int] = defaultdict(int)
    # A continued session's transcript starts with a copy of its
    # predecessor's history (same message ids and timestamps); the
    # predecessor names it in a `continued-in` entry and owns the copy.
    successor_of = {}
    for main in projects.glob("*/*.jsonl"):
        for entry in read_entries(main):
            if entry.get("type") == "continued-in" and entry.get("continuedInSessionId"):
                successor_of[main.stem] = entry["continuedInSessionId"]
    rank: dict[str, int] = {}
    for start in set(successor_of) - set(successor_of.values()):
        sid, depth = start, 0
        while sid in successor_of and depth < len(successor_of):
            sid, depth = successor_of[sid], depth + 1
            rank[sid] = depth
    for order, (session_id, path, _) in enumerate(transcripts(projects)):
        seen: dict[str, list] = {}
        for entry in read_entries(path):
            if entry.get("type") != "assistant" or entry.get("isApiErrorMessage"):
                continue
            msg = entry.get("message") or {}
            content = msg.get("content")
            for block in content if isinstance(content, list) else ():
                if isinstance(block, dict) and block.get("type") == "tool_use" \
                        and block.get("name") in ("WebFetch", "WebSearch"):
                    web_tool_calls[session_id] += 1
            mid, model = msg.get("id"), msg.get("model") or ""
            if not mid or model == "<synthetic>":
                continue
            if mid not in seen:
                seen[mid] = [entry.get("timestamp") or "", model, {}]
            seen[mid][2] = msg.get("usage") or {}
            seen[mid][1] = model or seen[mid][1]
        for mid, (ts, model, usage) in seen.items():
            # The first writer owns the message: a fork's copy of its
            # spawner's call is written later than the original. A
            # continuation's copy keeps the original timestamps; the earlier
            # session in the chain wins.
            key = (rank.get(session_id, 0), ts, order)
            prev = messages.get(mid)
            if prev is None or key < prev[0]:
                messages[mid] = (key, session_id, model, usage)

    sessions: dict[str, dict] = defaultdict(lambda: {
        "cost": 0.0, "messages": 0, "web_tool_calls": 0,
        "tokens": defaultdict(lambda: dict.fromkeys(TOKEN_KINDS, 0))})
    for session_id, n in web_tool_calls.items():
        sessions[session_id]["web_tool_calls"] = n
    for _, session_id, model, u in messages.values():
        inp = u.get("input_tokens", 0) or 0
        out = u.get("output_tokens", 0) or 0
        cr = u.get("cache_read_input_tokens", 0) or 0
        cc = u.get("cache_creation_input_tokens", 0) or 0
        tiers = u.get("cache_creation") or {}
        c5 = tiers.get("ephemeral_5m_input_tokens", 0) or 0
        c1 = tiers.get("ephemeral_1h_input_tokens", 0) or 0
        usage = {"input": inp, "output": out, "cache_read": cr, "cache_creation": cc}
        long_usage = ({**usage, "cache_5m": c5, "cache_1h": c1}
                      if inp + cr + cc > hook.LONG_CONTEXT_THRESHOLD else None)
        cost = hook.calculate_turn_cost(
            usage, model, c5, c1,
            speed=u.get("speed") or "",
            inference_geo=u.get("inference_geo") or "",
            web_search_requests=(u.get("server_tool_use") or {}).get("web_search_requests", 0) or 0,
            long_context_usage=long_usage,
        )[0]
        s = sessions[session_id]
        s["cost"] += cost
        s["messages"] += 1
        t = s["tokens"][model]
        t["input_tokens"] += inp
        t["output_tokens"] += out
        t["cache_read_tokens"] += cr
        t["cache_create_tokens"] += cc
    return sessions


# ---------------------------------------------------------------------------
# The hook, dry-run
# ---------------------------------------------------------------------------

def run_hook(projects: Path, hook, on_batch=None) -> dict:
    """Run the hook over every main transcript without sending anything.

    Returns {session_id: session_cost_usd score body (value + rollups)}.
    `on_batch` sees every batch the hook would have sent.
    """
    scores: dict[str, dict] = {}

    def capture(batch):
        if on_batch:
            on_batch(batch)
        for event in batch:
            body = event.get("body") or {}
            if event.get("type") == "score-create" and body.get("name") == "session_cost_usd":
                scores[body["sessionId"]] = body
        return True

    hook.send_to_langfuse = capture
    for main in sorted(projects.glob("*/*.jsonl")):
        session_id = hook.sanitize_id(main.stem)
        # A continuation flushes its predecessor first; a session already
        # processed that way has nothing new to send.
        if session_id in scores:
            continue
        with contextlib.redirect_stdout(io.StringIO()):
            hook.process_session(session_id, str(main), hook.extract_cwd(str(main)), final=True)
    return scores


# ---------------------------------------------------------------------------
# Check 2: Claude Code's cost-state
# ---------------------------------------------------------------------------

def side_calls(naive: dict, meta: dict) -> dict:
    """Calls the CLI makes but never writes as a message, by kind."""
    return {
        "classifier_requests": meta.get("classifier_requests") or 0,
        "compactions": (meta.get("compaction") or {}).get("count", 0),
        "away_summaries": len(meta.get("away_summaries") or ()),
        "web_tool_calls": naive.get("web_tool_calls", 0),
    }


def cost_state_findings(session_id: str, naive_all: dict, scores: dict, hook) -> list[str]:
    """Compare the recount's per-model tokens with Claude Code's cost-state."""
    meta = scores[session_id].get("metadata") or {}
    cli = meta.get("cli_cost_state") or {}
    if not cli or cli.get("partial") or "by_model" not in cli:
        return []
    group = [session_id]
    # A continuation's cost-state carries on from its predecessor's, so the
    # predecessor's messages and side calls count toward it.
    if "predecessor_cost_usd" in cli:
        group.append((meta.get("continued_from") or {}).get("session_id", ""))
    tokens: dict[str, dict] = defaultdict(lambda: dict.fromkeys(TOKEN_KINDS, 0))
    calls: dict[str, int] = defaultdict(int)
    partial = 0
    for sid in group:
        for m, t in naive_all.get(sid, {}).get("tokens", {}).items():
            if m:
                for k in TOKEN_KINDS:
                    tokens[m][k] += t[k]
        sid_meta = (scores.get(sid) or {}).get("metadata") or {}
        for k, n in side_calls(naive_all.get(sid, {}), sid_meta).items():
            calls[k] += n
        partial += (sid_meta.get("subagent_costs") or {}).get("partial_messages", 0)

    check = hook.check_cost(dict(tokens), cli["by_model"], dict(calls), partial)
    if check["status"] == "overcount":
        return [f"overcount: transcript tokens exceed the CLI's own count by "
                f"${check['overcount_usd']:.4f} ({check['overcount']})"]
    if check["status"] == "unreconciled":
        explained = ", ".join(f"{n} {k}" for k, n in calls.items() if n) or "no side calls"
        return [f"undercount: the CLI counted {check['output_excess_tokens']:,} more output tokens "
                f"than the transcript records, beyond the {check['output_allowance']:,} that "
                f"{explained} explain"]
    return []


# ---------------------------------------------------------------------------
# Check 3: what Langfuse holds
# ---------------------------------------------------------------------------

def langfuse_credentials(from_settings: bool) -> dict:
    if from_settings:
        settings = json.loads((Path.home() / ".claude" / "settings.json").read_text())
        for group in settings.get("hooks", {}).get("Stop", []):
            for h in group.get("hooks", []):
                cmd = h.get("command", "")
                if "langfuse-hook.py" in cmd:
                    env = dict(re.findall(r"(LANGFUSE_[A-Z_]+)=(\S+)", cmd))
                    if "LANGFUSE_PUBLIC_KEY" in env:
                        env.setdefault("LANGFUSE_HOST", "http://localhost:3100")
                        return env
        raise SystemExit("audit_costs: no langfuse-hook.py Stop hook with keys in ~/.claude/settings.json")
    env = {k: os.environ.get(k, "") for k in ("LANGFUSE_HOST", "LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY")}
    env["LANGFUSE_HOST"] = env["LANGFUSE_HOST"] or "http://localhost:3100"
    if not env["LANGFUSE_PUBLIC_KEY"] or not env["LANGFUSE_SECRET_KEY"]:
        raise SystemExit("audit_costs: set LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY, or pass --from-settings")
    return env


# The metrics API returns at most this many rows per query.
METRICS_ROW_LIMIT = 1000


def langfuse_session_costs(env: dict, since: datetime) -> dict:
    """{session_id: stored total cost} of observations since `since`."""
    now = datetime.now(timezone.utc)
    query = {
        "view": "observations",
        "metrics": [{"measure": "totalCost", "aggregation": "sum"}],
        "dimensions": [{"field": "sessionId"}],
        "filters": [],
        "fromTimestamp": since.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "toTimestamp": (now + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "orderBy": [{"field": "sum_totalCost", "direction": "desc"}],
        "config": {"row_limit": METRICS_ROW_LIMIT},
    }
    url = (f"{env['LANGFUSE_HOST'].rstrip('/')}/api/public/v2/metrics?query="
           + urllib.parse.quote(json.dumps(query)))
    auth = base64.b64encode(f"{env['LANGFUSE_PUBLIC_KEY']}:{env['LANGFUSE_SECRET_KEY']}".encode()).decode()
    req = urllib.request.Request(url, headers={"Authorization": f"Basic {auth}"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            rows = json.load(resp)["data"]
    except urllib.error.HTTPError as e:
        raise ValueError(f"metrics API {e.code}: {e.read().decode(errors='replace')[:300]}") from e
    if len(rows) >= METRICS_ROW_LIMIT:
        raise ValueError(f"metrics API returned {len(rows)} sessions, its row limit; results would be cut off")
    return {r["sessionId"]: float(r.get("sum_totalCost") or 0) for r in rows if r.get("sessionId")}


# ---------------------------------------------------------------------------

def audit(projects: Path, hook_path: Path, langfuse_env: dict | None) -> dict:
    with tempfile.TemporaryDirectory(prefix="audit-costs-") as tmp:
        hook = load_hook(hook_path, tmp)
        naive = recount(projects, hook)
        scores = run_hook(projects, hook)
        hook_log = Path(tmp, "hook.log").read_text() if Path(tmp, "hook.log").exists() else ""
    stored = None
    if langfuse_env:
        # Only sessions whose transcripts are still on disk can be checked;
        # one day of margin for a session that started before its first entry.
        starts = [t for t in map(first_timestamp, projects.glob("*/*.jsonl")) if t]
        first = min(starts, default=None)
        since = (first or datetime.now(timezone.utc)) - timedelta(days=1)
        stored = langfuse_session_costs(langfuse_env, since)

    mtimes = {p.stem: p.stat().st_mtime for p in projects.glob("*/*.jsonl")}
    now = time.time()
    rows, findings = [], []
    for session_id in sorted(set(naive) | set(scores)):
        n = naive.get(session_id, {})
        s = scores.get(session_id, {})
        n_cost = round(n.get("cost", 0.0), 6)
        h_cost = round(float(s.get("value", 0.0)), 6)
        cli = (s.get("metadata") or {}).get("cli_cost_state") or {}
        row = {"session": session_id, "recount_usd": n_cost, "hook_usd": h_cost,
               "cli_usd": cli.get("total_cost_usd"), "findings": []}
        if abs(n_cost - h_cost) > COST_TOLERANCE_USD:
            kind = "hook over recount" if h_cost > n_cost else "hook under recount"
            row["findings"].append(f"recount: {kind} by ${abs(h_cost - n_cost):.4f}")
        if s:
            row["findings"] += cost_state_findings(session_id, naive, scores, hook)
        if stored is not None:
            active = now - mtimes.get(session_id, 0) < ACTIVE_S
            got = stored.get(session_id)
            row["langfuse_usd"] = None if got is None else round(got, 6)
            if active:
                row["note"] = "active session, Langfuse check skipped"
            elif got is None:
                if h_cost > COST_TOLERANCE_USD:
                    row["findings"].append(f"langfuse: session missing (hook computes ${h_cost:.4f})")
            elif abs(got - h_cost) > COST_TOLERANCE_USD:
                kind = "stored over hook (spans exported twice?)" if got > h_cost else "stored under hook (turns not sent?)"
                row["findings"].append(f"langfuse: {kind} by ${abs(got - h_cost):.4f}")
        rows.append(row)
        findings += [f"{session_id}: {f}" for f in row["findings"]]

    warnings = sorted({line.split("] ", 1)[-1] for line in hook_log.splitlines()
                       if "[WARN]" in line and ("calculate_turn_cost" in line or "unrecognised" in line)})
    findings += [f"hook: {w}" for w in warnings]
    totals = {
        "sessions": len(rows),
        "recount_usd": round(sum(r["recount_usd"] for r in rows), 4),
        "hook_usd": round(sum(r["hook_usd"] for r in rows), 4),
    }
    if stored is not None:
        totals["langfuse_usd_local_sessions"] = round(
            sum(stored.get(r["session"], 0.0) for r in rows), 4)
    return {"totals": totals, "sessions": rows, "findings": findings}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--hook", type=Path, default=DEFAULT_HOOK, help="hook to audit (default: this checkout's)")
    ap.add_argument("--projects", type=Path, default=DEFAULT_PROJECTS, help="transcript root")
    ap.add_argument("--langfuse", action="store_true", help="also compare against stored Langfuse costs")
    ap.add_argument("--from-settings", action="store_true",
                    help="with --langfuse: take host and keys from the Stop hook in ~/.claude/settings.json")
    ap.add_argument("--json", action="store_true", help="machine-readable report")
    args = ap.parse_args(argv)

    try:
        env = langfuse_credentials(args.from_settings) if args.langfuse else None
        report = audit(args.projects, args.hook, env)
    except (OSError, urllib.error.URLError, KeyError, ValueError) as e:
        print(f"audit_costs: could not run: {type(e).__name__}: {e}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        t = report["totals"]
        print(f"{t['sessions']} sessions  recount ${t['recount_usd']:.2f}  hook ${t['hook_usd']:.2f}"
              + (f"  langfuse ${t['langfuse_usd_local_sessions']:.2f}" if "langfuse_usd_local_sessions" in t else ""))
        if report["findings"]:
            print(f"\n{len(report['findings'])} finding(s):")
            for f in report["findings"]:
                print(f"  {f}")
        else:
            print("OK: no findings")
    return 1 if report["findings"] else 0


if __name__ == "__main__":
    sys.exit(main())
