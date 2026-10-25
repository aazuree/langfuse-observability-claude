#!/usr/bin/env python3
"""Turn real local sessions into a cost golden case for CI.

A golden case is a set of transcripts reduced to what billing reads — entry
types, ordering, message ids, models, usage, tool names, subagent links —
with every id re-hashed (random salt, not stored), every timestamp shifted
to start at 2026-01-01, and all text replaced. `tests/test_cost_goldens.py`
then pins what the hook bills for them, so a counting change that the unit
tests miss still fails CI.

The repo is public: the scrubbed output is checked for leftover free text
(any string that is not an id, model, tool name or known marker) and the
case is refused if one is found. Review the files before committing anyway.

    python3 tools/make_cost_golden.py CASE SESSION_ID [SESSION_ID ...] --why "what it covers"
    python3 tools/make_cost_golden.py --update        # re-pin every case's expected cost

The scrubbed transcripts must bill exactly what the originals do; the tool
stops if they don't (the scrub dropped something billing reads).
"""
import argparse
import hashlib
import importlib.util
import json
import re
import secrets
import shutil
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

_spec = importlib.util.spec_from_file_location("audit_costs", Path(__file__).with_name("audit_costs.py"))
audit_costs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(audit_costs)

GOLDEN_DIR = audit_costs.REPO_ROOT / "tests" / "golden"
PROJECT = "-repo"
EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)

# Entry types billing or turn building reads; every other type is dropped.
KEEP_TYPES = {"user", "assistant", "system", "cost-state", "continued-in", "fork-context-ref"}
KEEP_SYSTEM_SUBTYPES = {"compact_boundary", "away_summary", "api_error", "turn_duration"}
# Entry fields kept as-is (flags and small enums).
KEEP_FLAGS = ("type", "subtype", "isMeta", "isSidechain", "isApiErrorMessage", "isCompactSummary",
              "effort", "perTurnEffort", "promptSource", "turnOrigin", "durationMs", "messageCount",
              "sessionKind", "userType")
# Entry fields holding ids, re-hashed consistently across the case.
ID_FIELDS = ("uuid", "parentUuid", "sessionId", "requestId", "serverClassifierRequest",
             "continuedInSessionId", "parentSessionId", "parentLastUuid", "agentId",
             "interruptedMessageId", "toolUseID", "sourceToolUseID")
COST_STATE_FIELDS = ("totalCostUSD", "totalAPIDuration", "totalAPIDurationWithoutRetries",
                     "totalToolDuration", "totalDuration", "totalLinesAdded", "totalLinesRemoved",
                     "hasUnknownModelCost")
# Leading markers turn building and prompt classification look at; the rest
# of the text is replaced.
TEXT_MARKERS = re.compile(
    r"^(<[a-z][a-z0-9-]*>|Base directory for this skill:|\[Request interrupted by user[^\]]*\]|\[Image)")
PLACEHOLDER = "redacted"


class Scrubber:
    def __init__(self):
        self.salt = secrets.token_bytes(16)
        self.t0 = None

    def id(self, value):
        if not isinstance(value, str) or not value:
            return value
        digest = hashlib.sha256(self.salt + value.encode()).hexdigest()
        if re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", value):
            return str(uuid.UUID(digest[:32]))
        prefix = re.match(r"^[a-z]+_(?:0\d)?", value)
        return (prefix.group(0) if prefix else "") + digest[:24]

    def ts(self, value):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (AttributeError, ValueError):
            return None
        return (EPOCH + (dt - self.t0)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    def text(self, value):
        if not isinstance(value, str):
            return PLACEHOLDER
        m = TEXT_MARKERS.match(value.strip())
        return f"{m.group(0)} {PLACEHOLDER}" if m else PLACEHOLDER

    def content(self, content):
        if isinstance(content, str):
            return self.text(content)
        out = []
        for block in content if isinstance(content, list) else ():
            if not isinstance(block, dict):
                continue
            kind = block.get("type")
            if kind == "text":
                out.append({"type": "text", "text": self.text(block.get("text"))})
            elif kind == "tool_use":
                out.append({"type": "tool_use", "id": self.id(block.get("id")),
                            "name": block.get("name"), "input": {}})
            elif kind == "tool_result":
                out.append({"type": "tool_result", "tool_use_id": self.id(block.get("tool_use_id")),
                            "is_error": bool(block.get("is_error")), "content": PLACEHOLDER})
            elif kind in ("thinking", "redacted_thinking"):
                out.append({"type": kind, "thinking": ""})
            elif kind == "image":
                out.append({"type": "image"})
        return out

    def entry(self, e):
        if e.get("type") not in KEEP_TYPES:
            return None
        if e.get("type") == "system" and e.get("subtype") not in KEEP_SYSTEM_SUBTYPES:
            return None
        out = {k: e[k] for k in KEEP_FLAGS if k in e}
        out.update({k: self.id(e[k]) for k in ID_FIELDS if k in e})
        if "timestamp" in e:
            out["timestamp"] = self.ts(e["timestamp"])
        if "cwd" in e:
            out["cwd"] = "/repo"
        if e.get("type") == "cost-state":
            out.update({k: e[k] for k in COST_STATE_FIELDS if k in e})
            if isinstance(e.get("startTime"), (int, float)):
                start = datetime.fromtimestamp(e["startTime"] / 1000, timezone.utc)
                out["startTime"] = int((EPOCH + (start - self.t0)).timestamp() * 1000)
            out["modelUsage"] = e.get("modelUsage") or {}
        if e.get("type") == "system" and e.get("compactMetadata"):
            out["compactMetadata"] = {k: v for k, v in e["compactMetadata"].items()
                                      if k in ("trigger", "preTokens", "postTokens", "durationMs")}
        msg = e.get("message")
        if isinstance(msg, dict):
            m = {"role": msg.get("role"), "content": self.content(msg.get("content"))}
            for k in ("model", "usage", "stop_reason"):
                if k in msg:
                    m[k] = msg[k]
            if "id" in msg:
                m["id"] = self.id(msg["id"])
            if isinstance(msg.get("stop_details"), dict):
                m["stop_details"] = {"category": msg["stop_details"].get("category")}
            out["message"] = m
        return out

    def meta(self, meta):
        return {k: (self.id(v) if k == "toolUseId" else v) for k, v in meta.items()
                if k in ("agentType", "toolUseId", "spawnDepth", "requestShape", "isBuiltIn")}


ALLOWED_STRING = re.compile(
    r"^(|redacted|/repo|[0-9a-f-]{8,}|[a-z]+_(?:0\d)?[0-9a-f]{24}|\d{4}-\d{2}-\d{2}T[\d:.]+Z"
    r"|claude-[a-z0-9.-]+|<synthetic>|[A-Za-z][A-Za-z0-9_-]{0,39}|<[a-z][a-z0-9-]*> redacted"
    r"|Base directory for this skill: redacted|\[Request interrupted by user[^\]]*\] redacted"
    r"|\[Image redacted)$")


def leftover_text(obj, path="$"):
    """Strings the scrub should not have let through."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k != "modelUsage":
                yield from leftover_text(v, f"{path}.{k}")
            else:  # model names as keys
                yield from (f"{path}.modelUsage key {m!r}" for m in v if not ALLOWED_STRING.match(m))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from leftover_text(v, f"{path}[{i}]")
    elif isinstance(obj, str) and not ALLOWED_STRING.match(obj):
        yield f"{path} = {obj[:60]!r}"


def session_files(projects: Path, sid: str):
    main = next(projects.glob(f"*/{sid}.jsonl"), None)
    if main is None:
        raise SystemExit(f"make_cost_golden: no transcript for {sid}")
    yield main, f"{sid}.jsonl"
    for sub in sorted((main.parent / sid / "subagents").glob("*")):
        yield sub, f"{sid}/subagents/{sub.name}"


def bill(projects: Path) -> dict:
    """{session: {hook_usd, recount_usd, generations}} for a projects dir."""
    generations: dict = {}

    def count(batch):
        sid = next((e["body"].get("sessionId") for e in batch if e.get("type") == "score-create"), None)
        generations[sid] = generations.get(sid, 0) + sum(e.get("type") == "generation-create" for e in batch)

    with tempfile.TemporaryDirectory(prefix="golden-") as tmp:
        hook = audit_costs.load_hook(audit_costs.DEFAULT_HOOK, tmp)
        recount = audit_costs.recount(projects, hook)
        scores = audit_costs.run_hook(projects, hook, on_batch=count)
    return {sid: {"hook_usd": round(float(b["value"]), 6),
                  "recount_usd": round(recount.get(sid, {}).get("cost", 0.0), 6),
                  "generations": generations.get(sid, 0)}
            for sid, b in sorted(scores.items())}


def make_case(name: str, sids: list[str], why: str, projects: Path) -> Path:
    scrub = Scrubber()
    # One clock for the whole case: continuations and forks compare times
    # across transcripts.
    starts = [audit_costs.first_timestamp(src) for sid in sids for src, _ in session_files(projects, sid)
              if src.suffix == ".jsonl"]
    scrub.t0 = min(t for t in starts if t)
    case = GOLDEN_DIR / name
    with tempfile.TemporaryDirectory(prefix="golden-src-") as tmp:
        # The originals, alone, as the reference: what the hook bills for them.
        src_projects = Path(tmp) / "projects"
        for sid in sids:
            for src, rel in session_files(projects, sid):
                dest = src_projects / PROJECT / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dest)
        original = bill(src_projects)

        out_projects = Path(tmp) / "scrubbed"
        problems = []
        for sid in sids:
            for src, rel in session_files(projects, sid):
                rel = re.sub(r"[0-9a-f]{8}-[0-9a-f-]{27}", lambda m: scrub.id(m.group(0)), rel)
                rel = re.sub(r"agent-([0-9a-f]+)", lambda m: "agent-" + scrub.id(m.group(1))[:17], rel)
                dest = out_projects / PROJECT / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                if src.name.endswith(".meta.json"):
                    data = scrub.meta(json.loads(src.read_text()))
                    problems += leftover_text(data, rel)
                    dest.write_text(json.dumps(data, sort_keys=True) + "\n")
                    continue
                lines = []
                for entry in audit_costs.read_entries(src):
                    out = scrub.entry(entry)
                    if out is not None:
                        problems += leftover_text(out, rel)
                        lines.append(json.dumps(out, sort_keys=True))
                dest.write_text("\n".join(lines) + "\n")
        if problems:
            raise SystemExit("make_cost_golden: free text survived the scrub:\n  " + "\n  ".join(problems[:20]))
        scrubbed = bill(out_projects)
        mapped = {scrub.id(sid): v for sid, v in original.items()}
        if mapped != scrubbed:
            raise SystemExit(f"make_cost_golden: the scrub changed billing\n  original {mapped}\n  scrubbed {scrubbed}")
        if case.exists():
            shutil.rmtree(case)
        shutil.copytree(out_projects, case / "projects")
    (case / "expected.json").write_text(json.dumps(
        {"covers": why, "sessions": scrubbed}, indent=2, sort_keys=True) + "\n")
    return case


def update_all() -> None:
    for case in sorted(p for p in GOLDEN_DIR.iterdir() if (p / "expected.json").exists()):
        expected = json.loads((case / "expected.json").read_text())
        expected["sessions"] = bill(case / "projects")
        (case / "expected.json").write_text(json.dumps(expected, indent=2, sort_keys=True) + "\n")
        print(f"{case.name}: {expected['sessions']}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("case", nargs="?", help="golden case name (directory under tests/golden)")
    ap.add_argument("sessions", nargs="*", help="session ids to include (a continuation needs both)")
    ap.add_argument("--why", default="", help="what the case covers (stored in expected.json)")
    ap.add_argument("--projects", type=Path, default=audit_costs.DEFAULT_PROJECTS)
    ap.add_argument("--update", action="store_true", help="re-pin every case's expected billing")
    args = ap.parse_args(argv)
    if args.update:
        update_all()
        return 0
    if not args.case or not args.sessions or not args.why:
        ap.error("CASE, at least one SESSION_ID and --why are required")
    case = make_case(args.case, args.sessions, args.why, args.projects)
    print(f"wrote {case.relative_to(audit_costs.REPO_ROOT)}: "
          f"{json.loads((case / 'expected.json').read_text())['sessions']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
