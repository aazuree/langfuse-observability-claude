#!/usr/bin/env python3
"""Claude Code Stop hook -> Langfuse ingestion.

Incrementally processes the full conversation transcript and sends:
  - One trace per session
  - One generation per user->assistant turn (with tokens, latency, TTFT, cost)
  - One span per tool use (with input, output, and duration)

Uses a state file to track what's already been sent, so each Stop hook
invocation only sends new messages.

Environment variables:
  LANGFUSE_PUBLIC_KEY  - Langfuse project public key
  LANGFUSE_SECRET_KEY  - Langfuse project secret key
  LANGFUSE_HOST        - Langfuse base URL (default: http://localhost:3100)
"""

import contextlib
import gzip
import hashlib
import json
import os
import re
import sys
import time
import traceback
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# Ensure langfuse_common can be imported from the same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from langfuse_common import (
    atomic_write_text,
    iter_transcript,
    log as common_log,
    make_auth_header,
    redact_secrets,
)
import langfuse_otlp

LANGFUSE_HOST = os.environ.get("LANGFUSE_HOST", "http://localhost:3100")
LANGFUSE_PUBLIC_KEY = os.environ.get("LANGFUSE_PUBLIC_KEY", "")
LANGFUSE_SECRET_KEY = os.environ.get("LANGFUSE_SECRET_KEY", "")
OTLP_URL = f"{LANGFUSE_HOST}/api/public/otel/v1/traces"
SCORES_URL = f"{LANGFUSE_HOST}/api/public/scores"

# LANGFUSE_HOOK_LOG lets the test suite redirect the log; without it a pytest run
# writes fixture sessions into the production log and makes the tail useless.
LOG_FILE = os.environ.get("LANGFUSE_HOOK_LOG") or os.path.expanduser(
    "~/.claude/langfuse-hook.log"
)
STATE_DIR = os.path.expanduser("~/.claude/langfuse-state")
PROJECTS_DIR = os.path.expanduser("~/.claude/projects")

MAX_TEXT = 10000
MAX_TOOL_IO = 5000
SAFE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")

# Synthetic user-message wrappers (slash commands, local-command output,
# system reminders, prompt-submit hooks). Used to skip these when picking
# a human-readable trace name from the first user prompt.
_SYNTHETIC_PROMPT_RE = re.compile(r"^<[a-z][a-z0-9-]*>")

# How long a fire waits for another fire of the same session to finish.
# Longer than a worst-case send: one 30s OTLP request plus up to four 30s
# score requests (150s). A fire that gives up waiting skips its lines, and a
# skipped SessionEnd fire would leave the last turn for a reprocess.
SESSION_LOCK_TIMEOUT_S = 180

MAX_SUBAGENT_DEPTH = 5  # CC 2.1.172: sub-agents can spawn sub-agents up to 5 levels




def log(msg: str) -> None:
    """Wrapper around common_log for backward compatibility."""
    common_log(LOG_FILE, msg)


def sanitize_id(value: str) -> str:
    """Sanitize an ID to prevent path traversal. Returns a safe fallback if invalid."""
    if SAFE_ID_RE.match(value):
        return value
    # Fall back to a hash of the value
    return hashlib.sha256(value.encode()).hexdigest()[:32]


def _entries(source):
    """Transcript entries from a path, or pass an already-parsed list through.

    Every extract_* function takes either. process_session parses the
    transcript once and hands the list to all of them; tests and one-off
    callers can still pass a path.
    """
    if isinstance(source, (list, tuple)):
        return source
    return iter_transcript(source)


# Statuses that no retry can fix: malformed, too large, or unprocessable.
PERMANENT_HTTP_STATUSES = frozenset({400, 413, 422})


def _post(url: str, data: bytes, headers: dict) -> str:
    """POST to Langfuse. Returns "ok", "permanent" or "transient"."""
    req = Request(url, data=data, method="POST", headers={
        "Content-Type": "application/json",
        "Authorization": make_auth_header(),
        **headers,
    })
    try:
        with urlopen(req, timeout=30) as resp:
            resp.read()
        return "ok"
    except HTTPError as e:
        try:
            detail = e.read().decode(errors="replace")[:500]
        except OSError:
            detail = ""
        if e.code in PERMANENT_HTTP_STATUSES:
            log(f"[ERROR] Langfuse rejected {url.rsplit('/api/public/', 1)[-1]} "
                f"(HTTP {e.code}): {detail}")
            return "permanent"
        # 401/403 (bad keys) get fixed by a human; 408/429/5xx pass. Either way
        # dropping the data now would lose it for good.
        log(f"Failed to send to Langfuse: HTTP {e.code} {e.reason}")
        return "transient"
    except TimeoutError as e:
        # Langfuse may already have stored the export; v4 keeps both copies
        # when the retry lands. Retrying still beats dropping it for good.
        log(f"[WARN] Langfuse did not answer in time ({e}); retrying next fire. "
            "If it stored the export, the retry duplicates it")
        return "transient"
    except (URLError, OSError) as e:
        log(f"Failed to send to Langfuse: {e}")
        return "transient"


def _post_json(method: str, url: str, payload: dict | None = None) -> str:
    """JSON request with the same outcome classification as _post."""
    data = json.dumps(payload).encode() if payload is not None else None
    req = Request(url, data=data, method=method, headers={
        "Content-Type": "application/json",
        "Authorization": make_auth_header(),
    })
    try:
        with urlopen(req, timeout=30) as resp:
            resp.read()
        return "ok"
    except HTTPError as e:
        log(f"[ERROR] Langfuse {method} {url.rsplit('/api/public/', 1)[-1]} failed: "
            f"HTTP {e.code} {e.reason}")
        return "permanent" if e.code in PERMANENT_HTTP_STATUSES else "transient"
    except (URLError, TimeoutError, OSError) as e:
        log(f"[ERROR] Langfuse {method} failed: {e}")
        return "transient"


def send_to_langfuse(batch: list[dict]) -> bool:
    """Export one fire's events: spans over OTLP, then scores.

    Returns True when the caller may advance the session state — the spans
    were delivered, or rejected in a way no retry can fix (logged [ERROR]).

    The spans go in a single gzipped request, so they land whole or not at
    all: Langfuse v4 does not deduplicate a re-exported span, and a retry
    after a partial success would duplicate the part that got through.
    Scores are sent only after the spans landed. They upsert by id and are
    recomputed every fire, so a failed score is logged and left to the next
    fire instead of holding the spans back.
    """
    export, scores = langfuse_otlp.build_export(batch)
    spans = export["resourceSpans"][0]["scopeSpans"][0]["spans"]
    if spans:
        result = _post(
            OTLP_URL,
            gzip.compress(json.dumps(export).encode()),
            {"Content-Encoding": "gzip", "x-langfuse-ingestion-version": "4"},
        )
        if result == "transient":
            return False
        if result == "ok":
            log(f"Langfuse accepted {len(spans)} span(s)")
    for score in scores:
        if _post(SCORES_URL, json.dumps(score).encode(), {}) != "ok":
            log(f"[WARN] Score {score.get('name')} not sent; it is recomputed next fire")
    return True


def load_state(session_id: str) -> tuple[int, int]:
    """Return (line_offset, turn_count). Handles legacy plain-int state files."""
    Path(STATE_DIR).mkdir(parents=True, exist_ok=True)
    state_file = os.path.join(STATE_DIR, f"{session_id}.offset")
    try:
        content = Path(state_file).read_text().strip()
        try:
            data = json.loads(content)
            if isinstance(data, dict):
                return data["lines"], data["turns"]
            # Legacy format: json.loads("52") → int 52
            return int(data), 0
        except (json.JSONDecodeError, KeyError):
            # Legacy format: non-JSON plain integer string
            return int(content), 0
    except (IOError, OSError, ValueError):
        return 0, 0


def save_state(session_id: str, line_offset: int, turn_count: int) -> None:
    Path(STATE_DIR).mkdir(parents=True, exist_ok=True)
    state_file = os.path.join(STATE_DIR, f"{session_id}.offset")
    atomic_write_text(state_file, json.dumps({"lines": line_offset, "turns": turn_count}))


def load_subagent_state(session_id: str) -> dict:
    """Load subagent processing state. Returns {} if no state exists."""
    Path(STATE_DIR).mkdir(parents=True, exist_ok=True)
    state_file = os.path.join(STATE_DIR, f"{session_id}.subagents.json")
    try:
        return json.loads(Path(state_file).read_text())
    except (IOError, OSError, json.JSONDecodeError):
        # File doesn't exist, can't be read, or contains invalid JSON
        return {}


def save_subagent_state(session_id: str, state: dict) -> None:
    """Save subagent processing state."""
    Path(STATE_DIR).mkdir(parents=True, exist_ok=True)
    state_file = os.path.join(STATE_DIR, f"{session_id}.subagents.json")
    atomic_write_text(state_file, json.dumps(state))


def _project_dir_from_cwd(cwd: str) -> str:
    """Mirror Claude Code's project-dir encoding: '/' and '.' both → '-'."""
    return cwd.replace("/", "-").replace(".", "-")


def _subagent_search_dirs(transcript_path: str, cwd: str = "") -> list:
    """Resolve candidate subagents/ directories for a session.

    Primary: <transcript without .jsonl>/subagents/. Fallback: cwd-derived
    project dir (sessions that moved into a worktree mid-run). Earlier
    entries win on duplicates.
    """
    base = transcript_path
    if base.endswith(".jsonl"):
        base = base[:-6]
    primary_dir = os.path.join(base, "subagents")

    search_dirs = []
    if os.path.isdir(primary_dir):
        search_dirs.append(primary_dir)

    if cwd:
        session_id = os.path.basename(base)
        alt_dir = os.path.join(
            PROJECTS_DIR,
            _project_dir_from_cwd(cwd),
            session_id,
            "subagents",
        )
        if alt_dir not in search_dirs and os.path.isdir(alt_dir):
            search_dirs.append(alt_dir)
    return search_dirs


def build_subagent_meta_index(search_dirs: list) -> dict:
    """Map Agent tool_use ids to subagent transcripts via .meta.json sidecars.

    Each agent-<id>.meta.json carries {"agentType", "description", "toolUseId"}
    where toolUseId is the Agent tool_use id in the *spawner's* transcript
    (main session at depth 1, the parent agent's transcript at depth 2+).
    The subagents/ dir is flat at every nesting depth.

    Returns {toolUseId: {"agent_id", "path", "agent_type", "description"}}.
    Earlier search_dirs win on duplicates. aside_question agents are skipped.
    """
    index = {}
    for sdir in search_dirs:
        try:
            fnames = sorted(os.listdir(sdir))
        except OSError:
            continue
        for fname in fnames:
            if not fname.startswith("agent-") or not fname.endswith(".meta.json"):
                continue
            if fname.startswith("agent-aside_question-"):
                continue
            agent_id = fname[len("agent-"):-len(".meta.json")]
            jsonl_path = os.path.join(sdir, f"agent-{agent_id}.jsonl")
            if not os.path.isfile(jsonl_path):
                continue
            try:
                meta = json.loads(Path(os.path.join(sdir, fname)).read_text())
            except (IOError, OSError, json.JSONDecodeError):
                continue
            tool_use_id = meta.get("toolUseId")
            if tool_use_id and tool_use_id not in index:
                index[tool_use_id] = {
                    "agent_id": agent_id,
                    "path": jsonl_path,
                    "agent_type": meta.get("agentType", ""),
                    "description": meta.get("description", ""),
                }
    return index


def _file_size(path: str) -> int | None:
    try:
        return os.path.getsize(path)
    except OSError:
        return None


# sa_state bookkeeping that is not reported in trace metadata (a path would
# also leak the local username).
_SA_STATE_ONLY_KEYS = frozenset({"path", "size", "parent_span_id", "trace_id", "offset", "correlation",
                                 "message_ids"})
# Per-agent facts ingest_subagent reads from the whole transcript.
_SA_OBSERVED_KEYS = ("models", "classifier_requests", "partial_messages",
                     "tokens_by_model", "web_tool_calls", "message_ids")


def _subagent_record(prev: dict, events: list, cost: dict, offset: int,
                     turn_count: int, status: str, **fields) -> dict:
    """The sa_state entry for one subagent after an ingest_subagent call.

    `cost` is the agent's whole cost (ingest_subagent re-reads the full
    transcript), so it replaces the previous totals instead of adding to
    them. When the call produced no events the agent had nothing new, and
    its recorded status and totals are kept.

    `path`, `size` and `parent_span_id` let a later fire resume the agent
    without finding its Agent call again — a background agent keeps writing
    after the parent turn that launched it has been sent.
    """
    # Read from the whole transcript on every full read, so they refresh even
    # when the read sent nothing new (an agent recorded before they existed).
    observed = {k: cost[k] for k in _SA_OBSERVED_KEYS if k in cost}
    if not events and prev:
        return {**prev, **fields, **observed, "offset": offset}
    return {
        **prev,
        **fields,
        **observed,
        "offset": offset,
        "turn_count": turn_count,
        "status": status,
        "total_cost": cost["total_cost"],
        "total_tokens": cost["total_tokens"],
        "cost_breakdown": cost["cost_breakdown"],
    }


def _usage_details(turn: dict) -> dict:
    usage = turn["usage"]
    return {
        "input": usage["input"],
        "output": usage["output"],
        "total": usage["total"],
        "cache_read": usage["cache_read"],
        "cache_create": usage["cache_creation"],
        "cache_5m": turn.get("cache_ephemeral_5m", 0),
        "cache_1h": turn.get("cache_ephemeral_1h", 0),
        # Subset of `output` (billed at the output rate), reported
        # alongside it the same way cache_5m/cache_1h break down cache_create.
        "output_thinking": turn.get("thinking_tokens", 0),
    }


def _tool_input(raw):
    """Redacted tool input; parsed back to JSON when it fits, else a preview."""
    text = redact_secrets(raw if isinstance(raw, str) else json.dumps(raw))
    if len(text) > MAX_TOOL_IO:
        return {"_truncated": True, "preview": text[:MAX_TOOL_IO]}
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return text


def _tool_output(raw: str) -> str:
    text = redact_secrets(raw)
    if len(text) > MAX_TOOL_IO:
        text = text[:MAX_TOOL_IO] + "...[truncated]"
    return text


# CC 2.1.289+ ends a subagent with this tool call (its report to the parent)
# instead of a final end_turn reply, so its last turn has no terminal stop_reason.
SUBAGENT_HANDBACK_TOOL = "SubagentHandback"


def _handed_back(entries: list) -> bool:
    """Whether the agent's last reply is a SubagentHandback call whose result
    is already written. Requiring the result keeps an async fire that reads
    between the two from exporting the handback span without its output."""
    for i in range(len(entries) - 1, -1, -1):
        entry = entries[i]
        if entry.get("type") != "assistant" or entry.get("isApiErrorMessage"):
            continue
        content = (entry.get("message") or {}).get("content")
        ids = {b.get("id") for b in content if isinstance(b, dict)
               and b.get("type") == "tool_use" and b.get("name") == SUBAGENT_HANDBACK_TOOL
               } if isinstance(content, list) else set()
        if not ids:
            return False
        for later in entries[i + 1:]:
            c = (later.get("message") or {}).get("content") if later.get("type") == "user" else None
            if isinstance(c, list) and any(isinstance(b, dict) and b.get("type") == "tool_result"
                                           and b.get("tool_use_id") in ids for b in c):
                return True
        return False
    return False


def ingest_subagent(
    agent_id: str,
    transcript_path: str,
    parent_span_id: str,
    trace_id: str,
    session_id: str,
    subagent_offset: int,
    prior_turn_count: int = 0,
    meta_index: dict = None,
    depth: int = 1,
    visited: set = None,
    sa_state: dict = None,
    final: bool = False,
) -> tuple:
    """Parse a subagent transcript and build events for its finished turns.

    Reads the whole transcript every time: a background agent is usually
    mid-turn when a fire reads it. Turns are sent once, from
    `prior_turn_count` up to the last finished one (`complete_turn_count`);
    an unfinished last turn waits for a later fire. `cost_summary` is the
    agent's *whole* cost — callers replace their totals with it.

    Returns nothing new (empty events) when the transcript has not grown
    since `subagent_offset` and `final` is not set.

    Recurses into nested Agent dispatches (CC 2.1.172+) resolved via
    meta_index, up to MAX_SUBAGENT_DEPTH levels; `visited` guards cycles.
    Nested children are recorded in sa_state with depth/parent lineage.

    Returns: (events, cost_summary, new_offset, new_turn_count, status)
    """
    if visited is None:
        visited = set()
    visited.add(agent_id)
    meta_index = meta_index or {}

    empty_cost = {"agent_id": agent_id, "total_cost": 0, "total_tokens": 0,
                  "turns": 0, "status": "partial",
                  "cost_breakdown": {"input": 0, "output": 0, "cache_read": 0, "cache_creation": 0}}

    entries, total_lines, read_ok = parse_transcript(transcript_path, warn_after=subagent_offset)
    if not read_ok:
        # Preserve the prior offset on read failure so the next fire retries.
        return [], empty_cost, subagent_offset, prior_turn_count, "partial"
    if not entries or (subagent_offset and total_lines == subagent_offset and not final):
        return [], empty_cost, total_lines, prior_turn_count, "partial"

    turns = build_turns(entries)
    if not turns:
        return [], empty_cost, total_lines, prior_turn_count, "partial"
    handed_back = _handed_back(entries)
    sendable = complete_turn_count(turns, final or handed_back)

    # Complete when the last assistant entry ended its turn. stop_reason lives
    # inside `message`; reading it off the entry itself never matched, so
    # every subagent used to report "partial".
    status = "complete" if handed_back else "partial"
    for entry in reversed(entries):
        if entry.get("type") == "assistant" and not entry.get("isApiErrorMessage"):
            if (entry.get("message") or {}).get("stop_reason") == "end_turn":
                status = "complete"
            break

    user_id = _user_id()
    events = []
    total_cost = 0.0
    total_tokens = 0
    cost_breakdown = {"input": 0.0, "output": 0.0, "cache_read": 0.0, "cache_creation": 0.0}

    for turn_idx, turn in enumerate(turns):
        model = turn.get("model") or ""
        turn_cost, _in, _out, cost_details = price_turn(turn)
        total_cost += turn_cost
        total_tokens += turn["usage"]["total"]
        if cost_details:
            cost_breakdown["input"] += cost_details.get("input", 0)
            cost_breakdown["output"] += cost_details.get("output", 0)
            cost_breakdown["cache_read"] += cost_details.get("cache_read_input_tokens", 0)
            cost_breakdown["cache_creation"] += cost_details.get("cache_creation_input_tokens", 0)
        if not prior_turn_count <= turn_idx < sendable:
            continue  # already sent, or not finished yet; counted in the totals only

        gen_id = f"gen-subagent-{agent_id}-{turn_idx}"
        start_time = turn["start_time"]
        events.append({"type": "generation-create", "body": {
            "id": gen_id,
            "traceId": trace_id,
            "parentObservationId": parent_span_id,
            "sessionId": session_id,
            "userId": user_id,
            "name": f"Subagent Turn {turn_idx + 1}: {redact_secrets(truncate(turn['display_prompt'], 80))}",
            "model": model,
            "input": redact_secrets(truncate(turn["user_input"], MAX_TEXT)) or None,
            "output": redact_secrets(truncate(turn["assistant_output"], MAX_TEXT)) or None,
            "startTime": start_time,
            "endTime": turn["end_time"],
            "usageDetails": _usage_details(turn),
            "costDetails": cost_details if cost_details else None,
            "metadata": {
                "subagent_id": agent_id,
                "agent_id": agent_id,
                "subagent_depth": depth,
                "tools_used": [tc["name"] for tc in turn["tool_calls"]],
                "tool_count": len(turn["tool_calls"]),
                "speed": turn.get("speed", ""),
                "service_tier": turn.get("service_tier", ""),
                "inference_geo": turn.get("inference_geo", ""),
                "request_ids": list(dict.fromkeys(turn.get("request_ids", []))),
                "web_search_requests": turn.get("web_search_requests", 0),
                "web_fetch_requests": turn.get("web_fetch_requests", 0),
                "cost_usd": round(turn_cost, 6),
                **gen_metadata_turn_signals(turn),
            },
            "attributes": _otel_genai_attrs(
                session_id=session_id,
                model=model,
                usage=turn["usage"],
                request_ids=turn.get("request_ids", []),
                stop_reason=turn.get("stop_reason", ""),
            ),
        }})

        for tc_idx, tc in enumerate(turn["tool_calls"]):
            span_id = f"span-subagent-{agent_id}-{turn_idx}-{tc_idx}"
            span_start = tc.get("start_time", start_time)
            events.append({"type": "span-create", "body": {
                "id": span_id,
                "traceId": trace_id,
                "parentObservationId": gen_id,
                "sessionId": session_id,
                "userId": user_id,
                "name": tc["name"],
                "startTime": span_start,
                "endTime": tc.get("end_time", span_start),
                "input": _tool_input(tc["input"]),
                "output": _tool_output(tc["output"]) or None,
                "metadata": {"tool_use_id": tc["id"], "subagent_id": agent_id},
            }})

            child = meta_index.get(tc["id"]) if tc["name"] == "Agent" else None
            if child and depth < MAX_SUBAGENT_DEPTH and child["agent_id"] not in visited:
                child_id = child["agent_id"]
                child_prev = (sa_state or {}).get(child_id, {})
                c_size = _file_size(child["path"])
                c_events, c_cost, c_offset, c_tc, c_status = ingest_subagent(
                    agent_id=child_id,
                    transcript_path=child["path"],
                    parent_span_id=span_id,
                    trace_id=trace_id,
                    session_id=session_id,
                    subagent_offset=child_prev.get("offset", 0),
                    prior_turn_count=child_prev.get("turn_count", 0),
                    meta_index=meta_index,
                    depth=depth + 1,
                    visited=visited,
                    sa_state=sa_state,
                    final=final,
                )
                events.extend(c_events)
                # Nested-child cost stays out of this agent's own cost_summary;
                # the session subagent_costs rollup sums all agents flat.
                if sa_state is not None:
                    sa_state[child_id] = _subagent_record(
                        child_prev, c_events, c_cost, c_offset, c_tc, c_status,
                        description=child["description"],
                        subagent_type=child["agent_type"],
                        path=child["path"], size=c_size,
                        parent_span_id=span_id, trace_id=trace_id,
                        depth=depth + 1, parent_agent_id=agent_id,
                    )

    cost_summary = {
        "agent_id": agent_id,
        "total_cost": round(total_cost, 6),
        "total_tokens": total_tokens,
        "turns": len(turns),
        "status": status,
        "cost_breakdown": {k: round(v, 6) for k, v in cost_breakdown.items()},
        "models": sorted(message_models(entries)),
        "classifier_requests": count_classifier_requests(entries),
        "partial_messages": count_partial_messages(entries),
        # For the session's cost_check (check_cost, duplicate billing).
        "tokens_by_model": tokens_by_model(turns),
        "web_tool_calls": count_web_tool_calls(turns),
        "message_ids": sorted({mid for t in turns for mid in t["api_call_ids"]}),
    }
    return events, cost_summary, total_lines, max(prior_turn_count, sendable), status


def extract_slug(transcript_path) -> str:
    """First non-empty `slug` field (the random 3-word session handle).

    Removed from transcripts in Claude Code v2.1.112 and back as a field on
    assistant/user entries from ~v2.1.211. Only used to label sessions in
    --reprocess output; never used for trace naming.
    """
    for entry in _entries(transcript_path):
        slug = entry.get("slug", "")
        if slug:
            return slug
    return ""


def extract_custom_title(transcript_path: str) -> str:
    """First non-empty customTitle entry (v2.1.110+ transcripts).

    Set by the user via the in-CLI title command. Absent in most sessions.
    """
    for entry in _entries(transcript_path):
        if entry.get("type") == "custom-title":
            title = entry.get("customTitle", "")
            if title:
                return title
    return ""


def extract_agent_name(transcript_path: str) -> str:
    """First non-empty agentName from type: 'agent-name' entries.

    Written mid-session (~30% through) once the model identifies the task.
    Absent in sessions that ended before Claude generated a name.
    """
    for entry in _entries(transcript_path):
        if entry.get("type") == "agent-name":
            name = entry.get("agentName", "")
            if name:
                return name
    return ""


def extract_worktree_state(transcript_path: str) -> dict | None:
    """Last type:'worktree-state' entry (worktree sessions, v2.1.16x+).

    Sessions can enter/exit worktrees mid-run; the last entry reflects the
    final state. None when the session never entered a worktree.
    """
    worktree = None
    for entry in _entries(transcript_path):
        if entry.get("type") == "worktree-state":
            ws = entry.get("worktreeSession") or {}
            if ws:
                worktree = {
                    "name": ws.get("worktreeName", ""),
                    "branch": ws.get("worktreeBranch", ""),
                    "original_cwd": ws.get("originalCwd", ""),
                    "original_branch": ws.get("originalBranch", ""),
                    "original_head_commit": ws.get("originalHeadCommit", ""),
                }
    return worktree


def extract_ai_title(transcript_path: str) -> str:
    """First non-empty aiTitle from type: 'ai-title' entries.

    Claude Code generates a short human-readable title once it has enough
    context to summarise the session (typically several turns in). The same
    title is written on every subsequent turn; first non-empty wins.
    Absent in sessions that ended before a title was produced.
    """
    for entry in _entries(transcript_path):
        if entry.get("type") == "ai-title":
            title = entry.get("aiTitle", "")
            if title:
                return title
    return ""


def extract_session_kind(transcript_path: str) -> str:
    """First non-empty sessionKind value.

    'bg' (background job) vs 'fg' (interactive foreground). Defaults to 'fg'
    when no entry carries the field (older transcripts).
    """
    for entry in _entries(transcript_path):
        kind = entry.get("sessionKind", "")
        if kind:
            return kind
    return "fg"


_LOCAL_CMD_WRAPPER_RE = re.compile(
    r"^\s*<local-command-(?:stdout|stderr)>(.*?)</local-command-(?:stdout|stderr)>\s*$",
    re.DOTALL,
)


def extract_local_commands(transcript_path: str, max_entries: int = 20, max_content: int = 200) -> list:
    """Collect non-empty system/local_command entries (slash-command output).

    Strips the `<local-command-stdout>…</local-command-stdout>` wrapper,
    runs the existing secret-redaction pass on the body, and caps both the
    number of entries and per-entry length to keep trace metadata small.
    """
    out = []
    for entry in _entries(transcript_path):
        if entry.get("type") != "system" or entry.get("subtype") != "local_command":
            continue
        content = entry.get("content", "") or ""
        match = _LOCAL_CMD_WRAPPER_RE.match(content)
        body = (match.group(1) if match else content).strip()
        if not body:
            continue
        out.append({
            "content": redact_secrets(truncate(body, max_content)),
            "timestamp": entry.get("timestamp", ""),
        })
        if len(out) >= max_entries:
            break
    return out


def extract_attachments(transcript_path: str) -> dict:
    """Count attachment entries and group by attachment.type.

    Attachments include hook outputs, file contents the user dropped in, and
    image pastes. Only counts + types are captured — payloads can be large
    and may contain sensitive content. Returns {} when no attachments exist.
    """
    by_type: dict[str, int] = {}
    total = 0
    for entry in _entries(transcript_path):
        if entry.get("type") != "attachment":
            continue
        total += 1
        att = entry.get("attachment", {}) or {}
        att_type = att.get("type", "unknown") if isinstance(att, dict) else "unknown"
        by_type[att_type] = by_type.get(att_type, 0) + 1
    if total == 0:
        return {}
    return {"count": total, "by_type": by_type}


def content_has_tool_result(content) -> bool:
    """True when a `user` message is a tool result rather than a prompt."""
    if not isinstance(content, list):
        return False
    return any(
        isinstance(block, dict) and block.get("type") == "tool_result"
        for block in content
    )


def content_text(content) -> str:
    """Flatten message content to text, for classification only."""
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    return " ".join(
        block.get("text", "")
        for block in content
        if isinstance(block, dict) and block.get("type") == "text"
    )


def classify_untagged_prompt(entry: dict, content) -> tuple[str | None, str]:
    """Classify a `user` entry that carries no origin.kind / promptSource.

    Returns (kind, source). `kind` is an origin bucket ("human",
    "agent-dispatch"), or "meta" / "interrupted" for entries that are not
    prompts, or None for entries to drop entirely.

    The markers are checked before `isMeta` because they are unambiguous: a
    slash-command entry is a real human prompt whatever else is set on it.
    """
    text = content_text(content)
    if "[Request interrupted by user" in text:
        return "interrupted", ""
    if "<local-command-stdout>" in text:
        return None, ""
    if "<command-name>" in text:
        return "human", "slash_command"
    if entry.get("isMeta"):
        return "meta", ""
    if entry.get("isSidechain") and not entry.get("parentUuid"):
        # The root entry of a sidechain transcript is the task prompt the
        # parent wrote to dispatch the subagent — machine-authored, so it must
        # not land in human_prompts.
        return "agent-dispatch", "dispatch"
    if not text.strip():
        return None, ""
    return "human", "untagged"


def extract_prompt_provenance(transcript_path: str) -> dict:
    """Classify `user` entries by who or what authored them (CC 2.1.2xx).

    Most `user` entries are tool results and carry none of these fields. The
    ones that do are actual prompt submissions, tagged with:
      - `origin.kind`: "human" | "task-notification" (a background task
        reporting back) | "peer" (a subagent's report injected into the parent)
      - `promptSource`: "typed" | "sdk" | "queued" | "suggestion_accepted"
      - `turnCompanion: true`: a system-reminder companion message, not a prompt

    Without this, machine-authored entries inflate the user-turn count.

    Those fields are only on a minority of entries, so untagged ones are
    classified from their shape instead (see classify_untagged_prompt): a
    slash-command invocation is a human prompt, a sidechain root is a subagent
    dispatch, and injected/interrupt/local-command entries are not prompts at
    all. Without that fallback `human_prompts` undercounts badly — a census of
    159 transcripts found 75 untagged slash-command prompts against 114 tagged
    human ones.

    Prompt text is never captured: a "peer" origin carries the subagent's whole
    report in `body`, same PII reasoning that keeps `content` out of the
    queue-operation rollup. Untagged entries are inspected only for the markers
    above; nothing is stored. Returns {} for a session with no prompt entries.
    """
    by_origin: dict[str, int] = {}
    by_source: dict[str, int] = {}
    by_peer_agent: dict[str, int] = {}
    companion_messages = 0
    prompt_entries = 0
    meta_entries = 0
    interruptions = 0

    for entry in _entries(transcript_path):
        if entry.get("type") != "user":
            continue
        if entry.get("turnCompanion") is True:
            companion_messages += 1
        content = (entry.get("message") or {}).get("content")
        if content_has_tool_result(content):
            continue
        origin = entry.get("origin")
        source = entry.get("promptSource")

        if not isinstance(origin, dict) and not source:
            kind, source = classify_untagged_prompt(entry, content)
            if kind == "meta":
                meta_entries += 1
                continue
            if kind == "interrupted":
                interruptions += 1
                continue
            if kind is None:
                continue  # local-command output or an empty entry: not a prompt
            prompt_entries += 1
            by_origin[kind] = by_origin.get(kind, 0) + 1
            by_source[source] = by_source.get(source, 0) + 1
            continue

        prompt_entries += 1
        if isinstance(origin, dict):
            kind = origin.get("kind")
            if kind:
                by_origin[kind] = by_origin.get(kind, 0) + 1
            if kind == "peer":
                # `from` is the agent *type* (e.g. "general-purpose"), not content.
                sender = origin.get("from") or "unknown"
                by_peer_agent[sender] = by_peer_agent.get(sender, 0) + 1
        if source:
            by_source[source] = by_source.get(source, 0) + 1

    if not prompt_entries and not companion_messages and not meta_entries \
            and not interruptions:
        return {}
    return {
        "prompt_entries": prompt_entries,
        "human_prompts": by_origin.get("human", 0),
        "by_origin": by_origin,
        "by_source": by_source,
        "by_peer_agent": by_peer_agent,
        "companion_messages": companion_messages,
        "meta_entries": meta_entries,
        "interruptions": interruptions,
    }


def extract_tool_denials(transcript_path: str) -> dict:
    """Count tool calls that were refused, grouped by `toolDenialKind`.

    Kinds seen in practice: "user-rejected" (the human declined the call),
    "automode-blocked" (the auto-mode classifier refused it) and
    "automode-unavailable" (the classifier timed out, so safety could not be
    determined). The first is a signal about the work; the other two are a
    signal about the harness — worth telling apart from each other and from a
    tool that genuinely broke.

    Returns {} when the session had no denials.
    """
    by_kind: dict[str, int] = {}

    for entry in _entries(transcript_path):
        if entry.get("type") != "user":
            continue
        kind = entry.get("toolDenialKind")
        if not kind:
            continue
        by_kind[kind] = by_kind.get(kind, 0) + 1

    if not by_kind:
        return {}
    return {"denial_count": sum(by_kind.values()), "by_kind": by_kind}


def extract_file_history_stats(transcript_path: str) -> dict:
    """Aggregate stats from file-history-snapshot entries.

    Returns snapshot_count (total entries) and tracked_files_count
    (unique file paths across all trackedFileBackups dicts).

    Also rolls up the sibling `file-history-delta` entries (CC 2.1.2xx), the
    per-edit records: delta_count (edits made), edited_files_count (distinct
    files touched) and max_backup_version (how many times the most-revised file
    was rewritten). Snapshots are the periodic full picture; deltas are what
    actually changed this session.

    File paths themselves are not captured to avoid leaking sensitive names.
    """
    snapshot_count = 0
    delta_count = 0
    max_backup_version = 0
    all_paths: set[str] = set()
    edited_paths: set[str] = set()
    for entry in _entries(transcript_path):
        etype = entry.get("type")
        if etype == "file-history-snapshot":
            snapshot_count += 1
            backups = entry.get("snapshot", {}).get("trackedFileBackups", {})
            if isinstance(backups, dict):
                all_paths.update(backups.keys())
        elif etype == "file-history-delta":
            delta_count += 1
            tracking_path = entry.get("trackingPath")
            if tracking_path:
                edited_paths.add(tracking_path)
            backup = entry.get("backup")
            if isinstance(backup, dict):
                version = backup.get("version") or 0
                if isinstance(version, int) and version > max_backup_version:
                    max_backup_version = version
    return {
        "snapshot_count": snapshot_count,
        "tracked_files_count": len(all_paths),
        "delta_count": delta_count,
        "edited_files_count": len(edited_paths),
        "max_backup_version": max_backup_version,
    }


def extract_stop_hook_stats(transcript_path: str) -> dict:
    """Aggregate stats from system/stop_hook_summary entries.

    Sums duration and error counts across all stop hook fires in the session.
    max_duration_ms tracks the single slowest hook execution across all fires.
    """
    total_fires = 0
    total_duration = 0
    max_duration = 0
    total_errors = 0
    prevented_count = 0

    for entry in _entries(transcript_path):
        if entry.get("type") != "system" or entry.get("subtype") != "stop_hook_summary":
            continue
        total_fires += 1
        for hook_info in entry.get("hookInfos", []):
            d = hook_info.get("durationMs", 0)
            total_duration += d
            if d > max_duration:
                max_duration = d
        total_errors += len(entry.get("hookErrors", []))
        if entry.get("preventedContinuation", False):
            prevented_count += 1

    return {
        "total_hook_fires": total_fires,
        "total_duration_ms": total_duration,
        "max_duration_ms": max_duration,
        "hook_errors": total_errors,
        "prevented_continuation_count": prevented_count,
    }


def extract_permission_mode(transcript_path: str) -> str:
    """Most recent permission-mode entry. The mode can change mid-session."""
    last = ""
    for entry in _entries(transcript_path):
        if entry.get("type") == "permission-mode":
            mode = entry.get("permissionMode", "")
            if mode:
                last = mode
    return last


def extract_permission_timeline(transcript_path: str) -> dict | None:
    """Ordered permission-mode changes across a session. None when no entries.

    permission-mode entries have no timestamps, so the sequence is file-order only.
    Consecutive duplicates are collapsed — a transition is a value change.
    """
    sequence = []
    for entry in _entries(transcript_path):
        if entry.get("type") == "permission-mode":
            mode = entry.get("permissionMode", "")
            if mode and (not sequence or sequence[-1] != mode):
                sequence.append(mode)
    if not sequence:
        return None
    return {
        "modes_used": sorted(set(sequence)),
        "sequence": sequence,
        "transition_count": len(sequence) - 1,
        "ever_bypass": "bypassPermissions" in sequence,
        "ever_accept_edits": "acceptEdits" in sequence,
    }


def extract_pr_links(transcript_path: str) -> list:
    """All pr-link entries in chronological order."""
    out = []
    for entry in _entries(transcript_path):
        if entry.get("type") == "pr-link":
            out.append({
                "number": entry.get("prNumber"),
                "url": entry.get("prUrl"),
                "repository": entry.get("prRepository"),
                "timestamp": entry.get("timestamp"),
            })
    return out


def extract_away_summaries(transcript_path: str) -> list:
    """All system/away_summary entries — content written when user steps away."""
    out = []
    for entry in _entries(transcript_path):
        if entry.get("type") == "system" and entry.get("subtype") == "away_summary":
            content = entry.get("content")
            if content:
                out.append({
                    "content": truncate(content, MAX_TEXT),
                    "timestamp": entry.get("timestamp"),
                })
    return out


def extract_compaction(transcript_path: str) -> dict | None:
    """Rollup of compaction events. None when the session was never compacted.

    Rich events come from system/compact_boundary (with compactMetadata: trigger,
    preTokens, postTokens, durationMs). Legacy transcripts only have type=="summary"
    with no metadata — counted with trigger "legacy" and no token data.
    Unknown trigger values pass through verbatim.
    """
    events = []
    triggers: dict[str, int] = {}
    total_pre = total_post = total_reclaimed = total_dur = 0

    for entry in _entries(transcript_path):
        etype = entry.get("type", "")
        subtype = entry.get("subtype", "")
        if etype == "system" and subtype == "compact_boundary":
            cm = entry.get("compactMetadata") or {}
            trigger = cm.get("trigger") or "unknown"
            triggers[trigger] = triggers.get(trigger, 0) + 1
            ev = {"trigger": trigger, "timestamp": entry.get("timestamp")}
            pre = cm.get("preTokens")
            post = cm.get("postTokens")
            dur = cm.get("durationMs")
            if pre is not None:
                ev["pre_tokens"] = pre
                total_pre += pre
            if post is not None:
                ev["post_tokens"] = post
                total_post += post
            if pre is not None and post is not None:
                reclaimed = pre - post
                ev["tokens_reclaimed"] = reclaimed
                total_reclaimed += reclaimed
            if dur is not None:
                ev["duration_ms"] = dur
                total_dur += dur
            events.append(ev)
        elif etype == "summary":
            events.append({"trigger": "legacy", "timestamp": entry.get("timestamp")})
        if len(events) >= 50:
            break

    if not events:
        return None
    return {
        "count": len(events),
        "triggers": triggers,
        "total_tokens_reclaimed": total_reclaimed,
        "total_pre_tokens": total_pre,
        "total_post_tokens": total_post,
        "total_duration_ms": total_dur,
        "events": events,
    }


def extract_bridge(transcript_path: str) -> dict | None:
    """Remote-control / bridge session info. None when the session was never bridged.

    bridge-session entries carry bridgeSessionId; system/bridge_status carries the
    shareable claude.ai url. Either may be absent independently. No timestamps.
    """
    out: dict = {}
    for entry in _entries(transcript_path):
        etype = entry.get("type", "")
        if etype == "bridge-session" and "bridge_session_id" not in out:
            bsid = entry.get("bridgeSessionId")
            if bsid:
                out["bridge_session_id"] = bsid
        elif etype == "system" and entry.get("subtype") == "bridge_status" and "url" not in out:
            url = entry.get("url")
            if url:
                out["url"] = redact_secrets(truncate(url, MAX_TEXT))
    return out or None


def parse_transcript(transcript_path: str, skip_lines: int = 0,
                     warn_after: int = 0) -> tuple[list[dict], int, bool]:
    """Parse a JSONL transcript starting after `skip_lines`.

    Malformed lines are skipped; only those past line `warn_after` are logged,
    so re-reading the whole transcript on every fire does not repeat the same
    warning each time.

    Returns (entries, total_lines, read_ok). read_ok=False signals an I/O
    failure (file missing, permission denied, etc.); the caller should NOT
    persist state in that case — a transient read failure must not clobber
    the existing line offset.
    """
    entries = []
    total = 0
    read_ok = True
    try:
        with open(transcript_path) as f:
            for line in f:
                total += 1
                if total <= skip_lines:
                    continue
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError as e:
                    if total > warn_after:
                        log(f"[WARN] Skipping malformed JSON at line {total} in {transcript_path}: {e}")
                    continue
    except (IOError, OSError) as e:
        # Can't open or read the transcript file
        log(f"Failed to read transcript: {e}")
        read_ok = False
    return entries, total, read_ok


def truncate(s: str, limit: int) -> str:
    return s[:limit] + "..." if len(s) > limit else s


def extract_text_blocks(content: list) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get("type") == "text":
                parts.append(block.get("text", ""))
        return "\n".join(parts)
    return str(content)


def extract_tool_uses(content) -> list[dict]:
    if not isinstance(content, list):
        return []
    return [b for b in content if isinstance(b, dict) and b.get("type") == "tool_use"]



def extract_tool_results(content, denial_kind: str = "") -> dict[str, str]:
    if not isinstance(content, list):
        return {}
    results = {}
    for block in content:
        if not isinstance(block, dict) or block.get("type") != "tool_result":
            continue
        tool_use_id = block.get("tool_use_id", "")
        result_content = block.get("content", "")
        if isinstance(result_content, list):
            parts = []
            for part in result_content:
                if isinstance(part, dict) and part.get("type") == "text":
                    parts.append(part.get("text", ""))
                elif isinstance(part, str):
                    parts.append(part)
            result_text = "\n".join(parts)
        elif isinstance(result_content, str):
            result_text = result_content
        else:
            result_text = str(result_content)
        if block.get("is_error", False):
            # A denied call never ran: mark it apart from a call that ran and
            # failed, so tool_error_rate can exclude it.
            prefix = f"[DENIED:{denial_kind}]" if denial_kind else "[ERROR]"
            result_text = f"{prefix} {result_text}"
        results[tool_use_id] = result_text
    return results


def parse_ts(ts_str: str) -> datetime | None:
    """Parse ISO timestamp string to datetime."""
    if not ts_str:
        return None
    try:
        return datetime.fromisoformat(ts_str)
    except ValueError:
        # Invalid ISO format timestamp string
        return None


def _is_synthetic_prompt(text: str) -> bool:
    """Wrapper-tag stubs and interrupt markers, which are not what anyone typed."""
    text = text.strip()
    return bool(_SYNTHETIC_PROMPT_RE.match(text)) or text.startswith("[Request interrupted")


_COMMAND_NAME_RE = re.compile(r"<command-name>\s*(.*?)\s*</command-name>", re.S)
_COMMAND_ARGS_RE = re.compile(r"<command-args>(.*?)</command-args>", re.S)
_SUMMARY_RE = re.compile(r"<summary>(.*?)</summary>", re.S)
_SKILL_BODY_RE = re.compile(r"^Base directory for this skill: (\S+)")


def _stub_label(text: str) -> str:
    """A readable name for input nobody typed as prose; "" when none fits."""
    text = text.strip()
    if text.startswith("<task-notification>"):
        m = _SUMMARY_RE.search(text)
        summary = " ".join(m.group(1).split()) if m else ""
        return f"Task notification: {summary}" if summary else "Task notification"
    m = _COMMAND_NAME_RE.search(text)
    if m and _is_synthetic_prompt(text):
        args = _COMMAND_ARGS_RE.search(text)
        return " ".join(filter(None, (m.group(1), " ".join(args.group(1).split()) if args else "")))
    m = _SKILL_BODY_RE.match(text)
    if m:
        return f"Skill: {os.path.basename(m.group(1).rstrip('/'))}"
    return ""


def _display_prompt(parts: list) -> str:
    """The part of a turn's input to name it by: the first typed text,
    else a label for what started the turn (slash command, task
    notification, skill body), else the first non-stub text, else
    whatever there is. `parts` is [(text, is_meta), ...] in file order.

    Each turn is its own trace, so this is the trace name."""
    for text, is_meta in parts:
        if not is_meta and not _is_synthetic_prompt(text):
            return text.strip()
    for text, _ in parts:
        label = _stub_label(text)
        if label:
            return label
    for text, _ in parts:
        if not _is_synthetic_prompt(text):
            return text.strip()
    return parts[0][0].strip() if parts else ""


def _add_model_share(by_model: dict, model: str, u: dict) -> None:
    """Add one message's usage to its model's share of a turn."""
    share = by_model.setdefault(model, {
        "usage": {"input": 0, "output": 0, "total": 0, "cache_read": 0, "cache_creation": 0},
        "cache_5m": 0, "cache_1h": 0, "web_search_requests": 0, "usage_long": None,
    })
    inp = u.get("input_tokens", 0)
    out = u.get("output_tokens", 0)
    cr = u.get("cache_read_input_tokens", 0)
    ccr = u.get("cache_creation_input_tokens", 0)
    tiers = u.get("cache_creation", {}) or {}
    c5 = tiers.get("ephemeral_5m_input_tokens", 0)
    c1 = tiers.get("ephemeral_1h_input_tokens", 0)
    for k, v in (("input", inp), ("output", out), ("total", inp + out),
                 ("cache_read", cr), ("cache_creation", ccr)):
        share["usage"][k] += v
    share["cache_5m"] += c5
    share["cache_1h"] += c1
    share["web_search_requests"] += (u.get("server_tool_use", {}) or {}).get("web_search_requests", 0)
    if inp + cr + ccr > LONG_CONTEXT_THRESHOLD:
        lu = share["usage_long"] = share["usage_long"] or {
            "input": 0, "output": 0, "cache_read": 0, "cache_creation": 0, "cache_5m": 0, "cache_1h": 0}
        for k, v in (("input", inp), ("output", out), ("cache_read", cr),
                     ("cache_creation", ccr), ("cache_5m", c5), ("cache_1h", c1)):
            lu[k] += v


def build_turns(entries: list[dict], copied_message_ids: frozenset = frozenset()) -> list[dict]:
    """Group transcript entries into turns with timing and token data.

    A turn is everything the user sent before the assistant replied, plus the
    reply. Consecutive user entries with no reply between them are one turn:
    Claude Code writes a slash command as a `<command-name>` entry plus an
    `isMeta` expansion, and a prompt with an image as the text plus an
    `isMeta` "[Image: ...]" entry. The turn's start_time is the *last* of
    those entries, so TTFT is measured from the input the reply answers.

    `copied_message_ids` are messages a continued session copied from its
    predecessor, which bills them: their usage and tool calls are left out,
    and a turn whose replies are all copied gets `copied: True`. Turn
    indices are unchanged.

    Returns list of turn dicts with keys:
      user_input, display_prompt, assistant_output, tool_calls, start_time,
      end_time, first_token_time, duration_ms, usage, model
    """
    msg_entries = []

    for entry in entries:
        etype = entry.get("type")
        # API error / auto-retry stubs (CC 2.1.179+/2.1.181): zero-usage synthetic
        # assistant entries whose content is error text ("You've hit your session
        # limit", "model not found"). Skip so they don't overwrite the turn's real
        # output / first-token time / stop_reason. Surfaced separately at trace level
        # via extract_api_error_messages().
        if entry.get("isApiErrorMessage"):
            continue
        if etype in ("user", "assistant"):
            msg = entry.get("message", {})
            msg_entries.append({
                "role": msg.get("role", etype),
                "content": msg.get("content", ""),
                "timestamp": entry.get("timestamp", ""),
                "uuid": entry.get("uuid", ""),
                "message_id": msg.get("id", ""),
                "model": msg.get("model", ""),
                "usage": msg.get("usage", {}),
                "request_id": entry.get("requestId", ""),
                "stop_reason": msg.get("stop_reason", ""),
                # Populated by the API only when stop_reason == "refusal":
                # {type, category, explanation}. category is an open set
                # (cyber, bio, reasoning_extraction, ...) or null.
                "stop_details": msg.get("stop_details") if etype == "assistant" else None,
                "diagnostics": msg.get("diagnostics", {}) if etype == "assistant" else {},
                "attribution_skill": entry.get("attributionSkill", "") if etype == "assistant" else "",
                "attribution_plugin": entry.get("attributionPlugin", "") if etype == "assistant" else "",
                # Per-turn effort level, new as a transcript field in CC 2.1.220.
                # Previously only reachable via $CLAUDE_EFFORT on live fires; this
                # source also survives --reprocess. perTurnEffort (2.1.29x) repeats
                # it on real turns (or is null) and is set alone only on synthetic
                # stubs, so it is a fallback, never an override.
                "effort": (entry.get("effort") or entry.get("perTurnEffort") or "")
                          if etype == "assistant" else "",
                # Subagent transcripts only: the agent type (general-purpose,
                # web-fetch, fork, ...), the same value as the .meta.json agentType.
                "attribution_agent": entry.get("attributionAgent", "") if etype == "assistant" else "",
                # Per-entry signals. requestedModel (CC 2.1.295) differs from
                # message.model only when Claude Code fell back to another model.
                # thinkingDurationMs (2.1.287) is written on every streamed entry
                # of a message with differing values; build_turns keeps the max.
                # advisorModel (2.1.280) echoes the /advisor setting, not a call.
                "requested_model": entry.get("requestedModel", "") if etype == "assistant" else "",
                "thinking_duration_ms": entry.get("thinkingDurationMs") if etype == "assistant" else None,
                "truncated_after_output": bool(entry.get("truncatedAfterOutput")) if etype == "assistant" else False,
                "aborted_mid_stream": bool(entry.get("isAbortedMidStream")) if etype == "assistant" else False,
                "advisor_model": entry.get("advisorModel", "") if etype == "assistant" else "",
                # human / sdk / task_notification / peer (CC 2.1.278), on the
                # user entry that opens a turn.
                "turn_origin": entry.get("turnOrigin", "") if etype == "user" else "",
                # The session cron task whose fire wrote this prompt.
                "scheduled_task_id": entry.get("scheduledTaskId", "") if etype == "user" else "",
                # A refused tool call, not a failed one (CC 2.1.2xx). Both arrive
                # with is_error:true, so this is what keeps a denial out of
                # tool_error_rate.
                "denial_kind": entry.get("toolDenialKind", "") if etype == "user" else "",
                "is_meta": bool(entry.get("isMeta")) if etype == "user" else False,
            })
        elif etype == "system" and entry.get("subtype") == "turn_duration":
            # Kept in file order: it belongs to the turn that just finished.
            msg_entries.append({"role": "_turn_duration",
                                "duration_ms": entry.get("durationMs", 0)})

    # Build tool_result lookup
    tool_results = {}
    # Also track tool_result timestamps for span endTime
    tool_result_timestamps = {}
    for me in msg_entries:
        if me["role"] == "user":
            results = extract_tool_results(me["content"], me.get("denial_kind", ""))
            tool_results.update(results)
            for tool_use_id in results:
                tool_result_timestamps[tool_use_id] = me["timestamp"]

    # Deduplicate assistant messages: multiple entries share same message_id (streaming).
    # For usage, take the LAST entry per message_id; taking one entry is what keeps
    # a streamed message from being counted twice. Main-thread entries carry
    # identical usage, but subagent transcripts (seen on CC 2.1.296) write a
    # running output_tokens count on early entries, without speed,
    # server_tool_use or output_tokens_details; only the last entry is final.
    # The first entry with a message_id gives us the first-token time.
    msg_id_first_ts = {}  # message_id -> first timestamp seen
    msg_id_final_usage = {}  # message_id -> final usage dict
    msg_id_diagnostics = {}  # message_id -> diagnostics dict
    msg_id_model = {}  # message_id -> the model that answered it

    for me in msg_entries:
        if me["role"] != "assistant" or not me["message_id"]:
            continue
        mid = me["message_id"]
        if mid not in msg_id_first_ts:
            msg_id_first_ts[mid] = me["timestamp"]
        if me["model"] and me["model"] != "<synthetic>":
            msg_id_model[mid] = me["model"]
        if me["usage"]:
            msg_id_final_usage[mid] = me["usage"]
        if me["diagnostics"] and mid not in msg_id_diagnostics:
            msg_id_diagnostics[mid] = me["diagnostics"]

    # Build turns
    turns = []
    current_turn = None

    for me in msg_entries:
        role = me["role"]

        if role == "user":
            text = extract_text_blocks(me["content"])

            if text.strip() and current_turn and not current_turn["api_call_ids"]:
                # No reply yet: this entry is more of the same input.
                current_turn["user_input"] += "\n\n" + text
                current_turn["_user_parts"].append((text, me.get("is_meta", False)))
                current_turn["start_time"] = me["timestamp"] or current_turn["start_time"]
                current_turn["end_time"] = current_turn["start_time"]
                current_turn["messages"].append(me)
                if not current_turn["turn_origin"]:
                    current_turn["turn_origin"] = me.get("turn_origin", "")
                if not current_turn["scheduled_task_id"]:
                    current_turn["scheduled_task_id"] = me.get("scheduled_task_id", "")
            elif text.strip():
                if current_turn:
                    turns.append(current_turn)
                current_turn = {
                    "user_input": text,
                    "_user_parts": [(text, me.get("is_meta", False))],
                    "assistant_output": "",
                    "tool_calls": [],
                    "_seen_tool_ids": set(),
                    "start_time": me["timestamp"],
                    "end_time": me["timestamp"],
                    "first_token_time": None,
                    "model": "",
                    "usage": {"input": 0, "output": 0, "total": 0},
                    "api_call_ids": set(),
                    "messages": [me],
                    "stop_reason": "",
                    "refusal_category": None,
                    "attribution_skill": "",
                    "attribution_plugin": "",
                    "attribution_skills_all": set(),
                    "attribution_plugins_all": set(),
                    "effort": "",
                    "requested_model": "",
                    "_thinking_ms_by_mid": {},
                    "truncated_after_output": False,
                    "aborted_mid_stream": False,
                    "advisor_model": "",
                    "turn_origin": me.get("turn_origin", ""),
                    "attribution_agent": "",
                    "scheduled_task_id": me.get("scheduled_task_id", ""),
                }
            elif current_turn:
                current_turn["messages"].append(me)

        elif role == "_turn_duration":
            # Claude Code writes turn_duration once the turn has ended — after
            # the Stop hook, and occasionally after the next prompt was
            # already queued. Attach it to the latest turn that produced
            # output and does not have a duration yet.
            candidates = ([current_turn] if current_turn else []) + turns[::-1]
            for t in candidates:
                if t["api_call_ids"]:
                    if "duration_ms" not in t and me["duration_ms"]:
                        t["duration_ms"] = me["duration_ms"]
                    break

        # Assistant entries before the first user text belong to no turn and are
        # not counted. The only ones seen are in forked subagents: a copy of the
        # spawner's Agent-call message (same message.id and usage), written after
        # `fork-context-ref`. The spawner's turns already bill it; counting it
        # here would bill it again for every fork of that message.
        elif role == "assistant" and current_turn:
            content = me["content"]
            mid = me["message_id"]
            model = me["model"]

            if model and model != "<synthetic>":
                current_turn["model"] = model

            # Track first token time (first assistant entry in this turn)
            if current_turn["first_token_time"] is None and me["timestamp"]:
                current_turn["first_token_time"] = me["timestamp"]

            # Latest non-empty stop_reason wins (final assistant entry in the
            # turn carries the terminal reason: end_turn, tool_use, etc.)
            if me.get("stop_reason"):
                current_turn["stop_reason"] = me["stop_reason"]
                # Tracks the terminal reason: a refusal later superseded in
                # the same turn must not leave its category behind.
                sd = me.get("stop_details")
                current_turn["refusal_category"] = (
                    (sd.get("category") if isinstance(sd, dict) else None)
                    if me["stop_reason"] == "refusal" else None
                )

            sk = me.get("attribution_skill", "")
            pl = me.get("attribution_plugin", "")
            if sk:
                if not current_turn["attribution_skill"]:
                    current_turn["attribution_skill"] = sk
                current_turn["attribution_skills_all"].add(sk)
            if pl:
                if not current_turn["attribution_plugin"]:
                    current_turn["attribution_plugin"] = pl
                current_turn["attribution_plugins_all"].add(pl)
            if me.get("attribution_agent") and not current_turn["attribution_agent"]:
                current_turn["attribution_agent"] = me["attribution_agent"]

            # Last non-empty effort wins — a mid-turn /effort change should be
            # attributed to the level the turn actually finished under.
            if me.get("effort"):
                current_turn["effort"] = me["effort"]

            # Last non-empty wins, as for effort and model.
            if me.get("requested_model"):
                current_turn["requested_model"] = me["requested_model"]
            if me.get("advisor_model"):
                current_turn["advisor_model"] = me["advisor_model"]
            if me.get("truncated_after_output"):
                current_turn["truncated_after_output"] = True
            if me.get("aborted_mid_stream"):
                current_turn["aborted_mid_stream"] = True
            tdm = me.get("thinking_duration_ms")
            if isinstance(tdm, (int, float)) and not isinstance(tdm, bool):
                by_mid = current_turn["_thinking_ms_by_mid"]
                by_mid[mid] = max(by_mid.get(mid, 0), tdm)

            current_turn["end_time"] = me["timestamp"]
            current_turn["api_call_ids"].add(mid)

            # Collect text output (keep last non-empty)
            text = extract_text_blocks(content)
            if text.strip():
                current_turn["assistant_output"] = text

            # Collect tool uses — skip duplicates (same tool_use_id in multiple streaming
            # entries) and a copied message's, which the predecessor already sent.
            tool_uses = extract_tool_uses(content) if mid not in copied_message_ids else []
            for tu in tool_uses:
                tool_use_id = tu.get("id", "")
                if tool_use_id and tool_use_id in current_turn["_seen_tool_ids"]:
                    continue
                if tool_use_id:
                    current_turn["_seen_tool_ids"].add(tool_use_id)
                tool_input = tu.get("input", {})
                current_turn["tool_calls"].append({
                    "id": tool_use_id,
                    "name": tu.get("name", "unknown"),
                    "input": tool_input,
                    "output": tool_results.get(tool_use_id, ""),
                    "start_time": me["timestamp"],
                    "end_time": tool_result_timestamps.get(tool_use_id, me["timestamp"]),
                })
            current_turn["messages"].append(me)

    if current_turn:
        turns.append(current_turn)

    # Aggregate usage per turn from deduplicated API calls. A message can span
    # two turns: when it calls Skill (or reads an image) and then another tool,
    # the skill body (or "[Image: ...]" note) is an isMeta user entry written
    # between its entries, and it opens a new turn. Each message counts once,
    # in the turn it started in.
    counted_mids = set()
    for turn in turns:
        usage = {"input": 0, "output": 0, "total": 0,
                 "cache_read": 0, "cache_creation": 0}
        speed = ""
        service_tier = ""
        inference_geo = ""
        web_search_requests = 0
        web_fetch_requests = 0
        cache_ephemeral_5m = 0
        cache_ephemeral_1h = 0
        iteration_count = 0
        thinking_tokens = 0
        cm_missed_tokens = 0
        cm_by_reason = {}
        # Usage from requests whose prompt exceeds LONG_CONTEXT_THRESHOLD. The
        # tier is decided per request, so it must be split here, before the
        # per-turn sums lose the request boundaries.
        usage_long = {"input": 0, "output": 0, "cache_read": 0,
                      "cache_creation": 0, "cache_5m": 0, "cache_1h": 0}
        long_requests = 0
        # Each model's share of the turn, for pricing when more than one
        # model answered in it (price_turn); the turn's `model` is the last.
        by_model: dict[str, dict] = {}
        for mid in turn["api_call_ids"]:
            if mid in copied_message_ids or mid in counted_mids:
                continue
            counted_mids.add(mid)
            u = msg_id_final_usage.get(mid, {})
            _add_model_share(by_model, msg_id_model.get(mid) or turn["model"], u)
            inp = u.get("input_tokens", 0)
            out = u.get("output_tokens", 0)
            cr = u.get("cache_read_input_tokens", 0)
            ccr = u.get("cache_creation_input_tokens", 0)
            usage["input"] += inp
            usage["output"] += out
            usage["total"] += inp + out
            usage["cache_read"] += cr
            usage["cache_creation"] += ccr
            if inp + cr + ccr > LONG_CONTEXT_THRESHOLD:
                long_requests += 1
                cc_tiers = u.get("cache_creation", {}) or {}
                usage_long["input"] += inp
                usage_long["output"] += out
                usage_long["cache_read"] += cr
                usage_long["cache_creation"] += ccr
                usage_long["cache_5m"] += cc_tiers.get("ephemeral_5m_input_tokens", 0)
                usage_long["cache_1h"] += cc_tiers.get("ephemeral_1h_input_tokens", 0)
            # Scalar fields: last non-empty value wins
            if u.get("speed"):
                speed = u["speed"]
            if u.get("service_tier"):
                service_tier = u["service_tier"]
            if u.get("inference_geo"):
                inference_geo = u["inference_geo"]
            # Summed fields from server_tool_use
            stu = u.get("server_tool_use", {})
            web_search_requests += stu.get("web_search_requests", 0)
            web_fetch_requests += stu.get("web_fetch_requests", 0)
            # Summed fields from cache_creation sub-dict
            cc = u.get("cache_creation", {})
            cache_ephemeral_5m += cc.get("ephemeral_5m_input_tokens", 0)
            cache_ephemeral_1h += cc.get("ephemeral_1h_input_tokens", 0)
            iteration_count += len(u.get("iterations", []) or [])
            # Extended-thinking token count. A breakdown *inside* output_tokens
            # (billed at the output rate), so it is never added to usage totals.
            otd = u.get("output_tokens_details")
            if isinstance(otd, dict):
                thinking_tokens += otd.get("thinking_tokens", 0) or 0
            cmr = msg_id_diagnostics.get(mid, {}).get("cache_miss_reason")
            if isinstance(cmr, dict):
                cm_missed_tokens += cmr.get("cache_missed_input_tokens", 0) or 0
                rtype = cmr.get("type")
                if rtype:
                    cm_by_reason[rtype] = cm_by_reason.get(rtype, 0) + 1
        # Collect request_ids from messages in this turn
        request_ids = [
            me.get("request_id")
            for me in turn["messages"]
            if me.get("request_id")
        ]
        turn["usage"] = usage
        turn["usage_long"] = usage_long if long_requests else None
        if len(by_model) > 1:
            turn["usage_by_model"] = by_model
        turn["long_context_requests"] = long_requests
        turn["thinking_duration_ms"] = sum(turn.pop("_thinking_ms_by_mid").values())
        turn["speed"] = speed
        turn["service_tier"] = service_tier
        turn["inference_geo"] = inference_geo
        turn["web_search_requests"] = web_search_requests
        turn["web_fetch_requests"] = web_fetch_requests
        turn["thinking_tokens"] = thinking_tokens
        turn["cache_ephemeral_5m"] = cache_ephemeral_5m
        turn["cache_ephemeral_1h"] = cache_ephemeral_1h
        turn["iteration_count"] = iteration_count
        turn["cache_miss"] = (
            {"missed_tokens": cm_missed_tokens, "by_reason": cm_by_reason}
            if cm_by_reason else None
        )
        turn["request_ids"] = request_ids
        if turn["api_call_ids"] and turn["api_call_ids"] <= copied_message_ids:
            turn["copied"] = True
        turn["api_call_ids"] = list(turn["api_call_ids"])  # make serializable
        turn["attribution_skills_all"] = sorted(turn["attribution_skills_all"])
        turn["attribution_plugins_all"] = sorted(turn["attribution_plugins_all"])
        turn["display_prompt"] = _display_prompt(turn.pop("_user_parts"))

    return turns


def extract_session_metadata(transcript_path) -> dict:
    """Extract session-level metadata from the first user entry in the transcript."""
    fields = {"cwd": "", "gitBranch": "", "version": "", "entrypoint": ""}
    for entry in _entries(transcript_path):
        if entry.get("type") == "user":
            for key in fields:
                val = entry.get(key, "")
                if val:
                    fields[key] = str(val)
            break
    return fields


def extract_api_errors(entries: list[dict]) -> dict:
    """Aggregate api_error system entries into a summary."""
    total_count = 0
    by_status: dict[str, int] = {}
    first_error_at = ""
    last_error_at = ""
    last_error = None

    for entry in entries:
        if entry.get("type") != "system" or entry.get("subtype") != "api_error":
            continue
        total_count += 1
        ts = entry.get("timestamp", "")
        if ts:
            if not first_error_at or ts < first_error_at:
                first_error_at = ts
            if not last_error_at or ts > last_error_at:
                last_error_at = ts
        error = entry.get("error", {})
        status = str(error.get("status", "unknown"))
        by_status[status] = by_status.get(status, 0) + 1
        last_error = {"status": error.get("status"),
                      "message": redact_secrets(str(error.get("message", "")))[:500]}

    return {
        "total_count": total_count,
        "by_status": by_status,
        "first_error_at": first_error_at,
        "last_error_at": last_error_at,
        "last_error": last_error,
    }


def extract_api_error_messages(transcript_path: str) -> dict | None:
    """Aggregate assistant-channel API error / auto-retry stubs.

    These are `type:"assistant"` entries with `isApiErrorMessage: True` (CC 2.1.179+/
    2.1.181 auto-retry), carrying `apiErrorStatus` (e.g. 404/429/529) and an `error`
    string (e.g. "model_not_found", "rate_limit"). Distinct from `system/api_error`
    entries (extract_api_errors) — a separate error channel that build_turns skips so
    it cannot pollute turn output. `max_retry_attempt` is emitted only when any entry
    carries a `retryAttempt`. Returns None when no such entries exist.
    """
    count = 0
    by_status: dict[str, int] = {}
    by_error: dict[str, int] = {}
    by_api_error: dict[str, int] = {}
    first_at = ""
    last_at = ""
    max_retry = None

    for entry in _entries(transcript_path):
        if not entry.get("isApiErrorMessage"):
            continue
        count += 1
        status = entry.get("apiErrorStatus")
        if status is not None:
            by_status[str(status)] = by_status.get(str(status), 0) + 1
        error = entry.get("error")
        if error:
            by_error[str(error)] = by_error.get(str(error), 0) + 1
        # The specific reason (e.g. usage_limit_reached for error rate_limit).
        api_error = entry.get("apiError")
        if api_error:
            by_api_error[str(api_error)] = by_api_error.get(str(api_error), 0) + 1
        ts = entry.get("timestamp", "")
        if ts:
            if not first_at or ts < first_at:
                first_at = ts
            if not last_at or ts > last_at:
                last_at = ts
        ra = entry.get("retryAttempt")
        if isinstance(ra, int):
            max_retry = ra if max_retry is None else max(max_retry, ra)

    if count == 0:
        return None
    out = {
        "count": count,
        "by_status": by_status,
        "by_error": by_error,
        "first_at": first_at,
        "last_at": last_at,
    }
    if by_api_error:
        out["by_api_error"] = by_api_error
    if max_retry is not None:
        out["max_retry_attempt"] = max_retry
    return out


def extract_interrupts(transcript_path: str) -> dict | None:
    """Count user-initiated interrupts (ESC mid-turn).

    `type:"user"` entries carry `interruptedMessageId` (the assistant message the user
    cut off) — a friction / misalignment signal. Returns {"count": N}, or None when the
    session had no interrupts.
    """
    count = 0
    for entry in _entries(transcript_path):
        if entry.get("type") == "user" and entry.get("interruptedMessageId"):
            count += 1
    if count == 0:
        return None
    return {"count": count}


# cost-state field -> rollup key; session totals, then per-model modelUsage.
_COST_STATE_KEYS = (
    ("totalAPIDuration", "api_duration_ms"),
    ("totalAPIDurationWithoutRetries", "api_duration_without_retries_ms"),
    ("totalToolDuration", "tool_duration_ms"),
    ("totalDuration", "wall_duration_ms"),
    ("totalLinesAdded", "lines_added"),
    ("totalLinesRemoved", "lines_removed"),
    ("hasUnknownModelCost", "has_unknown_model_cost"),
)
_COST_STATE_MODEL_KEYS = (
    ("costUSD", "cost_usd"),
    ("inputTokens", "input_tokens"),
    ("outputTokens", "output_tokens"),
    ("thinkingTokens", "thinking_tokens"),
    ("cacheReadInputTokens", "cache_read_tokens"),
    ("cacheCreationInputTokens", "cache_create_tokens"),
    ("webSearchRequests", "web_search_requests"),
)


# A cost-state whose process started this long after the transcript's first
# entry does not cover the whole session.
COST_STATE_PARTIAL_S = 120

# check_cost: transcript tokens against Claude Code's own cost-state. An
# overcount (transcript above the CLI) worth less than this is rounding.
OVERCOUNT_TOLERANCE_USD = 0.01
# Output tokens the CLI may count beyond the transcript per call it makes but
# never writes as a message. Fitted on the 2026-10 history (93 sessions) with
# headroom; a session with none of these matched within 8 tokens.
SIDE_CALL_OUTPUT_ALLOWANCE = {
    "classifier_requests": 40,   # auto-mode classifier, ~10 output tokens each
    "compactions": 20_000,       # /compact summary
    "away_summaries": 600,       # recap written when the user comes back
    "web_tool_calls": 3_000,     # Haiku summarising a WebFetch page / WebSearch
}
OUTPUT_SLACK_TOKENS = 200
TOKEN_KINDS = ("input_tokens", "output_tokens", "cache_read_tokens", "cache_create_tokens")
WEB_TOOLS = frozenset({"WebFetch", "WebSearch"})


def extract_cost_state(transcript_path: str) -> dict | None:
    """Claude Code's own running cost, from the last `cost-state` entry.

    Written when the CLI exits — before SessionEnd fires, never before Stop.
    Usually cumulative across resumes (`startTime` stays the session's first
    process), so the last entry is the session total. Not always: a process
    that did not restore an earlier one's cost starts a new total, so
    `partial` is set when `startTime` (`covers_from`) is more than
    COST_STATE_PARTIAL_S after the first entry. It counts API calls the
    transcript never records — the Haiku requests behind WebFetch and
    WebSearch, auto-mode classifier calls — so it can exceed the hook's cost.
    None without an entry that carries a total.
    """
    last = None
    first_ts = ""
    for entry in _entries(transcript_path):
        if not first_ts and entry.get("timestamp"):
            first_ts = entry["timestamp"]
        if entry.get("type") == "cost-state" and isinstance(entry.get("totalCostUSD"), (int, float)):
            last = entry
    if last is None:
        return None
    out = {"total_cost_usd": round(last["totalCostUSD"], 6)}
    for src, dst in _COST_STATE_KEYS:
        if isinstance(last.get(src), (int, float)):
            out[dst] = last[src]
    start_ms = last.get("startTime")
    covers_from = (_epoch_to_iso(start_ms / 1000)
                   if isinstance(start_ms, (int, float)) and not isinstance(start_ms, bool) else None)
    if covers_from:
        out["covers_from"] = covers_from
        first = parse_ts(first_ts)
        if first is not None:
            if first.tzinfo is None:
                first = first.replace(tzinfo=timezone.utc)
            out["partial"] = start_ms / 1000 - first.timestamp() > COST_STATE_PARTIAL_S
    by_model = {}
    usage = last.get("modelUsage")
    for model, u in (usage.items() if isinstance(usage, dict) else ()):
        if not isinstance(u, dict):
            continue
        by_model[model] = {
            dst: round(u[src], 6) if dst == "cost_usd" else u[src]
            for src, dst in _COST_STATE_MODEL_KEYS if isinstance(u.get(src), (int, float))
        }
    if by_model:
        out["by_model"] = by_model
    return out


def _from_first_user(entries) -> list[dict]:
    """Entries from the first user entry on. A forked subagent's transcript
    opens with a copy of its spawner's Agent-call message, which belongs to
    the spawner (see build_turns)."""
    out, seen_user = [], False
    for entry in _entries(entries):
        seen_user = seen_user or entry.get("type") == "user"
        if seen_user:
            out.append(entry)
    return out


def count_classifier_requests(entries) -> int:
    """Distinct auto-mode classifier calls (`serverClassifierRequest`).

    Each is an API call on the session's model, one per classified tool call
    (mostly cache reads, ~10 output tokens), that the transcript never records
    as a message: its cost is in cost-state only.
    """
    return len({e["serverClassifierRequest"] for e in _from_first_user(entries)
                if e.get("type") == "assistant" and e.get("serverClassifierRequest")})


def count_partial_messages(entries) -> int:
    """Messages whose last entry has no stop_reason, the last message excepted.

    Since CC 2.1.289 subagent transcripts often keep only a message's
    stream-start entries, whose output_tokens is a running count, so their
    output is under-counted (0-21% of subagent messages end on a final entry,
    99-100% on the main thread). The last message may still be streaming.
    """
    last_stop: dict[str, str] = {}
    for e in _from_first_user(entries):
        msg = e.get("message") or {}
        if e.get("type") == "assistant" and not e.get("isApiErrorMessage") and msg.get("id"):
            last_stop[msg["id"]] = msg.get("stop_reason") or ""
    mids = list(last_stop)[:-1]
    return sum(1 for mid in mids if not last_stop[mid])


def message_models(entries) -> set[str]:
    """Models that answered at least one message in the transcript."""
    return {(e.get("message") or {}).get("model") for e in _from_first_user(entries)
            if e.get("type") == "assistant" and not e.get("isApiErrorMessage")
            and (e.get("message") or {}).get("model") not in (None, "", "<synthetic>")}


def last_cost_state_start(transcript_path: str):
    """`startTime` of the last `cost-state` entry with a total, or None.

    Equal start times mean one CLI process: a continued session's cost-state
    carries on from its predecessor's.
    """
    start = None
    for entry in _entries(transcript_path):
        if entry.get("type") == "cost-state" and isinstance(entry.get("totalCostUSD"), (int, float)):
            start = entry.get("startTime")
    return start


def extract_async_hook_responses(transcript_path: str, max_stderr: int = 200) -> dict | None:
    """Outcomes of `"async": true` hooks, from `async_hook_response` attachments.

    Claude Code records each finished async hook with its exit code and
    stderr; nothing else shows a background hook failing (these entries
    caught this hook's Stop command exiting 2 while its file was moved away).
    Only the last failure's stderr is kept, redacted before it is truncated so
    a secret cut at the limit cannot slip past the patterns. None without any
    response.
    """
    count = failures = 0
    by_hook: dict[str, dict] = {}
    last_failure = None
    for entry in _entries(transcript_path):
        att = entry.get("attachment") if entry.get("type") == "attachment" else None
        if not isinstance(att, dict) or att.get("type") != "async_hook_response":
            continue
        name = att.get("hookName") or att.get("hookEvent") or "unknown"
        hook_counts = by_hook.setdefault(name, {"count": 0, "failures": 0})
        count += 1
        hook_counts["count"] += 1
        code = att.get("exitCode")
        if isinstance(code, int) and code != 0:
            failures += 1
            hook_counts["failures"] += 1
            last_failure = {
                "hook": name,
                "exit_code": code,
                "stderr": truncate(redact_secrets(str(att.get("stderr") or "").strip()), max_stderr),
                "timestamp": entry.get("timestamp", ""),
            }
    if count == 0:
        return None
    out = {"count": count, "failures": failures, "by_hook": by_hook}
    if last_failure:
        out["last_failure"] = last_failure
    return out


def _epoch_to_iso(value) -> str | None:
    """Epoch seconds -> ISO-8601 UTC, or None when it is not a usable number."""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    try:
        return datetime.fromtimestamp(value, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (OverflowError, OSError, ValueError):
        return None


def extract_rate_limits(transcript_path: str) -> dict | None:
    """Rate-limit state on assistant entries.

    `quotaLimits` (CC 2.1.29x), or `apiErrorParams.rate_limit_info` without
    it, rides on the stub written when a request was refused for quota:
    `status` (`rejected`), `rateLimitType` (e.g. `five_hour`), `resetsAt`
    (epoch seconds) and the overage state. `last` is the latest one. None
    when no entry carries it.
    """
    count = 0
    by_status: dict[str, int] = {}
    by_type: dict[str, int] = {}
    last = None
    for entry in _entries(transcript_path):
        if entry.get("type") != "assistant":
            continue
        quota = entry.get("quotaLimits")
        if not isinstance(quota, dict):
            params = entry.get("apiErrorParams")
            quota = params.get("rate_limit_info") if isinstance(params, dict) else None
        if not isinstance(quota, dict):
            continue
        count += 1
        status = str(quota.get("status") or "unknown")
        rtype = str(quota.get("rateLimitType") or "unknown")
        by_status[status] = by_status.get(status, 0) + 1
        by_type[rtype] = by_type.get(rtype, 0) + 1
        last = {
            "status": status,
            "type": rtype,
            "resets_at": _epoch_to_iso(quota.get("resetsAt")),
            "overage_status": quota.get("overageStatus"),
            "overage_disabled_reason": quota.get("overageDisabledReason"),
            "is_using_overage": quota.get("isUsingOverage"),
            "timestamp": entry.get("timestamp", ""),
        }
    if count == 0:
        return None
    return {"count": count, "by_status": by_status, "by_type": by_type, "last": last}


def extract_scheduled_task_fires(transcript_path: str) -> dict | None:
    """Session cron fires (`system/scheduled_task_fire`), grouped by task id.

    The fire's prompt is not stored: it is the input of the turn it opens,
    which carries `scheduled_task_id`. None without fires.
    """
    count = 0
    tasks: dict[str, dict] = {}
    for entry in _entries(transcript_path):
        if entry.get("type") != "system" or entry.get("subtype") != "scheduled_task_fire":
            continue
        count += 1
        ts = entry.get("timestamp", "")
        task = tasks.setdefault(str(entry.get("taskId") or "unknown"), {
            "cron": entry.get("cron", ""), "fires": 0, "first_at": ts, "last_at": ts})
        task["fires"] += 1
        if ts and (not task["first_at"] or ts < task["first_at"]):
            task["first_at"] = ts
        if ts > task["last_at"]:
            task["last_at"] = ts
    return {"count": count, "tasks": tasks} if count else None


def extract_continued_in(transcript_path: str) -> str:
    """Id of the session this one was continued in (`continued-in`), or ""."""
    out = ""
    for entry in _entries(transcript_path):
        if entry.get("type") == "continued-in" and entry.get("continuedInSessionId"):
            out = entry["continuedInSessionId"]
    return out


def extract_relocations(transcript_path: str, max_cwds: int = 10) -> dict | None:
    """Working-directory moves (`relocated` entries, e.g. into a worktree).

    `cwds` lists the distinct targets in first-seen order, capped at
    `max_cwds`; `count` counts every entry. None without relocations.
    """
    count = 0
    cwds: list[str] = []
    last_cwd = ""
    for entry in _entries(transcript_path):
        cwd = entry.get("relocatedCwd") if entry.get("type") == "relocated" else None
        if not cwd:
            continue
        count += 1
        last_cwd = cwd
        if cwd not in cwds and len(cwds) < max_cwds:
            cwds.append(cwd)
    return {"count": count, "cwds": cwds, "last_cwd": last_cwd} if count else None


def extract_cwd(transcript_path) -> str:
    """Scan the transcript for the first non-empty cwd field."""
    for entry in _entries(transcript_path):
        cwd = entry.get("cwd", "")
        if cwd:
            return cwd
    return ""


# Fast mode (research preview). Premium over base rates, per Opus generation:
# Opus 4.6/4.7 = 6x ($30/$150); Opus 4.8 and Opus 5 = 2x ($10/$50); Opus 5.5 =
# 2x ($8/$40). Multiplier
# applies uniformly to input, output, cache read, and both cache-write tiers.
#
# Fast mode is now offered on Opus 5.5 / Opus 5 / Opus 4.8 only — `speed="fast"` on Opus 4.7
# returns an API error, and the Opus 4.6 `-fast` model ID was retired (silently
# falls back to standard). The 4.6/4.7 entries are retained deliberately: turns
# recorded while fast mode was live on those generations were billed at 6x, and
# reprocessing must keep billing them that way (reprocess determinism).
# Source: platform.claude.com/docs/en/about-claude/pricing#fast-mode-pricing (verified 2026-08-01)
#
# Keys are the family keys returned by _opus_family(), not substrings: a
# substring lookup let "opus-5" claim claude-opus-5-5.
FAST_MODE_MULTIPLIERS = {
    "opus-4-6": 6.0,
    "opus-4-7": 6.0,
    "opus-4-8": 2.0,
    "opus-5": 2.0,
    "opus-5-5": 2.0,   # $8/$40
}
# Data residency: inference_geo="us" on Opus 4.6+/Sonnet 4.6+. 1.1x all categories.
# Source: platform.claude.com/docs/en/about-claude/pricing#data-residency-pricing
US_GEO_MULTIPLIER = 1.1
# Web search: $10 per 1,000 server-side search requests.
# Source: platform.claude.com/docs/en/about-claude/pricing#web-search-tool
WEB_SEARCH_COST_PER_REQUEST = 0.01


def _otel_genai_attrs(
    session_id: str,
    model: str,
    usage: dict,
    request_ids: list,
    stop_reason: str,
) -> dict:
    """Emit OpenTelemetry GenAI semantic-convention aliases for a generation.

    Lets an OTLP collector (or a future Langfuse mapping) read the trace
    without a custom transform processor. Zero behaviour change for the
    existing Langfuse dashboard.

    Reference: https://opentelemetry.io/docs/specs/semconv/gen-ai/
    """
    attrs = {
        "gen_ai.system": "anthropic",
        "gen_ai.operation.name": "chat",
        "gen_ai.provider.name": "anthropic",
        "gen_ai.conversation.id": session_id,
        "gen_ai.request.model": model,
        "gen_ai.response.model": model,
        "gen_ai.usage.input_tokens": usage.get("input", 0),
        "gen_ai.usage.output_tokens": usage.get("output", 0),
    }
    if request_ids:
        attrs["gen_ai.response.id"] = request_ids[0]
    if stop_reason:
        attrs["gen_ai.response.finish_reasons"] = [stop_reason]
    return attrs


def _has_billable_tokens(usage: dict) -> bool:
    """True when a turn has any cost-bearing tokens (input/output/cache).

    Used to suppress the missing-model warning/tag for zero-usage stub turns
    (slash commands, synthetic prompts) whose cost is legitimately $0.
    """
    return any(usage.get(k, 0) for k in ("input", "output", "cache_read", "cache_creation"))


# claude-opus-<major>[-<minor>][-<YYYYMMDD>], optionally prefixed (Bedrock
# "anthropic." / "us.anthropic."). The minor is 1-2 digits; an 8-digit group
# is a date snapshot, not a version, so claude-opus-5-20260401 is Opus 5.
_OPUS_ID_RE = re.compile(r"opus-(\d+)(?:-(\d{1,2}))?(?![0-9])")


def _opus_family(model: str) -> str:
    """Pricing family key for an Opus model ID ("opus-5-5", "opus-5",
    "opus-4-8", ...), or "" when the ID has no parseable Opus version.

    Legacy IDs with the version before the name (claude-3-opus-*) and the dated
    Opus 4.0 ID (claude-opus-4-20250514) return "opus-<major>" and are handled
    by the legacy branch of calculate_turn_cost().
    """
    match = _OPUS_ID_RE.search(model.lower())
    if not match:
        return ""
    major, minor = match.groups()
    return f"opus-{major}-{minor}" if minor else f"opus-{major}"


def _model_family(model: str, name: str) -> str:
    """Version key for a non-Opus family ("sonnet-4-5", "haiku-3-5", ...),
    or "" when no version can be parsed.

    Handles both naming schemes: version-after-name (claude-sonnet-4-5-20250929,
    Bedrock us.anthropic.claude-sonnet-4-5-...) and the Claude 3 era
    version-before-name (claude-3-5-sonnet-20241022). As in _opus_family, the
    minor is 1-2 digits and an 8-digit group is a date snapshot.
    """
    m = model.lower()
    old = re.search(rf"(\d+)-(?:(\d)-)?{name}(?![a-z])", m)
    if old:
        major, minor = old.groups()
        return f"{name}-{major}-{minor}" if minor else f"{name}-{major}"
    new = re.search(rf"{name}-(\d+)(?:-(\d{{1,2}}))?(?![0-9])", m)
    if new:
        major, minor = new.groups()
        return f"{name}-{major}-{minor}" if minor else f"{name}-{major}"
    return ""


# Per-1M rates (input, output, cache_read, cache_write_5m, cache_write_1h) and
# whether inference_geo="us" applies, keyed by _model_family(). Anything not
# listed is unrecognised and reported as $0 with a [WARN] — never priced at a
# neighbouring version's rate.
# Source: platform.claude.com/docs/en/about-claude/pricing (verified 2026-10-09)
_SONNET_RATES = {
    # Sonnet 5.5 cache reads are 0.05x input ($0.10), like Opus 5.5.
    "sonnet-5-5": ((2.00, 10.00, 0.10, 2.50, 4.00), True),
    # Sonnet 5: launched as "introductory through 2026-08-31", then made the
    # standard price; the $3/$15 step-up never happened.
    "sonnet-5":   ((2.00, 10.00, 0.20, 2.50, 4.00), True),
    "sonnet-4-6": ((3.00, 15.00, 0.30, 3.75, 6.00), True),
    "sonnet-4-5": ((3.00, 15.00, 0.30, 3.75, 6.00), False),
    "sonnet-4":   ((3.00, 15.00, 0.30, 3.75, 6.00), False),
    "sonnet-4-0": ((3.00, 15.00, 0.30, 3.75, 6.00), False),  # alias of sonnet-4
    "sonnet-3-7": ((3.00, 15.00, 0.30, 3.75, 6.00), False),
    "sonnet-3-5": ((3.00, 15.00, 0.30, 3.75, 6.00), False),
    "sonnet-3":   ((3.00, 15.00, 0.30, 3.75, 6.00), False),
}
_HAIKU_RATES = {
    # Haiku 5.5: rate for prompts up to LONG_CONTEXT_THRESHOLD; the higher
    # rate for longer prompts is in _LONG_CONTEXT_RATES. No /fast variant.
    "haiku-5-5": ((0.10, 0.50, 0.01, 0.125, 0.20), True),
    "haiku-4-5": ((1.00, 5.00, 0.10, 1.25, 2.00), False),
    "haiku-3-5": ((0.80, 4.00, 0.08, 1.00, 1.60), False),
    "haiku-3":   ((0.25, 1.25, 0.03, 0.30, 0.50), False),
}
# Prompt-length pricing. A request whose prompt (input + cache reads + cache
# writes) is over the threshold pays these rates for all of its tokens, output
# included, even when part of the prompt is a cache hit. Each request is tiered
# on its own. Keyed by pricing family; models not listed have one rate.
LONG_CONTEXT_THRESHOLD = 100_000
_LONG_CONTEXT_RATES = {
    "haiku-5-5": (0.50, 2.50, 0.05, 0.625, 1.00),
}


def _has_long_context_tier(model: str) -> bool:
    m = model.lower()
    return "haiku" in m and _model_family(m, "haiku") in _LONG_CONTEXT_RATES
# Fable 5 / Mythos 5 (Project Glasswing sibling): identical pricing. The 5.1
# releases cut cache reads to $0.25 (0.025x input). Neither has a /fast
# variant, and their data-residency multiplier is unverified.
_TOP_TIER_RATES = {
    "fable-5":    ((10.00, 50.00, 1.00, 12.50, 20.00), False),
    "fable-5-1":  ((10.00, 50.00, 0.25, 12.50, 20.00), False),
    "mythos-5":   ((10.00, 50.00, 1.00, 12.50, 20.00), False),
    "mythos-5-1": ((10.00, 50.00, 0.25, 12.50, 20.00), False),
}


# Models already warned about in this process. Every fire prices every turn of
# the session, so an unrecognised model would otherwise log once per turn.
_WARNED_MODELS: set = set()


def _warn_once(key: str, msg: str) -> None:
    if key not in _WARNED_MODELS:
        _WARNED_MODELS.add(key)
        log(msg)


def _unrecognised_model(model: str) -> tuple:
    _warn_once(model, f"[WARN] calculate_turn_cost: unrecognised model '{model}' — cost reported as $0. "
               "Update calculate_turn_cost() and CLAUDE.md if this is a new Anthropic model.")
    return 0.0, 0.0, 0.0, {}


def calculate_turn_cost(
    usage: dict,
    model: str,
    cache_5m: int = 0,
    cache_1h: int = 0,
    speed: str = "",
    inference_geo: str = "",
    web_search_requests: int = 0,
    turn_start_time: str = "",
    long_context_usage: dict | None = None,
) -> tuple:
    """Calculate cost for a turn's token usage.

    cache_5m / cache_1h: per-tier cache creation token counts (from usageDetails).
    When provided, costs are split by tier; otherwise all cache_creation billed at 5m rate.

    speed: "fast" applies a per-model premium (Opus 4.6/4.7 = 6x; 4.8, 5, 5.5 = 2x).
    inference_geo: "us" applies 1.1x multiplier on Opus 4.6+/Sonnet 4.6+.
    web_search_requests: server-side web search calls, billed at $10/1000.
    turn_start_time: ISO timestamp of the turn, for date-aware pricing windows so
        reprocessing bills a historical turn at the rate live at the time. No
        window is active today: Sonnet 5's $2/$10 "introductory" rate became its
        standard rate, so the date switch it used was removed.
    long_context_usage: the part of `usage` that came from requests whose
        prompt was over LONG_CONTEXT_THRESHOLD (build_turns' `usage_long`:
        input, output, cache_read, cache_creation, cache_5m, cache_1h). Billed
        at _LONG_CONTEXT_RATES for models that have them (Haiku 5.5); ignored
        for every other model.

    Returns: (turn_cost, input_cost, output_cost, cost_details)
    """
    m = model.lower()
    if not m:
        # No model on the turn. Do NOT fabricate a default — report $0 so the
        # dashboard shows an obvious gap. Warn only when tokens are actually
        # billable; zero-usage stub turns ($0 regardless) stay silent.
        if _has_billable_tokens(usage):
            _warn_once("", "[WARN] calculate_turn_cost: turn has no model field but carries "
                       "billable tokens — cost reported as $0. Upstream transcript is missing "
                       "the assistant 'model' value; FIX the source.")
        return 0.0, 0.0, 0.0, {}
    # Pricing per 1M tokens: (input, output, cache_read, cache_write_5m, cache_write_1h)
    # Source: platform.claude.com/docs/en/about-claude/pricing (verified 2026-09-23)
    # Every family is an explicit whitelist. A new model ID — including a new
    # version of a known family — reports $0 and logs a [WARN]; update this
    # function and the CLAUDE.md Cost Model table when that happens.
    supports_inference_geo = False  # Only Opus 4.6+/Sonnet 4.6+ accept inference_geo
    supports_fast_mode = False      # Only Opus 4.6+ (excluding 4.5) support /fast
    long_rates = None               # Prompt-length tier; only Haiku 5.5 has one
    if "haiku" in m:
        family = _model_family(m, "haiku")
        rates = _HAIKU_RATES.get(family)
        if rates is None:
            return _unrecognised_model(model)
        (p_in, p_out, p_cr, p_cc5, p_cc1), supports_inference_geo = rates
        long_rates = _LONG_CONTEXT_RATES.get(family)
    elif "opus" in m:
        # Whitelist of current Opus generations. Substring matching previously
        # over-billed any future "opus-4-9+" release at the legacy $15/$75 rate,
        # and let "opus-5" swallow claude-opus-5-5 at the $5/$25 rate. An
        # unrecognised generation hits the WARN path so the table gets updated.
        family = _opus_family(m)
        if family == "opus-5-5":
            # Opus 5.5: $4/$20, cache writes $5/$8, cache read $0.20 (0.05x
            # input, not the usual 0.1x). US data residency 1.1x (Claude 4.6+).
            # Source: platform.claude.com/docs/en/about-claude/pricing (verified 2026-09-23)
            p_in, p_out, p_cr, p_cc5, p_cc1 = 4.0, 20.0, 0.20, 5.00, 8.00
            supports_inference_geo = True
            supports_fast_mode = True
        elif family in ("opus-4-5", "opus-4-6", "opus-4-7", "opus-4-8", "opus-5"):
            p_in, p_out, p_cr, p_cc5, p_cc1 = 5.0, 25.0, 0.50, 6.25, 10.0
            if family != "opus-4-5":
                supports_inference_geo = True
                supports_fast_mode = True
        elif family in ("opus-4-1", "opus-4-0") or any(x in m for x in ("opus-4-20", "3-opus")):
            # Opus 4.1, 4.0 (claude-opus-4-2025* / claude-opus-4-0), 3 (claude-3-opus-*)
            p_in, p_out, p_cr, p_cc5, p_cc1 = 15.0, 75.0, 1.50, 18.75, 30.0
        else:
            return _unrecognised_model(model)
    elif "sonnet" in m:
        rates = _SONNET_RATES.get(_model_family(m, "sonnet"))
        if rates is None:
            return _unrecognised_model(model)
        (p_in, p_out, p_cr, p_cc5, p_cc1), supports_inference_geo = rates
    elif "fable" in m or "mythos" in m:
        # claude-mythos-preview is the invitation-only Mythos 5 preview.
        family = "mythos-5" if "mythos-preview" in m else (
            _model_family(m, "fable") or _model_family(m, "mythos"))
        rates = _TOP_TIER_RATES.get(family)
        if rates is None:
            return _unrecognised_model(model)
        (p_in, p_out, p_cr, p_cc5, p_cc1), supports_inference_geo = rates
    else:
        return _unrecognised_model(model)

    # Fast mode premium (per-model: Opus 4.6/4.7 = 6x; 4.8, 5, 5.5 = 2x).
    # Multipliers stack on top per spec. supports_fast_mode guarantees a key match.
    if speed == "fast" and supports_fast_mode:
        fast_mult = FAST_MODE_MULTIPLIERS.get(_opus_family(m), 1.0)
        p_in *= fast_mult
        p_out *= fast_mult
        p_cr *= fast_mult
        p_cc5 *= fast_mult
        p_cc1 *= fast_mult

    # Data residency 1.1x (Opus 4.6+/Sonnet 4.6+/Haiku 5.5). Stacks on top of
    # fast mode, and on Haiku 5.5 applies to the long-prompt rates as well.
    geo = US_GEO_MULTIPLIER if (inference_geo.lower() == "us" and supports_inference_geo) else 1.0
    rates = tuple(p * geo for p in (p_in, p_out, p_cr, p_cc5, p_cc1))

    # Split the turn into its short- and long-prompt parts, each priced at
    # its own rates. Without a long tier the whole turn is the short part.
    lu = long_context_usage if (long_rates and long_context_usage) else None
    parts = [(
        {k: usage.get(k, 0) - (lu or {}).get(k, 0)
         for k in ("input", "output", "cache_read", "cache_creation")},
        cache_5m - (lu or {}).get("cache_5m", 0),
        cache_1h - (lu or {}).get("cache_1h", 0),
        rates,
    )]
    if lu:
        parts.append((lu, lu.get("cache_5m", 0), lu.get("cache_1h", 0),
                      tuple(p * geo for p in long_rates)))

    input_token_cost = cache_read_cost = cc_cost = output_cost = 0.0
    for u, c5, c1, (r_in, r_out, r_cr, r_cc5, r_cc1) in parts:
        # Cache creation cost: use per-tier breakdown when available
        if c5 or c1:
            cc_cost += (c5 * r_cc5 + c1 * r_cc1) / 1_000_000
        else:
            cc_cost += u["cache_creation"] * r_cc5 / 1_000_000
        input_token_cost += u["input"] * r_in / 1_000_000
        cache_read_cost += u["cache_read"] * r_cr / 1_000_000
        output_cost += u["output"] * r_out / 1_000_000

    web_search_cost = web_search_requests * WEB_SEARCH_COST_PER_REQUEST
    input_cost = input_token_cost + cache_read_cost + cc_cost
    turn_cost = input_cost + output_cost + web_search_cost

    cost_details = {
        "input": input_token_cost,
        "output": output_cost,
        "cache_read_input_tokens": cache_read_cost,
        "cache_creation_input_tokens": cc_cost,
        "total": turn_cost,
    }
    if web_search_cost > 0:
        cost_details["web_search"] = web_search_cost
    return turn_cost, input_cost, output_cost, cost_details


# ---------------------------------------------------------------------------
# Hook-level score classifiers
# ---------------------------------------------------------------------------


def compute_cache_hit_rate(turns: list[dict]) -> float | None:
    """Calculate cache hit rate: cache_read / (cache_read + cache_creation).

    Returns None when there is no cache activity at all (fresh session, no
    prior context). Previously this collapsed to 0.0 and was indistinguishable
    from a fully-cold cache-miss session. Callers must filter None out before
    emitting numeric scores; Langfuse rejects None numeric values.
    """
    if not turns:
        return None

    cache_read = sum(turn.get("usage", {}).get("cache_read", 0) for turn in turns)
    cache_creation = sum(turn.get("usage", {}).get("cache_creation", 0) for turn in turns)
    denominator = cache_read + cache_creation

    if denominator == 0:
        return None  # No cache activity at all — distinct from cache-miss

    return round(cache_read / denominator, 4)


def calculate_tool_error_rate(turns: list[dict]) -> float | None:
    """Fraction of *executed* tool calls whose output is an [ERROR].

    Returns None when there are no executed tool calls (omit the score, same
    pattern as compute_cache_hit_rate) so an error-free session stays distinct
    from a session that ran no tools. The "[ERROR]" prefix is applied upstream
    in extract_tool_results.

    Denied calls are excluded from both numerator and denominator: this score
    measures execution health, and a call the user rejected or the auto-mode
    classifier blocked never reached the tool. They arrive with is_error:true
    like any failure, so counting them would read a permission event as a
    broken command. See calculate_tool_denial_rate for the other axis.
    """
    total = 0
    errors = 0
    for t in turns:
        for tc in t.get("tool_calls", []):
            output = str(tc.get("output", ""))
            if output.startswith("[DENIED:"):
                continue
            total += 1
            if output.startswith("[ERROR]"):
                errors += 1
    if total == 0:
        return None
    return round(errors / total, 4)


def calculate_tool_denial_rate(turns: list[dict]) -> float | None:
    """Fraction of attempted tool calls that were refused.

    Denominator is every call the assistant attempted, denied ones included —
    unlike tool_error_rate, which asks how the executed calls fared. Returns
    None when no tool calls were attempted at all.
    """
    total = 0
    denied = 0
    for t in turns:
        for tc in t.get("tool_calls", []):
            total += 1
            if str(tc.get("output", "")).startswith("[DENIED:"):
                denied += 1
    if total == 0:
        return None
    return round(denied / total, 4)


def model_shares(turn: dict) -> dict:
    """{model: share of the turn's usage}; one entry unless several models answered."""
    return turn.get("usage_by_model") or {turn.get("model") or "": {
        "usage": turn.get("usage") or {},
        "cache_5m": turn.get("cache_ephemeral_5m", 0),
        "cache_1h": turn.get("cache_ephemeral_1h", 0),
        "web_search_requests": turn.get("web_search_requests", 0),
        "usage_long": turn.get("usage_long"),
    }}


def _price_share(turn: dict, model: str, share: dict) -> tuple:
    return calculate_turn_cost(
        share["usage"],
        model,
        share["cache_5m"],
        share["cache_1h"],
        speed=turn.get("speed", ""),
        inference_geo=turn.get("inference_geo", ""),
        web_search_requests=share["web_search_requests"],
        turn_start_time=turn.get("start_time", ""),
        long_context_usage=share["usage_long"],
    )


def price_turn(turn: dict, model: str | None = None) -> tuple:
    """Price one built turn: (turn_cost, input_cost, output_cost, cost_details).

    A turn answered by more than one model (`usage_by_model`, e.g. a few Haiku
    messages inside an Opus turn) prices each model's share at its own rates;
    billing it all at the turn's model overcharged such turns (2 local
    sessions, $0.36, found by tools/audit_costs.py on 2026-10-24).
    `model` overrides the turn's own model, for counterfactual pricing of the
    same token usage; other models' shares keep their rates. Counterfactual
    pricing keeps the request split: it only applies if that model has a
    long-prompt tier.
    """
    own = turn.get("model") or ""
    shares = model_shares(turn)
    total = [0.0, 0.0, 0.0]
    details: dict = {}
    for share_model, share in shares.items():
        priced = _price_share(turn, model if model is not None and share_model == own else share_model, share)
        if len(shares) == 1:
            return priced
        for i in range(3):
            total[i] += priced[i]
        for k, v in priced[3].items():
            details[k] = details.get(k, 0) + v
    return total[0], total[1], total[2], details


def _turn_cost_usd(turn: dict, model: str | None = None) -> float:
    """Total cost of one built turn. `model` overrides the turn's own model,
    for counterfactual pricing of the same token usage."""
    if model is None and "_cost" in turn:
        return turn["_cost"][0]
    return price_turn(turn, model)[0]


def _turns_by_model(turns: list[dict]):
    """(turn, model, usage, cost) for each model's share of each turn."""
    for t in turns:
        shares = model_shares(t)
        if len(shares) == 1:
            ((model, share),) = shares.items()
            yield t, model, share["usage"], _turn_cost_usd(t)
            continue
        for model, share in shares.items():
            yield t, model, share["usage"], _price_share(t, model, share)[0]


def tokens_by_model(turns: list[dict]) -> dict:
    """{model: {input_tokens, output_tokens, cache_read_tokens, cache_create_tokens}}."""
    out: dict[str, dict] = {}
    for t in turns:
        for model, share in model_shares(t).items():
            if not model:
                continue
            u = share["usage"]
            b = out.setdefault(model, dict.fromkeys(TOKEN_KINDS, 0))
            b["input_tokens"] += u.get("input", 0)
            b["output_tokens"] += u.get("output", 0)
            b["cache_read_tokens"] += u.get("cache_read", 0)
            b["cache_create_tokens"] += u.get("cache_creation", 0)
    return out


def count_web_tool_calls(turns: list[dict]) -> int:
    return sum(1 for t in turns for tc in t["tool_calls"] if tc["name"] in WEB_TOOLS)


def check_cost(tokens: dict, cli_by_model: dict, side_calls: dict, partial_messages: int) -> dict:
    """Compare the tokens the hook bills with Claude Code's own count.

    `tokens` and `cli_by_model` are {model: {input_tokens, output_tokens,
    cache_read_tokens, cache_create_tokens}}. The CLI also counts calls the
    transcript never records, so the hook can never legitimately bill more
    of any model: excess worth over OVERCOUNT_TOLERANCE_USD is `overcount`.
    Every message has output tokens, so a message the transcript lost shows
    up as CLI output beyond what the unrecorded `side_calls` explain
    (SIDE_CALL_OUTPUT_ALLOWANCE): `unreconciled`. With partially recorded
    subagent messages the hook's output is a known lower bound and that
    check is skipped (`output-lower-bound`). tools/audit_costs.py runs this
    same check against an independent recount.
    """
    overcount, overcount_usd = {}, 0.0
    for model, t in tokens.items():
        c = cli_by_model.get(model, {})
        excess = {k: t[k] - c.get(k, 0) for k in TOKEN_KINDS if t[k] > c.get(k, 0)}
        if excess:
            overcount[model] = excess
            overcount_usd += calculate_turn_cost({
                "input": excess.get("input_tokens", 0), "output": excess.get("output_tokens", 0),
                "cache_read": excess.get("cache_read_tokens", 0),
                "cache_creation": excess.get("cache_create_tokens", 0)}, model)[0]
    allowance = OUTPUT_SLACK_TOKENS + sum(
        n * SIDE_CALL_OUTPUT_ALLOWANCE[k] for k, n in side_calls.items())
    cli_out = sum(c.get("output_tokens", 0) for m, c in cli_by_model.items() if m in tokens)
    output_excess = cli_out - sum(t["output_tokens"] for t in tokens.values())
    if overcount_usd > OVERCOUNT_TOLERANCE_USD:
        status = "overcount"
    elif partial_messages:
        status = "output-lower-bound"
    elif output_excess > allowance:
        status = "unreconciled"
    else:
        status = "ok"
    return {
        "status": status,
        "overcount_usd": round(overcount_usd, 6),
        "overcount": overcount or None,
        "output_excess_tokens": output_excess,
        "output_allowance": allowance,
        "side_calls": side_calls,
    }


def build_model_cost_breakdown(turns: list[dict]) -> dict | None:
    """Per-model rollup of turns, tokens and cost, split by effort level.

    `model_families` tags lump every Opus together, so a mid-session switch
    (Opus 5 -> Opus 5.5) is invisible without this. `by_effort` exists because
    Opus 5.5 defaults to effort `medium` where Opus 5 defaults to `high`:
    comparing the two models without it mixes a price change with an effort
    change. Turns without an effort field land in "unset". Turns without a
    model land in "_missing". A turn several models answered in counts under
    each of them, with that model's share of its tokens and cost. None when
    there are no turns.
    """
    if not turns:
        return None
    breakdown: dict[str, dict] = {}
    for t, model, usage, cost in _turns_by_model(turns):
        b = breakdown.setdefault(model or "_missing", {
            "turns": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_read_tokens": 0,
            "cache_create_tokens": 0,
            "cost_usd": 0.0,
            "by_effort": {},
        })
        b["turns"] += 1
        b["input_tokens"] += usage.get("input", 0)
        b["output_tokens"] += usage.get("output", 0)
        b["cache_read_tokens"] += usage.get("cache_read", 0)
        b["cache_create_tokens"] += usage.get("cache_creation", 0)
        b["cost_usd"] = round(b["cost_usd"] + cost, 6)
        e = b["by_effort"].setdefault(t.get("effort") or "unset",
                                      {"turns": 0, "cost_usd": 0.0})
        e["turns"] += 1
        e["cost_usd"] = round(e["cost_usd"] + cost, 6)
    return breakdown


def build_opus_5_5_savings(turns: list[dict]) -> dict | None:
    """What this batch's Opus 5.5 turns cost versus the same usage on Opus 5.

    Opus 5.5 shares Opus 5's tokenizer, so re-pricing identical token counts is
    a fair rate comparison. It is still an estimate: it assumes the turn would
    have used the same tokens on Opus 5, which a different model (and its
    different default effort) would not exactly do. None without Opus 5.5 turns.
    """
    o55 = [t for t in turns if _opus_family(t.get("model") or "") == "opus-5-5"]
    if not o55:
        return None
    actual = sum(_turn_cost_usd(t) for t in o55)
    baseline = sum(_turn_cost_usd(t, model="claude-opus-5") for t in o55)
    return {
        "turns": len(o55),
        "actual_cost_usd": round(actual, 6),
        "opus_5_equivalent_cost_usd": round(baseline, 6),
        "saved_usd": round(baseline - actual, 6),
    }


def build_skill_attribution_summary(turns: list[dict]) -> dict | None:
    """Aggregate skill / plugin attribution across all turns.

    Returns None when no turn has any attribution. Unattributed turns
    contribute to a "_unattributed" bucket inside skill_cost_breakdown
    but are excluded from skills_used and top_skill.
    """
    if not any(t.get("attribution_skill") or t.get("attribution_plugin") for t in turns):
        return None

    skills_set = set()
    plugins_set = set()
    skill_turn_counts: dict[str, int] = {}
    breakdown: dict[str, dict] = {}

    for t in turns:
        sk = t.get("attribution_skill") or ""
        pl = t.get("attribution_plugin") or ""
        if sk:
            skills_set.add(sk)
            skill_turn_counts[sk] = skill_turn_counts.get(sk, 0) + 1
        if pl:
            plugins_set.add(pl)

        bucket_key = sk if sk else "_unattributed"
        b = breakdown.setdefault(bucket_key, {
            "turns": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_read_tokens": 0,
            "cache_create_tokens": 0,
            "cost_usd": 0.0,
        })
        usage = t.get("usage") or {}
        b["turns"] += 1
        b["input_tokens"] += usage.get("input", 0)
        b["output_tokens"] += usage.get("output", 0)
        b["cache_read_tokens"] += usage.get("cache_read", 0)
        b["cache_create_tokens"] += usage.get("cache_creation", 0)
        b["cost_usd"] = round(b["cost_usd"] + _turn_cost_usd(t), 6)

    top_skill = ""
    if skill_turn_counts:
        top_skill = sorted(
            skill_turn_counts.items(),
            key=lambda kv: (-kv[1], kv[0]),
        )[0][0]

    return {
        "skills_used": sorted(skills_set),
        "plugins_used": sorted(plugins_set),
        "top_skill": top_skill,
        "skill_turn_counts": skill_turn_counts,
        "skill_cost_breakdown": breakdown,
    }


def build_cache_miss_summary(turns: list[dict]) -> dict | None:
    """Session rollup of per-turn cache misses; None when no turn missed cache."""
    total = 0
    by_reason = {}
    turns_with_miss = 0
    for t in turns:
        cm = t.get("cache_miss")
        if not cm:
            continue
        turns_with_miss += 1
        total += cm["missed_tokens"]
        for r, c in cm["by_reason"].items():
            by_reason[r] = by_reason.get(r, 0) + c
    if turns_with_miss == 0:
        return None
    return {
        "total_missed_tokens": total,
        "by_reason": by_reason,
        "turns_with_miss": turns_with_miss,
    }


def build_active_duration_summary(turns: list[dict]) -> dict | None:
    """Roll up per-turn wall-clock from `system`/`turn_duration` entries.

    Each turn's `duration_ms` is Claude Code's own authoritative turn wall-clock
    (turn start to end, including tool execution) attached in `build_turns`. Summed
    across turns this is *active* session time — distinct from the trace wall-clock
    span, which also counts idle time between turns (user reading / typing). None
    when no turn carried a duration (CC before ~2.1.19x never emitted the entry).
    Computed over the incremental `turns` batch, matching `total_iterations`.
    """
    durs = [t["duration_ms"] for t in turns if t.get("duration_ms")]
    if not durs:
        return None
    return {
        "total_ms": sum(durs),
        "turns_measured": len(durs),
        "max_ms": max(durs),
    }


def build_thinking_summary(turns: list[dict]) -> dict | None:
    """Roll up extended-thinking token usage across the incremental turn batch.

    `thinking_tokens` comes from `usage.output_tokens_details.thinking_tokens`
    (CC 2.1.2xx). It is a breakdown *inside* `output_tokens`, billed at the
    output rate, so this changes no cost — it exists to show what an effort
    level actually buys. Note the token *count* survives even though CC has
    stripped thinking *text* from the transcript since v2.1.112.

    Returns `{total_thinking_tokens, total_output_tokens, share_of_output,
    turns_with_thinking, max_thinking_tokens}`, or None when no turn in the batch
    did any thinking (so untouched traces carry no empty metadata key).
    `share_of_output` is None when output is 0. Computed over the incremental
    `turns` batch, matching `total_iterations` / `active_duration`.
    """
    total_thinking = sum(t.get("thinking_tokens", 0) or 0 for t in turns)
    if not total_thinking:
        return None
    total_output = sum((t.get("usage") or {}).get("output", 0) for t in turns)
    return {
        "total_thinking_tokens": total_thinking,
        "total_output_tokens": total_output,
        "share_of_output": (
            round(total_thinking / total_output, 4) if total_output else None
        ),
        "turns_with_thinking": sum(
            1 for t in turns if (t.get("thinking_tokens", 0) or 0) > 0
        ),
        "max_thinking_tokens": max(
            (t.get("thinking_tokens", 0) or 0) for t in turns
        ),
    }


# stop_reasons that are normal turn terminations (end_turn = done, tool_use =
# paused to call a tool). Both are high-frequency and low-signal, so they get a
# metadata rollup but no per-trace tag — only anomalous reasons (max_tokens =
# truncated output, refusal, pause_turn, stop_sequence, ...) are tagged,
# mirroring the has-errors / model-missing anomaly-flag pattern.
NORMAL_STOP_REASONS = frozenset({"end_turn", "tool_use"})


def build_stop_reasons_summary(turns: list[dict]) -> dict | None:
    """Roll up per-turn terminal `stop_reason` across the incremental turn batch.

    Each turn's `stop_reason` is the API's terminal reason for that turn
    (`end_turn`, `tool_use`, `max_tokens`, `stop_sequence`, `refusal`,
    `pause_turn`, ...), set in `build_turns` (latest non-empty per turn). Returns a
    `{by_reason, turns_counted, last}` rollup, or None when no turn carried a
    stop_reason. When any turn was refused, `refusal_categories` counts the
    `stop_details.category` values (null category -> "uncategorized"). `last` is the terminal reason of the final turn that had one —
    the session's outcome. Computed over the incremental `turns` batch, matching
    `total_iterations` / `active_duration`.
    """
    by_reason = {}
    refusal_categories = {}
    last = None
    for t in turns:
        r = t.get("stop_reason")
        if not r:
            continue
        by_reason[r] = by_reason.get(r, 0) + 1
        last = r
        if r == "refusal":
            cat = t.get("refusal_category") or "uncategorized"
            refusal_categories[cat] = refusal_categories.get(cat, 0) + 1
    if not by_reason:
        return None
    summary = {
        "by_reason": by_reason,
        "turns_counted": sum(by_reason.values()),
        "last": last,
    }
    if refusal_categories:
        summary["refusal_categories"] = refusal_categories
    return summary


def extract_queue_operations(transcript_path: str) -> dict | None:
    """Summarise prompt-queue activity from `type:"queue-operation"` entries.

    While the assistant is busy a user can queue follow-up prompts; CC logs each
    queue mutation as a `queue-operation` with `operation` in
    {`enqueue`, `dequeue`, `popAll`, `remove`} and a `content` (the prompt text).
    Wait time a prompt spends queued (enqueue -> dequeue|popAll) is a latency /
    friction signal: long waits mean the user is racing ahead of the agent.

    Waits are matched FIFO — each `dequeue` / `popAll`-flushed slot consumes the
    oldest pending `enqueue`. `remove` (user cancelled a queued prompt) also pops
    FIFO but is excluded from execution-wait stats. No id ties an op to a specific
    queued prompt, so the FIFO pairing is an approximation. `content` is NOT stored
    (raw prompt text = PII). None when the session queued nothing.
    """
    operations: dict[str, int] = {}
    pending: list[datetime] = []   # timestamps of still-queued enqueues
    waits: list[float] = []        # execution waits in ms (dequeue / popAll)
    removed = 0
    for entry in _entries(transcript_path):
        if entry.get("type") != "queue-operation":
            continue
        op = entry.get("operation") or "unknown"
        operations[op] = operations.get(op, 0) + 1
        ts = parse_ts(entry.get("timestamp", ""))
        if op == "enqueue":
            if ts:
                pending.append(ts)
        elif op == "dequeue":
            if pending:
                e = pending.pop(0)
                if ts and e and ts >= e:
                    waits.append((ts - e).total_seconds() * 1000)
        elif op == "popAll":
            for e in pending:
                if ts and ts >= e:
                    waits.append((ts - e).total_seconds() * 1000)
            pending = []
        elif op == "remove":
            removed += 1
            if pending:
                pending.pop(0)
    if not operations:
        return None
    return {
        "operations": operations,
        "queued": operations.get("enqueue", 0),
        "executed": len(waits),
        "removed": removed,
        "total_wait_ms": round(sum(waits)),
        "max_wait_ms": round(max(waits)) if waits else 0,
    }


def build_attribution_tags(summary: dict | None) -> list[str]:
    """Trace tags for skill / plugin attribution. Sorted, deduplicated; never
    includes the '_unattributed' breakdown bucket."""
    if not summary:
        return []
    tags = []
    for pl in summary.get("plugins_used") or []:
        tags.append(f"plugin:{pl}")
    for sk in summary.get("skills_used") or []:
        if sk == "_unattributed":
            continue
        tags.append(f"skill:{sk}")
    return sorted(set(tags))


# Every score the hook writes for a session. Ids depend only on session and
# name, so --delete-traces can remove them without asking Langfuse.
SESSION_SCORE_NAMES = ("session_cost_usd", "cache_hit_rate", "tool_error_rate",
                       "tool_denial_rate", "cli_cost_usd", "cost_residual_usd")


def session_score_id(session_id: str, name: str) -> str:
    return f"score-{uuid.uuid5(uuid.NAMESPACE_URL, f'{session_id}:score:{name}')}"


def build_hook_score_events(
    session_id: str,
    turns: list[dict],
    session_cost: float,
    rollups: dict | None = None,
    cli_cost: float | None = None,
    cost_residual: float | None = None,
) -> list[dict]:
    """Build the session-level score-create events.

    Scores attach to the session, not to a turn trace, and upsert on a stable
    id keyed on session + name, so every fire overwrites the previous value.

    - `session_cost_usd`: parent + subagent cost; its metadata carries every
      session-wide rollup (cost_by_model, subagent_costs, compaction, ...).
    - `cache_hit_rate`, `tool_error_rate`, `tool_denial_rate`: omitted when
      their classifier returns None (no cache activity / no executed calls /
      no attempted calls), so "absent" stays distinct from a genuine 0.0.
    - `cli_cost_usd`: Claude Code's own total from the `cost-state` entry,
      next to `session_cost_usd` for comparison; omitted until that entry
      is written (the CLI writes it on exit).
    - `cost_residual_usd`: CLI total − hook cost − the models that answered
      no message (`cli_cost_state.gap_usd − side_model_cost_usd`): spend on
      the session's own models that the transcript never records (classifier
      calls, compaction, partially recorded subagents). Negative means the
      hook bills more than the CLI. Omitted without a whole-session cost-state.
    """
    scores = [{"name": "session_cost_usd", "value": round(session_cost, 6),
               "metadata": rollups or {}}]
    if cli_cost is not None:
        scores.append({"name": "cli_cost_usd", "value": round(cli_cost, 6)})
    if cost_residual is not None:
        scores.append({"name": "cost_residual_usd", "value": round(cost_residual, 6)})
    for name, value in (
        ("cache_hit_rate", compute_cache_hit_rate(turns)),
        ("tool_error_rate", calculate_tool_error_rate(turns)),
        ("tool_denial_rate", calculate_tool_denial_rate(turns)),
    ):
        if value is not None:
            scores.append({"name": name, "value": value})

    events = []
    for score in scores:
        events.append({"type": "score-create", "body": {
            "id": session_score_id(session_id, score["name"]),
            "sessionId": session_id,
            "dataType": "NUMERIC",
            "source": "API",
            **score,
        }})
    return events


def gen_metadata_attribution(turn: dict) -> dict:
    """Per-generation attribution metadata; empty dict when nothing to emit."""
    out = {}
    sk = turn.get("attribution_skill") or ""
    pl = turn.get("attribution_plugin") or ""
    all_skills = turn.get("attribution_skills_all") or []
    if sk:
        out["attribution_skill"] = sk
    if pl:
        out["attribution_plugin"] = pl
    if len(all_skills) > 1:
        out["attribution_skills_all"] = list(all_skills)
    if turn.get("effort"):
        out["effort"] = turn["effort"]
    if turn.get("stop_reason") == "refusal":
        out["refusal_category"] = turn.get("refusal_category") or "uncategorized"
    return out


def gen_metadata_turn_signals(turn: dict) -> dict:
    """Per-generation transcript signals; empty dict when nothing to emit.

    requested_model is emitted only when it differs from the model that
    answered (a model fallback); on every other turn it repeats `model`.
    """
    out = {}
    req = turn.get("requested_model") or ""
    if req and req != (turn.get("model") or ""):
        out["requested_model"] = req
        out["model_fallback"] = True
    if turn.get("thinking_duration_ms"):
        out["thinking_duration_ms"] = turn["thinking_duration_ms"]
    if turn.get("truncated_after_output"):
        out["truncated_after_output"] = True
    if turn.get("aborted_mid_stream"):
        out["aborted_mid_stream"] = True
    if turn.get("advisor_model"):
        out["advisor_model"] = turn["advisor_model"]
    if turn.get("turn_origin"):
        out["turn_origin"] = turn["turn_origin"]
    if turn.get("attribution_agent"):
        out["attribution_agent"] = turn["attribution_agent"]
    if turn.get("scheduled_task_id"):
        out["scheduled_task_id"] = turn["scheduled_task_id"]
    # Only where it changed the price: other models bill long prompts at the
    # same rate, so the count would read as a surcharge that never happened.
    if turn.get("long_context_requests") and _has_long_context_tier(turn.get("model") or ""):
        out["long_context_requests"] = turn["long_context_requests"]
    return out


def build_turn_signal_summary(turns: list[dict]) -> dict | None:
    """Session rollup of gen_metadata_turn_signals; None when no turn has any."""
    signals = [gen_metadata_turn_signals(t) for t in turns]
    if not any(signals):
        return None
    origins: dict[str, int] = {}
    for s in signals:
        if s.get("turn_origin"):
            origins[s["turn_origin"]] = origins.get(s["turn_origin"], 0) + 1
    return {
        "turn_origins": origins,
        "model_fallback_turns": sum(1 for s in signals if s.get("model_fallback")),
        "truncated_turns": sum(1 for s in signals if s.get("truncated_after_output")),
        "aborted_turns": sum(1 for s in signals if s.get("aborted_mid_stream")),
        "thinking_duration_ms": sum(s.get("thinking_duration_ms", 0) for s in signals),
        "long_context_requests": sum(s.get("long_context_requests", 0) for s in signals),
        "scheduled_turns": sum(1 for s in signals if s.get("scheduled_task_id")),
    }


def gen_metadata_cache_miss(turn: dict) -> dict:
    """Per-generation cache-miss metadata; empty dict when no miss in the turn."""
    cm = turn.get("cache_miss")
    if not cm:
        return {}
    by = cm["by_reason"]
    dominant = max(by, key=by.get) if by else None
    return {
        "cache_miss_reason": dominant,
        "cache_missed_tokens": cm["missed_tokens"],
        "cache_miss_by_reason": by,
    }


def build_cost_check(own_turns: list[dict], agents: dict, cost_state: dict | None,
                     cli_cost_state: dict | None, compaction: dict | None, away_summaries: list,
                     classifier_requests: int, copied_message_ids: frozenset) -> dict:
    """The session's `cost_check` rollup, plus the tags it raises.

    `duplicate_messages` counts API messages billed by more than one of the
    parent and its subagents; each message must be billed exactly once. The
    cost-state comparison (check_cost) needs a cost-state that covers the
    whole session, which the CLI writes on exit; a continuation's carries on
    from its predecessor's, whose tokens this session does not bill, so it
    is not compared.
    """
    billed = Counter({mid for t in own_turns for mid in t["api_call_ids"]} - copied_message_ids)
    for agent in agents.values():
        billed.update(set(agent.get("message_ids") or ()))
    duplicates = sum(1 for n in billed.values() if n > 1)

    out: dict = {"duplicate_messages": duplicates}
    if not cost_state:
        out["status"] = "unchecked: no cost-state yet"
    elif cost_state.get("partial"):
        out["status"] = "unchecked: cost-state covers part of the session"
    elif "predecessor_cost_usd" in (cli_cost_state or {}):
        out["status"] = "unchecked: continuation"
    elif any("tokens_by_model" not in a for a in agents.values()):
        out["status"] = "unchecked: subagent state predates the check"
    else:
        tokens = tokens_by_model(own_turns)
        for agent in agents.values():
            for model, t in agent["tokens_by_model"].items():
                mine = tokens.setdefault(model, dict.fromkeys(TOKEN_KINDS, 0))
                for k in TOKEN_KINDS:
                    mine[k] += t.get(k, 0)
        side_calls = {
            "classifier_requests": classifier_requests,
            "compactions": (compaction or {}).get("count", 0),
            "away_summaries": len(away_summaries or ()),
            "web_tool_calls": count_web_tool_calls(own_turns)
                              + sum(a.get("web_tool_calls", 0) for a in agents.values()),
        }
        partial = sum(a.get("partial_messages", 0) for a in agents.values())
        out.update(check_cost(tokens, cost_state.get("by_model", {}), side_calls, partial))

    tags, notes = [], []
    if duplicates:
        tags.append("duplicate-billing")
        notes.append(f"{duplicates} message(s) billed more than once")
    if out["status"] == "overcount":
        tags.append("cost-overcount")
        notes.append(f"transcript tokens exceed the CLI's own count by ${out['overcount_usd']:.4f} "
                     f"({out['overcount']})")
    elif out["status"] == "unreconciled":
        tags.append("cost-unreconciled")
        notes.append(f"the CLI counted {out['output_excess_tokens']:,} more output tokens than the "
                     f"transcript, beyond the {out['output_allowance']:,} its side calls explain")
    out["tags"] = tags
    out["summary"] = "; ".join(notes)
    return out


@contextlib.contextmanager
def _session_lock(session_id: str):
    """Hold an exclusive per-session lock; yields False if it timed out.

    With the Stop hook registered as `"async": true`, a new fire can start
    while the previous one for the same session is still sending. Without the
    lock both read the same offset, send overlapping batches and race on the
    state files. The lock is advisory (flock) and released when the process
    exits, so a crashed fire cannot leave it stuck.
    """
    try:
        import fcntl
    except ImportError:  # not POSIX: run unlocked, as before
        yield True
        return
    Path(STATE_DIR).mkdir(parents=True, exist_ok=True)
    fd = os.open(os.path.join(STATE_DIR, f"{session_id}.lock"), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        deadline = time.monotonic() + SESSION_LOCK_TIMEOUT_S
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    yield False
                    return
                time.sleep(0.1)
        yield True
    finally:
        os.close(fd)


# Claude Code writes `continued-in` into the old transcript when it continues a
# session in a new one; only bookkeeping lines (last-prompt, cost-state) follow
# it, so the tail of each sibling transcript is enough to find the predecessor.
CONTINUATION_TAIL_BYTES = 64 * 1024


def find_predecessor(session_id: str, transcript_path: str) -> str:
    """Path of the transcript that was continued in this session, or "".

    Scans the tail of every other transcript in this one's project dir for a
    `continued-in` entry naming `session_id`. All 11 continuations seen so far
    (2026-10-18) stayed in the same project dir.
    """
    pdir = os.path.dirname(transcript_path)
    try:
        names = sorted(os.listdir(pdir))
    except OSError:
        return ""
    needle = session_id.encode()
    own = os.path.basename(transcript_path)
    for name in names:
        if not name.endswith(".jsonl") or name == own:
            continue
        path = os.path.join(pdir, name)
        try:
            with open(path, "rb") as f:
                f.seek(0, os.SEEK_END)
                f.seek(max(0, f.tell() - CONTINUATION_TAIL_BYTES))
                tail = f.read()
        except OSError:
            continue
        if needle not in tail:
            continue
        for line in tail.splitlines():
            if needle not in line:
                continue
            try:
                entry = json.loads(line)
            except ValueError:  # also the tail's cut-off first line
                continue
            if (isinstance(entry, dict) and entry.get("type") == "continued-in"
                    and entry.get("continuedInSessionId") == session_id):
                return path
    return ""


def resolve_continuation(session_id: str, transcript_path: str, entries: list[dict]) -> dict:
    """What this session copied from the session it continues; resolved once, cached.

    A continued session's transcript starts with a copy of the predecessor's
    history: same entry `uuid`s and message ids, `sessionId` rewritten. Returns
    {"predecessor": ""} when the session continues nothing. Otherwise the
    predecessor's id, transcript path and line count, the `uuids` and
    `message_ids` copied from it, and `cli_cost`: its Claude Code cost-state
    ({start_time, total_cost_usd}) as of its `continued-in`, or None.

    The copy and the `continued-in` are written before the session's own first
    entry, so the first fire sees both and a negative answer can be cached.
    The cache lives next to the other state files: `<session>.continuation.json`.
    """
    cache = os.path.join(STATE_DIR, f"{session_id}.continuation.json")
    try:
        info = json.loads(Path(cache).read_text())
        if isinstance(info, dict):
            return info
    except (OSError, ValueError):
        pass
    info = {"predecessor": ""}
    pred_path = find_predecessor(session_id, transcript_path)
    if pred_path:
        pred_entries, pred_lines, read_ok = parse_transcript(pred_path)
        if not read_ok:
            return info  # not cached: the next fire retries
        pred_uuids, pred_mids, cli_cost, last_cost_state = set(), set(), None, None
        for e in pred_entries:
            if e.get("uuid"):
                pred_uuids.add(e["uuid"])
            etype = e.get("type")
            if etype == "assistant" and not e.get("isApiErrorMessage"):
                mid = (e.get("message") or {}).get("id")
                if mid:
                    pred_mids.add(mid)
            elif etype == "cost-state" and isinstance(e.get("totalCostUSD"), (int, float)):
                last_cost_state = e
            elif (etype == "continued-in" and e.get("continuedInSessionId") == session_id
                    and last_cost_state):
                usage = last_cost_state.get("modelUsage")
                cli_cost = {"start_time": last_cost_state.get("startTime"),
                            "total_cost_usd": last_cost_state["totalCostUSD"],
                            "by_model": {m: u.get("costUSD") for m, u in usage.items()
                                         if isinstance(u, dict)} if isinstance(usage, dict) else {}}
        own_mids = {(e.get("message") or {}).get("id") for e in entries
                    if e.get("type") == "assistant"}
        info = {
            "predecessor": sanitize_id(os.path.basename(pred_path)[:-len(".jsonl")]),
            "path": pred_path,
            "lines": pred_lines,
            "uuids": sorted({e["uuid"] for e in entries if e.get("uuid") in pred_uuids}),
            "message_ids": sorted(own_mids & pred_mids),
            "cli_cost": cli_cost,
        }
    Path(STATE_DIR).mkdir(parents=True, exist_ok=True)
    atomic_write_text(cache, json.dumps(info))
    return info


_FLUSHING: set = set()


def flush_predecessor(continuation: dict) -> None:
    """Send what the predecessor has not sent yet, as itself, before its copy is skipped.

    A continuation can happen mid-turn, before the old session ever fired a
    Stop (0e8220a3, 2026-10-18, had no send at all), so the copy is safe to
    skip only once the predecessor has sent it. The predecessor is over, so it
    is processed as final. Runs while its state is behind its transcript, so a
    failed send is retried on the next fire.
    """
    pred, path = continuation.get("predecessor"), continuation.get("path")
    # A predecessor past Claude Code's 30-day retention has nothing left to send.
    if not pred or not path or pred in _FLUSHING or not os.path.isfile(path):
        return
    if load_state(pred)[0] >= continuation.get("lines", 0):
        return
    _FLUSHING.add(pred)
    try:
        process_session(pred, path, extract_cwd(path), final=True)
    finally:
        _FLUSHING.discard(pred)


def process_session(session_id: str, transcript_path: str, cwd: str, last_assistant_message: str = "", final: bool = False, background_tasks: list = None, session_crons: list = None) -> None:
    """Process one session transcript, one fire per session at a time."""
    with _session_lock(session_id) as acquired:
        if not acquired:
            log(f"[WARN] Session {session_id} still locked by another fire after "
                f"{SESSION_LOCK_TIMEOUT_S}s; skipping — the next fire picks up these lines")
            return
        _process_session_locked(session_id, transcript_path, cwd, last_assistant_message,
                                final, background_tasks, session_crons)


# A turn whose last reply ended for one of these reasons is finished. Anything
# else (tool_use, pause_turn, nothing yet) may still be running.
TERMINAL_STOP_REASONS = frozenset({"end_turn", "stop_sequence", "max_tokens", "refusal"})


def complete_turn_count(turns: list[dict], final: bool) -> int:
    """How many leading turns are finished and safe to export.

    Langfuse v4 keeps the first export of a span, so a turn is sent once, when
    it is complete. Every turn but the last is complete: a later turn started
    after it. The last one is complete when its reply ended for a terminal
    reason — on a Stop fire the turn that just finished has one. It is held
    back otherwise: an async fire can read the transcript after a queued
    prompt has already opened the next turn. `final` (SessionEnd, reprocess of
    an idle transcript) sends everything else.

    A last turn with no reply (`/exit`, `/model`, a prompt interrupted before
    any answer) is never sent, not even when `final`: user entries merge
    into one turn until a reply arrives, so if the session is resumed the
    next prompt and its reply join that turn. Sending it would mark the
    index as done and lose the reply's cost. It has none of its own.
    """
    if not turns:
        return 0
    if not turns[-1].get("api_call_ids"):
        return len(turns) - 1
    if final or (turns[-1].get("stop_reason") or "") in TERMINAL_STOP_REASONS:
        return len(turns)
    return len(turns) - 1


def _user_id() -> str:
    return os.environ.get("USER", "unknown")


def _process_session_locked(session_id: str, transcript_path: str, cwd: str, last_assistant_message: str = "", final: bool = False, background_tasks: list = None, session_crons: list = None) -> None:
    """Export a session's finished turns, one trace per turn.

    Each turn is sent once, as its own trace grouped by `session.id`.
    Session-wide rollups are recomputed over the whole transcript on every
    fire and sent as session scores, which upsert by id.
    """
    prev_line_offset, prev_turn_count = load_state(session_id)

    entries, total_lines, read_ok = parse_transcript(
        transcript_path, warn_after=prev_line_offset)

    if not read_ok:
        # Transient read failure (file moved, permission denied, etc.).
        # Do NOT call save_state — that would clobber the existing offset
        # with 0 and force a full re-ingest. Bail and retry on the next fire.
        log(f"Skipping state save for {session_id} due to transcript read failure")
        return

    if total_lines == prev_line_offset and not final:
        log(f"No new entries for session {session_id} (offset {prev_line_offset}, total {total_lines})")
        return

    # A continued session starts with a copy of its predecessor's history,
    # which the predecessor sends and bills (resolve_continuation).
    continuation = resolve_continuation(session_id, transcript_path, entries)
    flush_predecessor(continuation)
    copied_uuids = frozenset(continuation.get("uuids", ()))
    own_entries = ([e for e in entries if e.get("uuid") not in copied_uuids]
                   if copied_uuids else entries)

    turns = build_turns(entries, frozenset(continuation.get("message_ids", ())))
    if not turns:
        log(f"No turns found in {len(entries)} entries")
        save_state(session_id, total_lines, 0)
        return
    sendable = complete_turn_count(turns, final)
    # Rollups and scores cover this session's own part; indices stay those of `turns`.
    own_turns = [t for t in turns if not t.get("copied")]

    custom_title = extract_custom_title(own_entries)
    permission_mode = extract_permission_mode(own_entries)
    permission_timeline = extract_permission_timeline(own_entries)
    compaction = extract_compaction(own_entries)
    remote_control = extract_bridge(own_entries)
    pr_links = extract_pr_links(own_entries)
    away_summaries = extract_away_summaries(own_entries)
    agent_name = extract_agent_name(own_entries)
    ai_title = extract_ai_title(own_entries)
    session_kind = extract_session_kind(own_entries)
    attachments = extract_attachments(own_entries)
    local_commands = extract_local_commands(own_entries)
    file_history_stats = extract_file_history_stats(own_entries)
    prompt_provenance = extract_prompt_provenance(own_entries)
    tool_denials = extract_tool_denials(own_entries)
    stop_hook_stats = extract_stop_hook_stats(own_entries)
    worktree_state = extract_worktree_state(own_entries)
    api_error_messages = extract_api_error_messages(own_entries)
    interrupts = extract_interrupts(own_entries)
    queue_operations = extract_queue_operations(own_entries)
    cost_state = extract_cost_state(own_entries)
    async_hooks = extract_async_hook_responses(own_entries)
    rate_limits = extract_rate_limits(own_entries)
    scheduled_tasks = extract_scheduled_task_fires(own_entries)
    continued_in = extract_continued_in(own_entries)
    relocations = extract_relocations(own_entries)
    session_meta = extract_session_metadata(own_entries)
    api_errors = extract_api_errors(own_entries)
    has_errors = api_errors["total_count"] > 0

    # Price each turn once; every rollup below reads this.
    for t in turns:
        t["_cost"] = price_turn(t)
    session_cost = sum(t["_cost"][0] for t in own_turns)

    repo_name = os.path.basename(cwd.rstrip("/")) if cwd else ""
    user_id = _user_id()
    has_fast = any(t.get("speed") == "fast" for t in own_turns)
    turn_signals = build_turn_signal_summary(own_turns)
    # Last turn that carried a per-turn `effort` (transcript field, CC 2.1.220+).
    effort = next((t["effort"] for t in reversed(own_turns) if t.get("effort")), "")

    model_families = set()
    for t in own_turns:
        m = (t.get("model") or "").lower()
        if "haiku" in m:
            model_families.add("haiku")
        elif "opus" in m:
            model_families.add("opus")
        elif m and m != "<synthetic>":
            model_families.add("sonnet")

    # A turn with billable tokens but no model would otherwise be silently
    # mispriced (was: defaulted to Opus). Surface it as a filterable tag.
    has_missing_model = any(
        not (t.get("model") or "") and _has_billable_tokens(t.get("usage") or {})
        for t in own_turns
    )

    skill_attribution = build_skill_attribution_summary(own_turns)
    attribution_tags = build_attribution_tags(skill_attribution)
    sa_state = load_subagent_state(session_id)

    # Session title: custom title > ai title > agent name > first real prompt
    # > repo/branch. Synthetic stubs (slash commands, reminders) start with an
    # XML-like wrapper tag and are skipped.
    first_real_prompt = ""
    for t in own_turns:
        ui = (t.get("display_prompt") or "").strip()
        # A turn with no reply is a bare local command (/exit, /effort high).
        if ui and t.get("api_call_ids") and not _is_synthetic_prompt(ui):
            first_real_prompt = ui
            break
    session_title = (
        custom_title
        or ai_title
        or agent_name
        or truncate(redact_secrets(first_real_prompt), 80).strip()
        or f"{repo_name}/{session_meta.get('gitBranch', '')}".strip("/")
        or "Claude Code Session"
    )

    pr_tags = [f"pr:{p['number']}" for p in pr_links if p.get("number") is not None]
    compact_trigger_tags = [
        f"compact-trigger:{t}" for t in sorted((compaction or {}).get("triggers", {}))
        if t != "unknown"
    ]
    stop_reasons = build_stop_reasons_summary(own_turns)
    stop_reason_tags = [
        f"stop-reason:{r}"
        for r in sorted((stop_reasons or {}).get("by_reason", {}))
        if r not in NORMAL_STOP_REASONS
    ]
    refusal_tags = [
        f"refusal:{c}" for c in sorted((stop_reasons or {}).get("refusal_categories", {}))
    ]
    # Exact model IDs alongside the family tags, so Opus 5 and Opus 5.5
    # sessions can be filtered apart.
    model_tags = sorted({f"model:{t['model']}" for t in own_turns if t.get("model")})

    # Session state as of this fire, repeated on every turn trace so each one
    # filters on its own. Tags reflect the session so far.
    session_tags = [t for t in [
        "claude-code",
        repo_name or None,
        *sorted(model_families),
        *model_tags,
        session_meta.get("entrypoint") or None,
        "fast" if has_fast else None,
        "has-errors" if has_errors else None,
        "has-api-error-messages" if api_error_messages else None,
        "has-interrupts" if interrupts else None,
        "model-fallback" if (turn_signals or {}).get("model_fallback_turns") else None,
        "has-truncated-output" if (turn_signals or {}).get("truncated_turns") else None,
        "has-queued-prompts" if queue_operations else None,
        "has-background-tasks" if background_tasks else None,
        "model-missing" if has_missing_model else None,
        f"permission:{permission_mode}" if permission_mode else None,
        f"agent-name:{agent_name}" if agent_name else None,
        f"session-kind:{session_kind}" if session_kind else None,
        f"worktree:{worktree_state['name']}" if worktree_state and worktree_state.get("name") else None,
        f"effort:{effort}" if effort else None,
        "compacted" if compaction else None,
        "remote-control" if remote_control else None,
        "permission-bypass" if (permission_timeline or {}).get("ever_bypass") else None,
        "has-hook-failures" if (async_hooks or {}).get("failures") else None,
        "rate-limited" if (rate_limits or {}).get("by_status", {}).get("rejected") else None,
        "scheduled-task" if scheduled_tasks or (turn_signals or {}).get("scheduled_turns") else None,
        "continued" if continued_in else None,
        "continuation" if continuation.get("predecessor") else None,
        *compact_trigger_tags,
        *stop_reason_tags,
        *refusal_tags,
        *attribution_tags,
        *pr_tags,
    ] if t]
    session_state = {
        "session_title": session_title,
        "cwd": cwd,
        "repo_name": repo_name,
        "git_branch": session_meta.get("gitBranch", ""),
        "cli_version": session_meta.get("version", ""),
        "entrypoint": session_meta.get("entrypoint", ""),
        "custom_title": custom_title or None,
        "ai_title": ai_title or None,
        "agent_name": agent_name or None,
        "session_kind": session_kind or None,
        "permission_mode": permission_mode or None,
        "effort_level": effort or None,
        "worktree": worktree_state,
        "remote_control": remote_control,
    }

    batch = []
    sa_meta_index = build_subagent_meta_index(_subagent_search_dirs(transcript_path, cwd))
    sa_visited = set()
    for turn_idx in range(min(prev_turn_count, sendable), sendable):
        turn = turns[turn_idx]
        if turn.get("copied"):
            continue  # copied from the predecessor, which sent it
        # Keyed on the turn's index in the whole transcript, so a live fire
        # and a re-ingest after --delete-traces produce the same IDs.
        turn_id = uuid.uuid5(uuid.NAMESPACE_URL, f"{session_id}:turn:{turn_idx}")
        trace_id = turn_id.hex
        gen_id = f"gen-{turn_id}"
        prompt = redact_secrets(truncate(turn["display_prompt"], 80))

        batch.append({
            "type": "trace-create",
            "body": {
                "id": trace_id,
                "name": prompt.strip() or f"Turn {turn_idx + 1}",
                "sessionId": session_id,
                "userId": user_id,
                "tags": session_tags,
                "metadata": {**session_state, "turn_index": turn_idx + 1},
            },
        })

        start_time = turn["start_time"]
        first_token_time = turn.get("first_token_time")
        usage = turn["usage"]
        model = turn.get("model") or ""
        tool_names = [tc["name"] for tc in turn["tool_calls"]]

        ttft_ms = None
        if first_token_time and start_time:
            st = parse_ts(start_time)
            ft = parse_ts(first_token_time)
            if st and ft and ft > st:
                ttft_ms = round((ft - st).total_seconds() * 1000)

        cost_details = turn["_cost"][3]
        gen_body = {
            "id": gen_id,
            "traceId": trace_id,
            "name": f"Turn {turn_idx + 1}: {prompt}",
            "model": model,
            "input": redact_secrets(truncate(turn["user_input"], MAX_TEXT)) or None,
            "output": redact_secrets(truncate(turn["assistant_output"], MAX_TEXT)) or None,
            "startTime": start_time,
            "endTime": turn["end_time"],
            "completionStartTime": first_token_time,
            "usageDetails": _usage_details(turn),
            "costDetails": cost_details if cost_details else None,
            "metadata": {
                "tools_used": tool_names,
                "tool_count": len(tool_names),
                "api_calls": len(turn["api_call_ids"]),
                "speed": turn.get("speed", ""),
                "service_tier": turn.get("service_tier", ""),
                "inference_geo": turn.get("inference_geo", ""),
                "request_ids": list(dict.fromkeys(turn.get("request_ids", []))),
                "web_search_requests": turn.get("web_search_requests", 0),
                "web_fetch_requests": turn.get("web_fetch_requests", 0),
                "iteration_count": turn.get("iteration_count", 0),
                "cost_usd": round(turn["_cost"][0], 6),
                **gen_metadata_attribution(turn),
                **gen_metadata_turn_signals(turn),
                **gen_metadata_cache_miss(turn),
            },
            "attributes": _otel_genai_attrs(
                session_id=session_id,
                model=model,
                usage=usage,
                request_ids=turn.get("request_ids", []),
                stop_reason=turn.get("stop_reason", ""),
            ),
        }
        if ttft_ms is not None:
            gen_body["metadata"]["ttft_ms"] = ttft_ms
        if turn.get("duration_ms"):
            gen_body["metadata"]["duration_ms"] = turn["duration_ms"]
        batch.append({"type": "generation-create", "body": gen_body})

        agent_calls = []
        for tc_idx, tc in enumerate(turn["tool_calls"]):
            span_id = f"span-{turn_id}-{tc_idx}"
            batch.append({"type": "span-create", "body": {
                "id": span_id,
                "traceId": trace_id,
                "parentObservationId": gen_id,
                "name": tc["name"],
                "startTime": tc.get("start_time", start_time),
                "endTime": tc.get("end_time", tc.get("start_time", start_time)),
                "input": _tool_input(tc["input"]),
                "output": _tool_output(tc["output"]) or None,
                "metadata": {"tool_use_id": tc["id"]},
            }})
            # Each subagent's .meta.json names the Agent tool_use that spawned it.
            if tc["name"] == "Agent" and tc["id"] in sa_meta_index:
                agent_calls.append((tc_idx, sa_meta_index[tc["id"]]))

        for sa_tc_idx, meta in agent_calls:
            sa_id, sa_path = meta["agent_id"], meta["path"]
            sa_desc, sa_type = meta["description"], meta["agent_type"]
            if sa_id in sa_visited:
                continue
            sa_parent_span = f"span-{turn_id}-{sa_tc_idx}"
            prev = sa_state.get(sa_id, {})
            # Measured before the read: a write landing mid-read then shows
            # as growth on the next fire instead of being counted as seen.
            sa_size = _file_size(sa_path)
            sa_events, sa_cost, sa_offset, sa_tc, sa_status = ingest_subagent(
                agent_id=sa_id,
                transcript_path=sa_path,
                parent_span_id=sa_parent_span,
                trace_id=trace_id,
                session_id=session_id,
                subagent_offset=prev.get("offset", 0),
                prior_turn_count=prev.get("turn_count", 0),
                meta_index=sa_meta_index,
                visited=sa_visited,
                sa_state=sa_state,
                final=final,
            )
            batch.extend(sa_events)
            sa_state[sa_id] = _subagent_record(
                prev, sa_events, sa_cost, sa_offset, sa_tc, sa_status,
                description=sa_desc, subagent_type=sa_type,
                path=sa_path, size=sa_size, parent_span_id=sa_parent_span,
                trace_id=trace_id, depth=1,
            )
            for evt in batch:
                if evt["type"] == "span-create" and evt["body"]["id"] == sa_parent_span:
                    evt["body"]["metadata"].update({
                        "subagent_id": sa_id,
                        "subagent_type": sa_type,
                        "subagent_description": sa_desc,
                    })
                    break

    # Agents whose Agent call is in a turn sent by an earlier fire. A background
    # agent keeps writing after the parent turn that launched it ended; its
    # recorded trace and parent span let its new turns join that trace.
    for sa_id, prev in list(sa_state.items()):
        if sa_id in sa_visited or not isinstance(prev, dict):
            continue
        sa_path, sa_parent_span, sa_trace = prev.get("path"), prev.get("parent_span_id"), prev.get("trace_id")
        if not (sa_path and sa_parent_span and sa_trace):
            continue
        sa_size = _file_size(sa_path)
        if sa_size is None or (sa_size == prev.get("size") and not final):
            continue
        sa_events, sa_cost, sa_offset, sa_tc, sa_status = ingest_subagent(
            agent_id=sa_id,
            transcript_path=sa_path,
            parent_span_id=sa_parent_span,
            trace_id=sa_trace,
            session_id=session_id,
            subagent_offset=prev.get("offset", 0),
            prior_turn_count=prev.get("turn_count", 0),
            meta_index=sa_meta_index,
            depth=prev.get("depth", 1),
            visited=sa_visited,
            sa_state=sa_state,
            final=final,
        )
        batch.extend(sa_events)
        sa_state[sa_id] = _subagent_record(
            prev, sa_events, sa_cost, sa_offset, sa_tc, sa_status, size=sa_size)
        if sa_events:
            log(f"Resumed background subagent {sa_id}: {len(sa_events)} events")

    agents = {k: v for k, v in sa_state.items() if isinstance(v, dict)}
    subagent_costs = None
    if agents:
        summaries = [{"agent_id": aid, **{k: v for k, v in s.items() if k not in _SA_STATE_ONLY_KEYS}}
                     for aid, s in agents.items()]
        total_subagent_cost = sum(s.get("total_cost", 0) for s in summaries)
        partial_messages = sum(s.get("partial_messages", 0) for s in summaries)
        subagent_costs = {
            "agents": summaries,
            "total_subagent_cost": round(total_subagent_cost, 6),
            "parent_cost": round(session_cost, 6),
            "harness_total_cost": round(session_cost + total_subagent_cost, 6),
            "partial_messages": partial_messages,
        }
        # In place: every turn trace in this batch holds this same list.
        session_tags += ["has-subagents", f"subagents:{len(summaries)}"]
        if any(s.get("depth", 1) >= 2 for s in summaries):
            session_tags.append("nested-subagents")
        # Their output tokens are a lower bound (see count_partial_messages).
        if partial_messages:
            session_tags.append("subagent-usage-partial")

    classifier_requests = count_classifier_requests(own_entries) + sum(
        s.get("classifier_requests", 0) for s in agents.values())

    hook_cost = session_cost + (subagent_costs or {}).get("total_subagent_cost", 0)
    cli_cost_state = None
    cli_cost = None
    if cost_state:
        cli_cost = cost_state["total_cost_usd"]
        model_cost = {m: u.get("cost_usd", 0) for m, u in cost_state.get("by_model", {}).items()}
        cli_cost_state = {**cost_state, "hook_cost_usd": round(hook_cost, 6)}
        # A continuation runs in the predecessor's CLI process (same cost-state
        # startTime), and its cost-state carries on from the predecessor's
        # total, which the predecessor reports itself.
        pred_cli = continuation.get("cli_cost") or {}
        if (pred_cli.get("start_time") is not None
                and pred_cli["start_time"] == last_cost_state_start(own_entries)):
            cli_cost = round(cli_cost - pred_cli["total_cost_usd"], 6)
            cli_cost_state["predecessor_cost_usd"] = round(pred_cli["total_cost_usd"], 6)
            for m, c in (pred_cli.get("by_model") or {}).items():
                if m in model_cost and isinstance(c, (int, float)):
                    model_cost[m] = max(0.0, model_cost[m] - c)
        used_models = {t["model"] for t in own_turns if t.get("model")}
        for s in agents.values():
            used_models.update(s.get("models") or [])
        # Models no message used: mostly the Haiku behind WebFetch/WebSearch.
        cli_cost_state["side_model_cost_usd"] = round(
            sum(c for m, c in model_cost.items() if m not in used_models), 6)
        # Only meaningful when cost-state covers the whole transcript.
        if not cost_state.get("partial"):
            cli_cost_state["gap_usd"] = round(cli_cost - hook_cost, 6)
    cost_check = build_cost_check(own_turns, agents, cost_state, cli_cost_state, compaction,
                                  away_summaries, classifier_requests,
                                  frozenset(continuation.get("message_ids", ())))
    for tag in cost_check["tags"]:
        session_tags.append(tag)
        log(f"[WARN] {tag} in session {session_id}: {cost_check['summary']}")
    cost_residual = None
    if cli_cost_state and "gap_usd" in cli_cost_state:
        cost_residual = cli_cost_state["gap_usd"] - cli_cost_state["side_model_cost_usd"]
    continued_from = None
    if continuation.get("predecessor"):
        continued_from = {
            "session_id": continuation["predecessor"],
            "copied_turns": sum(1 for t in turns if t.get("copied")),
            "copied_messages": len(continuation.get("message_ids", ())),
        }

    session_rollups = {
        **{k: v for k, v in session_state.items()},
        "turn_count": len(own_turns),
        "tool_calls_total": sum(len(t["tool_calls"]) for t in own_turns),
        "total_tokens": sum(t["usage"]["total"] for t in own_turns),
        "total_input_tokens": sum(t["usage"]["input"] for t in own_turns),
        "total_output_tokens": sum(t["usage"]["output"] for t in own_turns),
        "session_cost_usd": round(session_cost, 6),
        "subagent_costs": subagent_costs,
        "api_errors": api_errors if has_errors else None,
        "pr_links": pr_links or None,
        "away_summaries": away_summaries or None,
        "attachments": attachments or None,
        "prompt_provenance": prompt_provenance or None,
        "tool_denials": tool_denials or None,
        "local_commands": local_commands or None,
        "file_snapshots": file_history_stats if file_history_stats["snapshot_count"] > 0 else None,
        "stop_hook": stop_hook_stats if stop_hook_stats["total_hook_fires"] > 0 else None,
        "skill_attribution": skill_attribution,
        "compaction": compaction,
        "permission_timeline": permission_timeline,
        "total_iterations": sum(t.get("iteration_count", 0) for t in own_turns),
        "active_duration": build_active_duration_summary(own_turns),
        "stop_reasons": stop_reasons,
        "cost_by_model": build_model_cost_breakdown(own_turns),
        "opus_5_5_savings": build_opus_5_5_savings(own_turns),
        "cache_miss": build_cache_miss_summary(own_turns),
        "thinking": build_thinking_summary(own_turns),
        "turn_signals": turn_signals,
        "background_tasks": background_tasks or None,
        "session_crons": session_crons or None,
        "api_error_messages": api_error_messages,
        "interrupts": interrupts,
        "queue_operations": queue_operations,
        "cli_cost_state": cli_cost_state,
        "cost_check": {k: v for k, v in cost_check.items() if k != "tags"},
        "classifier_requests": classifier_requests or None,
        "async_hooks": async_hooks,
        "rate_limits": rate_limits,
        "scheduled_tasks": scheduled_tasks,
        "continued_in": continued_in or None,
        "continued_from": continued_from,
        "relocations": relocations,
        "tags": session_tags,
    }
    batch.extend(build_hook_score_events(
        session_id=session_id,
        turns=own_turns,
        session_cost=hook_cost,
        rollups={k: v for k, v in session_rollups.items() if v is not None},
        cli_cost=cli_cost,
        cost_residual=cost_residual,
    ))

    sent_turns = sendable - min(prev_turn_count, sendable)
    summary = (
        f"session {session_id}: {sent_turns} turn trace(s), "
        f"{sum(1 for e in batch if e['type'] == 'span-create')} spans "
        f"(lines {prev_line_offset + 1}-{total_lines}"
        f"{', last turn held until complete' if sendable < len(turns) else ''})"
    )
    if send_to_langfuse(batch):
        save_state(session_id, total_lines, max(prev_turn_count, sendable))
        save_subagent_state(session_id, sa_state)
        log(f"Sent {summary}")
    else:
        log(f"[WARN] Send failed — {summary}; state not advanced, will retry next fire")


def delete_traces(trace_ids: list[str]) -> bool:
    """Delete traces and all their observations. True when the call succeeded.

    Langfuse deletes asynchronously (typically within 15 minutes) and does
    not confirm, so wait before re-ingesting: a cascade arriving after the
    new inserts would wipe them. Sent in groups of 30, the documented limit.
    """
    for i in range(0, len(trace_ids), 30):
        result = _post_json("DELETE", f"{LANGFUSE_HOST}/api/public/traces",
                            {"traceIds": trace_ids[i:i + 30]})
        if result != "ok":
            return False
    return True


def delete_scores(score_ids: list[str]) -> bool:
    """Delete scores by id. True when every call succeeded.

    Session scores belong to no trace, so deleting a session's traces leaves
    them behind, metadata and all. Langfuse merges score metadata key by key
    on upsert, so a re-ingest would otherwise keep keys the new run no longer
    writes. Deleting an id that does not exist succeeds.
    """
    ok = True
    for score_id in score_ids:
        ok &= _post_json("DELETE", f"{LANGFUSE_HOST}/api/public/scores/{score_id}") == "ok"
    return ok


def session_trace_ids(session_id: str, transcript_path: str) -> list[str]:
    """Every trace this hook can have written for a session.

    One per turn (current layout), plus the single `trace-<session>` trace
    written before the move to one trace per turn.
    """
    entries, _, _ = parse_transcript(transcript_path)
    ids = [uuid.uuid5(uuid.NAMESPACE_URL, f"{session_id}:turn:{i}").hex
           for i in range(len(build_turns(entries)))]
    return ids + [f"trace-{session_id}"]


def delete_all_traces() -> int:
    """Delete the traces and session scores of every local session and
    reset its hook state.

    Langfuse v4 keeps every export of a span, so re-ingesting a session
    without deleting it first doubles its cost. State is reset too, so the
    next fire re-sends the whole session. Returns the number of failures.

    Run --reprocess only after Langfuse has finished the deletes.
    """
    if not LANGFUSE_PUBLIC_KEY or not LANGFUSE_SECRET_KEY:
        print("Error: LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY must be set")
        return 1
    transcripts = sorted((Path.home() / ".claude" / "projects").glob("*/*.jsonl"))
    failures = 0
    for idx, transcript in enumerate(transcripts, 1):
        session_id = sanitize_id(transcript.stem)
        ok = delete_traces(session_trace_ids(session_id, str(transcript)))
        ok &= delete_scores([session_score_id(session_id, n) for n in SESSION_SCORE_NAMES])
        failures += not ok
        # Keep the state of a session whose delete failed: its traces are
        # still there, and resetting would make the next fire re-send them.
        if ok:
            for suffix in (".offset", ".subagents.json"):
                state_file = Path(STATE_DIR) / f"{session_id}{suffix}"
                if state_file.exists():
                    state_file.unlink()
        print(f"[{idx}/{len(transcripts)}] {'deleted' if ok else 'FAILED '} {session_id}")
    print(f"\nDone: {len(transcripts) - failures} deleted, {failures} failed. "
          "Langfuse deletes asynchronously — wait until the traces are gone "
          "from the UI before running --reprocess.")
    return failures


# A transcript untouched this long belongs to a session that has ended, so
# --reprocess sends its last turn even when that turn never finished.
REPROCESS_IDLE_S = 3600


def reprocess_all() -> None:
    """Send every local transcript's unsent turns.

    State is kept: each session sends only what its state says is unsent, so
    running this twice, or on sessions already in Langfuse, sends nothing new
    (v4 keeps every export of a span, so a blind re-send doubles cost). To
    re-send everything, run --delete-traces first — it resets state — and
    wait for Langfuse to finish deleting.
    """
    if not LANGFUSE_PUBLIC_KEY or not LANGFUSE_SECRET_KEY:
        print("Error: LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY must be set")
        return

    projects_dir = Path.home() / ".claude" / "projects"
    transcripts = sorted(projects_dir.glob("*/*.jsonl"))

    if not transcripts:
        print("No transcript files found under ~/.claude/projects/")
        return

    total = len(transcripts)
    print(f"Found {total} transcript(s) to reprocess.\n")

    for idx, transcript in enumerate(transcripts, 1):
        session_id = sanitize_id(transcript.stem)
        slug = extract_slug(str(transcript)) or extract_custom_title(str(transcript))
        cwd = extract_cwd(str(transcript))

        label = f"{session_id}"
        if slug:
            label += f" ({slug})"
        print(f"[{idx}/{total}] {label}")

        idle = time.time() - transcript.stat().st_mtime > REPROCESS_IDLE_S
        try:
            process_session(session_id, str(transcript), cwd, final=idle)
        except Exception as e:
            # One bad transcript must not abort every session after it.
            print(f"  Error: {type(e).__name__}: {e}")
            log(f"[ERROR] Reprocess failed for {session_id}: {type(e).__name__}: {e}\n"
                + traceback.format_exc())

    print(f"\nDone. Reprocessed {total} session(s).")


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--reprocess":
        reprocess_all()
        return
    if len(sys.argv) > 1 and sys.argv[1] == "--delete-traces":
        sys.exit(1 if delete_all_traces() else 0)

    try:
        hook_input = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        # Invalid JSON in stdin
        log(f"Failed to parse stdin: {e}")
        return
    if not isinstance(hook_input, dict):
        log(f"[ERROR] Hook stdin is not a JSON object: {type(hook_input).__name__}")
        return

    try:
        run_stop_hook(hook_input)
    except Exception as e:
        # Claude Code shows hook stderr at most once; the log is where a crash
        # has to land to be found later.
        log(f"[ERROR] Stop hook crashed for session "
            f"{hook_input.get('session_id', 'unknown')}: {type(e).__name__}: {e}\n"
            + traceback.format_exc())


def run_stop_hook(hook_input: dict) -> None:
    """Handle one Stop / StopFailure / SessionEnd payload.

    All three run the same export. StopFailure fires when a turn ends in an
    API error (Stop does not fire for it): it sends the turns finished before
    and refreshes the scores. The failed turn's last real reply is not
    terminal, so that turn follows once the next one starts, or at
    SessionEnd, the last chance to send a final turn that never finished.
    Exceptions propagate to main().
    """
    if not LANGFUSE_PUBLIC_KEY or not LANGFUSE_SECRET_KEY:
        log("LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY must be set")
        return

    session_id = sanitize_id(hook_input.get("session_id", "unknown"))
    transcript_path = hook_input.get("transcript_path", "")
    cwd = hook_input.get("cwd", "")
    last_assistant_message = hook_input.get("last_assistant_message", "")

    if hook_input.get("stop_hook_active", False):
        return

    if not transcript_path:
        log("No transcript_path provided")
        return

    process_session(session_id, transcript_path, cwd, last_assistant_message,
                    final=hook_input.get("hook_event_name") == "SessionEnd",
                    background_tasks=hook_input.get("background_tasks"),
                    session_crons=hook_input.get("session_crons"))


if __name__ == "__main__":
    main()
