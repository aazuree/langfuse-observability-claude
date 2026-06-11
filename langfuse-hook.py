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

import hashlib
import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

# Ensure langfuse_common can be imported from the same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from langfuse_common import iter_transcript, log as common_log, make_auth_header, redact_secrets

LANGFUSE_HOST = os.environ.get("LANGFUSE_HOST", "http://localhost:3100")
LANGFUSE_PUBLIC_KEY = os.environ.get("LANGFUSE_PUBLIC_KEY", "")
LANGFUSE_SECRET_KEY = os.environ.get("LANGFUSE_SECRET_KEY", "")
INGESTION_URL = f"{LANGFUSE_HOST}/api/public/ingestion"

LOG_FILE = os.path.expanduser("~/.claude/langfuse-hook.log")
STATE_DIR = os.path.expanduser("~/.claude/langfuse-state")
PROJECTS_DIR = os.path.expanduser("~/.claude/projects")

MAX_TEXT = 10000
MAX_TOOL_IO = 5000
SAFE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")

# Synthetic user-message wrappers (slash commands, local-command output,
# system reminders, prompt-submit hooks). Used to skip these when picking
# a human-readable trace name from the first user prompt.
_SYNTHETIC_PROMPT_RE = re.compile(r"^<[a-z][a-z0-9-]*>")

SUBAGENT_MATCH_WINDOW_S = 60  # Max seconds between Agent tool_use and subagent start


# Pro subscription: $0 marginal cost. Set to True to report equivalent API cost instead.
REPORT_API_EQUIVALENT_COST = True


def log(msg: str) -> None:
    """Wrapper around common_log for backward compatibility."""
    common_log(LOG_FILE, msg)


def sanitize_id(value: str) -> str:
    """Sanitize an ID to prevent path traversal. Returns a safe fallback if invalid."""
    if SAFE_ID_RE.match(value):
        return value
    # Fall back to a hash of the value
    return hashlib.sha256(value.encode()).hexdigest()[:32]


def send_to_langfuse(batch: list[dict]) -> bool:
    """Send events to Langfuse in batches of 50.

    Args:
        batch: List of event dicts to send

    Returns:
        True if all batches sent successfully, False if any failed
    """
    success = True
    for i in range(0, len(batch), 50):
        chunk = batch[i : i + 50]
        payload = json.dumps({"batch": chunk}).encode()
        req = Request(
            INGESTION_URL,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": make_auth_header(),
            },
            method="POST",
        )
        try:
            with urlopen(req, timeout=15) as resp:
                body = resp.read().decode()
                log(f"Langfuse response ({resp.status}): {body[:200]}")
        except (URLError, TimeoutError, OSError) as e:
            log(f"Failed to send to Langfuse: {e}")
            success = False
    return success


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
    Path(state_file).write_text(json.dumps({"lines": line_offset, "turns": turn_count}))


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
    Path(state_file).write_text(json.dumps(state))


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


def discover_subagents(
    transcript_path: str,
    agent_tool_uses: list,
    cwd: str = "",
    meta_index: dict = None,
) -> list:
    """Match Agent tool_uses to subagent transcripts by timestamp proximity.

    Args:
        transcript_path: Path to parent session JSONL (e.g., .../session.jsonl)
        agent_tool_uses: List of (description, subagent_type, timestamp, content_index)
            tuples; may carry a 5th element agent_id (from tool_result text) and a
            6th element tool_use_id (for meta.json correlation).
        cwd: Session cwd. When the session moved into a worktree mid-run,
            new subagent transcripts land under a cwd-derived project dir
            that differs from the parent transcript's project dir. Both
            locations are searched; agent_ids seen in the primary location
            win on duplicates.
        meta_index: Optional {toolUseId: {...}} map from build_subagent_meta_index.

    Returns:
        List of (agent_id, subagent_transcript_path, description, subagent_type,
        tool_use_content_index, correlation) where correlation is "meta"
        (.meta.json toolUseId link), "deterministic" (agentId link from
        tool_result) or "timestamp" (proximity fallback).
        Unmatched entries are omitted.
    """
    if not agent_tool_uses:
        return []

    search_dirs = _subagent_search_dirs(transcript_path, cwd)
    if not search_dirs:
        return []

    # List candidate subagent JSONL files, excluding aside_question.
    # Earlier search_dirs win — primary takes precedence over the cwd fallback.
    candidates = []
    seen_ids = set()
    for sdir in search_dirs:
        for fname in os.listdir(sdir):
            if not fname.startswith("agent-") or not fname.endswith(".jsonl"):
                continue
            if fname.startswith("agent-aside_question-"):
                continue
            agent_id = fname[len("agent-"):-len(".jsonl")]
            if agent_id in seen_ids:
                continue
            jsonl_path = os.path.join(sdir, fname)

            # Read first entry timestamp
            first_ts = None
            try:
                with open(jsonl_path) as f:
                    first_line = f.readline().strip()
                    if first_line:
                        entry = json.loads(first_line)
                        first_ts = parse_ts(entry.get("timestamp", ""))
            except (IOError, OSError, json.JSONDecodeError):
                # Can't open file, read it, or parse JSON; skip this subagent
                continue

            if first_ts is None:
                continue

            seen_ids.add(agent_id)
            candidates.append((agent_id, jsonl_path, first_ts))

    if not candidates:
        return []

    # Sort candidates by start timestamp; index them by agent_id for direct lookup.
    candidates.sort(key=lambda c: c[2])
    by_id = {aid: (path, ts) for (aid, path, ts) in candidates}

    # Sort tool_uses by content_index to preserve dispatch order.
    sorted_tool_uses = sorted(agent_tool_uses, key=lambda t: t[3])

    matched = []
    used_candidates = set()  # indices into `candidates` consumed by timestamp matching
    used_ids = set()         # agent_ids consumed by meta/deterministic matching
    pending_deterministic = []
    pending_timestamp = []   # tool_uses with no usable link at all
    meta_index = meta_index or {}

    # Pass 0: .meta.json toolUseId match (exact; the only link that works for
    # nested agent→agent dispatches, whose tool_results carry no agentId text).
    for tu in sorted_tool_uses:
        desc, sa_type, tu_ts_str, content_idx = tu[0], tu[1], tu[2], tu[3]
        tool_use_id = tu[5] if len(tu) > 5 else None
        m = meta_index.get(tool_use_id)
        if m and m["agent_id"] not in used_ids:
            used_ids.add(m["agent_id"])
            log(f"Matched subagent {m['agent_id']} via meta.json toolUseId")
            matched.append((m["agent_id"], m["path"], desc, sa_type, content_idx, "meta"))
        else:
            pending_deterministic.append(tu)

    # Pass 1: deterministic agentId match (exact, ignores timestamps).
    for tu in pending_deterministic:
        desc, sa_type, tu_ts_str, content_idx = tu[0], tu[1], tu[2], tu[3]
        agent_id = tu[4] if len(tu) > 4 else None
        if agent_id and agent_id in by_id and agent_id not in used_ids:
            path, _ = by_id[agent_id]
            used_ids.add(agent_id)
            log(f"Matched subagent {agent_id} deterministically via tool_result agentId")
            matched.append((agent_id, path, desc, sa_type, content_idx, "deterministic"))
        else:
            if agent_id and agent_id not in by_id:
                log(f"[WARN] tool_result agentId {agent_id} has no matching transcript; "
                    "falling back to timestamp matching")
            pending_timestamp.append(tu)

    # Reserve deterministically-claimed candidates so the timestamp pass skips them.
    for i, (aid, _p, _t) in enumerate(candidates):
        if aid in used_ids:
            used_candidates.add(i)

    # Pass 2: timestamp proximity fallback for the remainder.
    for tu in pending_timestamp:
        desc, sa_type, tu_ts_str, content_idx = tu[0], tu[1], tu[2], tu[3]
        tu_ts = parse_ts(tu_ts_str)
        if tu_ts is None:
            continue

        best = None
        best_diff = None
        for i, (agent_id, jsonl_path, sa_ts) in enumerate(candidates):
            if i in used_candidates:
                continue
            diff = (sa_ts - tu_ts).total_seconds()
            if diff < -1:  # Allow 1s clock skew
                continue
            if abs(diff) > SUBAGENT_MATCH_WINDOW_S:
                continue
            if best_diff is None or diff < best_diff:
                best = i
                best_diff = diff

        if best is not None:
            agent_id, jsonl_path, _ = candidates[best]
            used_candidates.add(best)

            # Log meta.json info for debugging (not required for matching).
            # meta.json sits alongside the jsonl in whichever subagents dir won.
            meta_path = os.path.join(os.path.dirname(jsonl_path), f"agent-{agent_id}.meta.json")
            try:
                meta = json.loads(Path(meta_path).read_text())
                meta_desc = meta.get("description", "")
                meta_type = meta.get("agentType", "")
                if meta_desc and meta_desc != desc:
                    log(f"Subagent {agent_id}: meta description '{meta_desc}' != tool_use '{desc}'")
                log(f"Matched subagent {agent_id} via timestamp (meta: type={meta_type}, desc={meta_desc})")
            except (IOError, OSError, json.JSONDecodeError):
                # .meta.json doesn't exist, can't be read, or contains invalid JSON
                log(f"Matched subagent {agent_id} via timestamp (no .meta.json)")

            matched.append((agent_id, jsonl_path, desc, sa_type, content_idx, "timestamp"))

    # Stable dispatch order regardless of which pass produced each match.
    matched.sort(key=lambda m: m[4])
    return matched


def ingest_subagent(
    agent_id: str,
    transcript_path: str,
    parent_span_id: str,
    trace_id: str,
    session_id: str,
    subagent_offset: int,
    prior_turn_count: int = 0,
    correlation: str = "timestamp",
) -> tuple:
    """Parse a subagent transcript and build Langfuse events.

    Returns: (events, cost_summary, new_offset, new_turn_count, status)
    """
    empty_cost = {"agent_id": agent_id, "total_cost": 0, "total_tokens": 0,
                  "turns": 0, "status": "partial",
                  "cost_breakdown": {"input": 0, "output": 0, "cache_read": 0, "cache_creation": 0}}

    entries, total_lines, read_ok = parse_transcript(transcript_path, skip_lines=subagent_offset)
    if not read_ok:
        # Preserve the prior offset on read failure so the next fire retries.
        return [], empty_cost, subagent_offset, prior_turn_count, "partial"
    if not entries:
        return [], empty_cost, total_lines, prior_turn_count, "partial"

    turns = build_turns(entries)
    if not turns:
        return [], empty_cost, total_lines, prior_turn_count, "partial"

    # Determine completeness from last entry
    status = "partial"
    for entry in reversed(entries):
        if entry.get("stop_reason") == "end_turn":
            status = "complete"
            break
        if entry.get("type") == "assistant":
            break

    now = datetime.now(timezone.utc).isoformat()
    events = []
    total_cost = 0.0
    total_tokens = 0
    cost_breakdown = {"input": 0.0, "output": 0.0, "cache_read": 0.0, "cache_creation": 0.0}

    for turn_idx, turn in enumerate(turns):
        gen_id = f"gen-subagent-{agent_id}-{prior_turn_count + turn_idx}"
        start_time = turn["start_time"]
        end_time = turn["end_time"]
        usage = turn["usage"]
        model = turn.get("model") or ""

        turn_cost, input_cost, output_cost, cost_details = calculate_turn_cost(
            usage,
            model,
            turn.get("cache_ephemeral_5m", 0),
            turn.get("cache_ephemeral_1h", 0),
            speed=turn.get("speed", ""),
            inference_geo=turn.get("inference_geo", ""),
            web_search_requests=turn.get("web_search_requests", 0),
        )

        total_cost += turn_cost
        total_tokens += usage["total"]
        if cost_details:
            cost_breakdown["input"] += cost_details.get("input", 0)
            cost_breakdown["output"] += cost_details.get("output", 0)
            cost_breakdown["cache_read"] += cost_details.get("cache_read_input_tokens", 0)
            cost_breakdown["cache_creation"] += cost_details.get("cache_creation_input_tokens", 0)

        usage_details = {
            "input": usage["input"], "output": usage["output"], "total": usage["total"],
            "cache_read": usage["cache_read"],
            "cache_create": usage["cache_creation"],
            "cache_5m": turn.get("cache_ephemeral_5m", 0),
            "cache_1h": turn.get("cache_ephemeral_1h", 0),
        }

        sa_metadata = {
            "subagent_id": agent_id,
            "agent_id": agent_id,
            "subagent_correlation": correlation,
            "tools_used": [tc["name"] for tc in turn["tool_calls"]],
            "tool_count": len(turn["tool_calls"]),
            "speed": turn.get("speed", ""),
            "service_tier": turn.get("service_tier", ""),
            "inference_geo": turn.get("inference_geo", ""),
            "request_ids": list(dict.fromkeys(turn.get("request_ids", []))),
            "web_search_requests": turn.get("web_search_requests", 0),
            "web_fetch_requests": turn.get("web_fetch_requests", 0),
            **_otel_genai_attrs(
                session_id=session_id,
                model=model,
                usage=usage,
                request_ids=turn.get("request_ids", []),
                stop_reason=turn.get("stop_reason", ""),
            ),
        }

        events.append({
            "id": f"evt-{gen_id}",
            "timestamp": start_time or now,
            "type": "generation-create",
            "body": {
                "id": gen_id,
                "traceId": trace_id,
                "parentObservationId": parent_span_id,
                "name": f"Subagent Turn {turn_idx + 1}: {redact_secrets(truncate(turn['user_input'], 80))}",
                "model": model,
                "input": redact_secrets(truncate(turn["user_input"], MAX_TEXT)) or None,
                "output": redact_secrets(truncate(turn["assistant_output"], MAX_TEXT)) or None,
                "startTime": start_time,
                "endTime": end_time,
                "usageDetails": usage_details,
                "costDetails": cost_details if cost_details else None,
                "metadata": sa_metadata,
            },
        })

        for tc_idx, tc in enumerate(turn["tool_calls"]):
            span_id = f"span-subagent-{agent_id}-{prior_turn_count + turn_idx}-{tc_idx}"
            tool_input = tc["input"]
            tool_input_str = json.dumps(tool_input) if not isinstance(tool_input, str) else tool_input
            tool_input_str = redact_secrets(tool_input_str)
            if len(tool_input_str) > MAX_TOOL_IO:
                tool_input = {"_truncated": True, "preview": tool_input_str[:MAX_TOOL_IO]}
            else:
                try:
                    tool_input = json.loads(tool_input_str)
                except (json.JSONDecodeError, TypeError):
                    tool_input = tool_input_str
            tool_output = redact_secrets(tc["output"])
            if len(tool_output) > MAX_TOOL_IO:
                tool_output = tool_output[:MAX_TOOL_IO] + "...[truncated]"
            span_start = tc.get("start_time", start_time)
            span_end = tc.get("end_time", span_start)

            events.append({
                "id": f"evt-{span_id}",
                "timestamp": span_start or now,
                "type": "span-create",
                "body": {
                    "id": span_id,
                    "traceId": trace_id,
                    "parentObservationId": gen_id,
                    "name": tc["name"],
                    "startTime": span_start,
                    "endTime": span_end,
                    "input": tool_input,
                    "output": tool_output or None,
                    "metadata": {"tool_use_id": tc["id"], "subagent_id": agent_id},
                },
            })

    new_turn_count = prior_turn_count + len(turns)
    cost_summary = {
        "agent_id": agent_id,
        "total_cost": round(total_cost, 6),
        "total_tokens": total_tokens,
        "turns": new_turn_count,
        "status": status,
        "cost_breakdown": {k: round(v, 6) for k, v in cost_breakdown.items()},
    }
    return events, cost_summary, total_lines, new_turn_count, status


def extract_slug(transcript_path: str) -> str:
    """Scan the transcript for the first non-empty slug field.

    Legacy: removed from Claude Code v2.1.112+ transcripts. Kept for back-compat
    with older transcripts processed via --reprocess.
    """
    try:
        with open(transcript_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    slug = entry.get("slug", "")
                    if slug:
                        return slug
                except json.JSONDecodeError:
                    continue
    except (IOError, OSError):
        # Can't open or read the transcript file
        pass
    return ""



def extract_custom_title(transcript_path: str) -> str:
    """First non-empty customTitle entry (v2.1.110+ transcripts).

    Set by the user via the in-CLI title command. Absent in most sessions.
    """
    for entry in iter_transcript(transcript_path):
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
    for entry in iter_transcript(transcript_path):
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
    for entry in iter_transcript(transcript_path):
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


_AGENT_ID_RE = re.compile(r"agentId:\s*([0-9a-f]+)")


def extract_agent_id_from_result(tool_output) -> str | None:
    """Extract the subagent agentId from an Agent tool_result.

    Both synchronous results ("agentId: <id> (use SendMessage ...)") and
    asynchronous ("Async agent launched successfully.\\nagentId: <id>") embed
    the id as a literal `agentId: <hex>` token. Returns the id, or None when
    the output is empty or carries no agentId.
    """
    if not tool_output:
        return None
    m = _AGENT_ID_RE.search(tool_output)
    return m.group(1) if m else None


def extract_ai_title(transcript_path: str) -> str:
    """First non-empty aiTitle from type: 'ai-title' entries.

    Claude Code generates a short human-readable title once it has enough
    context to summarise the session (typically several turns in). The same
    title is written on every subsequent turn; first non-empty wins.
    Absent in sessions that ended before a title was produced.
    """
    for entry in iter_transcript(transcript_path):
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
    for entry in iter_transcript(transcript_path):
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
    for entry in iter_transcript(transcript_path):
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
    for entry in iter_transcript(transcript_path):
        if entry.get("type") != "attachment":
            continue
        total += 1
        att = entry.get("attachment", {}) or {}
        att_type = att.get("type", "unknown") if isinstance(att, dict) else "unknown"
        by_type[att_type] = by_type.get(att_type, 0) + 1
    if total == 0:
        return {}
    return {"count": total, "by_type": by_type}


def extract_file_history_stats(transcript_path: str) -> dict:
    """Aggregate stats from file-history-snapshot entries.

    Returns snapshot_count (total entries) and tracked_files_count
    (unique file paths across all trackedFileBackups dicts).
    File paths themselves are not captured to avoid leaking sensitive names.
    """
    snapshot_count = 0
    all_paths: set[str] = set()
    for entry in iter_transcript(transcript_path):
        if entry.get("type") != "file-history-snapshot":
            continue
        snapshot_count += 1
        backups = entry.get("snapshot", {}).get("trackedFileBackups", {})
        if isinstance(backups, dict):
            all_paths.update(backups.keys())
    return {"snapshot_count": snapshot_count, "tracked_files_count": len(all_paths)}


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

    for entry in iter_transcript(transcript_path):
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
    for entry in iter_transcript(transcript_path):
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
    for entry in iter_transcript(transcript_path):
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
    for entry in iter_transcript(transcript_path):
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
    for entry in iter_transcript(transcript_path):
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

    for entry in iter_transcript(transcript_path):
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
    for entry in iter_transcript(transcript_path):
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


def parse_transcript(transcript_path: str, skip_lines: int = 0) -> tuple[list[dict], int, bool]:
    """Parse a JSONL transcript starting after `skip_lines`.

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



def extract_tool_results(content) -> dict[str, str]:
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
            result_text = f"[ERROR] {result_text}"
        results[tool_use_id] = result_text
    return results


def parse_ts(ts_str: str) -> datetime | None:
    """Parse ISO timestamp string to datetime."""
    if not ts_str:
        return None
    try:
        # Handle various ISO formats
        ts_str = ts_str.replace("Z", "+00:00")
        return datetime.fromisoformat(ts_str)
    except ValueError:
        # Invalid ISO format timestamp string
        return None


def build_turns(entries: list[dict]) -> list[dict]:
    """Group transcript entries into turns with timing and token data.

    Returns list of turn dicts with keys:
      user_input, assistant_output, tool_calls, start_time, end_time,
      first_token_time, duration_ms, usage, model
    """
    msg_entries = []
    turn_durations = []

    for entry in entries:
        etype = entry.get("type")
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
                "diagnostics": msg.get("diagnostics", {}) if etype == "assistant" else {},
                "attribution_skill": entry.get("attributionSkill", "") if etype == "assistant" else "",
                "attribution_plugin": entry.get("attributionPlugin", "") if etype == "assistant" else "",
            })
        elif etype == "system" and entry.get("subtype") == "turn_duration":
            turn_durations.append(entry)

    # Build tool_result lookup
    tool_results = {}
    # Also track tool_result timestamps for span endTime
    tool_result_timestamps = {}
    for me in msg_entries:
        if me["role"] == "user":
            results = extract_tool_results(me["content"])
            tool_results.update(results)
            for tool_use_id in results:
                tool_result_timestamps[tool_use_id] = me["timestamp"]

    # Deduplicate assistant messages: multiple entries share same message_id (streaming).
    # For usage, take the LAST entry per message_id (has final accumulated tokens).
    # The first entry with a message_id gives us the first-token time.
    msg_id_first_ts = {}  # message_id -> first timestamp seen
    msg_id_final_usage = {}  # message_id -> final usage dict
    msg_id_diagnostics = {}  # message_id -> diagnostics dict

    for me in msg_entries:
        if me["role"] != "assistant" or not me["message_id"]:
            continue
        mid = me["message_id"]
        if mid not in msg_id_first_ts:
            msg_id_first_ts[mid] = me["timestamp"]
        # All entries for a given message_id carry identical usage since v2.1.97; first is fine
        if me["usage"] and mid not in msg_id_final_usage:
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
            has_tool_results = bool(extract_tool_results(me["content"]))

            if text.strip() and not (has_tool_results and not text.replace(" ", "").strip()):
                if current_turn:
                    turns.append(current_turn)
                current_turn = {
                    "user_input": text,
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
                    "attribution_skill": "",
                    "attribution_plugin": "",
                    "attribution_skills_all": set(),
                    "attribution_plugins_all": set(),
                }
            elif current_turn:
                current_turn["messages"].append(me)

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

            current_turn["end_time"] = me["timestamp"]
            current_turn["api_call_ids"].add(mid)

            # Collect text output (keep last non-empty)
            text = extract_text_blocks(content)
            if text.strip():
                current_turn["assistant_output"] = text

            # Collect tool uses — skip duplicates (same tool_use_id in multiple streaming entries)
            for tu in extract_tool_uses(content):
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

    # Aggregate usage per turn from deduplicated API calls
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
        cm_missed_tokens = 0
        cm_by_reason = {}
        for mid in turn["api_call_ids"]:
            u = msg_id_final_usage.get(mid, {})
            inp = u.get("input_tokens", 0)
            out = u.get("output_tokens", 0)
            usage["input"] += inp
            usage["output"] += out
            usage["total"] += inp + out
            usage["cache_read"] += u.get("cache_read_input_tokens", 0)
            usage["cache_creation"] += u.get("cache_creation_input_tokens", 0)
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
        turn["speed"] = speed
        turn["service_tier"] = service_tier
        turn["inference_geo"] = inference_geo
        turn["web_search_requests"] = web_search_requests
        turn["web_fetch_requests"] = web_fetch_requests
        turn["cache_ephemeral_5m"] = cache_ephemeral_5m
        turn["cache_ephemeral_1h"] = cache_ephemeral_1h
        turn["iteration_count"] = iteration_count
        turn["cache_miss"] = (
            {"missed_tokens": cm_missed_tokens, "by_reason": cm_by_reason}
            if cm_by_reason else None
        )
        turn["request_ids"] = request_ids
        turn["api_call_ids"] = list(turn["api_call_ids"])  # make serializable
        turn["attribution_skills_all"] = sorted(turn["attribution_skills_all"])
        turn["attribution_plugins_all"] = sorted(turn["attribution_plugins_all"])

    # Match turn_duration entries to turns (by proximity of timestamps)
    for td in turn_durations:
        td_ts = parse_ts(td.get("timestamp", ""))
        if not td_ts:
            continue
        duration_ms = td.get("durationMs", 0)
        # Find the closest turn that ends near this turn_duration timestamp
        best_turn = None
        best_diff = None
        for turn in turns:
            turn_end = parse_ts(turn["end_time"])
            if not turn_end:
                continue
            diff = abs((td_ts - turn_end).total_seconds())
            if best_diff is None or diff < best_diff:
                best_diff = diff
                best_turn = turn
        if best_turn and best_diff is not None and best_diff < 30:
            best_turn["duration_ms"] = duration_ms

    return turns


def extract_session_metadata(transcript_path: str) -> dict:
    """Extract session-level metadata from the first user entry in the transcript."""
    fields = {"cwd": "", "gitBranch": "", "version": "", "entrypoint": ""}
    try:
        with open(transcript_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    if entry.get("type") == "user":
                        for key in fields:
                            val = entry.get(key, "")
                            if val:
                                fields[key] = str(val)
                        break
                except json.JSONDecodeError:
                    continue
    except (IOError, OSError):
        # Can't open or read the transcript file
        pass
    return fields


def extract_api_errors(entries: list[dict]) -> dict:
    """Aggregate api_error system entries into a summary."""
    total_count = 0
    by_status: dict[str, int] = {}
    first_error_at = ""
    last_error_at = ""

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

    return {
        "total_count": total_count,
        "by_status": by_status,
        "first_error_at": first_error_at,
        "last_error_at": last_error_at,
    }


def extract_cwd(transcript_path: str) -> str:
    """Scan the transcript for the first non-empty cwd field."""
    try:
        with open(transcript_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    cwd = entry.get("cwd", "")
                    if cwd:
                        return cwd
                except json.JSONDecodeError:
                    continue
    except (IOError, OSError):
        # Can't open or read the transcript file
        pass
    return ""


# Fast mode (research preview). Premium over base rates, per Opus generation:
# Opus 4.6/4.7 = 6x ($30/$150); Opus 4.8 = 2x ($10/$50). Multiplier applies
# uniformly to input, output, cache read, and both cache-write tiers.
# Source: platform.claude.com/docs/en/about-claude/pricing#fast-mode-pricing (verified 2026-06-01)
FAST_MODE_MULTIPLIERS = {
    "opus-4-6": 6.0,
    "opus-4-7": 6.0,
    "opus-4-8": 2.0,
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


def calculate_turn_cost(
    usage: dict,
    model: str,
    cache_5m: int = 0,
    cache_1h: int = 0,
    speed: str = "",
    inference_geo: str = "",
    web_search_requests: int = 0,
) -> tuple:
    """Calculate cost for a turn's token usage.

    cache_5m / cache_1h: per-tier cache creation token counts (from usageDetails).
    When provided, costs are split by tier; otherwise all cache_creation billed at 5m rate.

    speed: "fast" applies a per-model premium (Opus 4.6/4.7 = 6x, Opus 4.8 = 2x).
    inference_geo: "us" applies 1.1x multiplier on Opus 4.6+/Sonnet 4.6+.
    web_search_requests: server-side web search calls, billed at $10/1000.

    Returns: (turn_cost, input_cost, output_cost, cost_details)
    """
    if not REPORT_API_EQUIVALENT_COST:
        return 0.0, 0.0, 0.0, {}

    m = model.lower()
    if not m:
        # No model on the turn. Do NOT fabricate a default — report $0 so the
        # dashboard shows an obvious gap. Warn only when tokens are actually
        # billable; zero-usage stub turns ($0 regardless) stay silent.
        if _has_billable_tokens(usage):
            log("[WARN] calculate_turn_cost: turn has no model field but carries "
                "billable tokens — cost reported as $0. Upstream transcript is missing "
                "the assistant 'model' value; FIX the source.")
        return 0.0, 0.0, 0.0, {}
    # Pricing per 1M tokens: (input, output, cache_read, cache_write_5m, cache_write_1h)
    # Source: platform.claude.com/docs/en/about-claude/pricing (verified 2026-05-13)
    # When a new model releases, its name will fall through to Sonnet pricing and log a warning.
    # Update this function + CLAUDE.md Cost Model table when that happens.
    supports_inference_geo = False  # Only Opus 4.6+/Sonnet 4.6+ accept inference_geo
    supports_fast_mode = False      # Only Opus 4.6/4.7/4.8 support /fast
    if "haiku" in m:
        if "haiku-4" in m:                          # Haiku 4.5+
            p_in, p_out, p_cr, p_cc5, p_cc1 = 1.00, 5.00, 0.10, 1.25, 2.00
        elif "3-5" in m:                             # Haiku 3.5
            p_in, p_out, p_cr, p_cc5, p_cc1 = 0.80, 4.00, 0.08, 1.00, 1.60
        else:                                        # Haiku 3
            p_in, p_out, p_cr, p_cc5, p_cc1 = 0.25, 1.25, 0.03, 0.30, 0.50
    elif "opus" in m:
        # Whitelist of current Opus generations sharing the $5/$25 schedule.
        # Substring matching previously over-billed any future "opus-4-9+"
        # release at the legacy $15/$75 rate; whitelist forces the unknown-
        # model WARN log to surface so the table can be updated explicitly.
        if any(x in m for x in ("opus-4-5", "opus-4-6", "opus-4-7", "opus-4-8")):
            p_in, p_out, p_cr, p_cc5, p_cc1 = 5.0, 25.0, 0.50, 6.25, 10.0
            if any(x in m for x in ("opus-4-6", "opus-4-7", "opus-4-8")):
                supports_inference_geo = True
                supports_fast_mode = True
        elif any(x in m for x in ("opus-4-1", "opus-4-20", "3-opus")):
            # Opus 4.1, 4.0 (claude-opus-4-2025*), 3 (claude-3-opus-*) — legacy whitelist
            p_in, p_out, p_cr, p_cc5, p_cc1 = 15.0, 75.0, 1.50, 18.75, 30.0
        else:
            log(f"[WARN] calculate_turn_cost: unrecognised Opus variant '{model}' — cost reported as $0. "
                "Update calculate_turn_cost() and CLAUDE.md if this is a new Anthropic model.")
            return 0.0, 0.0, 0.0, {}
    elif "sonnet" in m:                              # Sonnet (all versions)
        p_in, p_out, p_cr, p_cc5, p_cc1 = 3.0, 15.0, 0.30, 3.75, 6.00
        if "sonnet-4-6" in m:
            supports_inference_geo = True
    elif "fable" in m:                               # Fable 5 (top tier, $10/$50)
        # supports_fast_mode / supports_inference_geo stay False: Fable has no
        # /fast variant, and its data-residency multiplier is unverified.
        p_in, p_out, p_cr, p_cc5, p_cc1 = 10.00, 50.00, 1.00, 12.50, 20.00
    else:
        log(f"[WARN] calculate_turn_cost: unrecognised model '{model}' — cost reported as $0. "
            "Update calculate_turn_cost() and CLAUDE.md if this is a new Anthropic model.")
        return 0.0, 0.0, 0.0, {}

    # Fast mode premium (per-model: Opus 4.6/4.7 = 6x, Opus 4.8 = 2x).
    # Multipliers stack on top per spec. supports_fast_mode guarantees a key match.
    if speed == "fast" and supports_fast_mode:
        fast_mult = next((v for k, v in FAST_MODE_MULTIPLIERS.items() if k in m), 1.0)
        p_in *= fast_mult
        p_out *= fast_mult
        p_cr *= fast_mult
        p_cc5 *= fast_mult
        p_cc1 *= fast_mult

    # Data residency 1.1x (Opus 4.6+/Sonnet 4.6+). Stacks on top of fast mode.
    if inference_geo.lower() == "us" and supports_inference_geo:
        p_in *= US_GEO_MULTIPLIER
        p_out *= US_GEO_MULTIPLIER
        p_cr *= US_GEO_MULTIPLIER
        p_cc5 *= US_GEO_MULTIPLIER
        p_cc1 *= US_GEO_MULTIPLIER

    # Cache creation cost: use per-tier breakdown when available
    if cache_5m or cache_1h:
        cc_cost = (cache_5m * p_cc5 + cache_1h * p_cc1) / 1_000_000
    else:
        cc_cost = usage["cache_creation"] * p_cc5 / 1_000_000

    input_token_cost = usage["input"] * p_in / 1_000_000
    cache_read_cost = usage["cache_read"] * p_cr / 1_000_000
    web_search_cost = web_search_requests * WEB_SEARCH_COST_PER_REQUEST
    input_cost = input_token_cost + cache_read_cost + cc_cost
    output_cost = usage["output"] * p_out / 1_000_000
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
    """Fraction of tool calls whose output is an [ERROR].

    Returns None when there are no tool calls at all (omit the score, same
    pattern as compute_cache_hit_rate) so an error-free session stays distinct
    from a session that ran no tools. The "[ERROR]" prefix is applied upstream
    in extract_tool_results.
    """
    total = 0
    errors = 0
    for t in turns:
        for tc in t.get("tool_calls", []):
            total += 1
            if str(tc.get("output", "")).startswith("[ERROR]"):
                errors += 1
    if total == 0:
        return None
    return round(errors / total, 4)


def detect_compaction(transcript_path: str) -> bool:
    """Return True if the transcript contains a compaction event.

    Matches: type=="summary" OR (type=="system" AND "compact" in subtype.lower()).
    Returns False when transcript_path is empty or the file does not exist.
    """
    for entry in iter_transcript(transcript_path):
        etype = entry.get("type", "")
        subtype = entry.get("subtype", "")
        if etype == "summary":
            return True
        if etype == "system" and "compact" in subtype.lower():
            return True
    return False


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
        turn_cost, _i, _o, _cd = calculate_turn_cost(
            usage,
            t.get("model") or "",
            t.get("cache_ephemeral_5m", 0),
            t.get("cache_ephemeral_1h", 0),
            speed=t.get("speed", ""),
            inference_geo=t.get("inference_geo", ""),
            web_search_requests=t.get("web_search_requests", 0),
        )
        b["cost_usd"] = round(b["cost_usd"] + turn_cost, 6)

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


def build_hook_score_events(
    trace_id: str,
    session_id: str,
    first_user_input: str,
    turns: list[dict],
    total_cost: float,
    transcript_path: str = "",
    last_assistant_message: str = "",
) -> list[dict]:
    """Build score-create events for the ingestion batch.

    Emits up to two NUMERIC scores: cache_hit_rate and tool_error_rate. Each is
    omitted when its classifier returns None (no cache activity / no tool calls)
    so "absent" stays distinct from a genuine 0.0. Unused params are kept for
    caller compatibility.
    """
    now = datetime.now(timezone.utc).isoformat()

    cache_hit = compute_cache_hit_rate(turns)
    tool_err = calculate_tool_error_rate(turns)

    scores = []
    if cache_hit is not None:
        scores.append({"name": "cache_hit_rate", "dataType": "NUMERIC", "value": cache_hit})
    if tool_err is not None:
        scores.append({"name": "tool_error_rate", "dataType": "NUMERIC", "value": tool_err})

    events = []
    for score in scores:
        # Stable ID keyed only on session+name so each run upserts the same
        # score rather than accumulating one per turn.
        score_id = str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"{session_id}:score:{score['name']}",
        ))
        events.append({
            "id": f"evt-{score_id}",
            "timestamp": now,
            "type": "score-create",
            "body": {
                "id": f"score-{score_id}",
                "traceId": trace_id,
                "name": score["name"],
                "dataType": score["dataType"],
                "value": score["value"],
                "source": "API",
            },
        })

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
    return out


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


def process_session(session_id: str, transcript_path: str, cwd: str, last_assistant_message: str = "", live: bool = True, background_tasks: list = None, session_crons: list = None) -> None:
    """Core processing logic for a single session transcript."""
    # effort comes from the live hook process environment ($CLAUDE_EFFORT,
    # v2.1.133+). It is NOT in the transcript, so it cannot be recovered on
    # reprocess; omit it then so Langfuse's upsert preserves any prior value.
    effort = os.environ.get("CLAUDE_EFFORT", "").strip() if live else ""
    prev_line_offset, prev_turn_count = load_state(session_id)
    custom_title = extract_custom_title(transcript_path)
    permission_mode = extract_permission_mode(transcript_path)
    permission_timeline = extract_permission_timeline(transcript_path)
    compaction = extract_compaction(transcript_path)
    remote_control = extract_bridge(transcript_path)
    pr_links = extract_pr_links(transcript_path)
    away_summaries = extract_away_summaries(transcript_path)
    agent_name = extract_agent_name(transcript_path)
    ai_title = extract_ai_title(transcript_path)
    session_kind = extract_session_kind(transcript_path)
    attachments = extract_attachments(transcript_path)
    local_commands = extract_local_commands(transcript_path)
    file_history_stats = extract_file_history_stats(transcript_path)
    stop_hook_stats = extract_stop_hook_stats(transcript_path)
    worktree_state = extract_worktree_state(transcript_path)
    entries, total_lines, read_ok = parse_transcript(transcript_path, skip_lines=prev_line_offset)

    if not read_ok:
        # Transient read failure (file moved, permission denied, etc.).
        # Do NOT call save_state — that would clobber the existing offset
        # with 0 and force a full re-ingest (potentially racing with the
        # async delete pipeline). Bail and retry on the next fire.
        log(f"Skipping state save for {session_id} due to transcript read failure")
        return

    if not entries:
        log(f"No new entries for session {session_id} (offset {prev_line_offset}, total {total_lines})")
        save_state(session_id, total_lines, prev_turn_count)
        return

    now = datetime.now(timezone.utc).isoformat()
    trace_id = f"trace-{session_id}"
    batch = []

    session_meta = extract_session_metadata(transcript_path)

    turns = build_turns(entries)
    if not turns:
        log(f"No turns found in {len(entries)} new entries")
        save_state(session_id, total_lines, prev_turn_count)
        return

    api_errors = extract_api_errors(entries)
    has_errors = api_errors["total_count"] > 0

    # Derive repo name from cwd
    repo_name = os.path.basename(cwd.rstrip("/")) if cwd else ""

    # Check if any turn used fast inference
    has_fast = any(t.get("speed") == "fast" for t in turns)

    # Collect unique model families used across all turns
    model_families = set()
    for t in turns:
        m = (t.get("model") or "").lower()
        if "haiku" in m:
            model_families.add("haiku")
        elif "opus" in m:
            model_families.add("opus")
        elif m and m != "<synthetic>":
            model_families.add("sonnet")

    # A turn with billable tokens but no model would otherwise be silently
    # mispriced (was: defaulted to Opus). Surface it as a filterable trace tag.
    has_missing_model = any(
        not (t.get("model") or "") and _has_billable_tokens(t.get("usage") or {})
        for t in turns
    )

    # Aggregate totals for the trace
    total_tokens = sum(t["usage"]["total"] for t in turns)
    total_input = sum(t["usage"]["input"] for t in turns)
    total_output = sum(t["usage"]["output"] for t in turns)
    total_tool_calls = sum(len(t["tool_calls"]) for t in turns)
    skill_attribution = build_skill_attribution_summary(turns)
    attribution_tags = build_attribution_tags(skill_attribution)
    total_cost = 0.0
    subagent_cost_summaries = []
    sa_state = load_subagent_state(session_id)

    first_user_input = turns[0]["user_input"] if turns else ""
    last_assistant_output = last_assistant_message
    if not last_assistant_output:
        for t in reversed(turns):
            if t["assistant_output"]:
                last_assistant_output = t["assistant_output"]
                break

    # For trace naming, skip synthetic stubs (slash commands, local-command
    # output, system reminders, prompt-submit hooks) which all start with an
    # XML-like wrapper tag. Real user prompts are plain text. Falls back to
    # repo/branch when every turn is a stub.
    first_real_prompt = ""
    for t in turns:
        ui = (t.get("user_input") or "").strip()
        if ui and not _SYNTHETIC_PROMPT_RE.match(ui):
            first_real_prompt = ui
            break

    trace_ts = turns[0].get("start_time") or now

    # Trace name precedence: custom title > ai title > agent name > first prompt > repo/branch
    trace_name = (
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

    # 1. Trace
    batch.append({
        "id": f"evt-{uuid.uuid4()}-trace",
        "timestamp": trace_ts,
        "type": "trace-create",
        "body": {
            "id": trace_id,
            "timestamp": trace_ts,
            "name": trace_name,
            "sessionId": session_id,
            "userId": os.environ.get("USER", "unknown"),
            "input": redact_secrets(truncate(first_user_input, MAX_TEXT)) or None,
            "output": redact_secrets(truncate(last_assistant_output, MAX_TEXT)) or None,
            "metadata": {
                "cwd": cwd,
                "turn_count": len(turns),
                "tool_calls_total": total_tool_calls,
                "total_tokens": total_tokens,
                "total_input_tokens": total_input,
                "total_output_tokens": total_output,
                "git_branch": session_meta.get("gitBranch", ""),
                "cli_version": session_meta.get("version", ""),
                "entrypoint": session_meta.get("entrypoint", ""),
                "repo_name": repo_name,
                "api_errors": api_errors if has_errors else None,
                "custom_title": custom_title or None,
                "permission_mode": permission_mode or None,
                "pr_links": pr_links or None,
                "away_summaries": away_summaries or None,
                "agent_name": agent_name or None,
                "ai_title": ai_title or None,
                "session_kind": session_kind or None,
                "attachments": attachments or None,
                "local_commands": local_commands or None,
                "file_snapshots": file_history_stats if file_history_stats["snapshot_count"] > 0 else None,
                "stop_hook": stop_hook_stats if stop_hook_stats["total_hook_fires"] > 0 else None,
                "skill_attribution": skill_attribution,
                "compaction": compaction,
                "compaction_occurred": detect_compaction(transcript_path),
                "remote_control": remote_control,
                "permission_timeline": permission_timeline,
                "total_iterations": sum(t.get("iteration_count", 0) for t in turns),
                "cache_miss": build_cache_miss_summary(turns),
                "effort_level": effort or None,
                "worktree": worktree_state,
                "background_tasks": background_tasks or None,
                "session_crons": session_crons or None,
            },
            "tags": [t for t in [
                "claude-code",
                repo_name or None,
                *sorted(model_families),
                session_meta.get("entrypoint") or None,
                "fast" if has_fast else None,
                "has-errors" if has_errors else None,
                "has-background-tasks" if background_tasks else None,
                "model-missing" if has_missing_model else None,
                f"permission:{permission_mode}" if permission_mode else None,
                f"agent-name:{agent_name}" if agent_name else None,
                f"session-kind:{session_kind}" if session_kind else None,
                f"worktree:{worktree_state['name']}" if worktree_state and worktree_state.get("name") else None,
                f"effort:{effort}" if effort else None,
                "compacted" if detect_compaction(transcript_path) else None,
                "remote-control" if remote_control else None,
                "permission-bypass" if (permission_timeline or {}).get("ever_bypass") else None,
                *compact_trigger_tags,
                *attribution_tags,
                *pr_tags,
            ] if t],
        },
    })

    # 2. Generation + spans per turn
    # Meta index rebuilt on every fire — subagent dirs appear mid-session.
    sa_search_dirs = _subagent_search_dirs(transcript_path, cwd)
    sa_meta_index = build_subagent_meta_index(sa_search_dirs)
    for turn_idx, turn in enumerate(turns):
        # Deterministic IDs so re-ingestion updates rather than duplicates
        turn_id = uuid.uuid5(uuid.NAMESPACE_URL, f"{session_id}:turn:{prev_turn_count + turn_idx}")
        gen_id = f"gen-{turn_id}"

        start_time = turn["start_time"]
        end_time = turn["end_time"]
        first_token_time = turn.get("first_token_time")
        duration_ms = turn.get("duration_ms")
        usage = turn["usage"]
        model = turn.get("model") or ""

        tool_names = [tc["name"] for tc in turn["tool_calls"]]

        # Compute TTFT in ms
        ttft_ms = None
        if first_token_time and start_time:
            st = parse_ts(start_time)
            ft = parse_ts(first_token_time)
            if st and ft and ft > st:
                ttft_ms = round((ft - st).total_seconds() * 1000)

        # If we have duration_ms from turn_duration, compute a proper endTime
        if duration_ms and start_time:
            st = parse_ts(start_time)
            if st:
                from datetime import timedelta
                computed_end = st + timedelta(milliseconds=duration_ms)
                end_time = computed_end.isoformat()

        # Cost: $0 for Pro subscription
        turn_cost, input_cost, output_cost, cost_details = calculate_turn_cost(
            usage,
            model,
            turn.get("cache_ephemeral_5m", 0),
            turn.get("cache_ephemeral_1h", 0),
            speed=turn.get("speed", ""),
            inference_geo=turn.get("inference_geo", ""),
            web_search_requests=turn.get("web_search_requests", 0),
        )
        total_cost += turn_cost

        usage_details = {
            "input": usage["input"],
            "output": usage["output"],
            "total": usage["total"],
            "cache_read": usage["cache_read"],
            "cache_create": usage["cache_creation"],
            "cache_5m": turn.get("cache_ephemeral_5m", 0),
            "cache_1h": turn.get("cache_ephemeral_1h", 0),
        }

        gen_body = {
            "id": gen_id,
            "traceId": trace_id,
            "name": f"Turn {prev_turn_count + turn_idx + 1}: {redact_secrets(truncate(turn['user_input'], 80))}",
            "model": model,
            "input": redact_secrets(truncate(turn["user_input"], MAX_TEXT)) or None,
            "output": redact_secrets(truncate(turn["assistant_output"], MAX_TEXT)) or None,
            "startTime": start_time,
            "endTime": end_time,
            "completionStartTime": first_token_time,
            "usageDetails": usage_details,
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
                # OpenTelemetry GenAI semantic-convention aliases. Zero behaviour
                # change for Langfuse, but lets an OTLP collector (or future
                # Langfuse mapping) consume the trace without a transform step.
                # See: opentelemetry.io/docs/specs/semconv/gen-ai/
                **_otel_genai_attrs(
                    session_id=session_id,
                    model=model,
                    usage=usage,
                    request_ids=turn.get("request_ids", []),
                    stop_reason=turn.get("stop_reason", ""),
                ),
                **gen_metadata_attribution(turn),
                **gen_metadata_cache_miss(turn),
            },
        }

        if ttft_ms is not None:
            gen_body["metadata"]["ttft_ms"] = ttft_ms
        if duration_ms:
            gen_body["metadata"]["duration_ms"] = duration_ms

        batch.append({
            "id": f"evt-{turn_id}-gen",
            "timestamp": start_time or now,
            "type": "generation-create",
            "body": gen_body,
        })

        # 3. Span per tool call
        agent_tool_uses_this_turn = []
        for tc_idx, tc in enumerate(turn["tool_calls"]):
            span_id = f"span-{turn_id}-{tc_idx}"

            tool_input = tc["input"]
            tool_input_str = json.dumps(tool_input) if not isinstance(tool_input, str) else tool_input
            tool_input_str = redact_secrets(tool_input_str)
            if len(tool_input_str) > MAX_TOOL_IO:
                tool_input = {"_truncated": True, "preview": tool_input_str[:MAX_TOOL_IO]}
            else:
                try:
                    tool_input = json.loads(tool_input_str)
                except (json.JSONDecodeError, TypeError):
                    tool_input = tool_input_str

            tool_output = redact_secrets(tc["output"])
            if len(tool_output) > MAX_TOOL_IO:
                tool_output = tool_output[:MAX_TOOL_IO] + "...[truncated]"

            span_start = tc.get("start_time", start_time)
            span_end = tc.get("end_time", span_start)

            batch.append({
                "id": f"evt-{turn_id}-span-{tc_idx}",
                "timestamp": span_start or now,
                "type": "span-create",
                "body": {
                    "id": span_id,
                    "traceId": trace_id,
                    "parentObservationId": gen_id,
                    "name": tc["name"],
                    "startTime": span_start,
                    "endTime": span_end,
                    "input": tool_input,
                    "output": tool_output or None,
                    "metadata": {
                        "tool_use_id": tc["id"],
                    },
                },
            })

            # Track Agent tool_uses for subagent correlation
            if tc["name"] == "Agent":
                tc_input = tc.get("input", {})
                if isinstance(tc_input, dict):
                    agent_tool_uses_this_turn.append((
                        tc_input.get("description", "unknown"),
                        tc_input.get("subagent_type", "unknown"),
                        tc.get("start_time", start_time),
                        tc_idx,
                        extract_agent_id_from_result(tc.get("output", "")),
                        tc["id"],
                    ))

        # Discover and ingest subagents for this turn
        if agent_tool_uses_this_turn:
            matches = discover_subagents(transcript_path, agent_tool_uses_this_turn,
                                         cwd=cwd, meta_index=sa_meta_index)
            for sa_id, sa_path, sa_desc, sa_type, sa_tc_idx, sa_corr in matches:
                sa_prev_offset = sa_state.get(sa_id, {}).get("offset", 0)
                sa_prior_turns = sa_state.get(sa_id, {}).get("turn_count", 0)
                sa_parent_span = f"span-{turn_id}-{sa_tc_idx}"

                sa_events, sa_cost, sa_new_offset, sa_new_tc, sa_status = ingest_subagent(
                    agent_id=sa_id,
                    transcript_path=sa_path,
                    parent_span_id=sa_parent_span,
                    trace_id=trace_id,
                    session_id=session_id,
                    subagent_offset=sa_prev_offset,
                    prior_turn_count=sa_prior_turns,
                    correlation=sa_corr,
                )
                batch.extend(sa_events)
                _prev = sa_state.get(sa_id, {})
                sa_state[sa_id] = {
                    "offset": sa_new_offset,
                    "turn_count": sa_new_tc,
                    "status": sa_status,
                    "total_cost": round(_prev.get("total_cost", 0.0) + sa_cost["total_cost"], 6),
                    "total_tokens": _prev.get("total_tokens", 0) + sa_cost["total_tokens"],
                    "cost_breakdown": {
                        k: round(_prev.get("cost_breakdown", {}).get(k, 0.0) + v, 6)
                        for k, v in sa_cost["cost_breakdown"].items()
                    },
                    "description": sa_desc,
                    "subagent_type": sa_type,
                }

                # Enrich Agent tool span metadata
                for evt in batch:
                    if evt["type"] == "span-create" and evt["body"]["id"] == sa_parent_span:
                        evt["body"].setdefault("metadata", {}).update({
                            "subagent_id": sa_id,
                            "subagent_type": sa_type,
                            "subagent_description": sa_desc,
                            "subagent_cost": sa_state[sa_id]["total_cost"],
                            "subagent_tokens": sa_state[sa_id]["total_tokens"],
                            "subagent_status": sa_status,
                        })
                        break

    # Rebuild subagent_cost_summaries from all known agents (cumulative across fires)
    # and store cumulative parent cost in sa_state for future fires.
    _known_agents = {k: v for k, v in sa_state.items()
                     if k != "_parent" and isinstance(v, dict)}
    if _known_agents:
        subagent_cost_summaries = [
            {"agent_id": aid, **state} for aid, state in _known_agents.items()
        ]
        cumulative_parent_cost = sa_state.get("_parent", {}).get("total_cost", 0.0) + total_cost
        sa_state["_parent"] = {"total_cost": round(cumulative_parent_cost, 6)}
    else:
        cumulative_parent_cost = total_cost

    # Add subagent cost summary to trace
    if subagent_cost_summaries:
        total_subagent_cost = sum(s["total_cost"] for s in subagent_cost_summaries)
        for evt in batch:
            if evt["type"] == "trace-create":
                evt["body"]["metadata"]["subagent_costs"] = {
                    "agents": subagent_cost_summaries,
                    "total_subagent_cost": round(total_subagent_cost, 6),
                    "parent_cost": round(cumulative_parent_cost, 6),
                    "harness_total_cost": round(cumulative_parent_cost + total_subagent_cost, 6),
                }
                evt["body"]["tags"].append("has-subagents")
                evt["body"]["tags"].append(f"subagents:{len(subagent_cost_summaries)}")
                break

    # Hook-level scores (trace-level)
    score_events = build_hook_score_events(
        trace_id=trace_id,
        session_id=session_id,
        first_user_input=first_user_input,
        turns=turns,
        total_cost=cumulative_parent_cost,
        transcript_path=transcript_path,
        last_assistant_message=last_assistant_message,
    )
    batch.extend(score_events)

    new_turn_count = prev_turn_count + len(turns)
    tool_span_count = sum(len(t["tool_calls"]) for t in turns)
    summary = (
        f"{len(batch)} events for session {session_id}: "
        f"{len(turns)} turns, {tool_span_count} tool spans, "
        f"{total_tokens} tokens (lines {prev_line_offset+1}-{total_lines})"
    )
    if send_to_langfuse(batch):
        save_state(session_id, total_lines, new_turn_count)
        save_subagent_state(session_id, sa_state)
        log(f"Sent {summary}")
    else:
        log(f"[WARN] Send failed — {summary}; state not advanced, will retry next fire")


def delete_trace(trace_id: str) -> None:
    """Delete a trace and all its observations from Langfuse."""
    url = f"{LANGFUSE_HOST}/api/public/traces/{trace_id}"
    req = Request(
        url,
        headers={"Authorization": make_auth_header()},
        method="DELETE",
    )
    try:
        with urlopen(req, timeout=15):
            pass
    except (URLError, TimeoutError, OSError):
        pass


def reprocess_all() -> None:
    """Find all transcript files and reprocess them from scratch."""
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

        # Reset state so process_session rebuilds the full turn list. We do NOT
        # call delete_trace here: Langfuse processes deletes asynchronously and
        # a cascade-delete arriving AFTER the new inserts wipes a subset of the
        # freshly ingested observations. Re-ingestion with deterministic
        # UUID5 event/observation IDs upserts existing rows cleanly.
        state_file = Path(STATE_DIR) / f"{session_id}.offset"
        if state_file.exists():
            state_file.unlink()
        sa_state_file = Path(STATE_DIR) / f"{session_id}.subagents.json"
        if sa_state_file.exists():
            sa_state_file.unlink()

        label = f"{session_id}"
        if slug:
            label += f" ({slug})"
        print(f"[{idx}/{total}] {label}")

        try:
            process_session(session_id, str(transcript), cwd, live=False)
        except (IOError, OSError, json.JSONDecodeError, ValueError) as e:
            # Broad exception from process_session (file I/O, JSON, parsing errors)
            print(f"  Error: {e}")
            log(f"Reprocess error for {session_id}: {e}")

    print(f"\nDone. Reprocessed {total} session(s).")


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--reprocess":
        reprocess_all()
        return

    try:
        hook_input = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        # Invalid JSON in stdin
        log(f"Failed to parse stdin: {e}")
        return

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
                    background_tasks=hook_input.get("background_tasks"),
                    session_crons=hook_input.get("session_crons"))


if __name__ == "__main__":
    main()
