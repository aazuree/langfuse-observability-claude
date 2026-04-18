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
  LANGFUSE_HOST        - Langfuse base URL (default: http://localhost:3000)
"""

import hashlib
import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator
from urllib.error import URLError
from urllib.request import Request, urlopen

# Ensure langfuse_common can be imported from the same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from langfuse_common import log as common_log, make_auth_header, redact_secrets

LANGFUSE_HOST = os.environ.get("LANGFUSE_HOST", "http://localhost:3000")
LANGFUSE_PUBLIC_KEY = os.environ.get("LANGFUSE_PUBLIC_KEY", "")
LANGFUSE_SECRET_KEY = os.environ.get("LANGFUSE_SECRET_KEY", "")
INGESTION_URL = f"{LANGFUSE_HOST}/api/public/ingestion"

LOG_FILE = os.path.expanduser("~/.claude/langfuse-hook.log")
STATE_DIR = os.path.expanduser("~/.claude/langfuse-state")

MAX_TEXT = 10000
MAX_TOOL_IO = 5000
MAX_LOG_BYTES = 10 * 1024 * 1024  # 10 MB

SAFE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")

# Synthetic user-message wrappers (slash commands, local-command output,
# system reminders, prompt-submit hooks). Used to skip these when picking
# a human-readable trace name from the first user prompt.
_SYNTHETIC_PROMPT_RE = re.compile(r"^<[a-z][a-z0-9-]*>")

SUBAGENT_MATCH_WINDOW_S = 60  # Max seconds between Agent tool_use and subagent start

# Cost tier thresholds (used for session classification in Langfuse dashboard)
COST_TIER_CHEAP_MAX = 0.10      # Sessions under $0.10
COST_TIER_MODERATE_MAX = 1.00   # Sessions $0.10-$1.00; above $1.00 = expensive

# Default model for cost calculation when model field is missing
DEFAULT_MODEL = "claude-opus-4-6"

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


def send_to_langfuse(batch: list[dict]) -> None:
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
        except URLError as e:
            log(f"Failed to send to Langfuse: {e}")


def load_state(session_id: str) -> int:
    Path(STATE_DIR).mkdir(parents=True, exist_ok=True)
    state_file = os.path.join(STATE_DIR, f"{session_id}.offset")
    try:
        return int(Path(state_file).read_text().strip())
    except Exception:
        return 0


def save_state(session_id: str, offset: int) -> None:
    Path(STATE_DIR).mkdir(parents=True, exist_ok=True)
    state_file = os.path.join(STATE_DIR, f"{session_id}.offset")
    Path(state_file).write_text(str(offset))


def load_subagent_state(session_id: str) -> dict:
    """Load subagent processing state. Returns {} if no state exists."""
    Path(STATE_DIR).mkdir(parents=True, exist_ok=True)
    state_file = os.path.join(STATE_DIR, f"{session_id}.subagents.json")
    try:
        return json.loads(Path(state_file).read_text())
    except Exception:
        return {}


def save_subagent_state(session_id: str, state: dict) -> None:
    """Save subagent processing state."""
    Path(STATE_DIR).mkdir(parents=True, exist_ok=True)
    state_file = os.path.join(STATE_DIR, f"{session_id}.subagents.json")
    Path(state_file).write_text(json.dumps(state))


def discover_subagents(
    transcript_path: str,
    agent_tool_uses: list,
) -> list:
    """Match Agent tool_uses to subagent transcripts by timestamp proximity.

    Args:
        transcript_path: Path to parent session JSONL (e.g., .../session.jsonl)
        agent_tool_uses: List of (description, subagent_type, timestamp, content_index)

    Returns:
        List of (agent_id, subagent_transcript_path, description, subagent_type, tool_use_content_index)
        Unmatched entries are omitted.
    """
    if not agent_tool_uses:
        return []

    # Derive subagents directory: <transcript without .jsonl>/subagents/
    base = transcript_path
    if base.endswith(".jsonl"):
        base = base[:-6]
    subagents_dir = os.path.join(base, "subagents")

    if not os.path.isdir(subagents_dir):
        return []

    # List candidate subagent JSONL files, excluding aside_question
    candidates = []
    for fname in os.listdir(subagents_dir):
        if not fname.startswith("agent-") or not fname.endswith(".jsonl"):
            continue
        if fname.startswith("agent-aside_question-"):
            continue
        agent_id = fname[len("agent-"):-len(".jsonl")]
        jsonl_path = os.path.join(subagents_dir, fname)

        # Read first entry timestamp
        first_ts = None
        try:
            with open(jsonl_path) as f:
                first_line = f.readline().strip()
                if first_line:
                    entry = json.loads(first_line)
                    first_ts = parse_ts(entry.get("timestamp", ""))
        except Exception:
            continue

        if first_ts is None:
            continue

        candidates.append((agent_id, jsonl_path, first_ts))

    if not candidates:
        return []

    # Sort candidates by start timestamp
    candidates.sort(key=lambda c: c[2])

    # Sort tool_uses by content_index to preserve dispatch order
    sorted_tool_uses = sorted(agent_tool_uses, key=lambda t: t[3])

    # Match 1:1 in order: first tool_use gets earliest unmatched subagent
    matched = []
    used_candidates = set()

    for desc, sa_type, tu_ts_str, content_idx in sorted_tool_uses:
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

            # Log meta.json info for debugging (not required for matching)
            meta_path = os.path.join(subagents_dir, f"agent-{agent_id}.meta.json")
            try:
                meta = json.loads(Path(meta_path).read_text())
                meta_desc = meta.get("description", "")
                meta_type = meta.get("agentType", "")
                if meta_desc and meta_desc != desc:
                    log(f"Subagent {agent_id}: meta description '{meta_desc}' != tool_use '{desc}'")
                log(f"Matched subagent {agent_id} (meta: type={meta_type}, desc={meta_desc})")
            except Exception:
                log(f"Matched subagent {agent_id} (no .meta.json)")

            matched.append((agent_id, jsonl_path, desc, sa_type, content_idx))

    return matched


def ingest_subagent(
    agent_id: str,
    transcript_path: str,
    parent_span_id: str,
    trace_id: str,
    session_id: str,
    subagent_offset: int,
    prior_turn_count: int = 0,
) -> tuple:
    """Parse a subagent transcript and build Langfuse events.

    Returns: (events, cost_summary, new_offset, new_turn_count, status)
    """
    empty_cost = {"agent_id": agent_id, "total_cost": 0, "total_tokens": 0,
                  "turns": 0, "status": "partial",
                  "cost_breakdown": {"input": 0, "output": 0, "cache_read": 0, "cache_creation": 0}}

    entries, total_lines = parse_transcript(transcript_path, skip_lines=subagent_offset)
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
        model = turn.get("model", "claude-sonnet-4-6") or "claude-sonnet-4-6"

        turn_cost, input_cost, output_cost, cost_details = calculate_turn_cost(
            usage, model, turn.get("cache_ephemeral_5m", 0), turn.get("cache_ephemeral_1h", 0)
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
            "tools_used": [tc["name"] for tc in turn["tool_calls"]],
            "tool_count": len(turn["tool_calls"]),
            "speed": turn.get("speed", ""),
            "service_tier": turn.get("service_tier", ""),
            "inference_geo": turn.get("inference_geo", ""),
            "request_ids": list(dict.fromkeys(turn.get("request_ids", []))),
            "web_search_requests": turn.get("web_search_requests", 0),
            "web_fetch_requests": turn.get("web_fetch_requests", 0),
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
    except Exception:
        pass
    return ""


def _iter_transcript(transcript_path: str) -> Iterator[dict]:
    try:
        with open(transcript_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue
    except Exception:
        return


def extract_custom_title(transcript_path: str) -> str:
    """First non-empty customTitle entry (v2.1.110+ transcripts).

    Set by the user via the in-CLI title command. Absent in most sessions.
    """
    for entry in _iter_transcript(transcript_path):
        if entry.get("type") == "custom-title":
            title = entry.get("customTitle", "")
            if title:
                return title
    return ""


def extract_permission_mode(transcript_path: str) -> str:
    """Most recent permission-mode entry. The mode can change mid-session."""
    last = ""
    for entry in _iter_transcript(transcript_path):
        if entry.get("type") == "permission-mode":
            mode = entry.get("permissionMode", "")
            if mode:
                last = mode
    return last


def extract_pr_links(transcript_path: str) -> list:
    """All pr-link entries in chronological order."""
    out = []
    for entry in _iter_transcript(transcript_path):
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
    for entry in _iter_transcript(transcript_path):
        if entry.get("type") == "system" and entry.get("subtype") == "away_summary":
            content = entry.get("content")
            if content:
                out.append({
                    "content": truncate(content, MAX_TEXT),
                    "timestamp": entry.get("timestamp"),
                })
    return out


def parse_transcript(transcript_path: str, skip_lines: int = 0) -> tuple[list[dict], int]:
    entries = []
    total = 0
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
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        log(f"Failed to read transcript: {e}")
    return entries, total


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
    except Exception:
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

    for me in msg_entries:
        if me["role"] != "assistant" or not me["message_id"]:
            continue
        mid = me["message_id"]
        if mid not in msg_id_first_ts:
            msg_id_first_ts[mid] = me["timestamp"]
        # Always overwrite — last one wins for usage
        if me["usage"]:
            msg_id_final_usage[mid] = me["usage"]

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
                    "start_time": me["timestamp"],
                    "end_time": me["timestamp"],
                    "first_token_time": None,
                    "model": "",
                    "usage": {"input": 0, "output": 0, "total": 0},
                    "api_call_ids": set(),
                    "messages": [me],
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

            current_turn["end_time"] = me["timestamp"]
            current_turn["api_call_ids"].add(mid)

            # Collect text output (keep last non-empty)
            text = extract_text_blocks(content)
            if text.strip():
                current_turn["assistant_output"] = text

            # Collect tool uses
            for tu in extract_tool_uses(content):
                tool_use_id = tu.get("id", "")
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
        turn["request_ids"] = request_ids
        turn["api_call_ids"] = list(turn["api_call_ids"])  # make serializable

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
    except Exception:
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
    except Exception:
        pass
    return ""


def calculate_turn_cost(usage: dict, model: str, cache_5m: int = 0, cache_1h: int = 0) -> tuple:
    """Calculate cost for a turn's token usage.

    cache_5m / cache_1h: per-tier cache creation token counts (from usageDetails).
    When provided, costs are split by tier; otherwise all cache_creation billed at 5m rate.

    Returns: (turn_cost, input_cost, output_cost, cost_details)
    """
    if not REPORT_API_EQUIVALENT_COST:
        return 0.0, 0.0, 0.0, {}

    m = model.lower()
    # Pricing per 1M tokens: (input, output, cache_read, cache_write_5m, cache_write_1h)
    # Source: platform.claude.com/docs/en/about-claude/pricing (verified 2026-04-17)
    # When a new model releases, its name will fall through to Sonnet pricing and log a warning.
    # Update this function + CLAUDE.md Cost Model table when that happens.
    if "haiku" in m:
        if "haiku-4" in m:                          # Haiku 4.5+
            p_in, p_out, p_cr, p_cc5, p_cc1 = 1.00, 5.00, 0.10, 1.25, 2.00
        elif "3-5" in m:                             # Haiku 3.5
            p_in, p_out, p_cr, p_cc5, p_cc1 = 0.80, 4.00, 0.08, 1.00, 1.60
        else:                                        # Haiku 3
            p_in, p_out, p_cr, p_cc5, p_cc1 = 0.25, 1.25, 0.03, 0.30, 0.50
    elif "opus" in m:
        if any(x in m for x in ("opus-4-5", "opus-4-6", "opus-4-7")):  # Opus 4.5 / 4.6 / 4.7
            p_in, p_out, p_cr, p_cc5, p_cc1 = 5.0, 25.0, 0.50, 6.25, 10.0
        else:                                        # Opus 4.1, 4.0, 3 (legacy)
            p_in, p_out, p_cr, p_cc5, p_cc1 = 15.0, 75.0, 1.50, 18.75, 30.0
    elif "sonnet" in m:                              # Sonnet (all versions)
        p_in, p_out, p_cr, p_cc5, p_cc1 = 3.0, 15.0, 0.30, 3.75, 6.00
    else:
        log(f"[WARN] calculate_turn_cost: unrecognised model '{model}' — cost reported as $0. "
            "Update calculate_turn_cost() and CLAUDE.md if this is a new Anthropic model.")
        return 0.0, 0.0, 0.0, {}

    # Cache creation cost: use per-tier breakdown when available
    if cache_5m or cache_1h:
        cc_cost = (cache_5m * p_cc5 + cache_1h * p_cc1) / 1_000_000
    else:
        cc_cost = usage["cache_creation"] * p_cc5 / 1_000_000

    input_cost = (
        usage["input"] * p_in / 1_000_000
        + usage["cache_read"] * p_cr / 1_000_000
        + cc_cost
    )
    output_cost = usage["output"] * p_out / 1_000_000
    turn_cost = input_cost + output_cost

    cost_details = {
        "input": usage["input"] * p_in / 1_000_000,
        "output": output_cost,
        "cache_read_input_tokens": usage["cache_read"] * p_cr / 1_000_000,
        "cache_creation_input_tokens": cc_cost,
        "total": turn_cost,
    }
    return turn_cost, input_cost, output_cost, cost_details


# ---------------------------------------------------------------------------
# Hook-level score classifiers
# ---------------------------------------------------------------------------

# Keyword lists for session_type classification (checked in priority order)
_SESSION_TYPE_PATTERNS = [
    ("bug-fix", re.compile(
        r"\b(fix|bug|error|broken|issue|crash|fail|debug|troubleshoot|regression|fault|defect)\b",
        re.IGNORECASE,
    )),
    ("refactor", re.compile(
        r"\b(refactor|clean\s*up|rename|reorganize|restructure|migrate|move|split|extract|simplify|deduplicate)\b",
        re.IGNORECASE,
    )),
    ("research", re.compile(
        r"\b(explain|what\s+does|how\s+does|why\s+does|understand|read\b.*\b(and|then)\s+(summarize|explain)|summarize|find\s+where|show\s+me|look\s+at|search\s+for|what\s+is|where\s+is|tell\s+me\s+about)\b",
        re.IGNORECASE,
    )),
    ("feature", re.compile(
        r"\b(add|create|implement|build|write|make|generate|setup|set\s*up|introduce|design|develop|new)\b",
        re.IGNORECASE,
    )),
]


def classify_session_type(first_user_input: str) -> str:
    """Classify a session based on the first user message.

    Returns one of: bug-fix, feature, refactor, research, exploratory.
    Patterns are checked in priority order (bug-fix > refactor > research > feature).
    """
    text = first_user_input.strip()
    if not text:
        return "exploratory"
    for label, pattern in _SESSION_TYPE_PATTERNS:
        if pattern.search(text):
            return label
    return "exploratory"


def calculate_token_efficiency(turns: list[dict]) -> float:
    """Calculate output-to-total token ratio across all turns.

    Returns a float between 0.0 and 1.0 (rounded to 4 decimals).
    Higher values mean more output relative to input/cache tokens.
    """
    total_output = 0
    total_all = 0
    for t in turns:
        u = t["usage"]
        total_output += u["output"]
        total_all += u["output"] + u["input"] + u["cache_read"] + u["cache_creation"]
    if total_all == 0:
        return 0.0
    return round(total_output / total_all, 4)


_FAILURE_PATTERNS = re.compile(
    r"\b(couldn't complete|failed to|unable to|error occurred|I encountered an error|"
    r"I'm unable|I cannot|not able to complete|could not)\b",
    re.IGNORECASE,
)

_QUESTION_PATTERNS = re.compile(
    r"(could you clarify|can you (provide|clarify|explain)|what do you mean|what you mean|"
    r"which (one|file|approach)|do you want me to|shall I|would you like me to)",
    re.IGNORECASE,
)


def classify_task_completed(turns: list[dict]) -> bool:
    """Heuristic: did the session likely complete its task?

    Checks the last turn's assistant output and tool results for error/failure
    signals. Returns True if the session appears to have finished successfully.
    """
    if not turns:
        return True  # no turns = nothing to fail

    last_turn = turns[-1]
    last_output = last_turn.get("assistant_output", "")

    # Check if last output indicates failure
    if _FAILURE_PATTERNS.search(last_output):
        return False

    # Check if last output is asking a clarifying question (incomplete)
    if _QUESTION_PATTERNS.search(last_output):
        return False

    # Check last turn's tool calls for errors
    tool_calls = last_turn.get("tool_calls", [])
    for tc in tool_calls:
        output = tc.get("output", "")
        if output.startswith("[ERROR]"):
            return False

    return True


def compute_cache_hit_rate(turns: list[dict]) -> float:
    """Compute cache hit rate across all turns.

    Cache hit rate = cache_read / (cache_read + cache_creation)
    0.0 = no cache activity or all cold misses, 1.0 = all cache hits.
    """
    if not turns:
        return 0.0

    cache_read = sum(turn.get("usage", {}).get("cache_read", 0) for turn in turns)
    cache_creation = sum(turn.get("usage", {}).get("cache_creation", 0) for turn in turns)
    denominator = cache_read + cache_creation

    if denominator == 0:
        return 0.0

    return round(cache_read / denominator, 4)


def classify_cost_tier(total_cost: float) -> str:
    """Classify session cost into a tier for dashboard filtering.

    < $0.10 = cheap, $0.10–$1.00 = moderate, ≥ $1.00 = expensive.
    """
    if total_cost < COST_TIER_CHEAP_MAX:
        return "cheap"
    elif total_cost < COST_TIER_MODERATE_MAX:
        return "moderate"
    else:
        return "expensive"


def build_hook_score_events(
    trace_id: str,
    session_id: str,
    first_user_input: str,
    turns: list[dict],
    total_cost: float,
) -> list[dict]:
    """Build score-create events for the ingestion batch.

    Returns 5 events: session_type (CATEGORICAL), token_efficiency (NUMERIC),
    task_completed (BOOLEAN as int 0/1), cache_hit_rate (NUMERIC),
    cost_tier (CATEGORICAL).
    """
    now = datetime.now(timezone.utc).isoformat()

    session_type = classify_session_type(first_user_input)
    token_eff = calculate_token_efficiency(turns)
    completed = classify_task_completed(turns)
    cache_hit = compute_cache_hit_rate(turns)
    cost_tier = classify_cost_tier(total_cost)

    scores = [
        {
            "name": "session_type",
            "dataType": "CATEGORICAL",
            "value": session_type,
        },
        {
            "name": "token_efficiency",
            "dataType": "NUMERIC",
            "value": token_eff,
        },
        {
            "name": "task_completed",
            "dataType": "BOOLEAN",
            "value": 1 if completed else 0,
        },
        {
            "name": "cache_hit_rate",
            "dataType": "NUMERIC",
            "value": cache_hit,
        },
        {
            "name": "cost_tier",
            "dataType": "CATEGORICAL",
            "value": cost_tier,
        },
    ]

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


def process_session(session_id: str, transcript_path: str, cwd: str) -> None:
    """Core processing logic for a single session transcript."""
    prev_offset = load_state(session_id)
    slug = extract_slug(transcript_path)
    custom_title = extract_custom_title(transcript_path)
    permission_mode = extract_permission_mode(transcript_path)
    pr_links = extract_pr_links(transcript_path)
    away_summaries = extract_away_summaries(transcript_path)
    entries, total_lines = parse_transcript(transcript_path, skip_lines=prev_offset)

    if not entries:
        log(f"No new entries for session {session_id} (offset {prev_offset}, total {total_lines})")
        save_state(session_id, total_lines)
        return

    now = datetime.now(timezone.utc).isoformat()
    trace_id = f"trace-{session_id}"
    batch = []

    session_meta = extract_session_metadata(transcript_path)

    turns = build_turns(entries)
    if not turns:
        log(f"No turns found in {len(entries)} new entries")
        save_state(session_id, total_lines)
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

    # Aggregate totals for the trace
    total_tokens = sum(t["usage"]["total"] for t in turns)
    total_input = sum(t["usage"]["input"] for t in turns)
    total_output = sum(t["usage"]["output"] for t in turns)
    total_tool_calls = sum(len(t["tool_calls"]) for t in turns)
    total_cost = 0.0
    subagent_cost_summaries = []

    first_user_input = turns[0]["user_input"] if turns else ""
    last_assistant_output = ""
    for t in reversed(turns):
        if t["assistant_output"]:
            last_assistant_output = t["assistant_output"]
            break

    # For trace naming, skip synthetic stubs (slash commands, local-command
    # output, system reminders, prompt-submit hooks) which all start with an
    # XML-like wrapper tag. Real user prompts are plain text. Falls back to
    # the raw first prompt if every turn is a stub.
    first_real_prompt = first_user_input
    for t in turns:
        ui = (t.get("user_input") or "").strip()
        if ui and not _SYNTHETIC_PROMPT_RE.match(ui):
            first_real_prompt = ui
            break

    trace_ts = turns[0].get("start_time") or now

    # Trace name precedence: custom title > legacy slug > truncated first prompt
    trace_name = (
        custom_title
        or slug
        or truncate(redact_secrets(first_real_prompt), 80).strip()
        or "Claude Code Session"
    )

    pr_tags = [f"pr:{p['number']}" for p in pr_links if p.get("number") is not None]

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
            },
            "tags": [t for t in [
                "claude-code",
                repo_name or None,
                *sorted(model_families),
                session_meta.get("entrypoint") or None,
                "fast" if has_fast else None,
                "has-errors" if has_errors else None,
                f"permission:{permission_mode}" if permission_mode else None,
                *pr_tags,
            ] if t],
        },
    })

    # 2. Generation + spans per turn
    for turn_idx, turn in enumerate(turns):
        # Deterministic IDs so re-ingestion updates rather than duplicates
        turn_id = uuid.uuid5(uuid.NAMESPACE_URL, f"{session_id}:turn:{prev_offset + turn_idx}")
        gen_id = f"gen-{turn_id}"

        start_time = turn["start_time"]
        end_time = turn["end_time"]
        first_token_time = turn.get("first_token_time")
        duration_ms = turn.get("duration_ms")
        usage = turn["usage"]
        model = turn.get("model") or DEFAULT_MODEL

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
            usage, model, turn.get("cache_ephemeral_5m", 0), turn.get("cache_ephemeral_1h", 0)
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
            "name": f"Turn {prev_offset + turn_idx + 1}: {redact_secrets(truncate(turn['user_input'], 80))}",
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
                    ))

        # Discover and ingest subagents for this turn
        if agent_tool_uses_this_turn:
            sa_state = load_subagent_state(session_id)
            matches = discover_subagents(transcript_path, agent_tool_uses_this_turn)
            for sa_id, sa_path, sa_desc, sa_type, sa_tc_idx in matches:
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
                )
                batch.extend(sa_events)
                sa_state[sa_id] = {"offset": sa_new_offset, "turn_count": sa_new_tc, "status": sa_status}

                # Enrich Agent tool span metadata
                for evt in batch:
                    if evt["type"] == "span-create" and evt["body"]["id"] == sa_parent_span:
                        evt["body"].setdefault("metadata", {}).update({
                            "subagent_id": sa_id,
                            "subagent_type": sa_type,
                            "subagent_description": sa_desc,
                            "subagent_cost": sa_cost["total_cost"],
                            "subagent_tokens": sa_cost["total_tokens"],
                            "subagent_status": sa_status,
                        })
                        break

                subagent_cost_summaries.append({
                    **sa_cost, "description": sa_desc, "subagent_type": sa_type,
                })

            save_subagent_state(session_id, sa_state)

    # Add subagent cost summary to trace
    if subagent_cost_summaries:
        total_subagent_cost = sum(s["total_cost"] for s in subagent_cost_summaries)
        for evt in batch:
            if evt["type"] == "trace-create":
                evt["body"]["metadata"]["subagent_costs"] = {
                    "agents": subagent_cost_summaries,
                    "total_subagent_cost": round(total_subagent_cost, 6),
                    "parent_cost": round(total_cost, 6),
                    "harness_total_cost": round(total_cost + total_subagent_cost, 6),
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
        total_cost=total_cost,
    )
    batch.extend(score_events)

    send_to_langfuse(batch)
    save_state(session_id, total_lines)

    tool_span_count = sum(len(t["tool_calls"]) for t in turns)
    log(
        f"Sent {len(batch)} events for session {session_id}: "
        f"{len(turns)} turns, {tool_span_count} tool spans, "
        f"{total_tokens} tokens (lines {prev_offset+1}-{total_lines})"
    )


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
    except URLError:
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

        # Delete existing trace to avoid duplicates, then reset state
        trace_id = f"trace-{session_id}"
        delete_trace(trace_id)
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
            process_session(session_id, str(transcript), cwd)
        except Exception as e:
            print(f"  Error: {e}")
            log(f"Reprocess error for {session_id}: {e}")

    print(f"\nDone. Reprocessed {total} session(s).")


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--reprocess":
        reprocess_all()
        return

    try:
        hook_input = json.load(sys.stdin)
    except Exception as e:
        log(f"Failed to parse stdin: {e}")
        return

    if not LANGFUSE_PUBLIC_KEY or not LANGFUSE_SECRET_KEY:
        log("LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY must be set")
        return

    session_id = sanitize_id(hook_input.get("session_id", "unknown"))
    transcript_path = hook_input.get("transcript_path", "")
    cwd = hook_input.get("cwd", "")

    if hook_input.get("stop_hook_active", False):
        return

    if not transcript_path:
        log("No transcript_path provided")
        return

    process_session(session_id, transcript_path, cwd)


if __name__ == "__main__":
    main()
