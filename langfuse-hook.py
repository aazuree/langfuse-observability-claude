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

import base64
import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

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

# Patterns to redact from text before sending to Langfuse
SECRET_PATTERNS = [
    re.compile(r"(?i)(api[_-]?key|apikey|secret[_-]?key|access[_-]?key|token|password|passwd|credential|auth)[\s]*[=:]\s*['\"]?([^\s'\"]{8,})['\"]?"),
    re.compile(r"(?i)(sk|pk|api|key|token|secret|password|bearer|ghp|gho|ghu|ghs|ghr|glpat|xox[bposatr]|AKIA|AGPA|AIDA|AROA|AIPA|ANPA|ANVA|ASIA)[-_]?[a-zA-Z0-9/+=]{16,}"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"(?i)Bearer\s+[a-zA-Z0-9._\-/+=]{20,}"),
]

# Pro subscription: $0 marginal cost. Set to True to report equivalent API cost instead.
REPORT_API_EQUIVALENT_COST = True


def log(msg: str) -> None:
    try:
        # Rotate log if it exceeds MAX_LOG_BYTES
        if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > MAX_LOG_BYTES:
            rotated = LOG_FILE + ".1"
            if os.path.exists(rotated):
                os.remove(rotated)
            os.rename(LOG_FILE, rotated)
        with open(LOG_FILE, "a") as f:
            f.write(f"{datetime.now(timezone.utc).isoformat()} {msg}\n")
    except Exception:
        pass


def sanitize_id(value: str) -> str:
    """Sanitize an ID to prevent path traversal. Returns a safe fallback if invalid."""
    if SAFE_ID_RE.match(value):
        return value
    # Fall back to a hash of the value
    import hashlib
    return hashlib.sha256(value.encode()).hexdigest()[:32]


def redact_secrets(text: str) -> str:
    """Redact known secret patterns from text."""
    for pattern in SECRET_PATTERNS:
        text = pattern.sub("[REDACTED]", text)
    return text


def make_auth_header() -> str:
    creds = base64.b64encode(
        f"{LANGFUSE_PUBLIC_KEY}:{LANGFUSE_SECRET_KEY}".encode()
    ).decode()
    return f"Basic {creds}"


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


def extract_text_blocks(content) -> str:
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
        for mid in turn["api_call_ids"]:
            u = msg_id_final_usage.get(mid, {})
            inp = u.get("input_tokens", 0)
            out = u.get("output_tokens", 0)
            usage["input"] += inp
            usage["output"] += out
            usage["total"] += inp + out
            usage["cache_read"] += u.get("cache_read_input_tokens", 0)
            usage["cache_creation"] += u.get("cache_creation_input_tokens", 0)
        turn["usage"] = usage
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


def main() -> None:
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

    prev_offset = load_state(session_id)
    entries, total_lines = parse_transcript(transcript_path, skip_lines=prev_offset)

    if not entries:
        log(f"No new entries for session {session_id} (offset {prev_offset}, total {total_lines})")
        save_state(session_id, total_lines)
        return

    now = datetime.now(timezone.utc).isoformat()
    trace_id = f"trace-{session_id}"
    batch = []

    turns = build_turns(entries)
    if not turns:
        log(f"No turns found in {len(entries)} new entries")
        save_state(session_id, total_lines)
        return

    # Aggregate totals for the trace
    total_tokens = sum(t["usage"]["total"] for t in turns)
    total_input = sum(t["usage"]["input"] for t in turns)
    total_output = sum(t["usage"]["output"] for t in turns)
    total_tool_calls = sum(len(t["tool_calls"]) for t in turns)
    total_cost = 0.0

    first_user_input = turns[0]["user_input"] if turns else ""
    last_assistant_output = ""
    for t in reversed(turns):
        if t["assistant_output"]:
            last_assistant_output = t["assistant_output"]
            break

    # 1. Trace
    batch.append({
        "id": f"evt-{uuid.uuid4()}-trace",
        "timestamp": now,
        "type": "trace-create",
        "body": {
            "id": trace_id,
            "timestamp": now,
            "name": "Claude Code Session",
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
            },
            "tags": ["claude-code"],
        },
    })

    # 2. Generation + spans per turn
    for turn_idx, turn in enumerate(turns):
        turn_id = str(uuid.uuid4())
        gen_id = f"gen-{turn_id}"

        start_time = turn["start_time"]
        end_time = turn["end_time"]
        first_token_time = turn.get("first_token_time")
        duration_ms = turn.get("duration_ms")
        usage = turn["usage"]
        model = turn.get("model", "claude-opus-4-6") or "claude-opus-4-6"

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
        turn_cost = 0.0
        if REPORT_API_EQUIVALENT_COST:
            # Anthropic API equivalent rates for claude-opus-4
            turn_cost = (
                usage["input"] * 15.0 / 1_000_000
                + usage["output"] * 75.0 / 1_000_000
                + usage["cache_read"] * 1.50 / 1_000_000
                + usage["cache_creation"] * 18.75 / 1_000_000
            )
        total_cost += turn_cost

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
            "usage": {
                "input": usage["input"],
                "output": usage["output"],
                "total": usage["total"],
                "unit": "TOKENS",
            },
            "totalCost": turn_cost,
            "metadata": {
                "tools_used": tool_names,
                "tool_count": len(tool_names),
                "api_calls": len(turn["api_call_ids"]),
                "cache_read_tokens": usage["cache_read"],
                "cache_creation_tokens": usage["cache_creation"],
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

    send_to_langfuse(batch)
    save_state(session_id, total_lines)

    tool_span_count = sum(len(t["tool_calls"]) for t in turns)
    log(
        f"Sent {len(batch)} events for session {session_id}: "
        f"{len(turns)} turns, {tool_span_count} tool spans, "
        f"{total_tokens} tokens (lines {prev_offset+1}-{total_lines})"
    )


if __name__ == "__main__":
    main()
