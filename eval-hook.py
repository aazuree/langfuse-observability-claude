#!/usr/bin/env python3
"""LLM-as-a-Judge evaluator for Langfuse traces.

Fetches unscored traces from Langfuse, evaluates them using the Claude CLI
(haiku model for cost efficiency), and posts quality scores back.

Evaluators:
  - task_completion   (CATEGORICAL: completed / partial / failed)
  - tool_appropriateness (CATEGORICAL: appropriate / questionable / inappropriate)
  - code_quality      (NUMERIC: 0.0 - 1.0)
  - response_quality  (NUMERIC: 0.0 - 1.0)

Environment variables:
  LANGFUSE_PUBLIC_KEY  - Langfuse project public key
  LANGFUSE_SECRET_KEY  - Langfuse project secret key
  LANGFUSE_HOST        - Langfuse base URL (default: http://localhost:3000)

Usage:
  python3 eval-hook.py                  # Evaluate all unscored traces
  python3 eval-hook.py --dry-run        # Preview without posting scores
  python3 eval-hook.py --trace ID       # Evaluate a single trace
  python3 eval-hook.py --rescore        # Re-evaluate already-scored traces
  python3 eval-hook.py --limit N        # Process at most N traces
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LANGFUSE_HOST = os.environ.get("LANGFUSE_HOST", "http://localhost:3000")
LANGFUSE_PUBLIC_KEY = os.environ.get("LANGFUSE_PUBLIC_KEY", "")
LANGFUSE_SECRET_KEY = os.environ.get("LANGFUSE_SECRET_KEY", "")

LOG_FILE = os.path.expanduser("~/.claude/langfuse-eval.log")
STATE_DIR = os.path.expanduser("~/.claude/langfuse-state/eval")

MAX_LOG_BYTES = 10 * 1024 * 1024  # 10 MB
EVAL_DELAY_SECONDS = 1  # delay between CLI calls
CLI_TIMEOUT_SECONDS = 30  # timeout for claude CLI invocations

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def log(msg: str) -> None:
    """Append a timestamped message to the log file (10 MB rotation)."""
    try:
        if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > MAX_LOG_BYTES:
            rotated = LOG_FILE + ".1"
            if os.path.exists(rotated):
                os.remove(rotated)
            os.rename(LOG_FILE, rotated)
        with open(LOG_FILE, "a") as f:
            f.write(f"{datetime.now(timezone.utc).isoformat()} {msg}\n")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


def make_auth_header() -> str:
    """Build HTTP Basic auth header from Langfuse keys."""
    creds = base64.b64encode(
        f"{LANGFUSE_PUBLIC_KEY}:{LANGFUSE_SECRET_KEY}".encode()
    ).decode()
    return f"Basic {creds}"


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="LLM-as-a-Judge evaluator for Langfuse traces."
    )
    parser.add_argument(
        "--rescore",
        action="store_true",
        help="Re-evaluate traces that have already been scored.",
    )
    parser.add_argument(
        "--trace",
        type=str,
        default=None,
        help="Evaluate a single trace by ID.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview evaluations without posting scores.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process at most N traces.",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# State management (marker files)
# ---------------------------------------------------------------------------


def is_scored(trace_id: str) -> bool:
    """Check whether a trace has already been scored."""
    marker = os.path.join(STATE_DIR, f"{trace_id}.scored")
    return os.path.exists(marker)


def mark_scored(trace_id: str) -> None:
    """Create a marker file indicating the trace has been scored."""
    Path(STATE_DIR).mkdir(parents=True, exist_ok=True)
    marker = os.path.join(STATE_DIR, f"{trace_id}.scored")
    Path(marker).write_text(datetime.now(timezone.utc).isoformat())


# ---------------------------------------------------------------------------
# Trace fetching
# ---------------------------------------------------------------------------


def fetch_traces(page: int = 1, limit: int = 50) -> dict:
    """GET /api/public/traces with pagination."""
    url = (f"{LANGFUSE_HOST}/api/public/traces"
           f"?orderBy=timestamp&order=DESC&limit={limit}&page={page}")
    req = Request(
        url,
        headers={
            "Authorization": make_auth_header(),
            "Accept": "application/json",
        },
        method="GET",
    )
    try:
        with urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except URLError as e:
        log(f"Failed to fetch traces (page={page}): {e}")
        return {}


def fetch_single_trace(trace_id: str) -> dict | None:
    """GET /api/public/traces/:id."""
    url = f"{LANGFUSE_HOST}/api/public/traces/{trace_id}"
    req = Request(
        url,
        headers={
            "Authorization": make_auth_header(),
            "Accept": "application/json",
        },
        method="GET",
    )
    try:
        with urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except URLError as e:
        log(f"Failed to fetch trace {trace_id}: {e}")
        return None


def get_unscored_traces(args: argparse.Namespace) -> list[dict]:
    """Return a list of trace dicts that need evaluation.

    Handles --trace (single), --rescore (skip marker check), --limit.
    Paginates through all traces when necessary.
    """
    # Single trace mode
    if args.trace:
        trace = fetch_single_trace(args.trace)
        if trace is None:
            return []
        if not args.rescore and is_scored(args.trace):
            log(f"Trace {args.trace} already scored (use --rescore to re-evaluate)")
            return []
        return [trace]

    # Paginated fetch
    traces: list[dict] = []
    page = 1
    max_traces = args.limit or 1000  # safety cap

    while len(traces) < max_traces:
        data = fetch_traces(page=page, limit=50)
        batch = data.get("data", [])
        if not batch:
            break
        for t in batch:
            tid = t.get("id", "")
            if not tid:
                continue
            if not t.get("input") or not t.get("output"):
                log(f"Skipping trace {tid}: missing input or output")
                continue
            if not args.rescore and is_scored(tid):
                continue
            traces.append(t)
            if len(traces) >= max_traces:
                break
        # Check if there are more pages
        meta = data.get("meta", {})
        total_pages = meta.get("totalPages", 1)
        if page >= total_pages:
            break
        page += 1

    return traces


# ---------------------------------------------------------------------------
# Evaluator definitions
# ---------------------------------------------------------------------------

EVALUATORS = [
    {
        "name": "task_completion",
        "data_type": "CATEGORICAL",
        "valid_values": ["completed", "partial", "failed"],
        "template": (
            "You are evaluating whether an AI coding assistant completed the "
            "user's requested task.\n\n"
            "## User Request\n{input}\n\n"
            "## Assistant Response\n{output}\n\n"
            "## Criteria\n"
            '- "completed": The assistant fully addressed what was asked. '
            "The task is done.\n"
            '- "partial": The assistant made progress but left parts unfinished, '
            "asked clarifying questions, or delivered an incomplete solution.\n"
            '- "failed": The assistant did not accomplish the task, misunderstood '
            "the request, or went off-track.\n\n"
            "Focus on whether the OUTCOME matches the REQUEST, not on style or "
            "verbosity.\n\n"
            "Respond with ONLY a JSON object, no other text:\n"
            '{{"score": "<completed|partial|failed>", "reasoning": '
            '"<one sentence explanation>"}}'
        ),
    },
    {
        "name": "tool_appropriateness",
        "data_type": "CATEGORICAL",
        "valid_values": ["appropriate", "questionable", "inappropriate"],
        "template": (
            "You are evaluating whether an AI coding assistant used appropriate "
            "tools for the task.\n\n"
            "## User Request\n{input}\n\n"
            "## Assistant Response (including tool calls)\n{output}\n\n"
            "## Criteria\n"
            '- "appropriate": Tools chosen match the task. Used Read instead of '
            "cat, Edit instead of sed, Grep instead of grep, etc. No unnecessary "
            "tool calls.\n"
            '- "questionable": Some tool choices were suboptimal but not harmful. '
            "E.g., used Bash where a dedicated tool exists, or made redundant "
            "calls.\n"
            '- "inappropriate": Tools were clearly wrong for the task, excessive, '
            "or harmful. E.g., destructive operations without need, reading files "
            "never used.\n\n"
            "Judge the tool SELECTION, not the task outcome.\n\n"
            "Respond with ONLY a JSON object, no other text:\n"
            '{{"score": "<appropriate|questionable|inappropriate>", "reasoning": '
            '"<one sentence explanation>"}}'
        ),
    },
    {
        "name": "code_quality",
        "data_type": "NUMERIC",
        "valid_values": None,  # 0.0-1.0 or null
        "template": (
            "You are evaluating the quality of code written by an AI coding "
            "assistant.\n\n"
            "## User Request\n{input}\n\n"
            "## Assistant Response\n{output}\n\n"
            "## Scoring (0.0 to 1.0)\n"
            "- 1.0: Code is correct, idiomatic, minimal, handles edge cases "
            "appropriately, no security issues\n"
            "- 0.7-0.9: Code works and is reasonable but has minor style or "
            "efficiency issues\n"
            "- 0.4-0.6: Code has noticeable problems — unnecessary complexity, "
            "poor naming, missing obvious cases\n"
            "- 0.1-0.3: Code has significant issues — bugs, security "
            "vulnerabilities, fundamentally wrong approach\n"
            "- 0.0: No code was written when it should have been, or code is "
            "completely broken\n\n"
            "If the session involved no code writing, respond with:\n"
            '{{"score": null, "reasoning": "No code was written in this '
            'session."}}\n'
            "The script will skip posting a code_quality score when score is "
            "null.\n\n"
            "Respond with ONLY a JSON object, no other text:\n"
            '{{"score": <float between 0.0 and 1.0>, "reasoning": '
            '"<one sentence explanation>"}}'
        ),
    },
    {
        "name": "response_quality",
        "data_type": "NUMERIC",
        "valid_values": None,  # 0.0-1.0
        "template": (
            "You are evaluating the communication quality of an AI coding "
            "assistant's response.\n\n"
            "## User Request\n{input}\n\n"
            "## Assistant Response\n{output}\n\n"
            "## Scoring (0.0 to 1.0)\n"
            "- 1.0: Concise, directly addresses the question, well-structured, "
            "no filler\n"
            "- 0.7-0.9: Good response with minor verbosity or slight "
            "misalignment to the question\n"
            "- 0.4-0.6: Overly verbose, includes unnecessary preamble, or "
            "partially misses the point\n"
            "- 0.1-0.3: Rambling, off-topic, or confusing structure\n"
            "- 0.0: Response is incoherent or completely unhelpful\n\n"
            "Value directness and relevance over length.\n\n"
            "Respond with ONLY a JSON object, no other text:\n"
            '{{"score": <float between 0.0 and 1.0>, "reasoning": '
            '"<one sentence explanation>"}}'
        ),
    },
]

# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

# Matches the first JSON object in a string (handles nested braces one level deep)
JSON_RE = re.compile(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}')


def parse_eval_response(cli_output: str) -> dict | None:
    """Parse the claude CLI JSON envelope and extract the eval result.

    The CLI outputs: {"type": "result", "subtype": "success", "result": "..."}
    The inner result is a string that may contain preamble text before the JSON.

    Returns dict with 'score' and 'reasoning' keys, or None on failure.
    """
    try:
        envelope = json.loads(cli_output)
    except (json.JSONDecodeError, TypeError):
        log(f"Failed to parse CLI envelope: {cli_output[:200]}")
        return None

    if envelope.get("type") != "result":
        log(f"Unexpected CLI output type: {envelope.get('type')}")
        return None

    result_text = envelope.get("result", "")
    if not isinstance(result_text, str):
        log(f"Result is not a string: {type(result_text)}")
        return None

    # Try direct parse first
    try:
        parsed = json.loads(result_text)
        if "score" in parsed:
            return parsed
    except (json.JSONDecodeError, TypeError):
        pass

    # Fall back to regex extraction (handles preamble text before JSON)
    match = JSON_RE.search(result_text)
    if not match:
        log(f"No JSON found in result: {result_text[:200]}")
        return None

    try:
        parsed = json.loads(match.group())
        if "score" in parsed:
            return parsed
    except json.JSONDecodeError:
        log(f"Failed to parse extracted JSON: {match.group()[:200]}")

    return None


# ---------------------------------------------------------------------------
# Score validation
# ---------------------------------------------------------------------------


def validate_score(evaluator: dict, score) -> bool:
    """Validate a score against the evaluator's constraints.

    Returns True if valid, False otherwise. None (null) is always valid (= skip).
    """
    if score is None:
        return True

    if evaluator["data_type"] == "CATEGORICAL":
        return score in evaluator["valid_values"]

    if evaluator["data_type"] == "NUMERIC":
        try:
            val = float(score)
        except (TypeError, ValueError):
            return False
        return 0.0 <= val <= 1.0

    return False


# ---------------------------------------------------------------------------
# Claude CLI invocation
# ---------------------------------------------------------------------------


def run_eval(evaluator: dict, trace_input: str, trace_output: str) -> dict | None:
    """Run a single evaluation using the claude CLI.

    Returns parsed result dict with 'score' and 'reasoning', or None on failure.
    """
    prompt = evaluator["template"].format(input=trace_input, output=trace_output)

    try:
        result = subprocess.run(
            ["claude", "-p", "--model", "haiku", "--output-format", "json"],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=CLI_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        log(f"CLI timeout for {evaluator['name']}")
        return None
    except FileNotFoundError:
        log("claude CLI not found — is it installed and on PATH?")
        return None

    if result.returncode != 0:
        log(f"CLI error for {evaluator['name']}: {result.stderr[:200]}")
        return None

    parsed = parse_eval_response(result.stdout)
    if parsed is None:
        return None

    if not validate_score(evaluator, parsed.get("score")):
        log(
            f"Invalid score for {evaluator['name']}: {parsed.get('score')} "
            f"(expected {evaluator['data_type']})"
        )
        return None

    return parsed


# ---------------------------------------------------------------------------
# Score posting
# ---------------------------------------------------------------------------

def build_score_payload(trace_id: str, evaluator: dict, result: dict) -> dict | None:
    """Build a Langfuse score payload. Returns None if score is null (skip)."""
    score = result.get("score")
    if score is None:
        return None

    payload = {
        "traceId": trace_id,
        "name": evaluator["name"],
        "dataType": evaluator["data_type"],
        "comment": result.get("reasoning", ""),
    }

    if evaluator["data_type"] == "NUMERIC":
        payload["value"] = float(score)
    else:
        payload["value"] = str(score)

    return payload


def post_score(payload: dict) -> bool:
    """Post a single score to Langfuse. Returns True on success.

    Raises ConnectionError if the API is unreachable (connection refused, DNS failure).
    Returns False for HTTP errors (400, 500, etc.) which are logged and skipped.
    """
    url = f"{LANGFUSE_HOST}/api/public/scores"
    data = json.dumps(payload).encode()
    req = Request(url, data=data, headers={
        "Content-Type": "application/json",
        "Authorization": make_auth_header(),
    }, method="POST")

    try:
        with urlopen(req, timeout=15) as resp:
            log(f"Score posted: {payload['name']}={payload['value']} "
                f"for {payload['traceId']} ({resp.status})")
            return True
    except URLError as e:
        if hasattr(e, 'code'):
            # HTTP error (400, 500, etc.) — log and skip this score
            log(f"HTTP {e.code} posting score {payload['name']} for "
                f"{payload['traceId']}: {e.reason}")
            return False
        # Connection error (refused, DNS, timeout) — fatal
        raise ConnectionError(
            f"Langfuse API unreachable: {e}") from e


# ---------------------------------------------------------------------------
# Main evaluation loop
# ---------------------------------------------------------------------------


def evaluate_trace(trace: dict, args: argparse.Namespace) -> int:
    """Evaluate a single trace with all evaluators. Returns number of scores evaluated."""
    trace_id = trace["id"]
    trace_input = trace.get("input", "")
    trace_output = trace.get("output", "")
    evaluated = 0

    for evaluator in EVALUATORS:
        result = run_eval(evaluator, trace_input, trace_output)
        if not result:
            continue

        payload = build_score_payload(trace_id, evaluator, result)
        if not payload:
            log(f"Skipping {evaluator['name']} for {trace_id}: null score")
            continue

        if args.dry_run:
            print(json.dumps(payload, indent=2))
        else:
            # post_score raises ConnectionError if API is unreachable (fatal)
            # Returns False for HTTP errors like 400 (logged, non-fatal)
            post_score(payload)

        evaluated += 1
        time.sleep(EVAL_DELAY_SECONDS)

    return evaluated


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if not LANGFUSE_PUBLIC_KEY or not LANGFUSE_SECRET_KEY:
        print("Error: LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY must be set",
              file=sys.stderr)
        return 1

    log("Starting evaluation run")

    try:
        traces = get_unscored_traces(args)
    except URLError as e:
        log(f"Langfuse API unreachable: {e}")
        print(f"Error: Langfuse API unreachable: {e}", file=sys.stderr)
        return 1

    if not traces:
        log("No unscored traces found")
        print("No unscored traces found.")
        return 0

    log(f"Found {len(traces)} traces to evaluate")
    total_scores = 0

    for trace in traces:
        trace_id = trace["id"]
        log(f"Evaluating trace {trace_id}")

        try:
            evaluated = evaluate_trace(trace, args)
            total_scores += evaluated
        except ConnectionError:
            print("Error: Langfuse API unreachable during scoring",
                  file=sys.stderr)
            return 1

        if not args.dry_run:
            if evaluated > 0:
                mark_scored(trace_id)
                log(f"Marked {trace_id} as scored ({evaluated} scores)")
            else:
                log(f"No scores for {trace_id} — not marking as scored, will retry next run")

    log(f"Evaluation complete: {len(traces)} traces, {total_scores} scores")
    print(f"Evaluated {len(traces)} traces, {total_scores} scores.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
