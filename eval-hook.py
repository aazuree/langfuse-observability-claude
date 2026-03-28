#!/usr/bin/env python3
"""LLM-as-a-Judge evaluator for Langfuse traces.

Fetches unscored traces from Langfuse, evaluates them using the Claude CLI
(haiku model for cost efficiency), and posts quality scores back.

Evaluators:
  - task_completion   (CATEGORICAL: completed / partial / failed / unclear)
  - tool_appropriateness (CATEGORICAL: appropriate / excessive / insufficient / mixed)
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

LOG_FILE = os.path.expanduser("~/.claude/eval-hook.log")
STATE_DIR = os.path.expanduser("~/.claude/langfuse-state/eval")

MAX_LOG_BYTES = 10 * 1024 * 1024  # 10 MB
EVAL_DELAY_SECONDS = 2  # Pause between evaluations to avoid rate limits
CLI_TIMEOUT_SECONDS = 120  # Timeout for claude CLI invocations

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
    url = f"{LANGFUSE_HOST}/api/public/traces?page={page}&limit={limit}"
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
        "valid_values": ["completed", "partial", "failed", "unclear"],
        "template": (
            "You are an evaluation judge. Assess whether the assistant completed "
            "the user's requested task.\n\n"
            "## User Request\n{input}\n\n"
            "## Assistant Response\n{output}\n\n"
            "## Instructions\n"
            "Evaluate task completion. Choose one of: completed, partial, failed, unclear.\n\n"
            "Respond with ONLY a JSON object (no other text):\n"
            '{{\"score\": \"<value>\", \"reasoning\": \"<1-2 sentence explanation>\"}}'
        ),
    },
    {
        "name": "tool_appropriateness",
        "data_type": "CATEGORICAL",
        "valid_values": ["appropriate", "excessive", "insufficient", "mixed"],
        "template": (
            "You are an evaluation judge. Assess whether the assistant's tool usage "
            "was appropriate for the task.\n\n"
            "## User Request\n{input}\n\n"
            "## Assistant Response\n{output}\n\n"
            "## Instructions\n"
            "Evaluate tool usage appropriateness. Choose one of: appropriate, excessive, "
            "insufficient, mixed.\n\n"
            "Respond with ONLY a JSON object (no other text):\n"
            '{{\"score\": \"<value>\", \"reasoning\": \"<1-2 sentence explanation>\"}}'
        ),
    },
    {
        "name": "code_quality",
        "data_type": "NUMERIC",
        "valid_values": None,  # 0.0 - 1.0
        "template": (
            "You are an evaluation judge. Assess the quality of any code written or "
            "modified in the assistant's response.\n\n"
            "## User Request\n{input}\n\n"
            "## Assistant Response\n{output}\n\n"
            "## Instructions\n"
            "Rate code quality from 0.0 (terrible) to 1.0 (excellent). Consider correctness, "
            "readability, efficiency, and best practices. If no code was written, respond with "
            "a null score.\n\n"
            "Respond with ONLY a JSON object (no other text):\n"
            '{{\"score\": <number or null>, \"reasoning\": \"<1-2 sentence explanation>\"}}'
        ),
    },
    {
        "name": "response_quality",
        "data_type": "NUMERIC",
        "valid_values": None,  # 0.0 - 1.0
        "template": (
            "You are an evaluation judge. Assess the overall quality of the assistant's "
            "response.\n\n"
            "## User Request\n{input}\n\n"
            "## Assistant Response\n{output}\n\n"
            "## Instructions\n"
            "Rate response quality from 0.0 (terrible) to 1.0 (excellent). Consider accuracy, "
            "helpfulness, completeness, and clarity.\n\n"
            "Respond with ONLY a JSON object (no other text):\n"
            '{{\"score\": <number or null>, \"reasoning\": \"<1-2 sentence explanation>\"}}'
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

    result_str = envelope.get("result", "")
    if not isinstance(result_str, str):
        log(f"Result is not a string: {type(result_str)}")
        return None

    # Find JSON object within the result string (may have preamble text)
    match = JSON_RE.search(result_str)
    if not match:
        log(f"No JSON object found in result: {result_str[:200]}")
        return None

    try:
        parsed = json.loads(match.group())
    except json.JSONDecodeError:
        log(f"Failed to parse inner JSON: {match.group()[:200]}")
        return None

    if "score" not in parsed:
        log(f"Missing 'score' key in parsed result: {parsed}")
        return None

    return parsed


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

    cmd = [
        "haiku",
        "-p", prompt,
        "--output-format", "json",
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=CLI_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        log(f"CLI timeout for evaluator {evaluator['name']}")
        return None
    except FileNotFoundError:
        log("'haiku' CLI command not found")
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
# Main (placeholder - Tasks 4-5 will add score posting and the full loop)
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()

    if not LANGFUSE_PUBLIC_KEY or not LANGFUSE_SECRET_KEY:
        print(
            "Error: LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY must be set",
            file=sys.stderr,
        )
        sys.exit(1)

    # TODO (Task 4): build_score_payload, post_score
    # TODO (Task 5): evaluate_trace, main loop over get_unscored_traces(args)
    log("eval-hook started (placeholder main)")


if __name__ == "__main__":
    main()
