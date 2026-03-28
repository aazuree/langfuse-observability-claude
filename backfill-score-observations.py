#!/usr/bin/env python3
"""Backfill existing Langfuse scores with observationId.

Re-posts existing scores with the last generation's observationId so they
appear in the Scores tab of the observation in the Langfuse UI.

No LLM calls — just reads existing scores and patches them.

Usage:
  PK=... SK=... python3 backfill-score-observations.py
  PK=... SK=... python3 backfill-score-observations.py --dry-run
"""

from __future__ import annotations

import base64
import json
import os
import sys
from urllib.request import Request, urlopen
from urllib.error import URLError

LANGFUSE_HOST = os.environ.get("LANGFUSE_HOST", "http://localhost:3000")
LANGFUSE_PUBLIC_KEY = os.environ.get("LANGFUSE_PUBLIC_KEY", "")
LANGFUSE_SECRET_KEY = os.environ.get("LANGFUSE_SECRET_KEY", "")


def auth_header() -> str:
    creds = base64.b64encode(
        f"{LANGFUSE_PUBLIC_KEY}:{LANGFUSE_SECRET_KEY}".encode()
    ).decode()
    return f"Basic {creds}"


def api_get(path: str) -> dict:
    req = Request(
        f"{LANGFUSE_HOST}{path}",
        headers={"Authorization": auth_header(), "Accept": "application/json"},
        method="GET",
    )
    with urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def fetch_all_scores() -> list[dict]:
    """Paginate through all scores."""
    scores = []
    page = 1
    while True:
        data = api_get(f"/api/public/scores?limit=100&page={page}")
        batch = data.get("data", [])
        if not batch:
            break
        scores.extend(batch)
        total_pages = data.get("meta", {}).get("totalPages", 1)
        if page >= total_pages:
            break
        page += 1
    return scores


def fetch_last_generation_id(trace_id: str) -> str | None:
    """Get the last generation observation for a trace."""
    data = api_get(f"/api/public/observations?traceId={trace_id}&type=GENERATION")
    observations = data.get("data", [])
    if not observations:
        return None
    return observations[-1].get("id")


def post_score(payload: dict) -> bool:
    data = json.dumps(payload).encode()
    req = Request(
        f"{LANGFUSE_HOST}/api/public/scores",
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": auth_header(),
        },
        method="POST",
    )
    try:
        with urlopen(req, timeout=15) as resp:
            return resp.status in (200, 201)
    except URLError as e:
        print(f"  Error posting score: {e}", file=sys.stderr)
        return False


def main() -> int:
    dry_run = "--dry-run" in sys.argv

    if not LANGFUSE_PUBLIC_KEY or not LANGFUSE_SECRET_KEY:
        print("Error: LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY must be set",
              file=sys.stderr)
        return 1

    print("Fetching existing scores...")
    scores = fetch_all_scores()

    # Filter to scores that are missing observationId
    needs_update = [s for s in scores if not s.get("observationId")]
    print(f"Found {len(scores)} total scores, {len(needs_update)} missing observationId")

    if not needs_update:
        print("Nothing to backfill.")
        return 0

    # Cache generation IDs per trace to avoid redundant API calls
    gen_cache: dict[str, str | None] = {}
    updated = 0
    skipped = 0

    for score in needs_update:
        trace_id = score.get("traceId", "")
        score_name = score.get("name", "")

        if trace_id not in gen_cache:
            gen_cache[trace_id] = fetch_last_generation_id(trace_id)

        obs_id = gen_cache[trace_id]
        if not obs_id:
            print(f"  Skip {score_name} for {trace_id}: no generation found")
            skipped += 1
            continue

        payload = {
            "traceId": trace_id,
            "observationId": obs_id,
            "name": score_name,
            "dataType": score.get("dataType", "NUMERIC"),
            "comment": score.get("comment", ""),
        }
        # Preserve the score value
        value = score.get("value") or score.get("stringValue")
        if value is None:
            skipped += 1
            continue
        payload["value"] = value

        if dry_run:
            print(f"  [dry-run] {score_name}={value} -> obs {obs_id}")
        else:
            if post_score(payload):
                print(f"  Updated {score_name}={value} for {trace_id} -> obs {obs_id}")
                updated += 1
            else:
                skipped += 1

    print(f"\nDone: {updated} updated, {skipped} skipped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
