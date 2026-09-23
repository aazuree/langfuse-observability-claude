#!/usr/bin/env python3
"""Check the hook's hardcoded pricing table against a public pricing feed.

Why this is a separate tool and not part of the hook
----------------------------------------------------
The hook is deliberately offline: stdlib only, fire-and-forget, no network call
on the Stop path. Fetching live prices at ingestion time would add latency to
every turn, make cost depend on a third party's uptime, and silently re-bill
historical turns whenever an upstream entry changed. Worse, a wrong feed entry
would produce confidently wrong numbers, whereas an unknown model today produces
an obvious `$0` plus a `[WARN]`.

So pricing stays hardcoded in `calculate_turn_cost()`, and this script is the
*drift detector*: run it manually or in CI to find out that a rate moved, then
update the table and CLAUDE.md by hand.

What it does NOT cover (still manual):
  * Fast-mode multipliers (`FAST_MODE_MULTIPLIERS`)
  * Data-residency multiplier (`US_GEO_MULTIPLIER` / `inference_geo`)
  * Date-aware introductory pricing windows (none active today) — the
    feed only ever carries *today's* live rate, which is exactly what this
    script compares against.
  * Server-side tool billing (web search per-request cost)
  * Models absent from the feed (e.g. Mythos 5, a Project Glasswing model) —
    those are reported as SKIP, not as a failure.

Usage
-----
    python3 tools/check_pricing_drift.py            # exit 1 on drift
    python3 tools/check_pricing_drift.py --json     # machine-readable report
    python3 tools/check_pricing_drift.py --feed /path/to/local.json
"""
import argparse
import importlib.util
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

# LiteLLM's community price map. Chosen over alternatives because it is the only
# widely-maintained feed carrying all five axes this hook bills on — including
# the 1-hour cache-write tier, which models.dev omits.
FEED_URL = (
    "https://raw.githubusercontent.com/BerriAI/litellm/main/"
    "model_prices_and_context_window.json"
)
FEED_TIMEOUT_S = 30

# Model IDs to verify, in the order they should be reported.
MODELS = [
    "claude-opus-5-5",
    "claude-opus-5",
    "claude-opus-4-8",
    "claude-opus-4-7",
    "claude-opus-4-6",
    "claude-opus-4-5",
    "claude-sonnet-5",
    "claude-sonnet-4-6",
    "claude-haiku-4-5",
    "claude-fable-5",
    "claude-mythos-5",
]

# Our axis name -> the feed's per-token field name.
AXES = [
    ("input",         "input_cost_per_token"),
    ("output",        "output_cost_per_token"),
    ("cache_read",    "cache_read_input_token_cost"),
    ("cache_write_5m", "cache_creation_input_token_cost"),
    ("cache_write_1h", "cache_creation_input_token_cost_above_1hr"),
]

MTOK = 1_000_000
# Rates are dollars per 1M tokens; a tenth of a cent is well below any real change.
TOLERANCE = 0.001


def load_hook():
    """Import the hyphenated hook module by path."""
    path = os.path.join(os.path.dirname(__file__), "..", "langfuse-hook.py")
    spec = importlib.util.spec_from_file_location("langfuse_hook", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def probe_rates(hook, model, now_iso):
    """Recover the hook's per-1M rates by calling calculate_turn_cost directly.

    Probing the real function rather than re-declaring a table here means this
    check cannot drift from the code it is meant to validate. `now_iso` is
    passed as the turn timestamp so date-aware pricing resolves to the rate
    that is live *today* — the same thing the feed publishes.
    """
    def zero():
        return {"input": 0, "output": 0, "cache_read": 0, "cache_creation": 0}

    def cost(usage, **kwargs):
        _total, _inp, _out, details = hook.calculate_turn_cost(
            usage, model, turn_start_time=now_iso, **kwargs
        )
        return details

    u = zero(); u["input"] = MTOK
    rates = {"input": cost(u).get("input", 0.0)}

    u = zero(); u["output"] = MTOK
    rates["output"] = cost(u).get("output", 0.0)

    u = zero(); u["cache_read"] = MTOK
    rates["cache_read"] = cost(u).get("cache_read_input_tokens", 0.0)

    u = zero(); u["cache_creation"] = MTOK
    rates["cache_write_5m"] = cost(u, cache_5m=MTOK, cache_1h=0).get(
        "cache_creation_input_tokens", 0.0
    )
    rates["cache_write_1h"] = cost(u, cache_5m=0, cache_1h=MTOK).get(
        "cache_creation_input_tokens", 0.0
    )
    return rates


def fetch_feed(source):
    """Load the pricing feed from a URL or a local path."""
    if os.path.exists(source):
        with open(source) as fh:
            return json.load(fh)
    req = urllib.request.Request(source, headers={"User-Agent": "langfuse-observability/pricing-check"})
    with urllib.request.urlopen(req, timeout=FEED_TIMEOUT_S) as resp:
        return json.loads(resp.read().decode("utf-8"))


def compare(hook, feed, now_iso):
    """Return (rows, drift_count). One row per model."""
    rows = []
    drift = 0
    for model in MODELS:
        entry = feed.get(model)
        if entry is None:
            rows.append({"model": model, "status": "SKIP",
                         "reason": "not present in feed", "deltas": {}})
            continue

        ours = probe_rates(hook, model, now_iso)
        deltas = {}
        for axis, field in AXES:
            if field not in entry:
                deltas[axis] = {"ours": ours[axis], "feed": None,
                                "note": "axis absent from feed"}
                continue
            theirs = entry[field] * MTOK
            if abs(ours[axis] - theirs) > TOLERANCE:
                deltas[axis] = {"ours": ours[axis], "feed": theirs}

        # An axis the feed does not carry is informational, not drift.
        real = {k: v for k, v in deltas.items() if "note" not in v}
        if real:
            drift += 1
            rows.append({"model": model, "status": "DRIFT",
                         "reason": "", "deltas": deltas})
        else:
            rows.append({"model": model, "status": "OK",
                         "reason": "", "deltas": deltas})
    return rows, drift


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--feed", default=FEED_URL,
                        help="pricing feed URL or local JSON path")
    parser.add_argument("--json", action="store_true",
                        help="emit a machine-readable report")
    args = parser.parse_args()

    hook = load_hook()
    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        feed = fetch_feed(args.feed)
    except (urllib.error.URLError, urllib.error.HTTPError, OSError, ValueError) as exc:
        print(f"error: could not load pricing feed {args.feed}: {exc}", file=sys.stderr)
        return 2

    rows, drift = compare(hook, feed, now_iso)

    if args.json:
        print(json.dumps({"checked_at": now_iso, "feed": args.feed,
                          "drift_count": drift, "results": rows}, indent=2))
        return 1 if drift else 0

    print(f"Pricing drift check — {now_iso}")
    print(f"Feed: {args.feed}\n")
    for row in rows:
        if row["status"] == "SKIP":
            print(f"  SKIP  {row['model']:<22} ({row['reason']})")
        elif row["status"] == "OK":
            note = [a for a, d in row["deltas"].items() if "note" in d]
            suffix = f"  [feed lacks: {', '.join(note)}]" if note else ""
            print(f"  OK    {row['model']:<22}{suffix}")
        else:
            print(f"  DRIFT {row['model']:<22}")
            for axis, d in row["deltas"].items():
                if "note" in d:
                    continue
                print(f"          {axis:<15} ours=${d['ours']:.4f}/MTok  "
                      f"feed=${d['feed']:.4f}/MTok")

    if drift:
        print(f"\n{drift} model(s) drifted. Update calculate_turn_cost() in "
              "langfuse-hook.py and the Cost Model table in CLAUDE.md.")
        print("Verify against platform.claude.com/docs/en/about-claude/pricing "
              "before trusting the feed — it is community-maintained.")
        return 1

    print("\nNo drift. Fast-mode and data-residency multipliers are not covered "
          "by this check — verify those manually.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
