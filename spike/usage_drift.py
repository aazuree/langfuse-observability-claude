"""How often does a message's first transcript entry carry different usage
than its last? Split by main-thread vs subagent transcripts. Read-only.

    python3 spike/usage_drift.py
"""
import glob
import importlib.util
import json
import os
import tempfile

os.environ["LANGFUSE_HOOK_LOG"] = os.path.join(tempfile.mkdtemp(), "log")
_spec = importlib.util.spec_from_file_location(
    "hook", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "langfuse-hook.py"))
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)
missed_usd = {}  # kind -> $ of output tokens counted from the first entry instead of the last


def _output_cost(tokens, model):
    usage = {"input": 0, "output": tokens, "total": tokens, "cache_read": 0, "cache_creation": 0}
    return hook.calculate_turn_cost(usage, model)[0]


stats = {}
for path in glob.glob(os.path.expanduser("~/.claude/projects/**/*.jsonl"), recursive=True):
    kind = "subagent" if "/subagents/" in path else "main"
    first, last, models = {}, {}, {}
    try:
        with open(path) as f:
            for line in f:
                try:
                    e = json.loads(line)
                except ValueError:
                    continue
                msg = e.get("message") if e.get("type") == "assistant" else None
                if not isinstance(msg, dict) or not msg.get("id") or not msg.get("usage"):
                    continue
                first.setdefault(msg["id"], msg["usage"])
                last[msg["id"]] = msg["usage"]
                models[msg["id"]] = msg.get("model") or ""
    except OSError:
        continue
    s = stats.setdefault(kind, {"msgs": 0, "drift": 0, "out_first": 0, "out_last": 0})
    for mid, u in last.items():
        f0 = first[mid]
        s["msgs"] += 1
        s["out_first"] += f0.get("output_tokens", 0) or 0
        s["out_last"] += u.get("output_tokens", 0) or 0
        if any((f0.get(k) or 0) != (u.get(k) or 0) for k in
               ("input_tokens", "output_tokens", "cache_read_input_tokens", "cache_creation_input_tokens")):
            s["drift"] += 1
        delta = (u.get("output_tokens", 0) or 0) - (f0.get("output_tokens", 0) or 0)
        if delta > 0:
            missed_usd[kind] = missed_usd.get(kind, 0.0) + _output_cost(delta, models[mid])

for kind, s in stats.items():
    pct = 100 * s["drift"] / s["msgs"] if s["msgs"] else 0
    print(f"{kind:9} messages={s['msgs']:>7} first!=last={s['drift']:>6} ({pct:.1f}%) "
          f"output tokens first={s['out_first']:>10} last={s['out_last']:>10} "
          f"uncounted ${missed_usd.get(kind, 0.0):.2f}")
