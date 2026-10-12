"""Cost parity: the hook's own cost vs Langfuse's cost for native OTel generations.

    python3 spike/cost_parity.py

For every spike session in .last-run/runs.jsonl, rebuild the hook's events
offline (send stubbed, temp state dir, so live hook state is untouched), then
compare each hook generation's cost with the summed Langfuse cost of the native
generations that share its request ids.
"""
import glob
import importlib.util
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
os.environ["LANGFUSE_HOOK_LOG"] = os.path.join(tempfile.mkdtemp(), "log")
sys.path.insert(0, ROOT)
from fetch import fetch  # noqa: E402

spec = importlib.util.spec_from_file_location("hook", os.path.join(ROOT, "langfuse-hook.py"))
hook = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hook)

runs = [json.loads(line) for line in open(os.path.join(HERE, ".last-run/runs.jsonl"))]
print(f"{'label':16} {'env':12} {'hook gens':>9} {'native gens':>11} {'hook $':>10} {'langfuse $':>10}  mismatches")
all_bad = 0
for run in runs:
    sid = run["session_id"]
    paths = glob.glob(os.path.expanduser(f"~/.claude/projects/*/{sid}.jsonl"))
    if not paths:
        print(f"{run['label']:16} transcript missing")
        continue
    batches = []
    hook.STATE_DIR = tempfile.mkdtemp()
    hook.send_to_langfuse = lambda b: batches.append(b) or True
    hook.process_session(sid, paths[0], hook.extract_cwd(paths[0]), final=True)
    hook_gens = [e["body"] for b in batches for e in b if e["type"] == "generation-create"]

    native = [o for o in fetch(sid) if o["type"] == "GENERATION"]
    by_req = {}
    for o in native:
        rid = (o.get("metadata") or {}).get("attributes.request_id")
        by_req.setdefault(rid, []).append(o)

    bad = []
    for g in hook_gens:
        hook_cost = (g.get("costDetails") or {}).get("total", 0)
        rids = (g.get("metadata") or {}).get("request_ids") or []
        lf_cost = sum(float(o.get("totalCost") or 0) for r in rids for o in by_req.get(r, []))
        if abs(hook_cost - lf_cost) > 1e-6:
            bad.append((g.get("name"), g.get("model"), round(hook_cost, 6), round(lf_cost, 6), rids))
    all_bad += len(bad)
    env = native[0].get("environment") if native else "-"
    hook_total = sum((g.get("costDetails") or {}).get("total", 0) for g in hook_gens)
    lf_total = sum(float(o.get("totalCost") or 0) for o in native)
    print(f"{run['label']:16} {env:12} {len(hook_gens):>9} {len(native):>11} {hook_total:>10.6f} {lf_total:>10.6f}  {len(bad)}")
    for b in bad[:5]:
        print("    ", b)
print("total mismatches:", all_bad)
