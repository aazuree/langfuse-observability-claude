"""Print the hook's per-generation usage and cost for spike sessions, plus
per-request output tokens from the raw transcript entries (first vs last
entry per request id), to see which one the hook counts.

    python3 spike/hook_usage.py SESSION_ID [...]
"""
import glob
import importlib.util
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
os.environ["LANGFUSE_HOOK_LOG"] = os.path.join(tempfile.mkdtemp(), "log")
spec = importlib.util.spec_from_file_location("hook", os.path.join(os.path.dirname(HERE), "langfuse-hook.py"))
hook = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hook)

KEYS = ("input", "output", "cache_read", "cache_create", "cache_5m", "cache_1h")
for sid in sys.argv[1:]:
    main = glob.glob(os.path.expanduser(f"~/.claude/projects/*/{sid}.jsonl"))[0]
    batches = []
    hook.STATE_DIR = tempfile.mkdtemp()
    hook.send_to_langfuse = lambda b: batches.append(b) or True
    hook.process_session(sid, main, hook.extract_cwd(main), final=True)
    for e in (e for b in batches for e in b):
        if e["type"] == "generation-create":
            b = e["body"]
            print(sid[:8], b["name"][:45].ljust(45), {k: b["usageDetails"].get(k) for k in KEYS},
                  "cost", b["costDetails"].get("total"), "reqs", len(b["metadata"]["request_ids"]))

    sub_dir = main[:-len(".jsonl")] + "/subagents"
    for path in [main] + sorted(glob.glob(sub_dir + "/*.jsonl")):
        per_req = {}
        for line in open(path):
            e = json.loads(line)
            if e.get("type") == "assistant" and e.get("requestId"):
                per_req.setdefault(e["requestId"], []).append(e["message"].get("usage", {}).get("output_tokens"))
        print("  ", os.path.basename(path)[:30].ljust(30), "entries/request + output_tokens first->last:",
              [(len(v), v[0], v[-1]) for v in per_req.values()])
