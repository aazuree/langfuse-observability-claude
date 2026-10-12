"""Dump one session's observations as a tree, with IO and the useful attributes.

    python3 spike/fetch.py SESSION_ID [--json OUT]

Emails are masked in the output.
"""
import json
import re
import sys

from api import call

EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
SKIP = ("resourceAttributes.", "scope.", "attributes.user.", "attributes.organization.",
        "attributes.terminal.", "attributes.session.id", "attributes.deployment.")


def fetch(session_id):
    rows, cursor = [], None
    while True:
        q = ("/v2/observations?limit=1000&fields=core,basic,usage,model,io,metadata"
             "&expandMetadata=attributes&sessionId=" + session_id)
        if cursor:
            q += f"&cursor={cursor}"
        status, out = call("GET", q)
        assert status == 200, out
        rows += [o for o in out["data"] if o.get("sessionId") == session_id]
        cursor = (out.get("meta") or {}).get("cursor")
        if not cursor:
            return json.loads(EMAIL.sub("<EMAIL>", json.dumps(rows)))


def show(rows):
    kids = {}
    for o in rows:
        kids.setdefault(o.get("parentObservationId"), []).append(o)
    ids = {o["id"] for o in rows}

    def walk(o, depth):
        md = {k.removeprefix("attributes."): v for k, v in (o.get("metadata") or {}).items()
              if not k.startswith(SKIP)}
        print("  " * depth + f"{o['type']} {o['name']} usage={o.get('usageDetails')} cost={o.get('totalCost')}")
        for k in ("input", "output"):
            if o.get(k) is not None:
                print("  " * depth + f"  {k}: {str(o[k])[:160]!r}")
        print("  " * depth + f"  attrs: {json.dumps(md)[:600]}")
        for c in sorted(kids.get(o["id"], []), key=lambda c: c["startTime"]):
            walk(c, depth + 1)

    roots = [o for o in rows if o.get("parentObservationId") not in ids]
    for r in sorted(roots, key=lambda r: r["startTime"]):
        walk(r, 0)


if __name__ == "__main__":
    rows = fetch(sys.argv[1])
    if "--json" in sys.argv:
        json.dump(rows, open(sys.argv[sys.argv.index("--json") + 1], "w"), indent=1)
    show(rows)
