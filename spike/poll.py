"""Record when each observation first becomes visible in Langfuse.

    python3 spike/poll.py SECONDS OUT.jsonl [FROM_ISO]

Polls /v2/observations every 2 s. One line per newly seen observation:
seen_at, id, name, type, startTime, endTime, sessionId.
"""
import json
import sys
import time
from datetime import datetime, timezone

from api import call

secs, out_path = float(sys.argv[1]), sys.argv[2]
since = sys.argv[3] if len(sys.argv) > 3 else datetime.now(timezone.utc).isoformat()
seen = set()
deadline = time.time() + secs
with open(out_path, "a") as out:
    while time.time() < deadline:
        status, body = call("GET", f"/v2/observations?limit=1000&fields=core,basic&fromStartTime={since}")
        now = datetime.now(timezone.utc).isoformat()
        if status == 200:
            for o in body["data"]:
                if o["id"] not in seen:
                    seen.add(o["id"])
                    out.write(json.dumps({"seen_at": now, "id": o["id"], "name": o.get("name"),
                                          "type": o.get("type"), "start": o.get("startTime"),
                                          "end": o.get("endTime"), "session": o.get("sessionId")}) + "\n")
                    out.flush()
        time.sleep(2)
