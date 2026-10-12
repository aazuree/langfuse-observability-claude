"""Tiny Langfuse public-API client for the spike. Usage:
    python3 docs/edits/spike_api.py GET '/traces?limit=1'
    python3 docs/edits/spike_api.py DELETE /traces '{"traceIds": ["..."]}'
"""
import base64
import json
import os
import sys
from urllib.error import HTTPError
from urllib.request import Request, urlopen

HOST = os.environ.get('LANGFUSE_HOST', 'http://localhost:4100')
PK = os.environ.get('LANGFUSE_PUBLIC_KEY', 'pk-lf-spike')
SK = os.environ.get('LANGFUSE_SECRET_KEY', 'sk-lf-spike')
AUTH = 'Basic ' + base64.b64encode(f'{PK}:{SK}'.encode()).decode()


def call(method, path, body=None, headers=None, raw=None):
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    req = Request(f'{HOST}/api/public{path}', data=data, method=method, headers={
        'Authorization': AUTH, 'Content-Type': 'application/json', **(headers or {})})
    try:
        with urlopen(req, timeout=60) as r:
            txt = r.read().decode()
            return r.status, (json.loads(txt) if txt else None)
    except HTTPError as e:
        txt = e.read().decode(errors='replace')
        try:
            return e.code, json.loads(txt)
        except ValueError:
            return e.code, txt


if __name__ == '__main__':
    m, p = sys.argv[1], sys.argv[2]
    b = json.loads(sys.argv[3]) if len(sys.argv) > 3 else None
    status, out = call(m, p, b)
    print(status)
    print(json.dumps(out, indent=1)[:int(os.environ.get('MAXC', 4000))])
