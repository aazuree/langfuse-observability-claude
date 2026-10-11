# tests/helpers.py
"""Readers for the batch process_session sends.

Each turn is its own trace; session-wide rollups ride on the session-level
`session_cost_usd` score's metadata.
"""


def turn_traces(batch):
    return [e["body"] for e in batch if e["type"] == "trace-create"]


def session_score(batch):
    return next(e["body"] for e in batch
                if e["type"] == "score-create" and e["body"]["name"] == "session_cost_usd")


def session_rollups(batch):
    return session_score(batch)["metadata"]
