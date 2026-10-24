"""Tests for tools/cost_diff.py: two hook versions billed over the same transcripts."""
import importlib.util
import json
import os

_spec = importlib.util.spec_from_file_location(
    "cost_diff", os.path.join(os.path.dirname(__file__), "..", "tools", "cost_diff.py"))
cost_diff = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cost_diff)

SID = "11111111-1111-4111-8111-111111111111"


def _transcript(path):
    path.parent.mkdir(parents=True)
    entries = [
        {"type": "user", "uuid": "u1", "sessionId": SID, "timestamp": "2026-10-20T10:00:00Z",
         "cwd": "/x/repo", "message": {"role": "user", "content": "hi"}},
        {"type": "assistant", "uuid": "a1", "sessionId": SID, "timestamp": "2026-10-20T10:00:01Z",
         "message": {"id": "m1", "role": "assistant", "model": "claude-opus-4-8",
                     "stop_reason": "end_turn", "content": [{"type": "text", "text": "hello"}],
                     "usage": {"input_tokens": 1000, "output_tokens": 100}}},
    ]
    path.write_text("".join(json.dumps(e) + "\n" for e in entries))


def test_same_hook_bills_identically(tmp_path):
    projects = tmp_path / "projects"
    _transcript(projects / "p" / f"{SID}.jsonl")
    hook = cost_diff.audit_costs.DEFAULT_HOOK
    billed = cost_diff.bill(hook, projects)
    assert billed[SID] == {"cost": 0.0075, "generations": 1}
    report = cost_diff.diff(billed, cost_diff.bill(hook, projects))
    assert report["changed"] == 0


def test_cost_or_generation_changes_are_listed():
    old = {"a": {"cost": 1.0, "generations": 3}, "b": {"cost": 2.0, "generations": 2},
           "c": {"cost": 0.5, "generations": 1}}
    new = {"a": {"cost": 1.0, "generations": 3}, "b": {"cost": 1.5, "generations": 2},
           "c": {"cost": 0.5, "generations": 2}}
    report = cost_diff.diff(old, new)
    assert [r["session"] for r in report["rows"]] == ["b", "c"]
    assert report["rows"][0]["delta_usd"] == -0.5
    assert (report["old_usd"], report["new_usd"]) == (3.5, 3.0)


def test_checkout_hook_writes_every_hook_module(tmp_path):
    """HEAD has all three; older refs may lack the helper modules."""
    path = cost_diff.checkout_hook("HEAD", tmp_path)
    assert path == tmp_path / "langfuse-hook.py"
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted(cost_diff.HOOK_FILES)
