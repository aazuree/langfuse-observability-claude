"""Pinned billing for real sessions, scrubbed (tests/golden, tools/make_cost_golden.py).

Each case is a set of real transcripts reduced to what billing reads. The
hook must bill exactly what `expected.json` pins, and the audit's naive
recount must agree with it. A change that moves any of these numbers is a
billing change: run tools/cost_diff.py, explain the delta in the PR, then
re-pin with `python3 tools/make_cost_golden.py --update`.
"""
import importlib.util
import json
import os
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "make_cost_golden", os.path.join(os.path.dirname(__file__), "..", "tools", "make_cost_golden.py"))
make_cost_golden = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(make_cost_golden)

CASES = sorted(p for p in (Path(__file__).parent / "golden").iterdir() if (p / "expected.json").exists())


@pytest.mark.parametrize("case", CASES, ids=[c.name for c in CASES])
def test_hook_bills_what_the_case_pins(case):
    expected = json.loads((case / "expected.json").read_text())["sessions"]
    billed = make_cost_golden.bill(case / "projects")
    assert billed.keys() == expected.keys()
    for sid, want in expected.items():
        got = billed[sid]
        assert got["hook_usd"] == pytest.approx(want["hook_usd"], abs=1e-6), sid
        assert got["generations"] == want["generations"], sid


@pytest.mark.parametrize("case", CASES, ids=[c.name for c in CASES])
def test_recount_agrees_with_the_hook(case):
    for sid, got in make_cost_golden.bill(case / "projects").items():
        assert got["recount_usd"] == pytest.approx(got["hook_usd"], abs=0.005), sid


@pytest.mark.parametrize("case", CASES, ids=[c.name for c in CASES])
def test_audit_finds_nothing(case, monkeypatch):
    monkeypatch.setattr(make_cost_golden.audit_costs, "ACTIVE_S", -1)
    report = make_cost_golden.audit_costs.audit(case / "projects", make_cost_golden.audit_costs.DEFAULT_HOOK, None)
    assert report["findings"] == []


def test_there_are_cases():
    assert len(CASES) >= 6
