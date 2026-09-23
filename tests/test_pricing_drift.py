# tests/test_pricing_drift.py
"""Tests for tools/check_pricing_drift.py.

All tests run offline against synthetic feeds — the drift checker must never
require network access to be verifiable.
"""
import importlib.util
import json
import os
from datetime import datetime, timezone

import pytest

_spec = importlib.util.spec_from_file_location(
    "check_pricing_drift",
    os.path.join(os.path.dirname(__file__), "..", "tools", "check_pricing_drift.py"),
)
drift = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(drift)

hook = drift.load_hook()

MTOK = 1_000_000
# Either side of the cancelled 2026-09-01 Sonnet 5 price step-up.
INTRO_TS = "2026-07-15T00:00:00+00:00"
STANDARD_TS = "2026-09-15T00:00:00+00:00"


def feed_entry(inp, out, read, write5m, write1h):
    """Build a feed entry from dollars-per-1M rates."""
    return {
        "input_cost_per_token": inp / MTOK,
        "output_cost_per_token": out / MTOK,
        "cache_read_input_token_cost": read / MTOK,
        "cache_creation_input_token_cost": write5m / MTOK,
        "cache_creation_input_token_cost_above_1hr": write1h / MTOK,
    }


class TestProbeRates:
    def test_probe_recovers_opus_5_rates(self):
        rates = drift.probe_rates(hook, "claude-opus-5", INTRO_TS)
        assert rates == pytest.approx({
            "input": 5.0, "output": 25.0, "cache_read": 0.50,
            "cache_write_5m": 6.25, "cache_write_1h": 10.0,
        })

    def test_probe_recovers_fable_rates(self):
        rates = drift.probe_rates(hook, "claude-fable-5", INTRO_TS)
        assert rates == pytest.approx({
            "input": 10.0, "output": 50.0, "cache_read": 1.0,
            "cache_write_5m": 12.5, "cache_write_1h": 20.0,
        })

    def test_probe_sonnet_5_flat_across_dates(self):
        """Sonnet 5 kept its $2/$10 launch rate past 2026-09-01, so the probe
        resolves the same rate either side of the old cutoff."""
        before = drift.probe_rates(hook, "claude-sonnet-5", INTRO_TS)
        after = drift.probe_rates(hook, "claude-sonnet-5", STANDARD_TS)
        assert before["input"] == after["input"] == pytest.approx(2.0)
        assert before["output"] == after["output"] == pytest.approx(10.0)

    def test_probe_reports_zero_for_unknown_model(self, monkeypatch):
        monkeypatch.setattr(hook, "log", lambda msg: None)
        rates = drift.probe_rates(hook, "claude-not-a-model", INTRO_TS)
        assert all(v == 0.0 for v in rates.values())


class TestCompare:
    def test_matching_feed_reports_ok(self, monkeypatch):
        monkeypatch.setattr(drift, "MODELS", ["claude-opus-5"])
        feed = {"claude-opus-5": feed_entry(5.0, 25.0, 0.50, 6.25, 10.0)}
        rows, count = drift.compare(hook, feed, INTRO_TS)
        assert count == 0
        assert rows[0]["status"] == "OK"

    def test_changed_rate_is_detected(self, monkeypatch):
        monkeypatch.setattr(drift, "MODELS", ["claude-opus-5"])
        feed = {"claude-opus-5": feed_entry(6.0, 25.0, 0.50, 6.25, 10.0)}
        rows, count = drift.compare(hook, feed, INTRO_TS)
        assert count == 1
        assert rows[0]["status"] == "DRIFT"
        assert rows[0]["deltas"]["input"]["ours"] == pytest.approx(5.0)
        assert rows[0]["deltas"]["input"]["feed"] == pytest.approx(6.0)
        assert "output" not in rows[0]["deltas"]

    def test_cache_tier_drift_is_detected(self, monkeypatch):
        monkeypatch.setattr(drift, "MODELS", ["claude-opus-5"])
        feed = {"claude-opus-5": feed_entry(5.0, 25.0, 0.50, 6.25, 11.0)}
        rows, count = drift.compare(hook, feed, INTRO_TS)
        assert count == 1
        assert rows[0]["deltas"]["cache_write_1h"]["feed"] == pytest.approx(11.0)

    def test_model_absent_from_feed_is_skip_not_drift(self, monkeypatch):
        monkeypatch.setattr(drift, "MODELS", ["claude-mythos-5"])
        rows, count = drift.compare(hook, {}, INTRO_TS)
        assert count == 0
        assert rows[0]["status"] == "SKIP"

    def test_axis_absent_from_feed_is_not_drift(self, monkeypatch):
        """A feed that omits the 1h cache tier must not be read as a rate change."""
        monkeypatch.setattr(drift, "MODELS", ["claude-opus-5"])
        entry = feed_entry(5.0, 25.0, 0.50, 6.25, 10.0)
        del entry["cache_creation_input_token_cost_above_1hr"]
        rows, count = drift.compare(hook, {"claude-opus-5": entry}, INTRO_TS)
        assert count == 0
        assert rows[0]["status"] == "OK"
        assert "note" in rows[0]["deltas"]["cache_write_1h"]

    def test_tolerance_ignores_float_noise(self, monkeypatch):
        monkeypatch.setattr(drift, "MODELS", ["claude-opus-5"])
        feed = {"claude-opus-5": feed_entry(5.0000001, 25.0, 0.50, 6.25, 10.0)}
        rows, count = drift.compare(hook, feed, INTRO_TS)
        assert count == 0

    def test_current_table_matches_a_snapshot_of_the_real_feed(self, monkeypatch):
        """Guards every priced model at once against a pinned copy of the
        upstream feed's values (verified against platform.claude.com 2026-08-01)."""
        monkeypatch.setattr(drift, "MODELS", [
            "claude-opus-5", "claude-opus-4-8", "claude-opus-4-6",
            "claude-sonnet-4-6", "claude-haiku-4-5", "claude-fable-5",
        ])
        feed = {
            "claude-opus-5":     feed_entry(5.0, 25.0, 0.50, 6.25, 10.0),
            "claude-opus-4-8":   feed_entry(5.0, 25.0, 0.50, 6.25, 10.0),
            "claude-opus-4-6":   feed_entry(5.0, 25.0, 0.50, 6.25, 10.0),
            "claude-sonnet-4-6": feed_entry(3.0, 15.0, 0.30, 3.75, 6.0),
            "claude-haiku-4-5":  feed_entry(1.0, 5.0, 0.10, 1.25, 2.0),
            "claude-fable-5":    feed_entry(10.0, 50.0, 1.00, 12.50, 20.0),
        }
        rows, count = drift.compare(hook, feed, INTRO_TS)
        assert count == 0, [r for r in rows if r["status"] == "DRIFT"]


class TestFetchFeed:
    def test_reads_local_path(self, tmp_path):
        payload = {"claude-opus-5": feed_entry(5.0, 25.0, 0.50, 6.25, 10.0)}
        path = tmp_path / "feed.json"
        path.write_text(json.dumps(payload))
        assert drift.fetch_feed(str(path)) == payload

    def test_missing_local_path_is_not_silently_treated_as_url(self, tmp_path):
        with pytest.raises(Exception):
            drift.fetch_feed(str(tmp_path / "does-not-exist.json"))
