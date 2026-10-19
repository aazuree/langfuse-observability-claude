"""Transcript fields first seen in Claude Code 2.1.29x-2.1.300 that the hook
did not capture: the `cost-state` entry (Claude Code's own running cost),
`async_hook_response` attachments, `quotaLimits` / `apiError` on rate-limit
stubs, `system/scheduled_task_fire` and `scheduledTaskId`, `continued-in`,
`relocated`, `attributionAgent` and `perTurnEffort`."""
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

from helpers import session_rollups

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook", Path(__file__).resolve().parent.parent / "langfuse-hook.py"
)
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)


def _write(entries, tmp_path):
    f = tmp_path / "session.jsonl"
    f.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
    return str(f)


def _user(text, ts="2026-10-18T10:00:00Z", **extra):
    return {"type": "user", "timestamp": ts, "cwd": "/x/repo",
            "message": {"role": "user", "content": text}, **extra}


def _asst(mid, *, model="claude-opus-4-8", inp=5, out=3,
          ts="2026-10-18T10:00:01Z", text="ok", **extra):
    return {
        "type": "assistant", "timestamp": ts, "requestId": f"req-{mid}",
        "message": {
            "id": mid, "role": "assistant", "model": model,
            "content": [{"type": "text", "text": text}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": inp, "output_tokens": out},
        },
        **extra,
    }


# 2026-10-18T10:00:00Z, the first entry of the fixtures below, in epoch ms.
_START_MS = int(datetime(2026, 10, 18, 10, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)


def _cost_state(total=0.5, start_ms=_START_MS, **extra):
    return {
        "type": "cost-state", "totalCostUSD": total,
        "totalAPIDuration": 9000, "totalAPIDurationWithoutRetries": 7000,
        "totalToolDuration": 1200, "totalLinesAdded": 12, "totalLinesRemoved": 4,
        "totalDuration": 60000, "startTime": start_ms,
        "hasUnknownModelCost": False,
        "modelUsage": {
            "claude-opus-5-5": {
                "inputTokens": 10, "outputTokens": 20, "thinkingTokens": 5,
                "cacheReadInputTokens": 300, "cacheCreationInputTokens": 40,
                "webSearchRequests": 0, "costUSD": 0.1,
            },
            "claude-haiku-5-5": {
                "inputTokens": 1, "outputTokens": 2, "thinkingTokens": 0,
                "cacheReadInputTokens": 0, "cacheCreationInputTokens": 0,
                "webSearchRequests": 33, "costUSD": 0.4,
            },
        },
        **extra,
    }


def _hook_response(event="Stop", exit_code=0, stderr="", ts="2026-10-18T10:00:05Z"):
    return {"type": "attachment", "timestamp": ts, "attachment": {
        "type": "async_hook_response", "processId": "async_hook_1",
        "hookName": event, "hookEvent": event, "response": {},
        "stdout": "", "stderr": stderr, "exitCode": exit_code,
    }}


def _quota_stub(ts="2026-10-18T10:00:02Z", **extra):
    return {
        "type": "assistant", "timestamp": ts, "isApiErrorMessage": True,
        "apiErrorStatus": 429, "error": "rate_limit",
        "apiError": "usage_limit_reached",
        "message": {"id": "stub", "role": "assistant", "model": "<synthetic>",
                    "content": [{"type": "text", "text": "limit"}],
                    "usage": {"input_tokens": 0, "output_tokens": 0}},
        **extra,
    }


_QUOTA = {"status": "rejected", "resetsAt": 1791811800, "rateLimitType": "five_hour",
          "overageStatus": "rejected", "overageDisabledReason": "org_level_disabled",
          "isUsingOverage": False}


# ------------------------------------------------------------ per-turn fields

class TestPerTurnEffortFallback:
    def test_per_turn_effort_fills_a_missing_effort(self):
        turns = hook.build_turns([_user("hi"), _asst("m1", perTurnEffort="medium")])
        assert turns[0]["effort"] == "medium"

    def test_effort_is_kept_when_per_turn_effort_is_null(self):
        turns = hook.build_turns([_user("hi"),
                                  _asst("m1", effort="high", perTurnEffort=None)])
        assert turns[0]["effort"] == "high"

    def test_effort_wins_over_per_turn_effort(self):
        turns = hook.build_turns([_user("hi"),
                                  _asst("m1", effort="high", perTurnEffort="low")])
        assert turns[0]["effort"] == "high"


class TestAttributionAgentAndScheduledTurn:
    def test_attribution_agent_in_turn_signals(self):
        turns = hook.build_turns([_user("hi"),
                                  _asst("m1", attributionAgent="general-purpose")])
        assert hook.gen_metadata_turn_signals(turns[0])["attribution_agent"] == "general-purpose"

    def test_scheduled_task_id_from_the_opening_user_entry(self):
        turns = hook.build_turns([_user("run it", scheduledTaskId="a41e83b0"),
                                  _asst("m1")])
        assert hook.gen_metadata_turn_signals(turns[0])["scheduled_task_id"] == "a41e83b0"

    def test_scheduled_turns_rolled_up(self):
        turns = hook.build_turns([
            _user("run it", scheduledTaskId="a41e83b0"), _asst("m1"),
            _user("by hand", ts="2026-10-18T10:01:00Z"),
            _asst("m2", ts="2026-10-18T10:01:01Z"),
        ])
        assert hook.build_turn_signal_summary(turns)["scheduled_turns"] == 1

    def test_plain_turn_emits_neither(self):
        turns = hook.build_turns([_user("hi"), _asst("m1")])
        signals = hook.gen_metadata_turn_signals(turns[0])
        assert "attribution_agent" not in signals
        assert "scheduled_task_id" not in signals


# ------------------------------------------------------------ cost-state

class TestExtractCostState:
    def test_none_without_entry(self, tmp_path):
        assert hook.extract_cost_state(_write([_user("hi")], tmp_path)) is None

    def test_last_entry_wins_and_fields_map(self, tmp_path):
        out = hook.extract_cost_state(_write([_cost_state(total=0.2), _user("hi"),
                                              _cost_state(total=0.5)], tmp_path))
        assert out == {
            "total_cost_usd": 0.5,
            "api_duration_ms": 9000,
            "api_duration_without_retries_ms": 7000,
            "tool_duration_ms": 1200,
            "wall_duration_ms": 60000,
            "lines_added": 12,
            "lines_removed": 4,
            "has_unknown_model_cost": False,
            "covers_from": "2026-10-18T10:00:00Z",
            "partial": False,
            "by_model": {
                "claude-opus-5-5": {
                    "cost_usd": 0.1, "input_tokens": 10, "output_tokens": 20,
                    "thinking_tokens": 5, "cache_read_tokens": 300,
                    "cache_create_tokens": 40, "web_search_requests": 0,
                },
                "claude-haiku-5-5": {
                    "cost_usd": 0.4, "input_tokens": 1, "output_tokens": 2,
                    "thinking_tokens": 0, "cache_read_tokens": 0,
                    "cache_create_tokens": 0, "web_search_requests": 33,
                },
            },
        }

    def test_entry_without_a_total_is_ignored(self, tmp_path):
        bad = {"type": "cost-state", "modelUsage": "nope"}
        assert hook.extract_cost_state(_write([bad], tmp_path)) is None

    def test_missing_optional_fields_are_omitted(self, tmp_path):
        out = hook.extract_cost_state(_write(
            [{"type": "cost-state", "totalCostUSD": 1.25, "modelUsage": "nope"}], tmp_path))
        assert out == {"total_cost_usd": 1.25}

    def test_partial_when_the_process_started_after_the_first_entry(self, tmp_path):
        # A resumed session whose earlier process's cost was not restored.
        late = _START_MS + 3 * 60 * 1000
        out = hook.extract_cost_state(_write([_user("hi"), _cost_state(start_ms=late)], tmp_path))
        assert out["partial"] is True
        assert out["covers_from"] == "2026-10-18T10:03:00Z"

    def test_not_partial_within_two_minutes(self, tmp_path):
        out = hook.extract_cost_state(_write(
            [_user("hi"), _cost_state(start_ms=_START_MS + 90 * 1000)], tmp_path))
        assert out["partial"] is False


# ------------------------------------------------------------ async hooks

class TestExtractAsyncHookResponses:
    def test_none_without_responses(self, tmp_path):
        assert hook.extract_async_hook_responses(_write([_user("hi")], tmp_path)) is None

    def test_counts_failures_by_hook(self, tmp_path):
        out = hook.extract_async_hook_responses(_write([
            _hook_response("Stop", 0),
            _hook_response("Stop", 2, "python3: can't open file 'x.py'\n",
                           ts="2026-10-18T10:00:09Z"),
            _hook_response("SessionEnd", 0),
        ], tmp_path))
        assert out["count"] == 3
        assert out["failures"] == 1
        assert out["by_hook"] == {"Stop": {"count": 2, "failures": 1},
                                  "SessionEnd": {"count": 1, "failures": 0}}
        assert out["last_failure"] == {
            "hook": "Stop", "exit_code": 2,
            "stderr": "python3: can't open file 'x.py'",
            "timestamp": "2026-10-18T10:00:09Z",
        }

    def test_no_last_failure_when_all_succeeded(self, tmp_path):
        out = hook.extract_async_hook_responses(_write([_hook_response()], tmp_path))
        assert out == {"count": 1, "failures": 0,
                       "by_hook": {"Stop": {"count": 1, "failures": 0}}}

    def test_stderr_is_redacted_and_truncated(self, tmp_path):
        secret = "api_key=" + "A" * 24
        out = hook.extract_async_hook_responses(_write(
            [_hook_response("Stop", 1, secret + " " + "x" * 500)], tmp_path))
        stderr = out["last_failure"]["stderr"]
        assert "A" * 24 not in stderr
        assert len(stderr) <= 200 + len("...")


# ------------------------------------------------------------ rate limits

class TestExtractRateLimits:
    def test_none_without_quota_data(self, tmp_path):
        assert hook.extract_rate_limits(_write([_user("hi"), _asst("m1")], tmp_path)) is None

    def test_quota_limits_on_a_stub(self, tmp_path):
        out = hook.extract_rate_limits(_write([_quota_stub(quotaLimits=_QUOTA)], tmp_path))
        assert out == {
            "count": 1,
            "by_status": {"rejected": 1},
            "by_type": {"five_hour": 1},
            "last": {
                "status": "rejected", "type": "five_hour",
                "resets_at": "2026-10-12T13:30:00Z",
                "overage_status": "rejected",
                "overage_disabled_reason": "org_level_disabled",
                "is_using_overage": False,
                "timestamp": "2026-10-18T10:00:02Z",
            },
        }

    def test_falls_back_to_api_error_params(self, tmp_path):
        stub = _quota_stub(apiErrorParams={"rate_limit_info": dict(_QUOTA)})
        out = hook.extract_rate_limits(_write([stub], tmp_path))
        assert out["by_type"] == {"five_hour": 1}
        assert out["last"]["resets_at"] == "2026-10-12T13:30:00Z"

    def test_api_error_counted_in_api_error_messages(self, tmp_path):
        out = hook.extract_api_error_messages(_write([_quota_stub()], tmp_path))
        assert out["by_api_error"] == {"usage_limit_reached": 1}

    def test_by_api_error_absent_without_api_error(self, tmp_path):
        stub = _quota_stub()
        del stub["apiError"]
        assert "by_api_error" not in hook.extract_api_error_messages(_write([stub], tmp_path))


# ------------------------------------------------------------ scheduled tasks

def _fire(task="a41e83b0", ts="2026-10-11T22:29:00Z"):
    return {"type": "system", "subtype": "scheduled_task_fire", "timestamp": ts,
            "content": "Running scheduled task (Oct 11 11:29pm)", "isMeta": False,
            "taskId": task, "cron": "30 23 11 10 *", "prompt": "secret plan",
            "sessionKind": "bg", "entrypoint": "cli"}


class TestExtractScheduledTaskFires:
    def test_none_without_fires(self, tmp_path):
        assert hook.extract_scheduled_task_fires(_write([_user("hi")], tmp_path)) is None

    def test_fires_grouped_by_task(self, tmp_path):
        out = hook.extract_scheduled_task_fires(_write([
            _fire(), _fire(ts="2026-10-12T22:29:00Z"), _fire("b2", "2026-10-12T08:00:00Z"),
        ], tmp_path))
        assert out == {"count": 3, "tasks": {
            "a41e83b0": {"cron": "30 23 11 10 *", "fires": 2,
                         "first_at": "2026-10-11T22:29:00Z", "last_at": "2026-10-12T22:29:00Z"},
            "b2": {"cron": "30 23 11 10 *", "fires": 1,
                   "first_at": "2026-10-12T08:00:00Z", "last_at": "2026-10-12T08:00:00Z"},
        }}

    def test_prompt_is_not_stored(self, tmp_path):
        out = hook.extract_scheduled_task_fires(_write([_fire()], tmp_path))
        assert "secret plan" not in json.dumps(out)


# ------------------------------------------------------------ session links

class TestContinuedInAndRelocations:
    def test_continued_in_last_wins(self, tmp_path):
        path = _write([{"type": "continued-in", "continuedInSessionId": "s1"},
                       {"type": "continued-in", "continuedInSessionId": "s2"}], tmp_path)
        assert hook.extract_continued_in(path) == "s2"

    def test_continued_in_empty_without_entry(self, tmp_path):
        assert hook.extract_continued_in(_write([_user("hi")], tmp_path)) == ""

    def test_relocations(self, tmp_path):
        path = _write([{"type": "relocated", "relocatedCwd": "/a"},
                       {"type": "relocated", "relocatedCwd": "/b"},
                       {"type": "relocated", "relocatedCwd": "/a"}], tmp_path)
        assert hook.extract_relocations(path) == {"count": 3, "cwds": ["/a", "/b"],
                                                  "last_cwd": "/a"}

    def test_relocation_cwds_capped(self, tmp_path):
        path = _write([{"type": "relocated", "relocatedCwd": f"/d{i}"} for i in range(15)],
                      tmp_path)
        out = hook.extract_relocations(path)
        assert out["count"] == 15
        assert len(out["cwds"]) == 10
        assert out["last_cwd"] == "/d14"

    def test_relocations_none_without_entry(self, tmp_path):
        assert hook.extract_relocations(_write([_user("hi")], tmp_path)) is None


# ------------------------------------------------------------ end to end

class TestProcessSession:
    def _run(self, entries, tmp_path, monkeypatch, session_id="sess-300"):
        transcript = tmp_path / "sess.jsonl"
        transcript.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        captured = {}
        monkeypatch.setattr(hook, "send_to_langfuse",
                            lambda batch: captured.setdefault("batch", batch) or True)
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))
        hook.process_session(session_id, str(transcript), "/x/repo", final=True)
        return captured["batch"]

    def test_all_captures_land(self, tmp_path, monkeypatch):
        batch = self._run([
            _fire(ts="2026-10-18T09:59:59Z"),
            _user("run it", scheduledTaskId="a41e83b0"),
            _asst("m1"),
            _quota_stub(ts="2026-10-18T10:00:02Z", quotaLimits=_QUOTA),
            _hook_response("Stop", 2, "boom"),
            {"type": "relocated", "relocatedCwd": "/x/repo/.claude/worktrees/w"},
            {"type": "continued-in", "continuedInSessionId": "next-session"},
            _cost_state(total=0.5),
        ], tmp_path, monkeypatch)

        md = session_rollups(batch)
        tags = next(e for e in batch if e["type"] == "trace-create")["body"]["tags"]
        gen = next(e for e in batch if e["type"] == "generation-create")["body"]

        # opus-4-8: 5 in * $5/M + 3 out * $25/M
        hook_cost = 5 * 5 / 1e6 + 3 * 25 / 1e6
        assert md["cli_cost_state"]["total_cost_usd"] == 0.5
        assert md["cli_cost_state"]["hook_cost_usd"] == round(hook_cost, 6)
        assert md["cli_cost_state"]["gap_usd"] == round(0.5 - hook_cost, 6)
        assert md["async_hooks"]["failures"] == 1
        assert md["rate_limits"]["by_type"] == {"five_hour": 1}
        assert md["scheduled_tasks"]["count"] == 1
        assert md["continued_in"] == "next-session"
        assert md["relocations"]["count"] == 1
        assert md["turn_signals"]["scheduled_turns"] == 1
        assert gen["metadata"]["scheduled_task_id"] == "a41e83b0"

        for tag in ("has-hook-failures", "rate-limited", "scheduled-task", "continued"):
            assert tag in tags

        scores = {e["body"]["name"]: e["body"] for e in batch if e["type"] == "score-create"}
        assert scores["cli_cost_usd"]["value"] == 0.5
        assert scores["cli_cost_usd"]["id"] == hook.session_score_id("sess-300", "cli_cost_usd")

    def test_nothing_new_on_a_plain_session(self, tmp_path, monkeypatch):
        batch = self._run([_user("hi"), _asst("m1")], tmp_path, monkeypatch)
        md = session_rollups(batch)
        tags = next(e for e in batch if e["type"] == "trace-create")["body"]["tags"]
        for key in ("cli_cost_state", "async_hooks", "rate_limits", "scheduled_tasks",
                    "continued_in", "relocations"):
            assert key not in md
        for tag in ("has-hook-failures", "rate-limited", "scheduled-task", "continued"):
            assert tag not in tags
        assert not any(e["type"] == "score-create" and e["body"]["name"] == "cli_cost_usd"
                       for e in batch)

    def test_cli_cost_score_is_deleted_with_the_session(self):
        assert "cli_cost_usd" in hook.SESSION_SCORE_NAMES


# ------------------------------------------------------------ gap attribution

def _sub_asst(mid, ts, *, stop_reason, classifier=None, model="claude-sonnet-5-5", out=5,
              content=None):
    entry = {
        "type": "assistant", "timestamp": ts, "isSidechain": True,
        "message": {"id": mid, "role": "assistant", "model": model,
                    "stop_reason": stop_reason,
                    "content": content or [{"type": "text", "text": "x"}],
                    "usage": {"input_tokens": 3, "output_tokens": out}},
    }
    if classifier:
        entry["serverClassifierRequest"] = classifier
    return entry


def _tool_use_block(tid, name="Bash"):
    return [{"type": "tool_use", "id": tid, "name": name, "input": {}}]


def _tool_result_entry(tid, ts):
    return {"type": "user", "timestamp": ts,
            "message": {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": tid, "content": "ok"}]}}


class TestGapAttribution:
    """Signals that explain cli_cost_state.gap_usd (2026-10-18 census of 85
    sessions: $65.79 = WebFetch/WebSearch Haiku backend $35.28 + auto-mode
    classifier ~$19.9 + under-recorded subagent output $6.31 + the CLI's
    Sonnet 5.5 cache-read rate ~$4.26)."""

    def _session(self, tmp_path, monkeypatch, cost_state=None):
        parent = [
            _user("delegate it"),
            {**_asst("pm1", model="claude-opus-5-5", ts="2026-10-18T10:00:01Z",
                     serverClassifierRequest="cA"),
             "message": {"id": "pm1", "role": "assistant", "model": "claude-opus-5-5",
                         "stop_reason": "tool_use",
                         "content": _tool_use_block("toolu_agent1", "Agent"),
                         "usage": {"input_tokens": 5, "output_tokens": 3}}},
            _tool_result_entry("toolu_agent1", "2026-10-18T10:05:00Z"),
            _asst("pm2", model="claude-opus-5-5", ts="2026-10-18T10:05:01Z",
                  serverClassifierRequest="cB"),
            _asst("pm2", model="claude-opus-5-5", ts="2026-10-18T10:05:02Z",
                  serverClassifierRequest="cB"),
        ]
        if cost_state:
            parent.append(cost_state)
        transcript = tmp_path / "sess.jsonl"
        transcript.write_text("\n".join(json.dumps(e) for e in parent) + "\n")

        sub_dir = tmp_path / "sess" / "subagents"
        sub_dir.mkdir(parents=True)
        sub = [
            _user("subtask", ts="2026-10-18T10:00:02Z"),
            # Stream-start entries only (CC 2.1.289+): no final entry, stop_reason null.
            _sub_asst("sm1", "2026-10-18T10:00:03Z", stop_reason=None, classifier="cS1",
                      content=_tool_use_block("t1")),
            _tool_result_entry("t1", "2026-10-18T10:00:04Z"),
            _sub_asst("sm2", "2026-10-18T10:00:05Z", stop_reason=None, classifier="cS2",
                      content=_tool_use_block("t2")),
            _tool_result_entry("t2", "2026-10-18T10:00:06Z"),
            _sub_asst("sm3", "2026-10-18T10:00:07Z", stop_reason="end_turn"),
        ]
        (sub_dir / "agent-sub1.jsonl").write_text("\n".join(json.dumps(e) for e in sub) + "\n")
        (sub_dir / "agent-sub1.meta.json").write_text(json.dumps(
            {"agentType": "general-purpose", "description": "task", "toolUseId": "toolu_agent1"}))

        captured = {}
        monkeypatch.setattr(hook, "send_to_langfuse",
                            lambda batch: captured.setdefault("batch", batch) or True)
        monkeypatch.setattr(hook, "STATE_DIR", str(tmp_path / "state"))
        hook.process_session("sess-gap", str(transcript), "/x/repo", final=True)
        batch = captured["batch"]
        tags = next(e for e in batch if e["type"] == "trace-create")["body"]["tags"]
        return session_rollups(batch), tags

    def test_classifier_requests_counted_across_main_and_subagents(self, tmp_path, monkeypatch):
        md, _ = self._session(tmp_path, monkeypatch)
        # main: cA, cB (cB on two entries of one message); subagent: cS1, cS2
        assert md["classifier_requests"] == 4
        agent = md["subagent_costs"]["agents"][0]
        assert agent["classifier_requests"] == 2

    def test_partial_subagent_messages_flagged(self, tmp_path, monkeypatch):
        md, tags = self._session(tmp_path, monkeypatch)
        agent = md["subagent_costs"]["agents"][0]
        assert agent["partial_messages"] == 2
        assert md["subagent_costs"]["partial_messages"] == 2
        assert "subagent-usage-partial" in tags

    def test_side_model_cost_is_models_no_message_used(self, tmp_path, monkeypatch):
        md, _ = self._session(tmp_path, monkeypatch, cost_state=_cost_state(total=0.5))
        # opus-5-5 answers the main thread, the subagent is sonnet-5-5;
        # haiku-5-5 (the WebFetch/WebSearch backend) appears in no message.
        assert md["subagent_costs"]["agents"][0]["models"] == ["claude-sonnet-5-5"]
        assert md["cli_cost_state"]["side_model_cost_usd"] == 0.4

    def test_partial_cost_state_has_no_gap(self, tmp_path, monkeypatch):
        late = _START_MS + 60 * 60 * 1000
        md, _ = self._session(tmp_path, monkeypatch,
                              cost_state=_cost_state(total=0.5, start_ms=late))
        assert md["cli_cost_state"]["partial"] is True
        assert "gap_usd" not in md["cli_cost_state"]

    def test_final_last_message_is_not_partial(self, tmp_path):
        entries = [_user("go"),
                   _sub_asst("a", "2026-10-18T10:00:01Z", stop_reason="end_turn"),
                   _user("more", ts="2026-10-18T10:00:02Z"),
                   _sub_asst("b", "2026-10-18T10:00:03Z", stop_reason=None)]
        # "b" is the last message and may still be streaming: not counted.
        assert hook.count_partial_messages(entries) == 0
