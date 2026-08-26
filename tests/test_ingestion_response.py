# tests/test_ingestion_response.py
"""Tests for honest handling of the ingestion endpoint's 207 response.

Langfuse's batch ingestion endpoint answers `207 Multi-Status`: the HTTP status
says the request was accepted, and the *body* says what happened to each event:

    {"successes": [{"id": "...", "status": 201}, ...],
     "errors":    [{"id": "...", "status": 400, "message": "..."}]}

`send_to_langfuse` used to log a truncated prefix of that body and return True
whenever the HTTP call did not raise. Since the caller advances the transcript
line offset on True, any per-event rejection silently and permanently skipped
those turns — the next fire started after them.

Two failure kinds have to be told apart, because treating them alike breaks
something either way:

  * transient (5xx, 429, network) — retrying works, so do not advance.
  * permanent (400/401/403/404/422) — retrying can never work. Advancing loses
    those events, but *not* advancing wedges the session forever: every later
    fire would resend an ever-growing window that can never drain. Advance,
    and shout about it.

This matters beyond today: on the Langfuse v4 `events_only` cutover the
endpoint keeps returning 207 while rejecting every event type except
`score-create`, which is exactly the shape this guards against.
"""
import importlib.util
import json
import os
from unittest import mock

_spec = importlib.util.spec_from_file_location(
    "langfuse_hook",
    os.path.join(os.path.dirname(__file__), "..", "langfuse-hook.py"),
)
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)

import langfuse_common


def _resp(body: str, status: int = 207):
    class MockResp:
        def __init__(self):
            self.status = status

        def read(self):
            return body.encode()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    return MockResp()


def _ok_body(n=2):
    return json.dumps({"successes": [{"id": f"evt-{i}", "status": 201} for i in range(n)],
                       "errors": []})


def _err_body(*statuses):
    return json.dumps({
        "successes": [{"id": "evt-ok", "status": 201}],
        "errors": [{"id": f"evt-bad-{i}", "status": s, "message": f"boom {s}"}
                   for i, s in enumerate(statuses)],
    })


class TestClassifyIngestionErrors:
    def test_clean_body_has_no_failures(self):
        assert hook.classify_ingestion_errors(_ok_body()) == (0, 0, [])

    def test_body_without_an_errors_key_is_a_success(self):
        """Not every 2xx body carries the successes/errors shape."""
        assert hook.classify_ingestion_errors('{"ok": true}') == (0, 0, [])

    def test_400_is_permanent(self):
        transient, permanent, details = hook.classify_ingestion_errors(_err_body(400))
        assert (transient, permanent) == (0, 1)
        assert "400" in details[0]

    def test_5xx_is_transient(self):
        assert hook.classify_ingestion_errors(_err_body(503))[:2] == (1, 0)

    def test_429_is_transient(self):
        assert hook.classify_ingestion_errors(_err_body(429))[:2] == (1, 0)

    def test_auth_and_validation_codes_are_permanent(self):
        assert hook.classify_ingestion_errors(_err_body(401, 403, 422))[:2] == (0, 3)

    def test_mixed_kinds_are_counted_separately(self):
        assert hook.classify_ingestion_errors(_err_body(400, 503))[:2] == (1, 1)

    def test_unparseable_body_is_transient(self):
        """A proxy error page is not proof the events were rejected."""
        assert hook.classify_ingestion_errors("<html>502 Bad Gateway</html>")[:2] == (1, 0)

    def test_error_without_a_status_is_permanent(self):
        """Unknown shape: do not retry forever on something we cannot read."""
        body = json.dumps({"errors": [{"id": "evt-x", "message": "no status field"}]})
        assert hook.classify_ingestion_errors(body)[:2] == (0, 1)

    def test_details_carry_id_and_message_for_the_log(self):
        details = hook.classify_ingestion_errors(_err_body(400))[2]
        assert "evt-bad-0" in details[0]
        assert "boom 400" in details[0]


class TestSendToLangfuseHonoursTheBody:
    def _setup(self, monkeypatch, body, status=207):
        monkeypatch.setattr(hook, "urlopen", lambda req, timeout=15: _resp(body, status))
        monkeypatch.setattr(langfuse_common, "LANGFUSE_PUBLIC_KEY", "pk-test")
        monkeypatch.setattr(langfuse_common, "LANGFUSE_SECRET_KEY", "sk-test")

    def test_all_accepted_allows_the_caller_to_advance(self, monkeypatch):
        self._setup(monkeypatch, _ok_body())
        assert hook.send_to_langfuse([{"type": "trace"}]) is True

    def test_transient_rejection_blocks_advancing(self, monkeypatch):
        """The whole window is retried next fire; ids are deterministic, so
        re-sending upserts rather than duplicating."""
        self._setup(monkeypatch, _err_body(503))
        assert hook.send_to_langfuse([{"type": "trace"}]) is False

    def test_permanent_rejection_still_advances(self, monkeypatch):
        """Retrying a validation error can never succeed — advancing is the
        only thing that keeps the session from wedging."""
        self._setup(monkeypatch, _err_body(400))
        assert hook.send_to_langfuse([{"type": "trace"}]) is True

    def test_permanent_rejection_is_logged_as_an_error(self, monkeypatch):
        self._setup(monkeypatch, _err_body(400))
        logged = []
        monkeypatch.setattr(hook, "log", lambda m: logged.append(m))
        hook.send_to_langfuse([{"type": "trace"}])
        assert any("[ERROR]" in m for m in logged)
        assert any("evt-bad-0" in m for m in logged), "rejected id must reach the log"

    def test_a_transient_failure_in_any_chunk_blocks_advancing(self, monkeypatch):
        """Batches are chunked at 50; one bad chunk must not be masked."""
        bodies = iter([_ok_body(), _err_body(503), _ok_body()])
        calls = []

        def fake_urlopen(req, timeout=15):
            calls.append(1)
            return _resp(next(bodies))

        monkeypatch.setattr(hook, "urlopen", fake_urlopen)
        monkeypatch.setattr(langfuse_common, "LANGFUSE_PUBLIC_KEY", "pk-test")
        monkeypatch.setattr(langfuse_common, "LANGFUSE_SECRET_KEY", "sk-test")
        batch = [{"id": f"evt-{i}"} for i in range(120)]
        assert hook.send_to_langfuse(batch) is False
        assert len(calls) == 3

    def test_transient_wins_over_permanent(self, monkeypatch):
        """With both kinds present, retry: the transient ones may yet land."""
        self._setup(monkeypatch, _err_body(400, 503))
        assert hook.send_to_langfuse([{"type": "trace"}]) is False

    def test_network_error_still_blocks_advancing(self, monkeypatch):
        from urllib.error import URLError
        monkeypatch.setattr(hook, "urlopen", mock.Mock(side_effect=URLError("nope")))
        monkeypatch.setattr(langfuse_common, "LANGFUSE_PUBLIC_KEY", "pk-test")
        monkeypatch.setattr(langfuse_common, "LANGFUSE_SECRET_KEY", "sk-test")
        assert hook.send_to_langfuse([{"type": "trace"}]) is False

    def test_success_log_does_not_dump_the_whole_body(self, monkeypatch):
        """The old log truncated at 200 chars, which cut off `errors` — the one
        field that mattered. Successes are now summarised by count instead."""
        self._setup(monkeypatch, _ok_body(3))
        logged = []
        monkeypatch.setattr(hook, "log", lambda m: logged.append(m))
        hook.send_to_langfuse([{"id": "evt-0"}, {"id": "evt-1"}, {"id": "evt-2"}])
        assert any("accepted 3 events" in m for m in logged)
        assert not any("successes" in m for m in logged), "raw body must not be dumped"
