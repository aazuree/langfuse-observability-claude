# tests/test_eval_hook.py
"""Tests for eval-hook.py"""
import os
import sys
import subprocess
import json
import tempfile

SCRIPT = os.path.join(os.path.dirname(__file__), "..", "eval-hook.py")


def test_missing_keys_exits_with_error():
    """Script should exit with error when LANGFUSE keys are missing."""
    env = {k: v for k, v in os.environ.items()
           if k not in ("LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY")}
    result = subprocess.run(
        [sys.executable, SCRIPT],
        capture_output=True, text=True, env=env
    )
    assert result.returncode != 0
    assert "LANGFUSE_PUBLIC_KEY" in result.stderr or "LANGFUSE_SECRET_KEY" in result.stderr


def test_help_flag():
    """--help should print usage and exit 0."""
    result = subprocess.run(
        [sys.executable, SCRIPT, "--help"],
        capture_output=True, text=True
    )
    assert result.returncode == 0
    assert "--dry-run" in result.stdout
    assert "--rescore" in result.stdout
    assert "--trace" in result.stdout
    assert "--limit" in result.stdout


def _load_module(tmp_path=None):
    """Import eval_hook module with optional STATE_DIR override."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("eval_hook", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["eval_hook"] = mod
    spec.loader.exec_module(mod)
    if tmp_path:
        mod.STATE_DIR = str(tmp_path / "eval")
    return mod


def test_is_scored_and_mark_scored(tmp_path):
    """is_scored and mark_scored use marker files correctly."""
    mod = _load_module(tmp_path)
    assert not mod.is_scored("trace-abc123")
    mod.mark_scored("trace-abc123")
    assert mod.is_scored("trace-abc123")


def test_parse_eval_response_categorical():
    mod = _load_module()
    cli_output = json.dumps({
        "type": "result",
        "subtype": "success",
        "result": '{"score": "completed", "reasoning": "Task was done."}'
    })
    result = mod.parse_eval_response(cli_output)
    assert result is not None
    assert result["score"] == "completed"
    assert result["reasoning"] == "Task was done."


def test_parse_eval_response_numeric():
    mod = _load_module()
    cli_output = json.dumps({
        "type": "result",
        "subtype": "success",
        "result": '{"score": 0.85, "reasoning": "Good quality."}'
    })
    result = mod.parse_eval_response(cli_output)
    assert result is not None
    assert result["score"] == 0.85


def test_parse_eval_response_null_score():
    mod = _load_module()
    cli_output = json.dumps({
        "type": "result",
        "subtype": "success",
        "result": '{"score": null, "reasoning": "No code written."}'
    })
    result = mod.parse_eval_response(cli_output)
    assert result is not None
    assert result["score"] is None


def test_parse_eval_response_with_preamble():
    mod = _load_module()
    cli_output = json.dumps({
        "type": "result",
        "subtype": "success",
        "result": 'Here is my evaluation:\n{"score": "failed", "reasoning": "Off track."}'
    })
    result = mod.parse_eval_response(cli_output)
    assert result is not None
    assert result["score"] == "failed"


def test_parse_eval_response_garbage():
    mod = _load_module()
    result = mod.parse_eval_response("not json at all")
    assert result is None


def test_render_template():
    template = "Request: {input}\nResponse: {output}\nJSON: {{\"score\": \"x\"}}"
    rendered = template.format(input="hello", output="world")
    assert "Request: hello" in rendered
    assert "Response: world" in rendered
    assert '{"score": "x"}' in rendered


def test_validate_categorical_score():
    mod = _load_module()
    evaluator = mod.EVALUATORS[0]  # task_completion
    assert mod.validate_score(evaluator, "completed") is True
    assert mod.validate_score(evaluator, "invalid") is False


def test_validate_numeric_score():
    mod = _load_module()
    evaluator = mod.EVALUATORS[2]  # code_quality
    assert mod.validate_score(evaluator, 0.85) is True
    assert mod.validate_score(evaluator, 1.5) is False
    assert mod.validate_score(evaluator, -0.1) is False
    assert mod.validate_score(evaluator, None) is True  # null = skip


def test_build_score_payload_categorical():
    mod = _load_module()
    evaluator = mod.EVALUATORS[0]  # task_completion
    payload = mod.build_score_payload(
        "trace-123", evaluator,
        {"score": "completed", "reasoning": "Done."}
    )
    assert payload["traceId"] == "trace-123"
    assert payload["name"] == "task_completion"
    assert payload["value"] == "completed"
    assert payload["dataType"] == "CATEGORICAL"
    assert payload["comment"] == "Done."


def test_build_score_payload_numeric():
    mod = _load_module()
    evaluator = mod.EVALUATORS[2]  # code_quality
    payload = mod.build_score_payload(
        "trace-456", evaluator,
        {"score": 0.85, "reasoning": "Good code."}
    )
    assert payload["traceId"] == "trace-456"
    assert payload["name"] == "code_quality"
    assert payload["value"] == 0.85
    assert payload["dataType"] == "NUMERIC"


def test_build_score_payload_null_returns_none():
    mod = _load_module()
    evaluator = mod.EVALUATORS[2]
    payload = mod.build_score_payload(
        "trace-789", evaluator,
        {"score": None, "reasoning": "No code."}
    )
    assert payload is None
