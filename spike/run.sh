#!/usr/bin/env bash
# One `claude -p` session with Claude Code's native OTel traces sent to the
# spike Langfuse stack. User/project settings are not loaded, so the
# production Langfuse hooks don't fire.
#
#   spike/run.sh LABEL PROMPT_FILE
#
# Env overrides: OTEL_TRACES_ENDPOINT (default: spike Langfuse), SPIKE_MODEL.
set -euo pipefail

SPIKE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LABEL="$1"
PROMPT_FILE="$(realpath "$2")"
OUT="$SPIKE_DIR/.last-run"
SANDBOX="$OUT/sandbox"
mkdir -p "$SANDBOX"
printf '# Sandbox\n\nThrowaway project for the OTel spike.\n\n- a\n- b\n- c\n' > "$SANDBOX/README.md"

PK="${LANGFUSE_PUBLIC_KEY:-pk-lf-spike}"
SK="${LANGFUSE_SECRET_KEY:-sk-lf-spike}"
AUTH_B64=$(printf '%s:%s' "$PK" "$SK" | base64 -w0)

export CLAUDE_CODE_ENABLE_TELEMETRY=1
export CLAUDE_CODE_ENHANCED_TELEMETRY_BETA=1
export OTEL_TRACES_EXPORTER=otlp
export OTEL_METRICS_EXPORTER=none
export OTEL_LOGS_EXPORTER=none
export OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT="${OTEL_TRACES_ENDPOINT:-http://127.0.0.1:4100/api/public/otel/v1/traces}"
export OTEL_EXPORTER_OTLP_TRACES_HEADERS="Authorization=Basic ${AUTH_B64},x-langfuse-ingestion-version=4"
export OTEL_TRACES_EXPORT_INTERVAL=1000
export OTEL_RESOURCE_ATTRIBUTES="deployment.environment=${SPIKE_ENV:-native-otel}"
export OTEL_LOG_USER_PROMPTS=1
export OTEL_LOG_ASSISTANT_RESPONSES=1
export OTEL_LOG_TOOL_DETAILS=1
export OTEL_LOG_TOOL_CONTENT=1
export OTEL_LOG_RAW_API_BODIES=1

SID=$(python3 -c 'import uuid; print(uuid.uuid4())')
START=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
cd "$SANDBOX"
set +e
claude -p --session-id "$SID" --model "${SPIKE_MODEL:-haiku}" \
  --setting-sources local --settings "$SPIKE_DIR/settings.json" \
  --allowedTools "Read" "Bash(wc:*)" "Bash(sleep:*)" "Bash(ls:*)" "Bash(echo:*)" "Glob" "Grep" "Agent" \
  < "$PROMPT_FILE" > "$OUT/$LABEL.out" 2>&1
RC=$?
set -e
END=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
printf '{"label":"%s","session_id":"%s","start":"%s","end":"%s","rc":%d}\n' \
  "$LABEL" "$SID" "$START" "$END" "$RC" | tee -a "$OUT/runs.jsonl"
