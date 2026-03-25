# Langfuse Observability for Claude Code

Self-hosted Langfuse that captures every Claude Code interaction via the `Stop` hook. No proxy, no third-party dependencies touching your API key.

## What gets captured

| Data | Langfuse Type |
|------|---------------|
| User prompts & assistant responses | Generation (per turn) |
| Tool calls (Bash, Read, Edit, Write, Grep...) | Span (with input + output) |
| Token usage (input/output/cache read/cache write) | Generation usage |
| Latency (per turn, per tool call) | startTime/endTime |
| Time to first token | completionStartTime |
| Cost estimate (Anthropic API equivalent rates) | totalCost |
| Session grouping | Trace per session |

## Quick Start

```bash
git clone git@github.com:aazuree/langfuse-observability.git
cd langfuse-observability
./setup.sh
```

The setup script:
1. Generates `.env` with random secrets
2. Starts Langfuse via Docker Compose (6 services)
3. Configures the Claude Code `Stop` hook in `~/.claude/settings.json`

**Dashboard**: http://localhost:3000
**Login**: `admin@example.com` / `changeme12345678`

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Claude Code CLI
- `openssl` (for secret generation)

## Architecture

```
Claude Code CLI
      |
      | (Stop hook fires after each response)
      v
langfuse-hook.py  ──POST──>  Langfuse API (localhost:3000)
                                    |
                              ┌─────┴─────┐
                              │  Langfuse  │
                              │  Dashboard │
                              └─────┬─────┘
                                    |
                    ┌───────┬───────┼───────┬────────┐
                    │       │       │       │        │
                PostgreSQL  ClickHouse  Redis  MinIO
```

## File Structure

```
langfuse-observability/
├── docker-compose.yml     # Langfuse stack (6 services)
├── langfuse-hook.py       # Stop hook: transcript -> Langfuse ingestion
├── setup.sh               # One-command setup (secrets, docker, hook config)
├── .env.example           # Template (safe to commit)
├── .env                   # Actual secrets (gitignored)
└── README.md
```

## How It Works

The hook script (`langfuse-hook.py`) is invoked by Claude Code after each assistant response:

1. **Incremental processing** — tracks a line offset per session, only sends new messages each turn
2. **Turn detection** — groups transcript entries into user→assistant turns
3. **Tool call matching** — pairs `tool_use` blocks with their `tool_result` responses via `tool_use_id`
4. **Token deduplication** — assistant messages with the same `message.id` are streaming updates; takes the last for final usage
5. **Timing** — uses message timestamps for latency, `turn_duration` entries for total turn time, first assistant timestamp for TTFT
6. **Cost** — computes equivalent Anthropic API cost from token counts (configurable via `REPORT_API_EQUIVALENT_COST` flag)

## Services

| Service | Port | Access |
|---------|------|--------|
| langfuse-web | 3000 | Public (dashboard + API) |
| langfuse-worker | 3030 | localhost only |
| postgres | 5432 | localhost only |
| clickhouse | 8123, 9000 | localhost only |
| redis | 6379 | localhost only |
| minio | 9090 | Public (S3 API) |

## Operations

```bash
# Start
docker compose up -d

# Stop (preserves data)
docker compose down

# Stop and delete all data
docker compose down -v

# View logs
docker compose logs langfuse-web -f
tail -f ~/.claude/langfuse-hook.log

# Query traces
curl -s http://localhost:3000/api/public/traces \
  -u "pk-lf-claude-code:sk-lf-claude-code"
```

## Hook Configuration

The Stop hook in `~/.claude/settings.json`:

```json
{
  "hooks": {
    "Stop": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "LANGFUSE_PUBLIC_KEY=pk-lf-claude-code LANGFUSE_SECRET_KEY=sk-lf-claude-code python3 /path/to/langfuse-hook.py"
          }
        ]
      }
    ]
  }
}
```

Update the path to match where you cloned the repo.

## Cost Estimation

Set `REPORT_API_EQUIVALENT_COST = True` in `langfuse-hook.py` (default) to report what each turn would cost at Anthropic API rates. Useful for tracking usage even on a Pro subscription where the actual marginal cost is $0.

## Gotchas

- **Langfuse init vars**: `LANGFUSE_INIT_USER_EMAIL` must be valid email format, `LANGFUSE_INIT_PROJECT_NAME` must not have spaces. Validation errors are generic with no detail.
- **First login**: If the browser shows an error, try an incognito window (stale CSRF tokens).
- **Hook does not block Claude Code**: runs asynchronously; if Langfuse is down, errors go to `~/.claude/langfuse-hook.log`.

## Why Not LiteLLM Proxy?

LiteLLM was rejected due to 17 CVEs (RCE, SSRF, SQLi, API key leaks) and an active supply chain attack on PyPI (March 2026). The hooks approach keeps your Anthropic API key out of any intermediary.
