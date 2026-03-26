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
1. Prompts for your admin email and password
2. Generates `.env` with random secrets (including unique project API keys)
3. Starts Langfuse via Docker Compose (6 services)
4. Backs up existing `~/.claude/settings.json` (if present) and configures the `Stop` hook

**Dashboard**: http://localhost:3000
**Login**: the credentials you entered during setup

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
| langfuse-web | 3000 | localhost only |
| langfuse-worker | 3030 | localhost only |
| postgres | 5432 | localhost only |
| clickhouse | 8123, 9000 | localhost only |
| redis | 6379 | localhost only |
| minio | 9090 | localhost only |

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

# Query traces (use the project keys printed during setup)
curl -s http://localhost:3000/api/public/traces \
  -u "YOUR_PUBLIC_KEY:YOUR_SECRET_KEY"
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
            "command": "LANGFUSE_PUBLIC_KEY=<your-pk> LANGFUSE_SECRET_KEY=<your-sk> python3 /path/to/langfuse-hook.py"
          }
        ]
      }
    ]
  }
}
```

Replace `<your-pk>` and `<your-sk>` with the project keys printed during `./setup.sh`, and update the path to match where you cloned the repo.

## Re-ingesting Past Sessions

If you set up Langfuse after already using Claude Code, or need to rebuild after a data reset:

```bash
# Source your project keys from .env
PK=$(grep '^LANGFUSE_INIT_PROJECT_PUBLIC_KEY=' .env | cut -d= -f2)
SK=$(grep '^LANGFUSE_INIT_PROJECT_SECRET_KEY=' .env | cut -d= -f2)

# Reprocess all sessions from ~/.claude/projects/
LANGFUSE_PUBLIC_KEY=$PK LANGFUSE_SECRET_KEY=$SK python3 langfuse-hook.py --reprocess
```

This finds all transcript files, deletes any existing traces to avoid duplicates, and re-ingests everything from scratch.

## Cost Estimation

Set `REPORT_API_EQUIVALENT_COST = True` in `langfuse-hook.py` (default) to report what each turn would cost at Anthropic API rates. Useful for tracking usage even on a Pro subscription where the actual marginal cost is $0.

## Security

- All ports are bound to `127.0.0.1` (localhost only) — not accessible from the network
- Secrets (database passwords, API keys, encryption keys) are randomly generated per installation
- Admin credentials are set interactively during setup — no hardcoded defaults
- The hook script redacts common secret patterns (API keys, tokens, private keys, passwords) before sending data to Langfuse
- Session IDs are sanitized to prevent path traversal
- Log files are automatically rotated at 10 MB
- Existing `~/.claude/settings.json` is backed up before modification

## Gotchas

- **Langfuse init vars**: `LANGFUSE_INIT_USER_EMAIL` must be valid email format, `LANGFUSE_INIT_PROJECT_NAME` must not have spaces. Validation errors are generic with no detail.
- **First login**: If the browser shows an error, try an incognito window (stale CSRF tokens).
- **Hook does not block Claude Code**: runs asynchronously; if Langfuse is down, errors go to `~/.claude/langfuse-hook.log`.
- **Secret redaction**: The hook redacts common patterns but cannot catch all secrets. Avoid pasting raw credentials into Claude Code prompts.

## Why Not LiteLLM Proxy?

LiteLLM was rejected due to 17 CVEs (RCE, SSRF, SQLi, API key leaks) and an active supply chain attack on PyPI (March 2026). The hooks approach keeps your Anthropic API key out of any intermediary.
