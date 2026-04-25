# Langfuse Observability for Claude Code

Self-hosted Langfuse that captures every Claude Code interaction via the `Stop` hook. No proxy, no third-party dependencies touching your API key.

## What gets captured

| Data | Langfuse Type |
|------|---------------|
| User prompts & assistant responses | Generation (per turn) |
| Tool calls (Bash, Read, Edit, Write, Grep...) | Span (with input + output) |
| Subagent invocations (Agent tool) | Nested generations + spans |
| Token usage (input/output/cache read/cache write) | Generation usage |
| Latency (per turn, per tool call) | startTime/endTime |
| Time to first token | completionStartTime |
| Cost estimate (Anthropic API equivalent rates) | costDetails |
| Per-subagent and total harness cost | Trace metadata |
| Session grouping | Trace per session |
| LLM-as-a-Judge evaluation | Scores (per observation) |

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

**Dashboard**: http://localhost:3100
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
langfuse-hook.py  ──POST──>  Langfuse API (localhost:3100)
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
├── eval-hook.py           # LLM-as-a-Judge evaluator (optional scoring)
├── setup.sh               # One-command setup (secrets, docker, hook config)
├── .env.example           # Template (safe to commit)
├── .env                   # Actual secrets (gitignored)
├── tests/
│   ├── test_eval_hook.py          # Unit tests for eval-hook
│   └── test_subagent_tracking.py  # Unit tests for subagent cost tracking
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
7. **Subagent tracking** — discovers subagent transcripts (from Agent tool invocations) via timestamp correlation, ingests them as nested observations, and rolls up per-subagent cost in trace metadata

## Services

| Service | Port | Access |
|---------|------|--------|
| langfuse-web | 3100 | localhost only |
| langfuse-worker | 3130 | localhost only |
| postgres | 5532 | localhost only |
| clickhouse | 8223, 9100 | localhost only |
| redis | 6479 | localhost only |
| minio | 9190 | localhost only |

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
curl -s http://localhost:3100/api/public/traces \
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

## Subagent Cost Tracking

When Claude Code spawns subagents via the Agent tool, the hook automatically discovers their transcripts at `<session>/subagents/agent-{id}.jsonl`, correlates them by timestamp, and ingests them as nested generations/spans under the parent Agent tool span.

Traces with subagents get `has-subagents` and `subagents:{count}` tags, and trace metadata includes a `subagent_costs` summary with per-agent cost breakdowns and total harness cost.

## LLM-as-a-Judge Evaluation

`eval-hook.py` scores each generation using Claude CLI (Haiku tier). Runs on-demand, not as part of the Stop hook:

```bash
PK=$(grep '^LANGFUSE_INIT_PROJECT_PUBLIC_KEY=' .env | cut -d= -f2)
SK=$(grep '^LANGFUSE_INIT_PROJECT_SECRET_KEY=' .env | cut -d= -f2)

# List unscored traces (safe, no LLM calls)
LANGFUSE_PUBLIC_KEY=$PK LANGFUSE_SECRET_KEY=$SK python3 eval-hook.py

# Score all unscored turns
LANGFUSE_PUBLIC_KEY=$PK LANGFUSE_SECRET_KEY=$SK python3 eval-hook.py --score
```

Scores: `task_completion` (categorical) and `response_quality` (0.0-1.0), linked to individual observations.

## Running Tests

```bash
uv run pytest tests/ -v
```

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
