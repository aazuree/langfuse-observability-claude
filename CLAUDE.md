# Langfuse Observability for Claude Code

Self-hosted observability that captures Claude Code CLI interactions via the `Stop` hook.
Sends prompts, responses, tool calls, tokens, latency, and cost to a local Langfuse instance.

## Repository Rules

- **No personal information in committed files.** Never include absolute paths containing usernames (e.g. `/home/<user>/...`), real email addresses, or other personally identifiable information in any file that will be committed. Use generic placeholders like `<REPO_ROOT>`, `/path/to/...`, or `~` instead.
- **`docs/` is gitignored.** Plans and specs live locally only — never commit or track files under `docs/`.

## Quick Reference

```bash
# Start services
docker compose up -d

# Stop services (preserves data)
docker compose down

# Reset all data
docker compose down -v

# View hook logs
tail -f ~/.claude/langfuse-hook.log

# Re-ingest all sessions
PK=$(grep '^LANGFUSE_INIT_PROJECT_PUBLIC_KEY=' .env | cut -d= -f2)
SK=$(grep '^LANGFUSE_INIT_PROJECT_SECRET_KEY=' .env | cut -d= -f2)
LANGFUSE_PUBLIC_KEY=$PK LANGFUSE_SECRET_KEY=$SK python3 langfuse-hook.py --reprocess
```

- **Dashboard**: http://localhost:3000
- **Hook log**: `~/.claude/langfuse-hook.log` (auto-rotates at 10 MB)
- **State files**: `~/.claude/langfuse-state/<session_id>.offset` (parent), `<session_id>.subagents.json` (subagents)

## Architecture

```
Claude Code CLI
    | (Stop hook fires after each response)
    v
langfuse-hook.py --> POST --> Langfuse API (localhost:3000)
                                  |
                    PostgreSQL, ClickHouse, Redis, MinIO
```

All services bound to `127.0.0.1` only. API key never leaves the machine.

## Tech Stack

- **Hook script**: Python 3.8+, stdlib only (zero external deps)
- **Backend**: Langfuse v3 (web + worker), PostgreSQL 17, ClickHouse 24, Redis 7, MinIO
- **Deployment**: Docker Compose (6 services)
- **Setup**: `./setup.sh` (generates secrets, starts containers, configures hook)

## Project Structure

```
langfuse-hook.py      # Core hook script (~700 lines) - parses transcripts, sends to Langfuse
eval-hook.py          # LLM-as-a-Judge evaluator (~250 lines) - evaluates traces via claude CLI
docker-compose.yml    # Full Langfuse stack (6 services)
setup.sh              # One-command setup (generates .env, starts services, configures hook)
.env.example          # Template for environment variables
.env                  # Generated secrets (gitignored)
tests/                # Test suite
  test_eval_hook.py   # Unit tests for eval-hook.py
  test_subagent_tracking.py  # Unit tests for subagent cost tracking
```

## Running Tests

Use `uv` to run tests (handles virtualenv and dependency resolution automatically):

```bash
uv run pytest tests/ -v              # Run all tests
uv run pytest tests/test_subagent_tracking.py -v  # Run specific test file
uv run pytest tests/ -k "discover" -v  # Run tests matching pattern
```

## Code Conventions

- **No external Python dependencies** - stdlib only (urllib, json, base64, uuid, pathlib)
- Constants at top of `langfuse-hook.py`: `MAX_TEXT=10000`, `MAX_TOOL_IO=5000`, `MAX_LOG_BYTES=10MB`
- Secret redaction via `SECRET_PATTERNS` regex list before any data leaves the machine
- Session IDs sanitized via `sanitize_id()` to prevent path traversal
- Incremental processing: state files track processed line offsets per session
- Deterministic event IDs (UUID5) prevent duplicates on re-ingestion
- Batch sends in chunks of 50 events to Langfuse ingestion API

## Key Data Flow

1. Parse JSONL transcript from `~/.claude/projects/<project>/<session>.jsonl`
2. Group messages into user->assistant turns, extract tool calls
3. Deduplicate streaming updates (last message per `message.id`)
4. Compute usage (tokens), latency, TTFT, cost
5. Build Langfuse events (trace -> generation -> span hierarchy)
6. POST batch to `/api/public/ingestion`, save state offset

## Cost Model

Pricing is model-aware (per 1M tokens):

| Model | Input | Output | Cache Read | Cache Create |
|-------|-------|--------|------------|--------------|
| Opus | $15.00 | $75.00 | $1.50 | $18.75 |
| Sonnet | $3.00 | $15.00 | $0.30 | $3.75 |
| Haiku | $0.80 | $4.00 | $0.08 | $1.00 |

Set `REPORT_API_EQUIVALENT_COST = False` in `langfuse-hook.py` to report $0.

## Tags and Metadata

Each trace is enriched with:

**Tags** (filterable in Langfuse UI):
- `claude-code` — always present
- repo/project name — derived from `cwd` (e.g., `langfuse-observability`)
- model family — `opus`, `sonnet`, or `haiku`
- entrypoint — `cli` or other launch method

**Metadata** (structured key-value on trace):
- `git_branch`, `cli_version`, `entrypoint`, `repo_name`, `cwd`
- `turn_count`, `tool_calls_total`, `total_tokens`, `total_input_tokens`, `total_output_tokens`

Extracted from the first `type: "user"` entry in the JSONL transcript via `extract_session_metadata()`.

## Langfuse API Gotchas

- **Use `usageDetails` + `costDetails`, not `usage` + `totalCost`**. Top-level `totalCost` on `generation-create` is silently ignored by Langfuse v3. Cost must go in `costDetails`; token counts in `usageDetails`.
- **Explicit costs override auto-calculation**. When both `costDetails` and a matching built-in model exist, Langfuse uses the explicit values.
- **Built-in model pricing lags new models**. Langfuse v3.73.1 has no pricing for `claude-opus-4-6` or `claude-sonnet-4-6` — only older model IDs like `claude-opus-4-20250514`. This is why we send explicit costs.
- **Cache tokens are invisible in `usage`**. Only `usageDetails` (flexible map) surfaces `cache_read_input_tokens` and `cache_creation_input_tokens` as separate line items in the UI.
- **ClickHouse ingestion is async**. Observations may take 5-10s to appear after the ingestion API returns 201.

## Token Anatomy

Most of the cost comes from cache tokens, not input/output. A typical heavy session:

```
input:           471 tokens    $0.01   (0%)
output:       49,515 tokens    $3.71  (10%)
cache_read: 16.1M tokens      $24.21  (63%)   <-- biggest cost driver
cache_create:  545K tokens     $10.22  (27%)
```

The `input + output` count in the Langfuse UI can be misleadingly small. Always check `cache_read_input_tokens` in `usageDetails` for the real volume.

## Subagent Cost Tracking

When Claude Code spawns subagents via the Agent tool, the hook automatically discovers and ingests their transcripts as nested observations under the parent trace.

**How it works:**
1. Hook detects `Agent` tool_use events in parent assistant messages
2. Discovers matching subagent transcripts at `<session>/subagents/agent-{id}.jsonl`
3. Correlates by timestamp proximity (within `SUBAGENT_MATCH_WINDOW_S = 60` seconds)
4. Ingests subagent turns as generations nested under the Agent tool span
5. Rolls up per-subagent and total harness cost in trace metadata

**Langfuse hierarchy:**
```
Trace (parent session)
├── Generation (parent turn)
│   └── Span (Agent tool call)
│       ├── Generation (subagent turn 1)
│       │   └── Span (Read tool)
│       └── Generation (subagent turn 2)
│           └── Span (Bash tool)
└── [metadata: subagent_costs summary]
```

**Exclusions:** `aside_question` subagents are skipped (internal sidechain queries).

**State:** Subagent offsets stored in `~/.claude/langfuse-state/<session_id>.subagents.json` (separate from parent `.offset` for rollback safety).

**Tags:** Traces with subagents get `has-subagents` and `subagents:{count}` tags for dashboard filtering.

## Important Notes

- `.env` contains generated secrets - never commit it
- Hook errors are logged but never block Claude Code (async, fire-and-forget)
- First Langfuse login may need incognito window (stale CSRF tokens)
- `setup.sh` backs up existing `~/.claude/settings.json` before modifying hooks
- All ports are localhost-only by design - do not expose to network

## LLM-as-a-Judge Evaluator (Phase 2)

`eval-hook.py` evaluates each generation (turn) independently using `claude` CLI with
Haiku (Pro subscription). Follows Langfuse best practice of observation-level scoring —
scores are linked to specific generations via `observationId` and appear in the Scores
tab of each observation in the Langfuse UI.

Scoring is **opt-in** via `--score` to control cost. Without it, the script lists
traces but does not evaluate.

### Scores

| Score | Type | Values |
|-------|------|--------|
| `task_completion` | Categorical | completed, partial, failed |
| `response_quality` | Numeric | 0.0–1.0 |

### Filtering

- Slash commands (`/clear`, `/help`, `/compact`, etc.) are automatically skipped
- Turns with empty assistant output (tool-use-only) are skipped
- Turns with empty user input are skipped

### Usage

```bash
# List unscored traces (no scoring, safe to run)
PK=$(grep '^LANGFUSE_INIT_PROJECT_PUBLIC_KEY=' .env | cut -d= -f2)
SK=$(grep '^LANGFUSE_INIT_PROJECT_SECRET_KEY=' .env | cut -d= -f2)
LANGFUSE_PUBLIC_KEY=$PK LANGFUSE_SECRET_KEY=$SK python3 eval-hook.py

# Evaluate all unscored turns (opt-in)
LANGFUSE_PUBLIC_KEY=$PK LANGFUSE_SECRET_KEY=$SK python3 eval-hook.py --score

# Dry run (print scores, don't post)
LANGFUSE_PUBLIC_KEY=$PK LANGFUSE_SECRET_KEY=$SK python3 eval-hook.py --score --dry-run --limit 5

# Evaluate turns in a single trace
LANGFUSE_PUBLIC_KEY=$PK LANGFUSE_SECRET_KEY=$SK python3 eval-hook.py --score --trace trace-xxx

# Re-evaluate already-scored turns
LANGFUSE_PUBLIC_KEY=$PK LANGFUSE_SECRET_KEY=$SK python3 eval-hook.py --score --rescore

# Cron (every 30 min, max 20 traces)
*/30 * * * * LANGFUSE_PUBLIC_KEY=pk-xxx LANGFUSE_SECRET_KEY=sk-xxx python3 /path/to/eval-hook.py --score --limit 20 >> ~/.claude/langfuse-eval.log 2>&1
```

### Configuration

- `EVAL_DELAY_SECONDS = 1` — delay between CLI calls (top of `eval-hook.py`)
- `CLI_TIMEOUT_SECONDS = 30` — max wait per CLI call
- State files: `~/.claude/langfuse-state/eval/<observation-id>.scored`
- Log: `~/.claude/langfuse-eval.log` (auto-rotates at 10 MB)

### Backfill

`backfill-score-observations.py` re-posts existing trace-level scores with `observationId`
(no LLM calls). Use for one-time migration of old scores.

Scores appear on individual generations in the Langfuse dashboard — filter, trend, and aggregate from there.

## Hook-Level Scores (Phase 1)

Three heuristic scores are automatically attached to every trace during ingestion:

| Score | Type | Values | Source |
|-------|------|--------|--------|
| `session_type` | Categorical | bug-fix, feature, refactor, research, exploratory | First user message keyword matching |
| `token_efficiency` | Numeric | 0.0-1.0 | output_tokens / total_all_tokens |
| `task_completed` | Boolean | true/false | Last turn error/question detection |

These are deterministic (no LLM calls) and run on every Stop hook invocation.
Scores use deterministic UUIDs so re-ingestion (`--reprocess`) updates rather than duplicates.

### Classifier Details

**`session_type`** — Priority order: bug-fix > refactor > research > feature > exploratory.
Matches keyword patterns in the first user message. Fallback is `exploratory`.

**`token_efficiency`** — `output_tokens / (output + input + cache_read + cache_creation)`.
Low values indicate context-heavy sessions (lots of cache reads). High values indicate
productive output sessions. Rounded to 4 decimal places.

**`task_completed`** — Checks last turn for failure patterns ("couldn't complete",
"encountered an error") and trailing clarifying questions. Also checks last turn's
tool outputs for `[ERROR]` prefix. Returns `true` if no failure signals detected.
