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

- **Dashboard**: http://localhost:3100
- **Hook log**: `~/.claude/langfuse-hook.log` (auto-rotates at 10 MB)
- **State files**: `~/.claude/langfuse-state/<session_id>.offset` (parent), `<session_id>.subagents.json` (subagents)

## Architecture

```
Claude Code CLI
    | (SessionStart hook fires immediately on session open)
    v
session-start-hook.py --> POST --> Langfuse API (localhost:3100)
                          (trace-create: source + model tags)

    | (Stop hook fires after each response)
    v
langfuse-hook.py --> POST --> Langfuse API (localhost:3100)
                                  |
                    PostgreSQL, ClickHouse, Redis, MinIO

    | (StopFailure hook fires when a turn ends in API error)
    v
session-start-hook.py --> POST --> Langfuse API (trace-update: stop-failure tag)
```

All services bound to `127.0.0.1` only. API key never leaves the machine.

## Tech Stack

- **Hook script**: Python 3.8+, stdlib only (zero external deps)
- **Backend**: Langfuse v3 (web + worker), PostgreSQL 17, ClickHouse 24, Redis 7, MinIO
- **Deployment**: Docker Compose (6 services)
- **Setup**: `./setup.sh` (generates secrets, starts containers, configures hook)

## Project Structure

```
langfuse-hook.py                 # Core hook script - parses transcripts, sends to Langfuse
session-start-hook.py            # SessionStart + StopFailure hook - creates early traces, tags failures
docker-compose.yml               # Full Langfuse stack (6 services)
setup.sh                         # One-command setup (generates .env, starts services, configures hook)
.env.example                     # Template for environment variables
.env                             # Generated secrets (gitignored)
tests/
  test_langfuse_hook.py          # Core hook unit tests
  test_session_hooks.py          # SessionStart + StopFailure hook tests
  test_hook_scores.py            # Hook-level score classifier tests
  test_subagent_tracking.py      # Subagent cost tracking tests
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
- Constants at top of `langfuse-hook.py`: `MAX_TEXT=10000`, `MAX_TOOL_IO=5000`. Log rotation threshold: `MAX_LOG_BYTES=10MB` in `langfuse_common.py`
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

Pricing is model-aware (per 1M tokens). Source: [platform.claude.com/docs/en/about-claude/pricing](https://platform.claude.com/docs/en/about-claude/pricing) (last verified 2026-05-13).

| Model | Input | Output | Cache Read | Cache Write 5m | Cache Write 1h |
|-------|-------|--------|------------|----------------|----------------|
| Opus 4.7 / 4.6 / 4.5 | $5.00 | $25.00 | $0.50 | $6.25 | $10.00 |
| Opus 4.1 / 4.0 (legacy) | $15.00 | $75.00 | $1.50 | $18.75 | $30.00 |
| Sonnet (all versions) | $3.00 | $15.00 | $0.30 | $3.75 | $6.00 |
| Haiku 4.5 | $1.00 | $5.00 | $0.10 | $1.25 | $2.00 |
| Haiku 3.5 | $0.80 | $4.00 | $0.08 | $1.00 | $1.60 |
| Haiku 3 (deprecated) | $0.25 | $1.25 | $0.03 | $0.30 | $0.50 |

**Opus 4.7 tokenizer note:** Opus 4.7 ships with a new tokenizer that may produce up to 35% more tokens for the same input text vs. prior models. Per-token rates are unchanged, but absolute session cost for equivalent workloads on 4.7 can be meaningfully higher than on 4.6.

Cache write cost is split by tier when `cache_5m` / `cache_1h` are available in `usageDetails`; otherwise all cache_create is billed at the 5m rate.

### Pricing Multipliers

These stack multiplicatively on the base rates above (and apply uniformly across input, output, cache read, and cache write tiers):

- **Fast mode (`speed="fast"`)**: 6x premium on Opus 4.6 / 4.7 only (per spec — Opus 4.5 and Sonnet/Haiku are ineligible). Other models with `speed="fast"` keep base rates.
- **Data residency (`inference_geo="us"`)**: 1.1x on Opus 4.6+/Sonnet 4.6+. Other models do not support the `inference_geo` parameter; multiplier is not applied.
- **Fast + US-geo stack**: 6x × 1.1x = 6.6x.

### Server-side Tool Billing

- **Web search**: $0.01 per request (`$10 / 1,000 searches`). Billed via `costDetails.web_search` line item using `web_search_requests` from `usage.server_tool_use`. Added to turn total.
- **Web fetch**: free (token cost only).
- **Code execution**: $0.05/container-hour after 1,550 free hours/org/month. Not currently emitted by Claude Code; not billed.

Set `REPORT_API_EQUIVALENT_COST = False` in `langfuse-hook.py` to report $0.

**Keeping prices up to date:** Pricing is hardcoded in `calculate_turn_cost()` (`langfuse-hook.py`). When Anthropic releases new models or changes prices, update that function and the table above. Unknown models return $0 and emit a `[WARN]` in `langfuse-hook.log` — that's the signal to update. We send explicit costs rather than relying on Langfuse's built-in model table because Langfuse's table lags new model releases by days/weeks.

## Tags and Metadata

Each trace is enriched with:

**Tags** (filterable in Langfuse UI):
- `claude-code` — always present
- repo/project name — derived from `cwd` (e.g., `langfuse-observability`)
- model family — `opus`, `sonnet`, or `haiku`
- entrypoint — `cli` or other launch method
- `fast` — present if any turn used `/fast` mode
- `has-errors` — present if API errors occurred during the session
- `permission:<mode>` — current permission mode (`default`, `acceptEdits`, `plan`, `bypassPermissions`)
- `pr:<N>` — one tag per PR linked from the session (via `pr-link` transcript entries)
- `agent-name:{slug}` — present when `type: "agent-name"` entry exists (e.g., `agent-name:langfuse-usagedetails-fix`)
- `session-kind:{bg|fg}` — background job vs interactive foreground session (from `sessionKind` field on transcript entries; defaults to `fg` when absent on older transcripts)

**Trace Name precedence:**
1. `customTitle` from `type: "custom-title"` (user-set via in-CLI title command)
2. `aiTitle` from `type: "ai-title"` (Claude-generated short title once enough session context exists)
3. `agentName` from `type: "agent-name"` (auto-generated mid-session slug, e.g. `langfuse-usagedetails-fix`)
4. Truncated first non-synthetic user prompt (80 chars)
5. `{repo_name}/{git_branch}` composite — stable fallback when prompt is empty
6. `"Claude Code Session"` hardcoded fallback

The auto-generated 3-word slug (e.g. `goofy-frolicking-dove`) was removed from
JSONL transcripts in Claude Code v2.1.112. The `agent-name` entry (~30% into a session)
provides a stable, descriptive slug once the model identifies the task. Early hook fires
use the first prompt; once `agent-name` appears, subsequent fires update the trace name
via Langfuse's upsert-on-id behaviour.

**Trace Metadata** (structured key-value on trace):
- `git_branch`, `cli_version`, `entrypoint`, `repo_name`, `cwd`
- `turn_count`, `tool_calls_total`, `total_tokens`, `total_input_tokens`, `total_output_tokens`
- `api_errors` — error summary: `total_count`, `by_status` (HTTP codes), `first_error_at`, `last_error_at`
- `custom_title` — user-set session title (when present)
- `permission_mode` — last permission mode observed
- `pr_links` — list of `{number, url, repository, timestamp}` from `pr-link` entries
- `away_summaries` — list of `{content, timestamp}` from `system/away_summary` entries
- `agent_name` — auto-generated session slug from `type: "agent-name"` entry (null when absent)
- `ai_title` — Claude-generated short title from `type: "ai-title"` entry (null when absent)
- `session_kind` — `bg` or `fg` from `sessionKind` field (defaults to `fg`)
- `attachments` — `{count, by_type}` summary of `type: "attachment"` entries (hook outputs, file/image attachments). Only counts + types are captured; payloads are not (PII + size). Null when no attachments.
- `local_commands` — list of `{content, timestamp}` from `system/local_command` entries (slash-command stdout, e.g. `/compact` summaries). Wrapper tags stripped, content truncated to 200 chars and run through `redact_secrets`. Capped at 20 entries. Null when none.
- `file_snapshots` — `{snapshot_count, tracked_files_count}` from `file-history-snapshot` entries (null when no snapshots)
- `stop_hook` — `{total_hook_fires, total_duration_ms, max_duration_ms, hook_errors, prevented_continuation_count}` from `system/stop_hook_summary` entries (null when none)

Extracted from the first `type: "user"` entry in the JSONL transcript via `extract_session_metadata()`.
API errors extracted from `type: "system"` / `subtype: "api_error"` entries via `extract_api_errors()`.
Custom title, AI title, agent name, session kind, attachments, permission mode, PR links,
and away summaries are extracted via `extract_custom_title()`, `extract_ai_title()`,
`extract_agent_name()`, `extract_session_kind()`, `extract_attachments()`,
`extract_permission_mode()`, `extract_pr_links()`, and `extract_away_summaries()` respectively.

**Per-Generation Metadata** (on each generation):
- `speed` — `standard` or `fast` (from `/fast` toggle)
- `service_tier` — API routing tier
- `inference_geo` — inference region (e.g., `us-east-1`)
- `request_ids` — list of Anthropic request IDs for server-side correlation
- `web_search_requests`, `web_fetch_requests` — server-side tool use counts

**OpenTelemetry GenAI semantic-convention aliases** (also on each generation):
The hook emits standard `gen_ai.*` attributes so an OTLP collector or future
Langfuse Claude Code mapping can consume traces without a custom transform.
Zero behaviour change for the existing dashboard.
- `gen_ai.system = "anthropic"`, `gen_ai.provider.name = "anthropic"`
- `gen_ai.operation.name = "chat"`
- `gen_ai.conversation.id` — session ID
- `gen_ai.request.model`, `gen_ai.response.model`
- `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`
- `gen_ai.response.id` — first Anthropic request ID for the turn (omitted when absent)
- `gen_ai.response.finish_reasons` — `[stop_reason]` (omitted when absent)

Reference: [opentelemetry.io/docs/specs/semconv/gen-ai/](https://opentelemetry.io/docs/specs/semconv/gen-ai/)

**Per-Generation usageDetails** (extended):
- `cache_read` — cache read tokens (shared across tiers)
- `cache_create` — cache creation tokens (split by tier below)
- `cache_5m` — cache creation tokens with 5-minute TTL
- `cache_1h` — cache creation tokens with 1-hour TTL

## Extended Thinking

Extended thinking capture was removed (Claude Code v2.1.112+). As of that version, Claude Code no longer writes thinking text to transcript JSONL files — `thinking` blocks always have an empty `thinking` field. The `has-thinking` tag and `thinking_chars` metadata are no longer emitted.

## Langfuse API Gotchas

- **Use `usageDetails` + `costDetails`, not `usage` + `totalCost`**. Top-level `totalCost` on `generation-create` is silently ignored by Langfuse v3. Cost must go in `costDetails`; token counts in `usageDetails`.
- **Explicit costs override auto-calculation**. When both `costDetails` and a matching built-in model exist, Langfuse uses the explicit values.
- **Built-in model pricing lags new models**. Langfuse v3.73.1 has no pricing for `claude-opus-4-6` or `claude-sonnet-4-6` — only older model IDs like `claude-opus-4-20250514`. This is why we send explicit costs.
- **Cache tokens are invisible in `usage`**. Only `usageDetails` (flexible map) surfaces `cache_read` and `cache_create` as separate line items in the UI, with tier breakdowns in `cache_5m` and `cache_1h`.
- **ClickHouse ingestion is async**. Observations may take 5-10s to appear after the ingestion API returns 201.

## Token Anatomy

Most of the cost comes from cache tokens, not input/output. A typical heavy session:

```
input:           471 tokens    $0.01   (0%)
output:       49,515 tokens    $3.71  (10%)
cache_read: 16.1M tokens      $24.21  (63%)   <-- biggest cost driver
cache_create:  545K tokens     $10.22  (27%)
```

The `input + output` count in the Langfuse UI can be misleadingly small. Always check `cache_read` in `usageDetails` for the real volume.

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

**State:** Subagent offsets stored in `~/.claude/langfuse-state/<session_id>.subagents.json`. Both state files are saved atomically with the main `.offset` — only on successful send — so a timeout retry re-ingests subagent events rather than skipping them.

**Tags:** Traces with subagents get `has-subagents` and `subagents:{count}` tags for dashboard filtering.

## Important Notes

- `.env` contains generated secrets - never commit it
- Hook errors are logged but never block Claude Code (async, fire-and-forget)
- First Langfuse login may need incognito window (stale CSRF tokens)
- `setup.sh` backs up existing `~/.claude/settings.json` before modifying hooks
- All ports are localhost-only by design - do not expose to network

## Hook-Level Scores

Five heuristic scores are automatically attached to every trace during ingestion:

| Score | Type | Values | Source |
|-------|------|--------|--------|
| `session_type` | Categorical | bug-fix, feature, refactor, research, exploratory | First user message keyword matching |
| `token_efficiency` | Numeric | 0.0-1.0 | output_tokens / total_all_tokens |
| `task_completed` | Boolean | true/false | Last turn error/question detection |
| `cache_hit_rate` | Numeric | 0.0-1.0 | cache_read / (cache_read + cache_creation) |
| `cost_tier` | Categorical | cheap, moderate, expensive | Total session cost bucketing |
| `tool_diversity` | Numeric | 0.0-1.0 | unique_tools / total_tool_calls |
| `compaction_occurred` | Boolean | 0/1 | Any context compaction in session |

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

**`cache_hit_rate`** — Measures cache warmth. Formula: `cache_read / (cache_read + cache_creation)`.
0.0 = cache-miss session (only writes, no hits), 1.0 = fully warm (all cache hits). Useful for
identifying sessions that benefit from prompt caching vs. sessions that are doing cache
initialization.

Sessions with **no cache activity at all** (fresh session, no prior context) **omit the score
entirely** rather than emitting 0.0 — this keeps them distinct from genuine cache-miss sessions
in the dashboard. Filter by `cache_hit_rate IS NULL` to find cold sessions.

**`cost_tier`** — Buckets session cost for dashboard filtering. `cheap` (< $0.10), `moderate` ($0.10–$1.00),
`expensive` (≥ $1.00). Enables one-click filtering to find expensive sessions or identify
cost trends across models.
