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
    | (Stop hook fires after each response)
    v
langfuse-hook.py --> POST --> Langfuse API (localhost:3100)
                          (trace-create from transcript: turns, tokens, cost)
                                  |
                    PostgreSQL, ClickHouse, Redis, MinIO

    | (StopFailure hook fires when a turn ends in API error)
    v
session-start-hook.py --> POST --> Langfuse API (trace-update: stop-failure tag)
```

All services bound to `127.0.0.1` only. API key never leaves the machine.

## Tech Stack

- **Hook script**: Python 3.8+, stdlib only (zero external deps)
- **Backend**: Langfuse v3 (web + worker), PostgreSQL 18, ClickHouse 26, Redis 8, MinIO
- **Deployment**: Docker Compose (6 services)
- **Setup**: `./setup.sh` (generates secrets, starts containers, configures hook)

## Project Structure

```
langfuse-hook.py                 # Core hook script - parses transcripts, sends to Langfuse
session-start-hook.py            # StopFailure hook - tags traces with stop-failure + last API error
docker-compose.yml               # Full Langfuse stack (6 services)
setup.sh                         # One-command setup (generates .env, starts services, configures hook)
.env.example                     # Template for environment variables
.env                             # Generated secrets (gitignored)
tests/
  test_langfuse_hook.py          # Core hook unit tests
  test_session_hooks.py          # StopFailure hook tests
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

Pricing is model-aware (per 1M tokens). Source: [platform.claude.com/docs/en/about-claude/pricing](https://platform.claude.com/docs/en/about-claude/pricing) (last verified 2026-06-30).

| Model | Input | Output | Cache Read | Cache Write 5m | Cache Write 1h |
|-------|-------|--------|------------|----------------|----------------|
| Fable 5 | $10.00 | $50.00 | $1.00 | $12.50 | $20.00 |
| Opus 4.8 / 4.7 / 4.6 / 4.5 | $5.00 | $25.00 | $0.50 | $6.25 | $10.00 |
| Opus 4.1 / 4.0 (legacy) | $15.00 | $75.00 | $1.50 | $18.75 | $30.00 |
| Sonnet 5 — intro (through 2026-08-31) | $2.00 | $10.00 | $0.20 | $2.50 | $4.00 |
| Sonnet 5 — standard (from 2026-09-01) | $3.00 | $15.00 | $0.30 | $3.75 | $6.00 |
| Sonnet 4.6 / 4.5 / 4 | $3.00 | $15.00 | $0.30 | $3.75 | $6.00 |
| Haiku 4.5 | $1.00 | $5.00 | $0.10 | $1.25 | $2.00 |
| Haiku 3.5 | $0.80 | $4.00 | $0.08 | $1.00 | $1.60 |
| Haiku 3 (deprecated) | $0.25 | $1.25 | $0.03 | $0.30 | $0.50 |

**Sonnet 5 intro pricing is date-aware.** `claude-sonnet-5` is billed at the introductory $2/$10 schedule for turns timestamped before `SONNET5_INTRO_END` (2026-09-01 UTC) and at standard $3/$15 from then on — selected by the turn's own `start_time`, so reprocessing a historical intro-period turn keeps billing it at intro rates (reprocess determinism). When a turn carries no parseable timestamp, Sonnet 5 falls back to the durable standard rate. All other Sonnet versions are flat $3/$15. **Flip reminder:** after 2026-08-31 the live rate is standard automatically (no code edit needed) — the constant and intro row stay for correct reprocessing of pre-cutoff turns.

**New-tokenizer note:** Opus 4.7+, Fable 5, **and Sonnet 5** ship a new tokenizer that produces ~30% more tokens for the same input text vs. prior models (Sonnet 4.6 and earlier keep the old tokenizer). Per-token rates are unchanged, but absolute session cost for equivalent workloads is meaningfully higher — the extra cost comes from token *counts* (already in `usageDetails`), not the rate table.

Cache write cost is split by tier when `cache_5m` / `cache_1h` are available in `usageDetails`; otherwise all cache_create is billed at the 5m rate.

### Pricing Multipliers

These stack multiplicatively on the base rates above (and apply uniformly across input, output, cache read, and cache write tiers):

- **Fast mode (`speed="fast"`)**: per-model premium — **6x** on Opus 4.6 / 4.7 ($30/$150), **2x** on Opus 4.8 ($10/$50). Opus 4.5 and Sonnet/Haiku are ineligible and keep base rates. Multipliers live in `FAST_MODE_MULTIPLIERS` in `langfuse-hook.py`.
- **Fable 5**: ineligible for fast mode (no `/fast` variant) and data residency (`inference_geo` multiplier unverified) — always billed at base $10/$50. Update `calculate_turn_cost` if Anthropic publishes Fable multipliers.
- **Data residency (`inference_geo="us"`)**: 1.1x on Opus 4.6+/Sonnet 4.6+ (including Sonnet 5). Other models do not support the `inference_geo` parameter; multiplier is not applied.
- **Fast + US-geo stack**: 6x × 1.1x = 6.6x (Opus 4.6/4.7); 2x × 1.1x = 2.2x (Opus 4.8).

### Server-side Tool Billing

- **Web search**: $0.01 per request (`$10 / 1,000 searches`). Billed via `costDetails.web_search` line item using `web_search_requests` from `usage.server_tool_use`. Added to turn total.
- **Web fetch**: free (token cost only).
- **Code execution**: $0.05/container-hour after 1,550 free hours/org/month. Not currently emitted by Claude Code; not billed.

Set `REPORT_API_EQUIVALENT_COST = False` in `langfuse-hook.py` to report $0.

**Keeping prices up to date:** Pricing is hardcoded in `calculate_turn_cost()` (`langfuse-hook.py`). When Anthropic releases new models or changes prices, update that function and the table above. Unknown models return $0 and emit a `[WARN]` in `langfuse-hook.log` — that's the signal to update. We send explicit costs rather than relying on Langfuse's built-in model table because Langfuse's table lags new model releases by days/weeks.

### AWS Bedrock Pricing (reference)

Claude Code sessions routed through AWS Bedrock carry provider-prefixed model IDs
(`anthropic.claude-*`, `us.anthropic.claude-*`, `eu.anthropic.claude-*`,
`global.anthropic.claude-*`). The substring matcher in `calculate_turn_cost`
already bills these at the **base** first-party rate (e.g. `anthropic.claude-opus-4-8`
→ $5/$25).

Bedrock list price matches the first-party Anthropic API for the same model. The
price axis on Bedrock is **endpoint type, not geography** — EU and US base rates are
identical (verified on the AWS Bedrock pricing page, June 2026: Claude 3.5 Sonnet
shows $6/$30 across both US East and Europe regions). Cross-region inference
(geo/regional `us.`/`eu.` endpoints) adds a **+10%** premium over the global
endpoint; the premium is the same for US and EU.

| Model | First-party API ($/1M in / out) | Bedrock global | Bedrock US/EU geo (`us.`/`eu.`, +10%) |
|-------|----------------------------------|----------------|----------------------------------------|
| Fable 5 | $10 / $50 | not on Bedrock | — |
| Opus 4.8 / 4.7 / 4.6 | $5 / $25 | $5 / $25 | $5.50 / $27.50 |
| Sonnet 5 (standard) | $3 / $15 | $3 / $15 | $3.30 / $16.50 |
| Sonnet 4.x | $3 / $15 | $3 / $15 | $3.30 / $16.50 |
| Haiku 4.5 | $1 / $5 | $1 / $5 | $1.10 / $5.50 |

**Known gap (not coded):** the +10% geo premium is **not** auto-applied — Claude Code
transcripts do not expose the Bedrock endpoint type, so the matcher cannot tell a
global call from a geo call and bills both at base. Fable 5 is not yet available on
Bedrock. Canonical source: aws.amazon.com/bedrock/pricing (verified June 2026).

## Tags and Metadata

> Transcript-field coverage verified against Claude Code **v2.1.197** (2026-06-30).
> Note: `effort.level`, `agent_id`/`parent_agent_id`, and skill `invocation_trigger` are
> OTel-span / hook-stdin fields, **not** transcript JSONL — unreachable by this hook's
> transcript parsing. effort is captured via the `$CLAUDE_EFFORT` env var instead.
>
> New in v2.1.169–v2.1.181: assistant-channel API-error/auto-retry stubs
> (`isApiErrorMessage`), user interrupts (`interruptedMessageId`), and the
> `system:informational` subtype (CLI warnings, e.g. "Unknown command"). The
> `isApiErrorMessage` stubs are zero-usage synthetic entries (`model:"<synthetic>"`)
> whose content is error text ("You've hit your session limit", "model not found");
> `build_turns` **skips** them so they cannot overwrite a turn's real output / timing /
> `stop_reason`. The high-frequency `mode` and `last-prompt` entry types are internal
> bookkeeping (current input mode, last-prompt UUID pointer) and deliberately not captured.
>
> New in v2.1.19x: `system/turn_duration` (per-turn wall-clock `durationMs` +
> `messageCount`) — captured as per-turn `duration_ms` (already drove generation
> `endTime`) and rolled up to `active_duration`; `queue-operation` (prompt-queue
> enqueue/dequeue/popAll/remove with prompt `content`) — captured as `queue_operations`
> wait-time analytics (content NOT stored, PII). The `agent-setting` entry
> (`{agentSetting: "claude"}` = the FleetView selected agent persona) is observed but
> **not captured** — always `"claude"` on first-party installs, so no signal yet; revisit
> if non-default agents appear (would become an `agent-setting:<slug>` tag).

Each trace is enriched with:

**Tags** (filterable in Langfuse UI):
- `claude-code` — always present
- repo/project name — derived from `cwd` (e.g., `langfuse-observability`)
- model family — `opus`, `sonnet`, or `haiku`
- entrypoint — `cli` or other launch method
- `fast` — present if any turn used `/fast` mode
- `has-errors` — present if API errors occurred during the session (`system/api_error` entries)
- `has-api-error-messages` — present when the session had assistant-channel API-error/auto-retry stubs (`isApiErrorMessage`); see `api_error_messages` metadata
- `has-interrupts` — present when the user interrupted (ESC) at least one assistant turn (`interruptedMessageId`)
- `has-queued-prompts` — present when the user queued at least one follow-up prompt mid-turn (`queue-operation` entries); see `queue_operations` metadata
- `model-missing` — present when any turn had **billable tokens but no `model` field**; that turn's cost is reported as `$0` (never defaulted to a priced model) and a `[WARN]` is logged. Filter on this to find sessions with under-reported cost from upstream transcript gaps.
- `permission:<mode>` — current permission mode (`default`, `acceptEdits`, `plan`, `bypassPermissions`)
- `pr:<N>` — one tag per PR linked from the session (via `pr-link` transcript entries)
- `agent-name:{slug}` — present when `type: "agent-name"` entry exists (e.g., `agent-name:langfuse-usagedetails-fix`)
- `session-kind:{bg|fg}` — background job vs interactive foreground session (from `sessionKind` field on transcript entries; defaults to `fg` when absent on older transcripts)
- `effort:<level>` — active effort level at Stop-fire time (`low`/`medium`/`high`/`max`), from `$CLAUDE_EFFORT`. Live fires only (absent on reprocessed sessions)
- `skill:<slug>` — one per distinct `attributionSkill` observed in the session (e.g., `skill:superpowers:brainstorming`)
- `plugin:<name>` — one per distinct `attributionPlugin` observed (e.g., `plugin:superpowers`)
- `compacted` — present when the session was context-compacted (mirrors `compaction_occurred`)
- `compact-trigger:<trigger>` — one per distinct compaction trigger (`manual`/`auto`; `unknown` omitted)
- `remote-control` — present when the session was bridged to claude.ai (Remote Control / `/remote-control`)
- `permission-bypass` — present when `bypassPermissions` was active at any point in the session
- `worktree:<name>` — present when the session ran inside a Claude Code worktree (from the last `worktree-state` entry)
- `has-background-tasks` — present when the Stop payload carried running background tasks
- `nested-subagents` — present when any ingested subagent was spawned by another subagent (depth ≥ 2; CC 2.1.172+)

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
- `api_errors` — error summary from `system/api_error` entries: `total_count`, `by_status` (HTTP codes), `first_error_at`, `last_error_at`
- `api_error_messages` — **separate** assistant-channel error/retry summary from `isApiErrorMessage` entries (CC 2.1.179+/2.1.181 auto-retry): `{count, by_status (apiErrorStatus, e.g. 404/429/529), by_error (e.g. model_not_found/rate_limit), first_at, last_at}`, plus `max_retry_attempt` when any entry carries `retryAttempt`. Null when none. From `extract_api_error_messages()`. Distinct channel from `api_errors`; these stubs are skipped by `build_turns` so they don't pollute turn output.
- `interrupts` — `{count}` of user interrupts (ESC mid-turn) from `interruptedMessageId` on `user` entries. A friction / misalignment signal. Null when none. From `extract_interrupts()`.
- `queue_operations` — prompt-queue activity from `type:"queue-operation"` entries (prompts the user queued while the assistant was busy): `{operations: {<op>: n}, queued, executed, removed, total_wait_ms, max_wait_ms}`. `executed` = prompts pulled from the queue to run (`dequeue`/`popAll`); `removed` = queued prompts the user cancelled. Wait = time a prompt sat queued (enqueue→dequeue/popAll), matched **FIFO** (no per-prompt id → approximate); `remove` is excluded from wait stats. Prompt `content` is **not stored** (PII). Long waits = the user racing ahead of the agent (friction signal). Null when nothing was queued. From `extract_queue_operations()`.
- `active_duration` — session rollup of per-turn wall-clock from `system/turn_duration` entries: `{total_ms, turns_measured, max_ms}`. `total_ms` is *active* time (Σ per-turn `durationMs`, each turn start→end incl. tool execution) — distinct from the trace wall-clock span, which also counts idle time between turns. Computed over the incremental turn batch (matches `total_iterations`). Null when no turn carried a duration (CC before ~2.1.19x never emitted the entry). From `build_active_duration_summary()`.
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
- `skill_attribution` — per-session rollup from `attributionSkill`/`attributionPlugin` on assistant entries: `{skills_used, plugins_used, top_skill, skill_turn_counts, skill_cost_breakdown}`. `skill_cost_breakdown[<skill>]` = `{turns, input_tokens, output_tokens, cache_read_tokens, cache_create_tokens, cost_usd}`. Turns lacking attribution go to a `_unattributed` bucket inside the breakdown and are excluded from `skills_used`/`top_skill`. Null when no attribution data in session.
- `compaction_occurred` — `true`/`false`; whether the session was context-compacted (from `type: "summary"` or a `system`/`compact*` subtype entry). Demoted from a score to metadata.
- `total_iterations` — sum of per-turn `iteration_count` across the session (server-side agentic-loop iterations from `usage.iterations`).
- `cache_miss` — session rollup of cache misses from `message.diagnostics.cache_miss_reason`: `{total_missed_tokens, by_reason: {<type>: count}, turns_with_miss}`. Explains *why* `cache_hit_rate` is low and how many input tokens were re-sent (e.g. `tools_changed`). Null when no turn missed cache.
- `effort_level` — active effort level (`low`/`medium`/`high`/`max`) read from `$CLAUDE_EFFORT` at Stop-fire time. Session-level, last-observed. Live fires only; null on reprocessed sessions.
- `compaction` — rollup of `system/compact_boundary` events: `{count, triggers: {<trigger>: n}, total_tokens_reclaimed (Σ preTokens−postTokens), total_pre_tokens, total_post_tokens, total_duration_ms, events: [{trigger, pre_tokens, post_tokens, tokens_reclaimed, duration_ms, timestamp}]}`. Surfaces context-window pressure and the token/time cost of compaction. Legacy `type:"summary"` entries (pre-`compactMetadata`) count with `trigger:"legacy"` and no token fields. Unknown triggers pass through verbatim. Null when never compacted. The bare `compaction_occurred` bool (via `detect_compaction`) is retained alongside for back-compat. From `extract_compaction()`.
- `remote_control` — `{bridge_session_id, url}` from `bridge-session` + `system/bridge_status` entries (the Remote Control / phone-web bridge). Either field may be absent independently; `url` correlates with the claude.ai session. **No timestamps** on these entries. Null when the session was never bridged. From `extract_bridge()`.
- `permission_timeline` — `{modes_used (sorted distinct), sequence (file-order, consecutive dups collapsed), transition_count, ever_bypass, ever_accept_edits}` from `permission-mode` entries. Complements the last-observed `permission_mode`/`permission:<mode>` tag with the full transition history. **No timestamps** on these entries → sequence is order-only, not timed. Null when no `permission-mode` entries. From `extract_permission_timeline()`.
- `worktree` — `{name, branch, original_cwd, original_branch, original_head_commit}` from the last `type: "worktree-state"` entry. Null when the session never entered a worktree. From `extract_worktree_state()`.
- `background_tasks` / `session_crons` — raw lists from the Stop hook stdin payload (v2.1.145+). Live fires only; null on reprocess.

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
- `attribution_skill`, `attribution_plugin` — primary skill / plugin for the turn (first non-empty observed)
- `attribution_skills_all` — list of all distinct skills observed in the turn (only emitted when more than one)
- `iteration_count` — number of server-side iterations in the turn (length of `usage.iterations`; 0 when absent).
- `ttft_ms` — time-to-first-token in ms (first assistant-token timestamp − turn start). Omitted when not derivable.
- `duration_ms` — turn wall-clock in ms from the matching `system/turn_duration` entry (turn start→end, incl. tool execution). Also drives the generation `endTime`. Omitted when no `turn_duration` matched the turn (CC before ~2.1.19x). Rolled up session-wide as `active_duration`.
- `cache_miss_reason` — dominant cache-miss reason type for the turn (e.g. `tools_changed`), from `message.diagnostics`. Omitted when the turn had no cache miss.
- `cache_missed_tokens` — total input tokens that missed cache in the turn. Omitted when no miss.
- `cache_miss_by_reason` — `{<type>: count}` tally of miss reasons in the turn. Omitted when no miss.

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

Visible model reasoning is gated by the API `thinking.display` parameter (default `"omitted"` on Opus 4.7/4.8 and Fable 5, `"summarized"` on older models) — a model-level default applied uniformly across providers. Any difference you observe between Bedrock and first-party/Pro reflects the requesting client's chosen `display` value, not provider-specific gating. Either way it is moot for this hook, which never sees thinking text post-v2.1.112.

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
3. Correlates via three passes: `.meta.json` `toolUseId` match (exact) → tool_result
   `agentId` regex (exact) → timestamp proximity (within
   `SUBAGENT_MATCH_WINDOW_S = 60` seconds). Per-generation
   `subagent_correlation` records which pass matched (`meta` / `deterministic` /
   `timestamp`).
4. Ingests subagent turns as generations nested under the Agent tool span
5. Rolls up per-subagent and total harness cost in trace metadata

**Nested sub-agents (CC 2.1.172+):** sub-agents can spawn sub-agents up to 5 levels deep.
Transcripts stay in the flat `<session>/subagents/` dir; each `.meta.json`'s `toolUseId`
points at the `Agent` tool_use in the *spawner's* transcript. `ingest_subagent` recurses
(depth cap 5, cycle guard), nesting child generations under the parent agent's tool span.
Each agent appears once in `subagent_costs.agents` with `depth` and `parent_agent_id`.

**Langfuse hierarchy:**
```
Trace (parent session)
├── Generation (parent turn)
│   └── Span (Agent tool call)
│       ├── Generation (subagent turn 1)
│       │   └── Span (Read tool)
│       └── Generation (subagent turn 2)
│           └── Span (Agent tool call — nested dispatch)
│               └── Generation (nested subagent turn 1)
│                   └── Span (Bash tool)
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

Two heuristic scores are attached to every trace during ingestion:

| Score | Type | Values | Source |
|-------|------|--------|--------|
| `cache_hit_rate` | Numeric | 0.0-1.0 | cache_read / (cache_read + cache_creation) |
| `tool_error_rate` | Numeric | 0.0-1.0 | tool calls with `[ERROR]` output / total tool calls |

Both are deterministic (no LLM calls) and run on every Stop hook invocation.
Scores use deterministic UUIDs so re-ingestion (`--reprocess`) updates rather
than duplicates. Each score is omitted entirely when its denominator is zero
(no cache activity / no tool calls), keeping "absent" distinct from a genuine 0.0.

### Classifier Details

**`cache_hit_rate`** — Measures cache warmth. `cache_read / (cache_read + cache_creation)`.
0.0 = cache-miss session (only writes), 1.0 = fully warm. Omitted when there is
no cache activity at all (filter by `cache_hit_rate IS NULL` to find cold sessions).

**`tool_error_rate`** — Fraction of tool calls whose result was an error
(`[ERROR]` prefix, applied in `extract_tool_results`). High values flag flaky
sessions where tools repeatedly failed. Omitted when the session made no tool calls.
