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

# Re-ingest all sessions (LOCAL stack — reads keys from this checkout's .env)
PK=$(grep '^LANGFUSE_INIT_PROJECT_PUBLIC_KEY=' .env | cut -d= -f2)
SK=$(grep '^LANGFUSE_INIT_PROJECT_SECRET_KEY=' .env | cut -d= -f2)
LANGFUSE_PUBLIC_KEY=$PK LANGFUSE_SECRET_KEY=$SK python3 langfuse-hook.py --reprocess

# Re-ingest all sessions (REMOTE stack, e.g. Pi — copy host+keys from the Stop hook
# command in ~/.claude/settings.json; the local .env does NOT hold the remote keys)
LANGFUSE_HOST=http://<remote-ip>:3100 \
  LANGFUSE_PUBLIC_KEY=pk-lf-... LANGFUSE_SECRET_KEY=sk-lf-... \
  python3 langfuse-hook.py --reprocess
```

> **Reprocessing against a remote stack:** `--reprocess` takes its target from
> `LANGFUSE_HOST` (default `http://localhost:3100`) and its credentials from the
> environment — it never reads `.env`. Omitting `LANGFUSE_HOST` on a remote deployment
> sends everything to localhost and fails with `Connection refused` for every session
> (state is not advanced, so nothing is lost — but nothing is updated either). The
> remote instance also has its **own** project keys: the `.env` in this checkout belongs
> to the local stack and will not authenticate against the remote one. Always source
> both host and keys from the `Stop` hook command in `~/.claude/settings.json`.
> Run it from the **main checkout**, not a worktree — `.env` is gitignored and absent
> from worktrees.

- **Dashboard**: http://localhost:3100
- **Hook log**: `~/.claude/langfuse-hook.log` (auto-rotates at 10 MB)
- **State files**: `~/.claude/langfuse-state/<session_id>.offset` (parent), `<session_id>.subagents.json` (subagents)

### Reading the ingestion result

The ingestion endpoint answers **`207 Multi-Status`**: the HTTP status only says
the request was accepted, and the body reports each event's fate:

```json
{"successes": [{"id": "...", "status": 201}], "errors": [{"id": "...", "status": 400, "message": "..."}]}
```

`classify_ingestion_errors()` (in `langfuse_common.py`, shared by both hooks)
splits those per-event failures into two kinds, because they need opposite
handling:

| kind | statuses | `send_to_langfuse` returns | effect |
|---|---|---|---|
| transient | 5xx, 429, 408, network, unparseable body | `False` | offset held back, retried next fire |
| permanent | 400/401/403/404/422, unreadable error entry | `True` + `[ERROR]` log | events dropped, offset advances |

Advancing on a permanent rejection is deliberate. Retrying a validation error
can never succeed, and holding the offset back would wedge the session: every
later fire would resend an ever-growing window that can never drain. Losing
those events loudly beats ingesting nothing forever. Grep the log for
`[ERROR] Langfuse rejected` to find them.

> This is also the failure shape of the Langfuse v4 `events_only` cutover, where
> the endpoint keeps returning 207 while rejecting every event type except
> `score-create`. See REMOTE-DEPLOY.md.

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
- **Backend**: Langfuse v4 in legacy write mode (web + worker), PostgreSQL 18, ClickHouse 26, Redis 8, MinIO
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
tools/
  check_pricing_drift.py         # Compares hardcoded pricing against a public feed (manual/CI, never run by the hook)
tests/
  test_langfuse_hook.py          # Core hook unit tests
  conftest.py                    # Redirects LANGFUSE_HOOK_LOG so tests never touch the real log
  test_pricing_drift.py          # Pricing drift-checker tests (offline, synthetic feeds)
  test_thinking_tokens.py        # output_tokens_details.thinking_tokens capture + rollup
  test_prompt_provenance.py      # origin.kind / promptSource / turnCompanion capture
  test_prompt_provenance_fallback.py  # shape-based classification of untagged prompt entries
  test_tool_denials.py           # toolDenialKind capture; denials kept out of tool_error_rate
  test_file_history_delta.py     # file-history-delta rollup (edits, distinct files, versions)
  test_log_isolation.py          # LOG_FILE env override + production-log isolation
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
- `LOG_FILE` honours the `LANGFUSE_HOOK_LOG` env var (both hooks), defaulting to `~/.claude/langfuse-hook.log`. `tests/conftest.py` sets it to a temp dir — **without that, a `pytest` run appends fixture sessions to the production log and its tail can no longer be trusted for live diagnosis.** Because `LOG_FILE` is evaluated at import time, the override must be set before the hook module is imported.
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

Pricing is model-aware (per 1M tokens). Source: [platform.claude.com/docs/en/about-claude/pricing](https://platform.claude.com/docs/en/about-claude/pricing) (last verified 2026-09-23).

| Model | Input | Output | Cache Read | Cache Write 5m | Cache Write 1h |
|-------|-------|--------|------------|----------------|----------------|
| Fable 5 / Mythos 5 | $10.00 | $50.00 | $1.00 | $12.50 | $20.00 |
| Opus 5.5 | $4.00 | $20.00 | $0.20 | $5.00 | $8.00 |
| Opus 5 / 4.8 / 4.7 / 4.6 / 4.5 | $5.00 | $25.00 | $0.50 | $6.25 | $10.00 |
| Opus 4.1 / 4.0 (legacy) | $15.00 | $75.00 | $1.50 | $18.75 | $30.00 |
| Sonnet 5 | $2.00 | $10.00 | $0.20 | $2.50 | $4.00 |
| Sonnet 4.6 / 4.5 / 4 | $3.00 | $15.00 | $0.30 | $3.75 | $6.00 |
| Haiku 4.5 | $1.00 | $5.00 | $0.10 | $1.25 | $2.00 |
| Haiku 3.5 | $0.80 | $4.00 | $0.08 | $1.00 | $1.60 |
| Haiku 3 (deprecated) | $0.25 | $1.25 | $0.03 | $0.30 | $0.50 |

**Sonnet 5 is flat $2/$10.** It launched at $2/$10 billed as "introductory through 2026-08-31", with a step-up to $3/$15 on 2026-09-01. Anthropic cancelled the step-up and made $2/$10 the standard price (pricing page, verified 2026-09-23). The date switch (`SONNET5_INTRO_END`) was removed; `calculate_turn_cost` keeps its `turn_start_time` parameter for any future date-aware pricing window. All other Sonnet versions are flat $3/$15.

**Fable 5.1 / Mythos 5.1** cache reads are $0.25 (0.025x input), not the $1.00 of Fable 5 / Mythos 5. Other rates are identical.

**Opus 5** (`claude-opus-5`) bills at the same $5/$25 schedule as Opus 4.8 — a drop-in
upgrade on price. It is a **separate rate-limit bucket** from the combined Opus 4.x pool,
which matters for capacity planning but not for cost. **Mythos 5** (`claude-mythos-5`,
Project Glasswing) is Fable 5's sibling: identical pricing and API surface, different ID —
`calculate_turn_cost` matches `mythos` alongside `fable` so the invitation-only
`claude-mythos-preview` resolves too.

**Opus 5.5** (`claude-opus-5-5`) is **cheaper** than Opus 5: $4/$20, cache read $0.20. The
cache reads are **0.05x** input (not 0.1x); cache writes $5 / $8. Same tokenizer as Opus 5, so token counts are comparable. Its
**default effort is `medium`** (Opus 5: `high`), which is why `cost_by_model` splits by effort.
Opus IDs resolve through `_opus_family()`, which parses `opus-<major>[-<minor>]` and treats an
8-digit group as a date snapshot. It replaced substring matching: `"opus-5" in m` also
matched `claude-opus-5-5` and billed it at $5/$25. An unknown minor (e.g. `claude-opus-5-6`)
now hits the `[WARN]` + `$0` path instead of inheriting its predecessor's rate.

**New-tokenizer note:** Opus 4.7+, Fable 5, **and Sonnet 5** ship a new tokenizer that produces ~30% more tokens for the same input text vs. prior models (Sonnet 4.6 and earlier keep the old tokenizer). Per-token rates are unchanged, but absolute session cost for equivalent workloads is meaningfully higher — the extra cost comes from token *counts* (already in `usageDetails`), not the rate table.

Cache write cost is split by tier when `cache_5m` / `cache_1h` are available in `usageDetails`; otherwise all cache_create is billed at the 5m rate.

**Extended-thinking tokens are NOT a separate billing category.** `usage.output_tokens_details.thinking_tokens` is a breakdown *inside* `output_tokens`, billed at the plain output rate. It is reported as the `output_thinking` line in `usageDetails` and rolled up in trace metadata `thinking` (`{total_thinking_tokens, total_output_tokens, share_of_output, turns_with_thinking, max_thinking_tokens}`) purely for analytics — never added to a cost total, and `calculate_turn_cost` does not take it as an input. It answers "what is the effort level actually buying", which the `effort_level` tag alone cannot. Note the token *count* is present even though CC has stripped thinking *text* from transcripts since v2.1.112.

### Pricing Multipliers

These stack multiplicatively on the base rates above (and apply uniformly across input, output, cache read, and cache write tiers):

- **Fast mode (`speed="fast"`)**: per-model premium — **2x** on Opus 5.5 ($8/$40), Opus 5 and Opus 4.8 ($10/$50), **6x** on Opus 4.6 / 4.7 ($30/$150). Opus 4.5 and Sonnet/Haiku are ineligible and keep base rates. Multipliers live in `FAST_MODE_MULTIPLIERS` in `langfuse-hook.py`.
  - Fast mode is now offered on **Opus 5.5 / 5 / 4.8 only** — `speed="fast"` on Opus 4.7 returns an API error, and the Opus 4.6 `-fast` model ID was retired (requests silently fall back to standard). The 4.6/4.7 entries stay in the table on purpose: turns recorded while fast mode was live on those generations *were* billed at 6x, and reprocessing must keep billing them that way.
- **Fable 5 / Mythos 5**: ineligible for fast mode (no `/fast` variant) and data residency (`inference_geo` multiplier unverified) — always billed at base $10/$50. Update `calculate_turn_cost` if Anthropic publishes multipliers for them.
- **Data residency (`inference_geo="us"`)**: 1.1x on Opus 4.6+ (including Opus 5 and 5.5) / Sonnet 4.6+ (including Sonnet 5). Other models do not support the `inference_geo` parameter; multiplier is not applied.
- **Fast + US-geo stack**: 2x × 1.1x = 2.2x (Opus 5.5 / 5 / 4.8); 6x × 1.1x = 6.6x (Opus 4.6/4.7).

### Server-side Tool Billing

- **Web search**: $0.01 per request (`$10 / 1,000 searches`). Billed via `costDetails.web_search` line item using `web_search_requests` from `usage.server_tool_use`. Added to turn total.
- **Web fetch**: free (token cost only).
- **Code execution**: $0.05/container-hour after 1,550 free hours/org/month. Not currently emitted by Claude Code; not billed.

Set `REPORT_API_EQUIVALENT_COST = False` in `langfuse-hook.py` to report $0.

**Keeping prices up to date:** Pricing is hardcoded in `calculate_turn_cost()` (`langfuse-hook.py`). When Anthropic releases new models or changes prices, update that function and the table above. Unknown models return $0 and emit a `[WARN]` in `langfuse-hook.log` — that's the signal to update. We send explicit costs rather than relying on Langfuse's built-in model table because Langfuse's table lags new model releases by days/weeks.

Two signals catch pricing staleness, and they fail differently:

| Signal | Catches | Misses |
|--------|---------|--------|
| `[WARN] unrecognised … model` in the hook log | A **new** model ID (cost shows as $0 — loud) | A **rate change** on a model already in the table |
| `tools/check_pricing_drift.py` | Rate changes **and** new models | Multipliers, intro-window boundaries, server-tool rates |

```bash
python3 tools/check_pricing_drift.py            # exit 1 on drift
python3 tools/check_pricing_drift.py --json     # machine-readable
python3 tools/check_pricing_drift.py --feed /path/to/local.json
```

The script probes `calculate_turn_cost()` itself (so it cannot drift from the code it
validates) across all five billed axes — input, output, cache read, cache write 5m,
cache write 1h — and diffs them against LiteLLM's community price map. Models absent from
the feed (e.g. Mythos 5) report `SKIP`, not a failure.

**The feed is a tripwire, not a source of truth.** It is community-maintained and can
itself be wrong or stale. A `DRIFT` result means *go read
platform.claude.com/docs/en/about-claude/pricing and decide* — never paste feed numbers
into `calculate_turn_cost()` unverified.

**Why the hook does not fetch prices at runtime:** the hook is deliberately offline
(stdlib only, fire-and-forget, no network on the Stop path). Live lookups would add
latency to every turn, make cost depend on a third party's uptime, and silently re-bill
historical turns whenever an upstream entry changed — and a wrong feed entry would produce
confidently wrong costs, where today an unknown model produces an obvious `$0` plus a
`[WARN]`. Any date-aware pricing window needs the rate that applied *at the turn's
timestamp*; a live feed only ever carries today's rate.

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
| Opus 5.5 | $4 / $20 | unverified | unverified |
| Opus 5 | $5 / $25 | $5 / $25 | $5.50 / $27.50 |
| Opus 4.8 / 4.7 / 4.6 | $5 / $25 | $5 / $25 | $5.50 / $27.50 |
| Sonnet 5 | $2 / $10 | unverified | unverified |
| Sonnet 4.x | $3 / $15 | $3 / $15 | $3.30 / $16.50 |
| Haiku 4.5 | $1 / $5 | $1 / $5 | $1.10 / $5.50 |

**Known gap (not coded):** the +10% geo premium is **not** auto-applied — Claude Code
transcripts do not expose the Bedrock endpoint type, so the matcher cannot tell a
global call from a geo call and bills both at base. Fable 5 is not yet available on
Bedrock. Canonical source: aws.amazon.com/bedrock/pricing (verified June 2026).
Opus 5 ships on Bedrock as `anthropic.claude-opus-5` (plus `us.`/`eu.`/`au.`/`jp.`
geo prefixes); `_opus_family()` bills all of them (and `anthropic.claude-opus-5-5`) at
the first-party base rate. The Bedrock rates for Opus 5.5 and Sonnet 5 have not been
checked against aws.amazon.com/bedrock/pricing.

## Tags and Metadata

> Transcript-field coverage verified against Claude Code **v2.1.252** (2026-09-01). Changelog
> v2.1.221–v2.1.252 reviewed for new transcript-JSONL fields — nothing found (changes were
> hook-event additions (`PreModelSwitch`/`PostModelSwitch`), worktree/UI/TUI fixes, not new
> transcript entry types).
> Note: `agent_id`/`parent_agent_id` and skill `invocation_trigger` are OTel-span /
> hook-stdin fields, **not** transcript JSONL — unreachable by this hook's transcript
> parsing.
>
> New in v2.1.220: **`effort` is now a per-assistant-entry transcript field** (previously
> reachable only via the `$CLAUDE_EFFORT` env var, session-level and live-fires-only).
> Captured per-generation as `effort` metadata and rolled up to the session
> `effort_level` / `effort:<level>` tag. Transcript wins over the env var — it is per-turn
> and survives `--reprocess`; the env fallback still covers pre-2.1.220 transcripts.
> Within a turn the **last** non-empty value wins, so a mid-turn `/effort` change is
> attributed to the level the turn finished under.
>
> Also observed in v2.1.211+: the random 3-word `slug` returned as a field on
> `assistant`/`user` entries (see Trace Name precedence — deliberately not used for
> naming), and `file-history-delta` entries alongside `file-history-snapshot`.
> `promptSource` (`typed`/`queued`/`suggestion_accepted`/`sdk`/`system`) and `origin`
> (`{kind: human|task-notification|peer}`) on user entries are captured as
> `prompt_provenance`. Both are present on only a minority of prompt entries — 159 of
> 599 across a 159-transcript census — so untagged ones are classified by shape
> instead (`classify_untagged_prompt`): a `<command-name>` entry is a human prompt
> via `slash_command` (75 of them, the largest untagged group), a sidechain root
> (`isSidechain` with no `parentUuid`) is an `agent-dispatch` — the task prompt the
> parent wrote for a subagent — and `isMeta` / `[Request interrupted by user]` /
> `<local-command-stdout>` entries are counted separately as `meta_entries` and
> `interruptions` rather than as prompts. Text is read only to match those markers,
> never stored.
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
- `model:<id>` — one per distinct exact model ID in the batch (e.g. `model:claude-opus-5-5`), so Opus 5 and Opus 5.5 sessions can be filtered apart
- `refusal:<category>` — one per distinct `stop_details.category` on refused turns (`cyber`, `bio`, `reasoning_extraction`, ...; null category → `uncategorized`). Opus 5.5 added `bio` and `reasoning_extraction` classifiers
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
- `effort:<level>` — effort level for the session (`low`/`medium`/`high`/`xhigh`/`max`). Sourced from the per-turn transcript `effort` field (CC 2.1.220+, last turn that carried one) and therefore present on reprocessed sessions too; falls back to `$CLAUDE_EFFORT` at Stop-fire time on older transcripts (live fires only)
- `skill:<slug>` — one per distinct `attributionSkill` observed in the session (e.g., `skill:superpowers:brainstorming`)
- `plugin:<name>` — one per distinct `attributionPlugin` observed (e.g., `plugin:superpowers`)
- `compacted` — present when the session was context-compacted (mirrors `compaction_occurred`)
- `compact-trigger:<trigger>` — one per distinct compaction trigger (`manual`/`auto`; `unknown` omitted)
- `remote-control` — present when the session was bridged to claude.ai (Remote Control / `/remote-control`)
- `permission-bypass` — present when `bypassPermissions` was active at any point in the session
- `worktree:<name>` — present when the session ran inside a Claude Code worktree (from the last `worktree-state` entry)
- `has-background-tasks` — present when the Stop payload carried running background tasks
- `nested-subagents` — present when any ingested subagent was spawned by another subagent (depth ≥ 2; CC 2.1.172+)
- `stop-reason:<reason>` — one per distinct **anomalous** terminal `stop_reason` in the session (`max_tokens`, `stop_sequence`, `refusal`, `pause_turn`, or any unknown reason). The high-frequency normal reasons `end_turn` (turn done) and `tool_use` (paused to call a tool) are **not** tagged — they'd flood every trace with no signal (`NORMAL_STOP_REASONS`). Full per-reason counts live in the `stop_reasons` metadata regardless. Filter on `stop-reason:max_tokens` to find turns whose output was truncated at the token cap.

**Trace Name precedence:**
1. `customTitle` from `type: "custom-title"` (user-set via in-CLI title command)
2. `aiTitle` from `type: "ai-title"` (Claude-generated short title once enough session context exists)
3. `agentName` from `type: "agent-name"` (auto-generated mid-session slug, e.g. `langfuse-usagedetails-fix`)
4. Truncated first non-synthetic user prompt (80 chars)
5. `{repo_name}/{git_branch}` composite — stable fallback when prompt is empty
6. `"Claude Code Session"` hardcoded fallback

The auto-generated 3-word slug (e.g. `goofy-frolicking-dove`) was removed from
JSONL transcripts in Claude Code v2.1.112 and **returned in ~v2.1.211** — no longer as its
own entry type, but as a `slug` field on `assistant`/`user` entries. It is deliberately
**not** used for trace naming: it is a random handle (`sharded-shimmying-hummingbird`),
whereas `agentName` is descriptive of the actual task (`context window optimization`).
The `agent-name` entry (~30% into a session)
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
- `stop_reasons` — rollup of per-turn terminal `stop_reason` (the API's reason each turn ended): `{by_reason: {<reason>: count}, turns_counted, last}`. `last` = the terminal reason of the session's final turn that carried one (the outcome). Computed over the incremental turn batch (matches `total_iterations`/`active_duration`). Null when no turn carried a stop_reason. Anomalous reasons also surface as `stop-reason:<reason>` tags. When any turn was refused, adds `refusal_categories: {<category>: count}` from the API's `stop_details.category` (null → `uncategorized`), also tagged `refusal:<category>`. From `build_stop_reasons_summary()`.
- `cost_by_model` — per exact model ID: `{turns, input_tokens, output_tokens, cache_read_tokens, cache_create_tokens, cost_usd, by_effort: {<effort|unset>: {turns, cost_usd}}}`. Turns with no model go to `_missing`. Computed over the incremental turn batch. From `build_model_cost_breakdown()`.
- `opus_5_5_savings` — Opus 5.5 turns re-priced at Opus 5 rates: `{turns, actual_cost_usd, opus_5_equivalent_cost_usd, saved_usd}`. An estimate: same token counts assumed (same tokenizer, but a different model and default effort would not spend exactly the same). Null without Opus 5.5 turns. From `build_opus_5_5_savings()`.
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
- `effort_level` — session effort level (`low`/`medium`/`high`/`xhigh`/`max`), last-observed. Primary source is the per-turn transcript `effort` field (CC 2.1.220+), so this survives `--reprocess`; on older transcripts it falls back to `$CLAUDE_EFFORT` read at Stop-fire time (live fires only, null on reprocess). See per-generation `effort` for the per-turn value.
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
- `effort` — effort level for this turn (`low`/`medium`/`high`/`xhigh`/`max`), from the per-assistant-entry transcript field (CC 2.1.220+). Last non-empty value in the turn wins. Omitted on older transcripts. Pair with `usageDetails` to compare token spend across effort levels.
- `refusal_category` — present only when the turn's terminal `stop_reason` is `refusal`: the `stop_details.category` (or `uncategorized`).
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
- All ports are localhost-only by default. LAN exposure of the dashboard is opt-in via `LANGFUSE_WEB_BIND` in `.env` (bind to host LAN IP; HTTP only, no TLS) — see `REMOTE-DEPLOY.md`. All other services stay `127.0.0.1`.

## Hook-Level Scores

Two heuristic scores are attached to every trace during ingestion:

| Score | Type | Values | Source |
|-------|------|--------|--------|
| `cache_hit_rate` | Numeric | 0.0-1.0 | cache_read / (cache_read + cache_creation) |
| `tool_error_rate` | Numeric | 0.0-1.0 | executed calls with `[ERROR]` output / executed calls |
| `tool_denial_rate` | Numeric | 0.0-1.0 | denied calls / all attempted calls |

All three are deterministic (no LLM calls) and run on every Stop hook invocation.
Scores use deterministic UUIDs so re-ingestion (`--reprocess`) updates rather
than duplicates. Each score is omitted entirely when its denominator is zero
(no cache activity / no executed calls / no attempted calls), keeping "absent"
distinct from a genuine 0.0.

### Classifier Details

**`cache_hit_rate`** — Measures cache warmth. `cache_read / (cache_read + cache_creation)`.
0.0 = cache-miss session (only writes), 1.0 = fully warm. Omitted when there is
no cache activity at all (filter by `cache_hit_rate IS NULL` to find cold sessions).

**`tool_error_rate`** — Fraction of *executed* tool calls whose result was an
error (`[ERROR]` prefix, applied in `extract_tool_results`). High values flag
flaky sessions where tools repeatedly failed. Omitted when the session executed
no tool calls.

Denied calls are excluded from both numerator and denominator. Claude Code marks
a refused call `is_error: true` exactly like a failed one, so without the
`toolDenialKind` split a permission event reads as a broken command — in a
census of 180 local sessions this inflated the score in 19 of them, one from a
true 8.3% to 21.4%.

**`tool_denial_rate`** — Fraction of *attempted* tool calls that were refused,
so the denominator includes the denied ones. `extract_tool_denials` puts the
breakdown in trace metadata as `tool_denials: {denial_count, by_kind}`. The
kinds separate a judgement about the work from a property of the harness:
`user-rejected` (the human declined), `automode-blocked` (the auto-mode
classifier refused) and `automode-unavailable` (the classifier timed out).
Omitted when the session attempted no tool calls.
