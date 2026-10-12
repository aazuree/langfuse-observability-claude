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

# Re-ingest all sessions (LOCAL stack — reads keys from this checkout's .env).
# Delete first: Langfuse v4 keeps every export of a span, so re-sending a
# session that is still there doubles its cost. Deletes are asynchronous —
# wait until the traces are gone from the UI (typically <15 min) before --reprocess.
PK=$(grep '^LANGFUSE_INIT_PROJECT_PUBLIC_KEY=' .env | cut -d= -f2)
SK=$(grep '^LANGFUSE_INIT_PROJECT_SECRET_KEY=' .env | cut -d= -f2)
LANGFUSE_PUBLIC_KEY=$PK LANGFUSE_SECRET_KEY=$SK python3 langfuse-hook.py --delete-traces
LANGFUSE_PUBLIC_KEY=$PK LANGFUSE_SECRET_KEY=$SK python3 langfuse-hook.py --reprocess

# Re-ingest all sessions (REMOTE stack, e.g. Pi — copy host+keys from the Stop hook
# command in ~/.claude/settings.json; the local .env does NOT hold the remote keys)
export LANGFUSE_HOST=http://<remote-ip>:3100 LANGFUSE_PUBLIC_KEY=pk-lf-... LANGFUSE_SECRET_KEY=sk-lf-...
python3 langfuse-hook.py --delete-traces   # then wait for the deletes
python3 langfuse-hook.py --reprocess
```

> **Reprocessing against a remote stack:** `--delete-traces` and `--reprocess` take
> their target from `LANGFUSE_HOST` (default `http://localhost:3100`) and their
> credentials from the environment — they never read `.env`. Omitting `LANGFUSE_HOST` on
> a remote deployment sends everything to localhost and fails with `Connection refused`
> for every session. The remote instance also has its **own** project keys: the `.env`
> in this checkout belongs to the local stack and will not authenticate against the
> remote one. Always source both host and keys from the `Stop` hook command in
> `~/.claude/settings.json`. Run it from the **main checkout**, not a worktree — `.env`
> is gitignored and absent from worktrees.

- **Dashboard**: http://localhost:3100
- **Hook log**: `~/.claude/langfuse-hook.log` (auto-rotates at 10 MB)
- **State files**: `~/.claude/langfuse-state/<session_id>.offset` (parent: line offset + number of turns sent), `<session_id>.subagents.json` (subagents), `<session_id>.lock` (per-session `flock`: fires of one session run one at a time, waiting up to `SESSION_LOCK_TIMEOUT_S = 180`; needed because the hook runs `"async": true`)

### Reading the send result

The hook sends over **OTLP** (`POST /api/public/otel/v1/traces`, header
`x-langfuse-ingestion-version: 4`) — Langfuse v4 retires the legacy
`/api/public/ingestion` batch API (Cloud sunset 2026-11-16; self-hosted `legacy` write
mode is removed in an upcoming major). `langfuse_otlp.py` translates the hook's
internal trace / generation / span dicts into OTLP spans with the `langfuse.*`
attributes Langfuse maps natively; scores go separately to `POST /api/public/scores`.

**Langfuse v4 keeps every export of a span** — it does not deduplicate a re-sent span
ID. So each fire's spans go in **one gzipped request** (they land whole or not at all)
and are sent exactly once. `send_to_langfuse` returns whether the caller may advance
the session state:

| outcome | statuses | returns | effect |
|---|---|---|---|
| delivered | 2xx | `True` | state advances |
| permanent | 400 / 413 / 422 | `True` + `[ERROR] Langfuse rejected …` | spans dropped loudly, state advances |
| transient | 401 / 403 / 408 / 429 / 5xx / network | `False` | state held, whole batch retried next fire |

Langfuse answers a **structurally** malformed export (missing span/trace IDs,
attributes not a KeyValue list) with a whole-batch 400 and an explanation (read from
its source, `processOtelIngestion.ts`). Anything else gets a 200 and is processed
asynchronously — **semantic problems are never reported back**, so check a trace in the
UI after changing the attribute mapping. Scores are sent only after the spans landed;
they upsert by id and are recomputed every fire, so a failed score logs `[WARN]` and
does not hold the spans back.

## Architecture

```
Claude Code CLI
    | Stop        — a turn finished
    | StopFailure — a turn ended in an API error (Stop does not fire)
    | SessionEnd  — last chance for an unfinished final turn
    v
langfuse-hook.py ── OTLP spans ──> /api/public/otel/v1/traces
                 └─ session scores ─> /api/public/scores
                                  |
                    PostgreSQL, ClickHouse, Redis, MinIO
```

All services bound to `127.0.0.1` only. API key never leaves the machine.

## Tech Stack

- **Hook script**: Python 3.10+ (PEP 604 annotations are evaluated at import; CI runs on 3.10), stdlib only (zero external deps)
- **Backend**: Langfuse v4 (web + worker), PostgreSQL 18, ClickHouse 26, Redis 8, MinIO. The hook needs only the OTLP endpoint and the scores API, so the stack can run `LANGFUSE_MIGRATION_V4_WRITE_MODE=events_only`
- **Deployment**: Docker Compose (6 services)
- **Setup**: `./setup.sh` (generates secrets, starts containers, configures hook)

## Project Structure

```
langfuse-hook.py                 # Core hook script - parses transcripts, sends to Langfuse
langfuse_otlp.py                 # Hook events -> OTLP/JSON export (langfuse.* attributes), scores split out
langfuse_common.py               # Redaction, logging, auth header, atomic writes
docker-compose.yml               # Full Langfuse stack (6 services)
clickhouse/config.d/
  z_system_logs.xml              # Caps ClickHouse's own system.* log tables (Langfuse never reads them)
setup.sh                         # One-command setup (generates .env, starts services, configures hook)
.env.example                     # Template for environment variables
.env                             # Generated secrets (gitignored)
tools/
  check_pricing_drift.py         # Compares hardcoded pricing against a public feed (manual/CI, never run by the hook)
  render_env.py                  # setup.sh: fills the .env template literally (no sed), escapes $ for compose
  configure_hooks.py             # setup.sh: registers langfuse-hook.py on Stop + StopFailure + SessionEnd (async)
tests/
  test_langfuse_hook.py          # Core hook unit tests
  conftest.py                    # Redirects LANGFUSE_HOOK_LOG so tests never touch the real log; repo root on sys.path
  helpers.py                     # Readers for a sent batch: turn traces, session score / rollups
  test_otlp.py                   # Event -> OTLP span translation (IDs, attributes, trace attrs on the root)
  test_send.py                   # One gzipped OTLP request per fire; status handling; scores after spans
  test_pricing_drift.py          # Pricing drift-checker tests (offline, synthetic feeds)
  test_thinking_tokens.py        # output_tokens_details.thinking_tokens capture + rollup
  test_prompt_provenance.py      # origin.kind / promptSource / turnCompanion capture
  test_prompt_provenance_fallback.py  # shape-based classification of untagged prompt entries
  test_tool_denials.py           # toolDenialKind capture; denials kept out of tool_error_rate
  test_file_history_delta.py     # file-history-delta rollup (edits, distinct files, versions)
  test_log_isolation.py          # LOG_FILE env override + production-log isolation
  test_hook_scores.py            # Session score classifier tests
  test_subagent_tracking.py      # Subagent cost tracking tests
  test_subagent_resume.py        # Background subagents resumed after their parent turn was sent
  test_session_rollups.py        # Session rollups cover the whole session; each turn sent once; unfinished turns held
  test_turn_merging.py           # Consecutive user entries before a reply form one turn
  test_session_lock.py           # Per-session lock for overlapping (async) fires
  test_robustness.py             # Atomic state writes, logged crashes
  test_delete_traces.py          # --delete-traces (every per-turn trace, the old session trace, session scores)
  test_setup_tools.py            # tools/render_env.py and tools/configure_hooks.py
```

## Running Tests

Use `uv` to run tests (handles virtualenv and dependency resolution automatically):

```bash
uv run pytest tests/ -v              # Run all tests
uv run pytest tests/test_subagent_tracking.py -v  # Run specific test file
uv run pytest tests/ -k "subagent" -v  # Run tests matching pattern
```

## Code Conventions

- **No external Python dependencies** - stdlib only (urllib, json, gzip, base64, uuid, pathlib)
- Constants at top of `langfuse-hook.py`: `MAX_TEXT=10000`, `MAX_TOOL_IO=5000`. Log rotation threshold: `MAX_LOG_BYTES=10MB` in `langfuse_common.py`
- `LOG_FILE` honours the `LANGFUSE_HOOK_LOG` env var, defaulting to `~/.claude/langfuse-hook.log`. `tests/conftest.py` sets it to a temp dir — **without that, a `pytest` run appends fixture sessions to the production log and its tail can no longer be trusted for live diagnosis.** Because `LOG_FILE` is evaluated at import time, the override must be set before the hook module is imported.
- Secret redaction via `SECRET_PATTERNS` regex list before any data leaves the machine
- Session IDs sanitized via `sanitize_id()` to prevent path traversal
- State files are written with `atomic_write_text()` (temp file + `os.replace`), so a crash mid-write cannot truncate an offset back to 0
- The hook wraps its entry point: an unexpected exception is logged as `[ERROR] … crashed` with its traceback instead of vanishing to stderr. `--reprocess` logs and skips a failing session rather than aborting the run
- Incremental processing: state files track the processed line offset and the number of turns sent per session. A fire with no new lines sends nothing (unless it is a `SessionEnd`)
- **One trace per turn, sent once.** Each finished turn is its own trace (`id = uuid5(session:turn:<index>).hex`), grouped by `session.id`. Every turn but the last is finished; the last is finished when its reply ended for a terminal reason (`TERMINAL_STOP_REASONS`: `end_turn`, `stop_sequence`, `max_tokens`, `refusal`) — on a Stop fire the turn that just ended has one. Otherwise it is held for a later fire: an async fire can read the transcript after a queued prompt has already opened the next turn. `SessionEnd` and `--reprocess` of a transcript idle > `REPROCESS_IDLE_S` (1 h) send it anyway (`complete_turn_count`)
- **Every session rollup covers the whole session.** Each fire reads the transcript once, builds every turn and recomputes all session rollups from all of them; they ride on the session-level `session_cost_usd` score, which upserts by id. A rollup built from only the new lines would overwrite the session's numbers with one fire's worth
- `system/turn_duration` is written *after* the Stop hook runs, so a live fire usually misses it; it is captured when already present (re-ingest) and rolled up in `active_duration`
- Deterministic IDs (UUID5 → hex trace/span IDs in `langfuse_otlp`) make a re-ingest after `--delete-traces` reproduce the same traces

## Key Data Flow

1. Parse JSONL transcript from `~/.claude/projects/<project>/<session>.jsonl`
2. Group messages into user->assistant turns, extract tool calls. Consecutive user
   entries with no reply between them are **one** turn: a slash command is written as a
   `<command-name>` entry plus an `isMeta` expansion, and a prompt with an image as the
   text plus an `isMeta` "[Image: ...]" entry. The turn starts at the last of them, so
   TTFT is measured from the input the reply answers
3. Deduplicate streaming updates (one usage per `message.id` — every streamed entry carries the same usage since v2.1.97, so the first is taken)
4. Compute usage (tokens), latency, TTFT, cost
5. Build events for the finished, not-yet-sent turns (turn trace -> generation -> tool spans -> subagent generations), plus the session scores
6. Translate to OTLP (`langfuse_otlp.build_export`), POST one gzipped export, then the scores; save state

## Cost Model

Pricing is model-aware (per 1M tokens). Source: [platform.claude.com/docs/en/about-claude/pricing](https://platform.claude.com/docs/en/about-claude/pricing) (last verified 2026-10-09).

| Model | Input | Output | Cache Read | Cache Write 5m | Cache Write 1h |
|-------|-------|--------|------------|----------------|----------------|
| Fable 5 / Mythos 5 | $10.00 | $50.00 | $1.00 | $12.50 | $20.00 |
| Opus 5.5 | $4.00 | $20.00 | $0.20 | $5.00 | $8.00 |
| Opus 5 / 4.8 / 4.7 / 4.6 / 4.5 | $5.00 | $25.00 | $0.50 | $6.25 | $10.00 |
| Opus 4.1 / 4.0 (legacy) | $15.00 | $75.00 | $1.50 | $18.75 | $30.00 |
| Sonnet 5.5 | $2.00 | $10.00 | $0.10 | $2.50 | $4.00 |
| Sonnet 5 | $2.00 | $10.00 | $0.20 | $2.50 | $4.00 |
| Sonnet 4.6 / 4.5 / 4 | $3.00 | $15.00 | $0.30 | $3.75 | $6.00 |
| Haiku 5.5 (prompt ≤ 100K) | $0.10 | $0.50 | $0.01 | $0.125 | $0.20 |
| Haiku 5.5 (prompt > 100K) | $0.50 | $2.50 | $0.05 | $0.625 | $1.00 |
| Haiku 4.5 | $1.00 | $5.00 | $0.10 | $1.25 | $2.00 |
| Haiku 3.5 | $0.80 | $4.00 | $0.08 | $1.00 | $1.60 |
| Haiku 3 (deprecated) | $0.25 | $1.25 | $0.03 | $0.30 | $0.50 |

**Sonnet 5 is flat $2/$10.** It launched at $2/$10 billed as "introductory through 2026-08-31", with a step-up to $3/$15 on 2026-09-01. Anthropic cancelled the step-up and made $2/$10 the standard price (pricing page, verified 2026-09-23). The date switch (`SONNET5_INTRO_END`) was removed; `calculate_turn_cost` keeps its `turn_start_time` parameter for any future date-aware pricing window. Sonnet 5.5 launched at the same $2/$10 (verified 2026-10-01), but its cache reads are **0.05x** input ($0.10), like Opus 5.5 — the hook billed them at $0.20 until 2026-10-09, so Sonnet 5.5 traces ingested before then overstate cache-read cost 2x until reprocessed. All Sonnet 4.x and earlier versions are flat $3/$15.

**Haiku 5.5** (`claude-haiku-5-5`, default Haiku since CC 2.1.293) is priced by **prompt length per request**: a request whose prompt (input + cache reads + cache writes) is over 100,000 tokens pays the higher rates for *all* its tokens, output included, even when part of the prompt is a cache hit. Because a turn sums several API requests, the tier cannot be decided from turn totals: `build_turns` puts each request over `LONG_CONTEXT_THRESHOLD` into a per-turn `usage_long` bucket (and counts them in `long_context_requests` metadata), and `calculate_turn_cost(long_context_usage=...)` bills that part at `_LONG_CONTEXT_RATES`. Models without an entry there ignore the bucket. Data residency 1.1x applies to both tiers; no fast mode.

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
Sonnet, Haiku, Fable and Mythos work the same way: `_model_family()` parses both the
`claude-sonnet-4-5` and the Claude-3-era `claude-3-5-sonnet` schemes, and the result is
looked up in `_SONNET_RATES` / `_HAIKU_RATES` / `_TOP_TIER_RATES`. `claude-sonnet-6` or
`claude-haiku-6` therefore warn instead of silently billing at a neighbour's rate.

**New-tokenizer note:** Opus 4.7+, Fable 5, **and Sonnet 5 / 5.5** ship a new tokenizer that produces ~30% more tokens for the same input text vs. prior models (Sonnet 4.6 and earlier keep the old tokenizer). Per-token rates are unchanged, but absolute session cost for equivalent workloads is meaningfully higher — the extra cost comes from token *counts* (already in `usageDetails`), not the rate table.

Cache write cost is split by tier when `cache_5m` / `cache_1h` are available in `usageDetails`; otherwise all cache_create is billed at the 5m rate.

**Extended-thinking tokens are NOT a separate billing category.** `usage.output_tokens_details.thinking_tokens` is a breakdown *inside* `output_tokens`, billed at the plain output rate. It is reported as the `output_thinking` line in `usageDetails` and rolled up in the session rollup `thinking` (`{total_thinking_tokens, total_output_tokens, share_of_output, turns_with_thinking, max_thinking_tokens}`) purely for analytics — never added to a cost total, and `calculate_turn_cost` does not take it as an input. It answers "what is the effort level actually buying", which the `effort_level` tag alone cannot. Note the token *count* is present even though CC has stripped thinking *text* from transcripts since v2.1.112.

### Pricing Multipliers

These stack multiplicatively on the base rates above (and apply uniformly across input, output, cache read, and cache write tiers):

- **Fast mode (`speed="fast"`)**: per-model premium — **2x** on Opus 5.5 ($8/$40), Opus 5 and Opus 4.8 ($10/$50), **6x** on Opus 4.6 / 4.7 ($30/$150). Opus 4.5 and Sonnet/Haiku are ineligible and keep base rates. Multipliers live in `FAST_MODE_MULTIPLIERS` in `langfuse-hook.py`.
  - Fast mode is now offered on **Opus 5.5 / 5 / 4.8 only** — `speed="fast"` on Opus 4.7 returns an API error, and the Opus 4.6 `-fast` model ID was retired (requests silently fall back to standard). The 4.6/4.7 entries stay in the table on purpose: turns recorded while fast mode was live on those generations *were* billed at 6x, and reprocessing must keep billing them that way.
- **Fable 5 / Mythos 5**: ineligible for fast mode (no `/fast` variant) and data residency (`inference_geo` multiplier unverified) — always billed at base $10/$50. Update `calculate_turn_cost` if Anthropic publishes multipliers for them.
- **Data residency (`inference_geo="us"`)**: 1.1x on Opus 4.6+ (including Opus 5 and 5.5) / Sonnet 4.6+ (including Sonnet 5 and 5.5) / Haiku 5.5 (both prompt-length tiers). Other models do not support the `inference_geo` parameter; multiplier is not applied.
- **Fast + US-geo stack**: 2x × 1.1x = 2.2x (Opus 5.5 / 5 / 4.8); 6x × 1.1x = 6.6x (Opus 4.6/4.7).

### Server-side Tool Billing

- **Web search**: $0.01 per request (`$10 / 1,000 searches`). Billed via `costDetails.web_search` line item using `web_search_requests` from `usage.server_tool_use`. Added to turn total.
- **Web fetch**: free (token cost only).
- **Code execution**: $0.05/container-hour after 1,550 free hours/org/month. Not currently emitted by Claude Code; not billed.

**Keeping prices up to date:** Pricing is hardcoded in `calculate_turn_cost()` (`langfuse-hook.py`). When Anthropic releases new models or changes prices, update that function and the table above. Unknown models return $0 and emit a `[WARN]` in `langfuse-hook.log` — that's the signal to update. We send explicit costs rather than relying on Langfuse's built-in model table because Langfuse's table lags new model releases by days/weeks.

Two signals catch pricing staleness, and they fail differently:

| Signal | Catches | Misses |
|--------|---------|--------|
| `[WARN] unrecognised … model` in the hook log | A **new** model ID (cost shows as $0 — loud) | A **rate change** on a model already in the table |
| `tools/check_pricing_drift.py` | Rate changes on `MODELS`, **and** any current first-party (`litellm_provider: anthropic`) feed model the hook prices at $0 (reported `NEW`) | Multipliers, intro-window boundaries, server-tool rates |

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
`global.anthropic.claude-*`). The family parsers in `calculate_turn_cost`
(`_opus_family` / `_model_family`) already bill these at the **base** first-party rate (e.g. `anthropic.claude-opus-4-8`
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
| Sonnet 5.5 / 5 | $2 / $10 | unverified | unverified |
| Sonnet 4.x | $3 / $15 | $3 / $15 | $3.30 / $16.50 |
| Haiku 5.5 | $0.10 / $0.50 (≤100K) | unverified | unverified |
| Haiku 4.5 | $1 / $5 | $1 / $5 | $1.10 / $5.50 |

**Known gap (not coded):** the +10% geo premium is **not** auto-applied — Claude Code
transcripts do not expose the Bedrock endpoint type, so the matcher cannot tell a
global call from a geo call and bills both at base. Fable 5 is not yet available on
Bedrock. Canonical source: aws.amazon.com/bedrock/pricing (verified June 2026).
Opus 5 ships on Bedrock as `anthropic.claude-opus-5` (plus `us.`/`eu.`/`au.`/`jp.`
geo prefixes); `_opus_family()` bills all of them (and `anthropic.claude-opus-5-5`) at
the first-party base rate. The Bedrock rates for Opus 5.5, Sonnet 5.5 and Sonnet 5 have not been
checked against aws.amazon.com/bedrock/pricing.

## Tags and Metadata

> Transcript-field coverage verified against Claude Code **v2.1.295** (2026-10-09) by a
> **census of real transcripts** (242 sessions), not just the changelog. The changelog
> rarely announces transcript fields: the v2.1.221–v2.1.286 reviews below found "nothing",
> yet the census found these uncaptured fields from that range. Now captured per generation
> (`gen_metadata_turn_signals`) and rolled up to the session `turn_signals` metadata:
> - `requestedModel` (assistant, 2.1.295) — the model asked for. Equal to `message.model`
>   except after a fallback, so it is emitted as `requested_model` + `model_fallback: true`
>   only when the two differ; session tag `model-fallback`.
> - `thinkingDurationMs` (assistant, 2.1.287) — written on every streamed entry of a
>   message with differing values; max per message, summed per turn → `thinking_duration_ms`.
> - `truncatedAfterOutput` (2.1.289) / `isAbortedMidStream` (2.1.252) — turn flags
>   `truncated_after_output` / `aborted_mid_stream`; session tag `has-truncated-output`.
> - `advisorModel` (2.1.280) — echo of the `/advisor` setting on every entry, not proof the
>   advisor ran; emitted as `advisor_model`.
> - `turnOrigin` (user, 2.1.278: `human`/`sdk`/`task_notification`/`peer`) — on the entry
>   that opens a turn; `turn_origin` per generation, `turn_origins` counts per session.
>   Overlaps the older `origin`/`promptSource` used by `prompt_provenance`, but is present
>   on every opening entry rather than a minority.
>
> Seen but **not captured**: `perTurnEffort` (duplicates `effort`), `quotaLimits`
> (rate-limit status on rejected requests — candidate for a future event), `turnPosition`,
> `apiBlockIndex`, `attributionAgent`, `scheduledTaskId`, `serverClassifierRequest`,
> `wireToolInputs` / `wireIngestContext` (raw tool inputs, already in tool spans),
> `usage.fallback_credit` (always null so far), and the `cost-state` entry (Claude Code's
> own running cost total — usable as a cross-check against `session_cost_usd`).
>
> Changelog v2.1.253–v2.1.286 reviewed for new transcript-JSONL fields or hook-input changes — nothing
> found (changes were fixes to redaction, subagents, retries and model fallback). One behaviour
> to note: since 2.1.286 a refused default model is retried once on the previous model of the
> same tier, so a session can carry turns from two models; per-turn `model` already handles it.
> Changelog v2.1.221–v2.1.252 reviewed for new transcript-JSONL fields — nothing found (changes were
> hook-event additions (`PreModelSwitch`/`PostModelSwitch`), worktree/UI/TUI fixes, not new
> transcript entry types).
> Note: `agent_id`/`parent_agent_id` and skill `invocation_trigger` are OTel-span /
> hook-stdin fields, **not** transcript JSONL — unreachable by this hook's transcript
> parsing.
>
> New in v2.1.220: **`effort` is now a per-assistant-entry transcript field** (previously
> reachable only via the `$CLAUDE_EFFORT` env var, session-level and live-fires-only).
> Captured per-generation as `effort` metadata and rolled up to the session
> `effort_level` / `effort:<level>` tag. It survives `--reprocess`. The `$CLAUDE_EFFORT`
> env fallback was removed 2026-10-11: every transcript still on disk (Claude Code keeps
> 30 days) carries the field.
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
> `messageCount`) — captured as per-turn `duration_ms` when already written at send time
> and rolled up to `active_duration`; `queue-operation` (prompt-queue
> enqueue/dequeue/popAll/remove with prompt `content`) — captured as `queue_operations`
> wait-time analytics (content NOT stored, PII). The `agent-setting` entry
> (`{agentSetting: "claude"}` = the FleetView selected agent persona) is observed but
> **not captured** — always `"claude"` on first-party installs, so no signal yet; revisit
> if non-default agents appear (would become an `agent-setting:<slug>` tag).

Each turn is its own trace, grouped by `session.id` (Langfuse **Sessions** view sums
cost and tokens across them). A turn trace carries:

- **name** — the turn's prompt (80 chars, redacted); fallback `Turn <n>`. A turn with no typed
  prose is named by what started it (`_stub_label`): `/<command> <args>` for a slash command,
  `Task notification: <summary>` for a background task finishing, `Skill: <name>` for a skill
  body. Without this, 147 of 483 local turns were named by raw `<task-notification>` /
  `<local-command-caveat>` markup or the first line of a skill body
- **metadata** — session state as of the fire that sent it: `session_title` (see
  precedence below), `cwd`, `repo_name`, `git_branch`, `cli_version`, `entrypoint`,
  `custom_title`, `ai_title`, `agent_name`, `session_kind`, `permission_mode`,
  `effort_level`, `worktree`, `remote_control`, plus `turn_index`
- **tags** — the session's tags as of that fire (below). An early turn therefore lacks tags
  that only appear later (e.g. `pr:<N>`); the session score's `tags` rollup has them all.

Session-wide rollups are not on the traces: they ride on the session-level
**`session_cost_usd` score** (see Hook-Level Scores), whose `metadata` holds every
"Session Rollups" key below and which every fire overwrites.

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
- `model-fallback` — present when any turn was answered by a different model than its `requestedModel` (CC 2.1.295+)
- `has-truncated-output` — present when any turn carried `truncatedAfterOutput`
- `has-queued-prompts` — present when the user queued at least one follow-up prompt mid-turn (`queue-operation` entries); see `queue_operations` metadata
- `model-missing` — present when any turn had **billable tokens but no `model` field**; that turn's cost is reported as `$0` (never defaulted to a priced model) and a `[WARN]` is logged. Filter on this to find sessions with under-reported cost from upstream transcript gaps.
- `permission:<mode>` — current permission mode (`default`, `acceptEdits`, `plan`, `bypassPermissions`)
- `pr:<N>` — one tag per PR linked from the session (via `pr-link` transcript entries)
- `agent-name:{slug}` — present when `type: "agent-name"` entry exists (e.g., `agent-name:langfuse-usagedetails-fix`)
- `session-kind:{bg|fg}` — background job vs interactive foreground session (from `sessionKind` field on transcript entries; defaults to `fg` when absent on older transcripts)
- `effort:<level>` — effort level for the session (`low`/`medium`/`high`/`xhigh`/`max`). Sourced from the per-turn transcript `effort` field (CC 2.1.220+, last turn that carried one), so it is present on reprocessed sessions too
- `skill:<slug>` — one per distinct `attributionSkill` observed in the session (e.g., `skill:superpowers:brainstorming`)
- `plugin:<name>` — one per distinct `attributionPlugin` observed (e.g., `plugin:superpowers`)
- `compacted` — present when the session was context-compacted (a `compaction` rollup exists)
- `compact-trigger:<trigger>` — one per distinct compaction trigger (`manual`/`auto`; `unknown` omitted)
- `remote-control` — present when the session was bridged to claude.ai (Remote Control / `/remote-control`)
- `permission-bypass` — present when `bypassPermissions` was active at any point in the session
- `worktree:<name>` — present when the session ran inside a Claude Code worktree (from the last `worktree-state` entry)
- `has-background-tasks` — present when the Stop payload carried running background tasks
- `has-subagents`, `subagents:<count>` — present when the session dispatched subagents
- `nested-subagents` — present when any ingested subagent was spawned by another subagent (depth ≥ 2; CC 2.1.172+)
- `stop-reason:<reason>` — one per distinct **anomalous** terminal `stop_reason` in the session (`max_tokens`, `stop_sequence`, `refusal`, `pause_turn`, or any unknown reason). The high-frequency normal reasons `end_turn` (turn done) and `tool_use` (paused to call a tool) are **not** tagged — they'd flood every trace with no signal (`NORMAL_STOP_REASONS`). Full per-reason counts live in the `stop_reasons` metadata regardless. Filter on `stop-reason:max_tokens` to find turns whose output was truncated at the token cap.

**`session_title` precedence:**
1. `customTitle` from `type: "custom-title"` (user-set via in-CLI title command)
2. `aiTitle` from `type: "ai-title"` (Claude-generated short title once enough session context exists)
3. `agentName` from `type: "agent-name"` (auto-generated mid-session slug, e.g. `langfuse-usagedetails-fix`)
4. Truncated first non-synthetic user prompt (80 chars) — the turn's `display_prompt`: the first typed (non-`isMeta`) text that is not a `<tag>` stub or an interrupt marker, else the stub label (`/command args`, `Skill: <name>`)
5. `{repo_name}/{git_branch}` composite — stable fallback when prompt is empty
6. `"Claude Code Session"` hardcoded fallback

The auto-generated 3-word slug (e.g. `goofy-frolicking-dove`) was removed from
JSONL transcripts in Claude Code v2.1.112 and **returned in ~v2.1.211** — no longer as its
own entry type, but as a `slug` field on `assistant`/`user` entries. It is deliberately
**not** used for trace naming: it is a random handle (`sharded-shimmying-hummingbird`),
whereas `agentName` is descriptive of the actual task (`context window optimization`).
The `agent-name` entry (~30% into a session)
provides a stable, descriptive slug once the model identifies the task. Turns sent early
carry the first prompt as `session_title`; turns sent after `agent-name` appears carry the
slug.

**Session Rollups** (`metadata` of the `session_cost_usd` score):
All rollups below are computed over the **whole session** on every fire, and the score
upserts, so it always shows the latest. Keys whose value would be null are omitted. It
also repeats the session state listed above and the full `tags` list.
- `git_branch`, `cli_version`, `entrypoint`, `repo_name`, `cwd`
- `turn_count`, `tool_calls_total`, `total_tokens`, `total_input_tokens`, `total_output_tokens`
- `session_cost_usd` — cost of every parent (non-subagent) turn in the session; equals `subagent_costs.parent_cost` when subagents exist
- `api_errors` — error summary from `system/api_error` entries: `total_count`, `by_status` (HTTP codes), `first_error_at`, `last_error_at`
- `api_error_messages` — **separate** assistant-channel error/retry summary from `isApiErrorMessage` entries (CC 2.1.179+/2.1.181 auto-retry): `{count, by_status (apiErrorStatus, e.g. 404/429/529), by_error (e.g. model_not_found/rate_limit), first_at, last_at}`, plus `max_retry_attempt` when any entry carries `retryAttempt`. Null when none. From `extract_api_error_messages()`. Distinct channel from `api_errors`; these stubs are skipped by `build_turns` so they don't pollute turn output.
- `interrupts` — `{count}` of user interrupts (ESC mid-turn) from `interruptedMessageId` on `user` entries. A friction / misalignment signal. Null when none. From `extract_interrupts()`.
- `queue_operations` — prompt-queue activity from `type:"queue-operation"` entries (prompts the user queued while the assistant was busy): `{operations: {<op>: n}, queued, executed, removed, total_wait_ms, max_wait_ms}`. `executed` = prompts pulled from the queue to run (`dequeue`/`popAll`); `removed` = queued prompts the user cancelled. Wait = time a prompt sat queued (enqueue→dequeue/popAll), matched **FIFO** (no per-prompt id → approximate); `remove` is excluded from wait stats. Prompt `content` is **not stored** (PII). Long waits = the user racing ahead of the agent (friction signal). Null when nothing was queued. From `extract_queue_operations()`.
- `stop_reasons` — rollup of per-turn terminal `stop_reason` (the API's reason each turn ended): `{by_reason: {<reason>: count}, turns_counted, last}`. `last` = the terminal reason of the session's final turn that carried one (the outcome). Null when no turn carried a stop_reason. Anomalous reasons also surface as `stop-reason:<reason>` tags. When any turn was refused, adds `refusal_categories: {<category>: count}` from the API's `stop_details.category` (null → `uncategorized`), also tagged `refusal:<category>`. From `build_stop_reasons_summary()`.
- `cost_by_model` — per exact model ID: `{turns, input_tokens, output_tokens, cache_read_tokens, cache_create_tokens, cost_usd, by_effort: {<effort|unset>: {turns, cost_usd}}}`. Turns with no model go to `_missing`. From `build_model_cost_breakdown()`.
- `opus_5_5_savings` — Opus 5.5 turns re-priced at Opus 5 rates: `{turns, actual_cost_usd, opus_5_equivalent_cost_usd, saved_usd}`. An estimate: same token counts assumed (same tokenizer, but a different model and default effort would not spend exactly the same). Null without Opus 5.5 turns. From `build_opus_5_5_savings()`.
- `active_duration` — session rollup of per-turn wall-clock from `system/turn_duration` entries: `{total_ms, turns_measured, max_ms}`. `total_ms` is *active* time (Σ per-turn `durationMs`, each turn start→end incl. tool execution) — distinct from the trace wall-clock span, which also counts idle time between turns. Null when no turn carried a duration (CC before ~2.1.19x never emitted the entry). From `build_active_duration_summary()`.
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
- `total_iterations` — sum of per-turn `iteration_count` across the session (server-side agentic-loop iterations from `usage.iterations`).
- `cache_miss` — session rollup of cache misses from `message.diagnostics.cache_miss_reason`: `{total_missed_tokens, by_reason: {<type>: count}, turns_with_miss}`. Explains *why* `cache_hit_rate` is low and how many input tokens were re-sent (e.g. `tools_changed`). Null when no turn missed cache.
- `effort_level` — session effort level (`low`/`medium`/`high`/`xhigh`/`max`), last-observed. From the per-turn transcript `effort` field (CC 2.1.220+), so it survives `--reprocess`. See per-generation `effort` for the per-turn value.
- `compaction` — rollup of `system/compact_boundary` events: `{count, triggers: {<trigger>: n}, total_tokens_reclaimed (Σ preTokens−postTokens), total_pre_tokens, total_post_tokens, total_duration_ms, events: [{trigger, pre_tokens, post_tokens, tokens_reclaimed, duration_ms, timestamp}]}`. Surfaces context-window pressure and the token/time cost of compaction. Legacy `type:"summary"` entries (pre-`compactMetadata`) count with `trigger:"legacy"` and no token fields. Unknown triggers pass through verbatim. Omitted when never compacted. From `extract_compaction()`.
- `remote_control` — `{bridge_session_id, url}` from `bridge-session` + `system/bridge_status` entries (the Remote Control / phone-web bridge). Either field may be absent independently; `url` correlates with the claude.ai session. **No timestamps** on these entries. Null when the session was never bridged. From `extract_bridge()`.
- `permission_timeline` — `{modes_used (sorted distinct), sequence (file-order, consecutive dups collapsed), transition_count, ever_bypass, ever_accept_edits}` from `permission-mode` entries. Complements the last-observed `permission_mode`/`permission:<mode>` tag with the full transition history. **No timestamps** on these entries → sequence is order-only, not timed. Null when no `permission-mode` entries. From `extract_permission_timeline()`.
- `worktree` — `{name, branch, original_cwd, original_branch, original_head_commit}` from the last `type: "worktree-state"` entry. Null when the session never entered a worktree. From `extract_worktree_state()`.
- `background_tasks` / `session_crons` — raw lists from the Stop hook stdin payload (v2.1.145+). Live fires only; omitted on reprocess.
- `subagent_costs` — see Subagent Cost Tracking.

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
- `duration_ms` — turn wall-clock in ms from the `system/turn_duration` entry that follows the turn in the transcript (turn start→end, incl. tool execution). CC writes that entry *after* the Stop hook runs, and a turn is sent once, so a live fire usually omits it; a re-ingest has it. Rolled up session-wide as `active_duration`.
- `cost_usd` — the turn's cost (same as `costDetails.total`), for filtering.
- `cache_miss_reason` — dominant cache-miss reason type for the turn (e.g. `tools_changed`), from `message.diagnostics`. Omitted when the turn had no cache miss.
- `cache_missed_tokens` — total input tokens that missed cache in the turn. Omitted when no miss.
- `cache_miss_by_reason` — `{<type>: count}` tally of miss reasons in the turn. Omitted when no miss.

**OpenTelemetry GenAI semantic-convention attributes** (raw span attributes on each generation):
Standard `gen_ai.*` attributes next to the `langfuse.*` ones, so any OTLP backend
(e.g. Laminar, an OTel collector) can read the spans without a custom transform.
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

- **Export each span once.** Langfuse v4 does not deduplicate a re-exported span ID on the read path; a re-send creates a second observation and doubles its cost. That is why turns are sent only when finished, why each fire is one request, and why a re-ingest must follow `--delete-traces`.
- **Usage and cost are JSON strings.** `langfuse.observation.usage_details` / `langfuse.observation.cost_details` (a JSON object as a string). Our explicit cost overrides Langfuse's own model-price inference, whose built-in table lags new models by days or weeks — which is why we send explicit costs.
- **Cache tokens only show via usage details.** `cache_read` / `cache_create` (with `cache_5m` / `cache_1h` tier breakdowns) appear as separate line items only because they are keys of `usage_details`.
- **Metadata values are strings.** Each key is its own attribute (`langfuse.trace.metadata.<k>`, `langfuse.observation.metadata.<k>`) so it is filterable; non-string values are JSON-encoded. Any other attribute lands under `metadata.attributes`.
- **Trace-level attributes go on the root span** (the turn's generation): `langfuse.trace.name`, `langfuse.trace.tags`, `langfuse.trace.metadata.*`. `session.id` / `user.id` go on every span so filters work on child observations too.
- **Scores are not spans.** They go to `POST /api/public/scores` and upsert by id. The value is replaced, but **metadata merges key by key** (verified 4.48.0, 4.56.0): a key a later fire no longer writes keeps its old value. Scores belong to no trace, so a trace delete leaves them; `--delete-traces` deletes them by id (`session_score_id`).
- **Ingestion is async.** A 200 means "queued"; observations appear a few seconds later, and malformed-but-parseable attributes are dropped silently.

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

When Claude Code spawns subagents via the Agent tool, the hook ingests their transcripts as nested observations under the Agent tool span, in the trace of the turn that launched them.

**How it works:**
1. Hook detects `Agent` tool_use events in parent assistant messages
2. Discovers matching subagent transcripts at `<session>/subagents/agent-{id}.jsonl`
3. Correlates via the `.meta.json` sidecar next to each transcript, whose `toolUseId` is the
   spawning `Agent` tool_use id (`build_subagent_meta_index`). Every subagent on disk has one
   (147/147, 2026-10-11). The old agentId-regex and timestamp-proximity fallbacks were
   removed — the timestamp pass also skipped any subagent whose first line has no
   timestamp, which is how Claude Code now starts forked agents (`fork-context-ref`), so
   those were silently dropped before.
4. Ingests each subagent's **finished** turns as generations nested under the Agent tool span
5. Rolls up per-subagent and total harness cost in the session rollup `subagent_costs`

**Background agents and resume:** an async Agent call returns "Async agent launched" at
once, so the parent turn ends while the subagent is still working. Each `sa_state` entry
records the agent's transcript `path`, its `size` at last read, the `parent_span_id` of
its Agent tool span and the `trace_id` of the turn that launched it. Every fire re-reads any
known agent whose file has grown and sends its newly **finished** turns into that trace (an
unfinished last turn waits, same rule as the parent). `ingest_subagent` always reads the
whole subagent transcript (a resumed agent is usually mid-turn, and a tail with no user
prompt builds no turns) and returns the agent's **whole** cost; `_subagent_record` replaces
the stored totals with it rather than adding to them.

**Nested sub-agents (CC 2.1.172+):** sub-agents can spawn sub-agents up to 5 levels deep.
Transcripts stay in the flat `<session>/subagents/` dir; each `.meta.json`'s `toolUseId`
points at the `Agent` tool_use in the *spawner's* transcript. `ingest_subagent` recurses
(depth cap 5, cycle guard), nesting child generations under the parent agent's tool span.
Each agent appears once in `subagent_costs.agents` with `depth` and `parent_agent_id`.

**Langfuse hierarchy:**
```
Trace (one parent turn; session.id groups the turns)
├── Generation (parent turn)
│   └── Span (Agent tool call)
│       ├── Generation (subagent turn 1)
│       │   └── Span (Read tool)
│       └── Generation (subagent turn 2)
│           └── Span (Agent tool call — nested dispatch)
│               └── Generation (nested subagent turn 1)
│                   └── Span (Bash tool)
```
Session score `session_cost_usd` → metadata `subagent_costs` summary

**Exclusions:** `aside_question` subagents are skipped (internal sidechain queries).

**State:** Subagent offsets stored in `~/.claude/langfuse-state/<session_id>.subagents.json`. Both state files are saved only after a successful send — so a timeout retry re-ingests subagent events rather than skipping them. The parent's own cost is recomputed from the whole transcript each fire (`session_cost_usd` rollup).

**Tags:** Turn traces of a session with subagents get `has-subagents` and `subagents:{count}` tags.

## Important Notes

- **Moved to OTLP, one trace per turn (2026-10-11).** Before, every session was one
  `trace-<session>` trace sent through the legacy ingestion API. Migrate a stack with
  `--delete-traces` **while the stack still runs `legacy`** (deletes every per-turn trace,
  the old `trace-<session>` one and the session scores for each local transcript, and
  resets hook state), wait until the traces are gone, run the one-time `dual` + historic
  backfill so sessions older than Claude Code's 30-day transcript retention stay visible
  (they cannot be re-sent), switch the stack to `LANGFUSE_MIGRATION_V4_WRITE_MODE=events_only`
  (the compose default), then `--reprocess`. Exact steps: REMOTE-DEPLOY.md, "Langfuse v4". `--reprocess` keeps hook state and sends only turns it marks unsent, so it is
  safe to repeat; a full re-send needs `--delete-traces` first, which resets that state.
  (It used to reset state itself, and v4 keeps every export, so a reprocess without a
  completed delete doubled cost.)

- **ClickHouse system logs are capped** by `clickhouse/config.d/z_system_logs.xml`: the noisy `system.*` log tables are disabled, `query_log` / `part_log` keep 30 days, the server log is at `warning`. Left alone they reached 20 GiB on the Pi next to 87 MiB of Langfuse data. The config only shapes newly created tables; an existing stack drops the old ones once (REMOTE-DEPLOY.md, "ClickHouse system logs")
- `.env` contains generated secrets - never commit it
- The hook never blocks Claude Code: `setup.sh` registers it on Stop / StopFailure / SessionEnd with `"async": true`, and every error, including an unexpected crash, goes to the hook log. A hook registered without `async` runs in the foreground (measured: median 0.8s, max 23s per Stop)
- First Langfuse login may need incognito window (stale CSRF tokens)
- `setup.sh` backs up existing `~/.claude/settings.json` before modifying hooks
- All ports are localhost-only by default. LAN exposure of the dashboard is opt-in via `LANGFUSE_WEB_BIND` in `.env` (bind to host LAN IP; HTTP only, no TLS) — see `REMOTE-DEPLOY.md`. All other services stay `127.0.0.1`.

## Hook-Level Scores

Scores attach to the **session** (`sessionId`, not a turn trace) and are recomputed over the whole session on every fire:

| Score | Type | Values | Source |
|-------|------|--------|--------|
| `session_cost_usd` | Numeric | USD | parent + subagent cost; `metadata` = every session rollup |
| `cache_hit_rate` | Numeric | 0.0-1.0 | cache_read / (cache_read + cache_creation) |
| `tool_error_rate` | Numeric | 0.0-1.0 | executed calls with `[ERROR]` output / executed calls |
| `tool_denial_rate` | Numeric | 0.0-1.0 | denied calls / all attempted calls |

All are deterministic (no LLM calls). Score IDs are `uuid5(session:score:<name>)`, so
each fire overwrites the previous value. The three rates are omitted entirely when their denominator is zero
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
