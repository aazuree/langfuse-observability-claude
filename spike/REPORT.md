# Native OTel traces re-spike — report

**Date:** 2026-10-12 · **Claude Code:** 2.1.296 · **Langfuse:** 4.56.0 (`events_only`, throwaway `lfspike` stack) · **Collector:** otelcol-contrib 0.128.0
**Previous spike:** 2026-05-13 (Claude Code 2.1.140, Langfuse 3.172.1): verdict KEEP-AS-IS.

## Question

Can Claude Code's native OTel traces give a live view in Langfuse with correct cost and no PII? Either by replacing the Stop hook, or by running next to it.

## Method

- `spike/run.sh` runs one `claude -p` session per prompt (Haiku 5.5). The beta trace exporter is on (`CLAUDE_CODE_ENHANCED_TELEMETRY_BETA=1`) with every `OTEL_LOG_*` content gate on, export interval 1 s, and the `x-langfuse-ingestion-version=4` header.
- `--setting-sources local --settings spike/settings.json` keeps the production hooks from firing. Verified: the hook log and hook state have no entry for the spike sessions.
- **Phase A** sends straight to `/api/public/otel`. **Phase B** sends through `spike/collector.yaml`, which deletes PII, maps tokens, sets inputs and redacts a secret pattern.
- Prompts:
  - S1: text only
  - S2: Read + Bash
  - S3: Agent subagent
  - S4: three 20 s Bash calls in one turn, for live lag
  - S5: a prompt containing a fake `sk-` key
- `spike/poll.py` records when each observation first becomes visible.
- `spike/cost_parity.py` compares each hook generation's cost (offline, temp state dir) with Langfuse's cost for the native generations that share its request IDs.

## Results

| # | Check | A: direct | B: collector | Notes |
|---|-------|-----------|--------------|-------|
| 1 | Generation type + model | PASS | PASS | `llm_request` is inferred as a generation; `gen_ai.request.model` is mapped |
| 2 | Tokens in `usageDetails` | FAIL | PASS | Claude Code sends plain `input_tokens`/`output_tokens`/`cache_*_tokens` attributes. The collector rewrites them into `langfuse.observation.usage_details` |
| 3 | Cost | FAIL ($0) | PARTIAL | Langfuse prices Haiku 5.5 from its own table, including the >100K tier. Per-request tokens match the transcript exactly (14/14). **The span has no 5m/1h cache-write split**, so 1h writes are priced at the 5m rate. Main-thread generations come out 28–30% low |
| 4 | Turn input | FAIL | PASS | The prompt is only in the `user_prompt` attribute. The collector copies it to the input |
| 5 | Assistant output / tool output | FAIL | FAIL | Never sent, even with `OTEL_LOG_ASSISTANT_RESPONSES`/`OTEL_LOG_TOOL_CONTENT`. Content attributes need detailed beta tracing, which needs Anthropic to allowlist the org |
| 6 | Tool spans | PASS | PASS | Fixed since May. `tool_name`, `tool_use_id` (same as the hook's), `full_command`/`file_path`, permission decision, duration |
| 7 | Session grouping | PASS | PASS | `session.id` becomes the Langfuse session; `deployment.environment` becomes the environment |
| 8 | Subagent nesting | PASS | PASS | Fixed since May. Subagent LLM calls and tools nest under the Agent tool's `execution` span, with `agent_id` |
| 9 | PII | FAIL | PASS | Every span carries plaintext `user.email`, `user.account_id`/`account_uuid` and `organization.id` (`user.id` is hashed). The collector deletes them, and 0 remain in storage |
| 10 | Secret redaction | n/a | PASS | The fake key was stored 0 times. `replace_pattern` covers the prompt, the command and the input. The hook's full `SECRET_PATTERNS` would have to be ported to OTTL |
| 11 | **Real-time** | **PASS** | **PASS** | Each LLM call and tool shows up **1–3.5 s after it ends**, up to 60 s before the turn ends (S4). A span is sent only when it ends, so a running tool stays invisible. The root `interaction` span arrives at turn end |

Not available natively, so these stay with the hook: the hook's scores, tags, agent-name/PR/title signals, `api_errors`, the fast-mode and US-geo multipliers (the span has `speed`, but Langfuse doesn't apply it), and web-search fees.

## Side finding: hook bug (production)

`langfuse-hook.py:1379` keeps the **first** transcript entry's usage per message. The comment says "all entries for a given message_id carry identical usage since v2.1.97". **That doesn't hold for subagent transcripts.** There, the early entries carry partial streaming `output_tokens` (for example 4, then 476).

Measured with `spike/usage_drift.py` over all local transcripts:

| Transcripts | Messages | First ≠ last | Output tokens counted (first) | Real (last) | Uncounted cost |
|---|---|---|---|---|---|
| Main thread | 4,974 | 1 | 4.22M | 4.22M | $0.01 |
| Subagent | 5,378 | 3,767 (70%) | 267K | 2.72M | **$25.27** |

Subagent output tokens are counted about 10× low. Fix: take the last entry with usage per message (or the max `output_tokens`), then `--reprocess`. This is a separate PR.

## Verdict

**Replace: NO.** Native traces still lack assistant and tool output (allowlist-gated), the 5m/1h cache split, and every hook-derived signal and score. Replacing would also add a collector the hook path doesn't need.

**Hybrid live view: FEASIBLE, optional.** The work is config only, with no hook code:
- a collector container
- the env vars above in `settings.json`
- a separate Langfuse project (or a stripped `usage_details`), so native generations don't double-count cost next to the hook's

It gives about 2 s live visibility of LLM calls and tools during long turns and subagent runs. Cost: one more always-on component, the PII and redaction rules kept in OTTL, and partial data until Stop.

The May re-evaluation triggers now stand as follows:
1. Langfuse mapping Claude Code's attributes: still not met; the collector covers it.
2. A PII opt-out: still not met; the collector covers it.
3. Running a collector: shown to work.

**Recommended order:** fix the subagent usage bug first (wrong numbers in production). Adopt the live view only if long background turns make waiting for Stop a real problem. The June realtime-hook trial found no actionable value in a live view.
