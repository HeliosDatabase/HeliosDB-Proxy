# Research interview script — AI-platform teams running on PostgreSQL

**Goal**: 10 interviews × 30 minutes. Validate (or kill) the T2.2
plugin bundle: AI-traffic classifier, token budget, LLM guardrail,
pgvector router. Also collect signal for T2.4-P1 (column mask) since
PII enters AI workflows hard.

**Recruiting**: r/PostgreSQL, r/MachineLearning, Hacker News "Who's
Hiring", LinkedIn DMs to AI-platform leads at companies in our
target list (next page). Prefer companies *already in production*
with LLM features rather than greenfield.

## Logistics

- 30 min over Zoom / Meet. Recorded with consent.
- Notes go in `/Helios/research/<company>-<date>.md` (separate
  private repo).
- Anonymised quotes only in any public artefact.

## Target list (seed — refine after first 3 interviews)

- High-volume B2C with LLM features: Notion, Linear, Vercel, Retool,
  Hex, Cal.com, Posthog.
- AI-native infra: LangChain, LlamaIndex Cloud, MindsDB, Continue.
- Vertical AI: Hippocratic AI, Glean, Copy.ai, Jasper.
- Open-source tooling teams: Supabase, Neon, Crunchy Bridge, Aiven.

## Interview structure

### 1. Warm-up (3 min)

> Tell me about your stack — how many tenants / users, what's the
> AI feature you're shipping (RAG, agents, copilots, code-gen…),
> and where PostgreSQL lives in the request path.

Listen for:
- Single-tenant vs multi-tenant
- Vector-DB-native vs pgvector
- Per-tenant DB vs shared DB with RLS

### 2. Traffic shape (5 min)

> Walk me through a typical agent or LLM-generated query. What hits
> Postgres, in what volume, and how predictable is the shape?

Listen for:
- Burstiness — "the model decided to scan 10M rows because it
  hallucinated a JOIN"
- Query unpredictability — "we can't pre-tune the planner because
  the SQL changes every session"
- N+1 patterns from agent loops

### 3. Cost / quota story (5 min)

> How do you currently know how much DB time / cost a single
> agent run consumes? How do you stop runaway agents?

Listen for:
- "We don't" (most common)
- Manual heuristics ("pg_stat_statements + Slack alerts")
- Application-side soft limits
- Existing rate-limiting at the API layer (which doesn't help if
  the same agent makes 100 small queries)

### 4. Bad-SQL incidents (5 min)

> Have you been burned by LLM-generated SQL? `DROP`, missing
> `WHERE`, infinite cursor, anything?

Listen for:
- Concrete incident stories — get permission to anonymise + cite
- Mitigations they put in place
- Whether they trust the LLM at all (pre-validation rules,
  hand-written allowlists)

### 5. Validation: would they buy this? (8 min)

Walk through the four T2.2 plugins:

- **ai-classifier**: detects LLM-generated queries via heuristics +
  explicit tags.
- **token-budget**: per-agent / per-model cost budgets.
- **llm-guardrail**: rejects DROP, requires WHERE on tagged tables.
- **pgvector-router**: pins HNSW similarity queries to a vector-
  specialised replica.

> If your proxy could enforce these without code changes in your
> app, would you (a) install it tomorrow, (b) install it in the
> next quarter, (c) install it never?

Push for **why**. The "never" answers are the most valuable —
they show us the gap between our story and their reality.

### 6. Adjacent (4 min)

> Quick yes/no with one-line reason:
> - Tamper-evident audit log (compliance) — would you turn it on?
> - Column-level masking for PII (per role) — would you turn it on?
> - Time-travel replay (replay yesterday's queries against staging
>   to debug) — would you use it?

These are T2.4 + T2.5 features; we want signal even though they
weren't the headline.

## Adoption-signal score

After every interview, fill:

| Question | Score 0-3 | Notes |
|---|---|---|
| Has the pain we're solving |  |  |
| Currently has a workaround |  |  |
| Would install tomorrow |  |  |
| Would install in 3 months |  |  |
| Would pay for support |  |  |

Aggregate: ≥ 3.0/5 across ≥ 7 interviews → ship T2.2.
Below that → restructure or kill the bundle.

## Output

After 10 interviews, a 1-2 page memo in
`docs/research/ai-platform-findings.md` (this repo) with:

- 5 most-cited pain points (anonymised quotes)
- 3 features the bundle is missing
- 2 features in the bundle nobody wants
- Recommendation: ship as-is / restructure / kill

## Pre-interview checklist

- [ ] Calendar invite with 30-min Zoom link.
- [ ] One-line "what we're building" pre-read in the invite.
- [ ] Recording consent confirmation in the meeting.
- [ ] Notes template open in `/Helios/research/`.
- [ ] Five-minute hard stop before the budgeted hour to write the
      adoption-signal score while context is fresh.
