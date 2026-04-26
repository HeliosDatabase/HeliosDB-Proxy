# Demo 13 — `token-budget` plugin

**Module brief:** [§Module 13](../../../docs/website-brief-v0.4.0.md)

## UVP

> Per-`(agent, model)` cost ceiling for AI traffic. Estimated from
> response bytes (≈ 4 bytes per token). Stops a runaway agent
> before the AWS bill hits.

## Use cases

- **LLM agent cost control.** Each agent ID gets a daily token
  budget; runaway loops stop at the budget instead of running
  unbounded.
- **Per-model billing throttle.** Different cost ceilings per
  model — `claude-opus` has a higher budget than `gpt-3.5`
  because each query costs more.

## What this demo shows

1. Seed `(rag-bot, claude-opus)` budget at minute=10, day=100.
2. Agent makes a query → estimated cost computed from response
   bytes.
3. Repeat until daily budget is exhausted (~100 queries).
4. Next query → blocked with retry-in-N-seconds message.

## Run it

```bash
cd demos/v0.4.0/13-token-budget
./demo.sh
```

Interactive script that:

- Loads `ai-classifier.wasm` + `token-budget.wasm` into the proxy.
- Connects with `application_name=rag-bot-claude-opus-4-7` so
  `ai-classifier` tags `agent_id=rag-bot...` + `model_id=claude-opus`.
- Runs queries in a loop, prints the running cost.
- Stops at the budget block.

## Implementation pointer

`HDB-HeliosDB-Proxy-Plugins/token-budget/src/lib.rs`. Cost model:
`tokens ≈ response_bytes / 4`. Budget windows: minute + day. Day
overrides minute (same precedence as cost-governor).

## HeliosDB compatibility

Backend-agnostic.
