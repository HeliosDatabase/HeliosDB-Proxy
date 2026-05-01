---
name: heliosproxy-<verb>
description: <One-sentence imperative description, including likely user phrasings ("when the user asks about X", "set up Y", "check Z").>
allowed-tools: Bash(curl *), Bash(psql *), Bash(docker *), Read, Grep
related: [heliosproxy-overview, heliosproxy-<related-skill>]
---

# <Title — what the operation accomplishes>

<One-line elevator pitch. What this skill is for, in plain language.>

## When to use

- <Concrete user-facing scenario A>
- <Concrete user-facing scenario B>
- <Concrete user-facing scenario C>

🔵 Read-only — safe to run anywhere, anytime
🟠 Mutating — changes proxy state; safe in dev, controlled in prod
🟡 Mutating, may fail — pre-flight before running in prod

## Surfaces

| Surface | When to use |
|---|---|
| Admin REST (`localhost:9090`) | Programmatic / CI / scripted |
| `psql -h localhost -p 6432`   | Interactive / SQL-side check |
| `demo.sh`                      | Reproducing the canonical demo |

## Recipes

### Recipe 1: <verb>

```bash
curl -s http://localhost:9090/<endpoint> | jq .
```

What it returns and how to read it.

### Recipe 2: <verb>

```bash
curl -s -X POST http://localhost:9090/<endpoint> \
  -H 'Content-Type: application/json' \
  -d '{...}'
```

### Recipe 3: <verb> (combined / advanced)

3–5 recipes total. Show real, copy-pasteable commands. Use real
hostnames/ports from the proxy default config (PG: 6432, admin: 9090).

## Pitfalls

- **Common error: `<message>`** — what it means and how to recover.
- **Don't <X>** because <Y>; instead <Z>.
- Feature gate: this op needs `<feature-flag>` enabled in `Cargo.toml`
  / `proxy.toml`. Check with `curl /version` or `grep features Cargo.toml`.

## See also

- `heliosproxy-overview` — pick the next skill from here
- `heliosproxy-<related>` — what it covers, why you'd hop there
- Demo: [`demos/v0.4.0/NN-name/`](../../demos/v0.4.0/NN-name/) — runnable end-to-end
- Code: [`src/admin.rs:NNN`](../../src/admin.rs) — endpoint implementation
