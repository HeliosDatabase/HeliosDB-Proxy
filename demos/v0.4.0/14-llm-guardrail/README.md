# Demo 14 — `llm-guardrail` plugin

**Module brief:** [§Module 14](../../../docs/website-brief-v0.4.0.md)

## UVP

> Hard refuses the four most dangerous LLM-output-shaped queries:
> DROP/TRUNCATE, DELETE/UPDATE without WHERE, SELECT against large
> tables without LIMIT, missing tenant_id filter on tenant-scoped
> tables.

## Use cases

- **Agent safety net.** Hallucinating agent writes
  `DROP TABLE users` — the proxy refuses; the agent sees an error
  and (hopefully) corrects.
- **Tenant isolation enforcement.** Agent forgets the
  `WHERE tenant_id = $X` filter — proxy refuses, preventing a
  cross-tenant read.

## What this demo shows

Connect with `application_name=claude-bot` to trigger AI-traffic
classification, then attempt each of the four dangerous patterns.
All are refused; the same patterns from `application_name=psql`
pass through.

```bash
PGPASSWORD=postgres psql -h localhost -p 6432 -U postgres -d demo \
  -d 'application_name=claude-bot' \
  -c "DROP TABLE events"
# ERROR:  Query blocked by plugin: llm-guardrail: DROP/TRUNCATE
#         forbidden in LLM-tagged traffic

PGPASSWORD=postgres psql -h localhost -p 6432 -U postgres -d demo \
  -d 'application_name=claude-bot' \
  -c "DELETE FROM users"
# ERROR:  Query blocked by plugin: llm-guardrail: DELETE/UPDATE
#         without WHERE forbidden in LLM-tagged traffic

PGPASSWORD=postgres psql -h localhost -p 6432 -U postgres -d demo \
  -d 'application_name=claude-bot' \
  -c "SELECT * FROM events"
# ERROR:  Query blocked by plugin: llm-guardrail: SELECT without
#         LIMIT against large table

PGPASSWORD=postgres psql -h localhost -p 6432 -U postgres -d demo \
  -d 'application_name=claude-bot' \
  -c "SELECT * FROM users LIMIT 5"
# ERROR:  Query blocked by plugin: llm-guardrail: missing tenant_id
#         filter on tenant-scoped table

PGPASSWORD=postgres psql -h localhost -p 6432 -U postgres -d demo \
  -d 'application_name=claude-bot' \
  -c "SELECT * FROM users WHERE tenant_id = 'acme' LIMIT 5"
#  id | tenant_id | name | email | ssn ...
#   1 | acme      | user-1 | ...
```

Same queries from `application_name=psql` pass through unchanged
(no AI tag → no enforcement).

## Run it

```bash
cd demos/v0.4.0/14-llm-guardrail
./demo.sh
```

## Implementation pointer

`HDB-HeliosDB-Proxy-Plugins/llm-guardrail/src/lib.rs`. The
`is_ai_traffic` helper is intentionally self-contained
(application_name keyword scan) so the guardrail works even
without `ai-classifier` deployed.

## HeliosDB compatibility

Backend-agnostic.
