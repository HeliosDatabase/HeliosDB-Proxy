# Demo 16 — `column-mask` plugin

**Module brief:** [§Module 16](../../../docs/website-brief-v0.4.0.md)

## UVP

> Per-role PII masking via SQL rewriting at the proxy. No DB-side
> view sprawl; no schema migrations.

## Use cases

- **GDPR / HIPAA / PCI scope reduction.** Application code stays
  unchanged; PII columns appear masked unless the user has the
  `pii_reader` role.
- **Vendor data sharing.** Read-only consultant role gets masked
  PII; internal investigators with `pii_reader` see raw.

## What this demo shows

The shared `init.sql` provisions:

- `users.ssn` and `users.email` columns
- SQL functions `mask_ssn(text)` → `XXX-XX-1234` and
  `mask_email(text)` → `u***@example.com`
- Roles `app_user` (no PII) and `pii_reader` (sees raw)

Mask rules seeded into the plugin's KV namespace:

```json
[
  {"table":"users","column":"ssn","mask_function":"mask_ssn","unmask_role":"pii_reader"},
  {"table":"users","column":"email","mask_function":"mask_email","unmask_role":"pii_reader"}
]
```

Two queries, two outcomes:

```bash
PGPASSWORD=app psql -h localhost -p 6432 -U app_user -d demo \
  -c "SELECT name, email, ssn FROM users WHERE id = 1"
#  name   |          email          |     ssn
# --------+--------------------------+-------------
#  user-1 | u***@example.com         | XXX-XX-0001

PGPASSWORD=pii psql -h localhost -p 6432 -U pii_reader -d demo \
  -c "SELECT name, email, ssn FROM users WHERE id = 1"
#  name   |       email        |     ssn
# --------+--------------------+-------------
#  user-1 | user-1@example.com | 100-00-0001
```

## Run it

```bash
cd demos/v0.4.0/16-column-mask
./demo.sh
```

## Implementation pointer

`HDB-HeliosDB-Proxy-Plugins/column-mask/src/lib.rs`. SQL-rewriting
logic in `apply_rules` (substring-based, not a parser);
`find_word` whole-word matcher avoids false positives on `_ssn` /
`ssna`. 8 unit tests including idempotence.

## HeliosDB compatibility

Backend-agnostic (the mask functions are SQL-side; HeliosDB
supports `CREATE FUNCTION` identically to PG).
