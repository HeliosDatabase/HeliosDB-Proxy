# Group A-2 — admin exposure (H1, M8)

Second slice of the security milestone. The admin API runs privileged
operations (arbitrary SQL via `/api/sql`, forced failover via `/api/chaos`,
migration cutover, branch `CREATE`/`DROP DATABASE`, replay/shadow against
operator-chosen targets), yet the shipped default bound `0.0.0.0:9090` with
`admin_token = None` — anonymous privileged access to anyone who could reach the
port.

## Changes (H1 — refuse anonymous non-loopback admin)
- **Default `admin_address` → `127.0.0.1:9090`** (config struct + CLI `--admin`).
  A fresh install is loopback-only and safe.
- **`ProxyConfig::validate` refuses to start** when `admin_address` resolves to
  a non-loopback IP AND `admin_token` is unset AND `admin_allow_insecure` is
  false — with a clear, actionable error (set a token, bind loopback, or opt in).
  Enforced for both file and CLI configs (`from_file` and the CLI path both
  validate). A hostname (non-IP) admin_address is left to the operator's
  DNS/network policy.
- **New `admin_allow_insecure: bool`** (default false) — explicit escape hatch
  for operators who front the admin port with their own auth/network policy.

**Breaking-change note:** a deployment that explicitly set a non-loopback
`admin_address` with no token will now fail to start until it sets `admin_token`
or `admin_allow_insecure = true`. This is intentional (fail-closed on a critical
hole); the escape hatch is one line.

## M8 — admin connection cap
The admin accept loop now bounds concurrent connections with a `Semaphore`
(`MAX_ADMIN_CONNS = 256`); excess connections are dropped, not queued, so a
connection flood can't spawn unbounded tasks (each of which may buffer up to the
8 MiB body cap).

## Gate
- Unit: `test_validate_refuses_anonymous_nonloopback_admin` (loopback+no-token OK;
  non-loopback+no-token refused; +token OK; +allow_insecure OK).
- End-to-end: the binary refuses to start on `0.0.0.0` admin without a token
  (clear error) and starts with `admin_allow_insecure = true`.
- No regression: `admin-auth-test` 5/5 (admin still works with a token on
  loopback); PG regression 9/9; clippy `-D warnings` ×4; 276 default + 1413
  all-features lib tests. All test-harness configs already use loopback admin.
