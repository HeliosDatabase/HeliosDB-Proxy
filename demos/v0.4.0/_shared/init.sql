-- Shared workload schema for v0.4.0 demos.
--   users       — small (1 000 rows). Has SSN + email columns the
--                 column-mask demo depends on.
--   orders      — medium (10 000 rows). Joins to users.
--   events      — large (100 000 rows). The llm-guardrail demo
--                 considers this a "large table" and refuses
--                 unbounded SELECTs against it.
-- One tenant per row so multi-tenant demos can filter by tenant_id.

CREATE TABLE users (
    id         SERIAL PRIMARY KEY,
    tenant_id  TEXT NOT NULL,
    name       TEXT NOT NULL,
    email      TEXT NOT NULL,
    ssn        TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    id         SERIAL PRIMARY KEY,
    tenant_id  TEXT NOT NULL,
    user_id    INTEGER REFERENCES users(id),
    amount     NUMERIC(10,2) NOT NULL,
    status     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE events (
    id         BIGSERIAL PRIMARY KEY,
    tenant_id  TEXT NOT NULL,
    user_id    INTEGER,
    kind       TEXT NOT NULL,
    payload    JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO users (tenant_id, name, email, ssn)
SELECT
  CASE WHEN i % 4 = 0 THEN 'acme'
       WHEN i % 4 = 1 THEN 'globex'
       WHEN i % 4 = 2 THEN 'initech'
       ELSE 'umbrella' END,
  'user-' || i,
  'user-' || i || '@example.com',
  to_char(100000000 + i, 'FM000-00-0000')
FROM generate_series(1, 1000) AS i;

INSERT INTO orders (tenant_id, user_id, amount, status)
SELECT
  u.tenant_id,
  u.id,
  (random() * 1000)::numeric(10,2),
  (ARRAY['pending', 'paid', 'shipped', 'cancelled'])[1 + floor(random() * 4)::int]
FROM users u, generate_series(1, 10) AS s;

INSERT INTO events (tenant_id, user_id, kind, payload)
SELECT
  CASE WHEN i % 4 = 0 THEN 'acme'
       WHEN i % 4 = 1 THEN 'globex'
       WHEN i % 4 = 2 THEN 'initech'
       ELSE 'umbrella' END,
  1 + (i % 1000),
  (ARRAY['login', 'logout', 'view', 'click', 'purchase'])[1 + (i % 5)],
  jsonb_build_object('seq', i)
FROM generate_series(1, 100000) AS i;

CREATE INDEX ON orders (tenant_id);
CREATE INDEX ON events (tenant_id, created_at);

-- SQL-side mask functions used by the column-mask demo.
CREATE OR REPLACE FUNCTION mask_ssn(s TEXT) RETURNS TEXT
  LANGUAGE sql IMMUTABLE STRICT AS
  $$ SELECT 'XXX-XX-' || RIGHT(s, 4) $$;

CREATE OR REPLACE FUNCTION mask_email(e TEXT) RETURNS TEXT
  LANGUAGE sql IMMUTABLE STRICT AS
  $$ SELECT REGEXP_REPLACE(e, '^(.).*(@.*)$', '\1***\2') $$;

-- Roles used by the column-mask demo.
CREATE ROLE app_user LOGIN PASSWORD 'app';
CREATE ROLE pii_reader LOGIN PASSWORD 'pii';
GRANT SELECT ON users, orders, events TO app_user, pii_reader;
