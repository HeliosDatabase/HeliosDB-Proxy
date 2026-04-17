-- Bank Ledger Demo Schema
-- Total invariant: SUM(balance) must always equal $1,000,000.00

CREATE TABLE IF NOT EXISTS accounts (
    id      INT PRIMARY KEY,
    name    TEXT NOT NULL,
    balance NUMERIC(15,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS transfers (
    id        SERIAL PRIMARY KEY,
    from_acct INT NOT NULL REFERENCES accounts(id),
    to_acct   INT NOT NULL REFERENCES accounts(id),
    amount    NUMERIC(15,2) NOT NULL,
    ts        TIMESTAMP DEFAULT now()
);

-- Seed 100 accounts with $10,000 each = $1,000,000 total
INSERT INTO accounts
SELECT g, 'Account ' || g, 10000.00
FROM generate_series(1, 100) g
ON CONFLICT (id) DO NOTHING;
