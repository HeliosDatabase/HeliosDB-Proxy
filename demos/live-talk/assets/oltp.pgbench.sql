\set aid random(1, :accounts)
\set delta random(-5, 5)
BEGIN;
UPDATE demo_accounts
   SET balance = balance + :delta,
       version = version + 1
 WHERE id = :aid;
INSERT INTO demo_ledger(account_id, delta, note)
VALUES (:aid, :delta, 'wire-oltp');
SELECT balance FROM demo_accounts WHERE id = :aid;
COMMIT;
