#!/bin/bash
# Allow streaming replication from any Docker bridge network address.
echo "host replication all all trust" >> "$PGDATA/pg_hba.conf"
