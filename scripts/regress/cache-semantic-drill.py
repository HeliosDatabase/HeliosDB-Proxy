#!/usr/bin/env python3
"""HeliosProxy semantic cache tier (L3), table exclusions and stripped cache hints, live.

Usage: python3 -I scripts/regress/cache-semantic-drill.py --proxy-binary PATH
          [--dir DIR] [--output evidence.json] [--stub-port N]
          [--nano-backend host:port:user:password:database]

Backend: REGRESS_BACKEND=host:port:user:password:database (default
127.0.0.1:25433:bench:benchpass:benchdb, the shared PG 18.4). The drill creates and drops
the tables `kv` and `secret` there, and for the differential corpus the tables `l3dc_NN`
(each with a function `l3dc_total(l3dc_NN)` and a schema `l3dc_NN` holding a table of the
same name) and the schemas `l3dc_s1` and `l3dc_s2` (with a collation `l3dc_s1.c` and an
operator `l3dc_s1.<` that compares like `>`), and for the approximate-refusal corpus the
same for `l3da_NN`, each with a helper table `l3da_NN_emb`, and for the verbatim corpus
the tables `l3dv_NN` and `l3dv_NNx` (all dropped at the end).
Phase 3 needs a disposable HeliosDB-Nano: --nano-backend, default $REGRESS_NANO_BACKEND
(never the deployed standby); the drill creates and drops the tables `l3dn_NN`,
`l3dn_NNsecurity` and the quoted `"L3dn_NN"` there.
Without one, phase 3 is skipped: printed as SKIP and listed under `skipped` in the
evidence, never counted as a pass. Needs psycopg 3 (`python3 -m pip install
'psycopg[binary]'`) and the standard library; no Docker. It starts
scripts/regress/embed-stub.py (127.0.0.1:--stub-port, default $HELIOS_EMBED_STUB_PORT or
18434) and the proxy binary (built with all features) with
scripts/regress/proxy-pg-cache-semantic.toml (:6432, admin :9099; the node rewritten to the
backend) itself, and stops both at the end. Every read goes through a new session, as a
simple-protocol query (psycopg sends a query without parameters that way); a "direct"
write goes to the backend, which the proxy never sees, so a read that still returns the
old value was served from the cache.

Phase 0 - the config as shipped (semantic_match at its default "verbatim", routing hints
off so every hint reaches the backend as written):
  verbatim/<name>     the differential check below on the VERBATIM_CORPUS pairs: a hint in
                      the middle of a statement is part of its key (removing it would glue
                      `{t}` and `x` into the table `{t}x`), and a canonical paraphrase or
                      a case variant is never an L3 hit; the `l2` pair (a whitespace
                      variant) must be served by L2, whose PostgreSQL key collapses
                      whitespace, so it never reaches L3 and covers L2 only (the verbatim
                      L3 key's whitespace is the unit test
                      the_verbatim_tier_serves_only_the_same_text's)
  verbatim_stores     the tier stored entries and served no L3 hit to another text
Phase 1 - semantic_match = "canonical" (the drill sets it), semantic_similarity_threshold at
its default 1.0 (exact canonical matches only):
  exact_canonical     a hinted read is cached; after a direct write, a paraphrase (another
                      alias and qualifier spelling, AS) on a new session returns the OLD
                      value from L3 (hits.l3 + 1; the L2 keys of the two texts differ)
  leading_hint_strip  /*helios:cache=semantic*/ before the statement reaches the cache
                      through [routing_hints] strip_hints: another paraphrase hits L3
  c02_invalidation    a write through the proxy retires the entry: the paraphrase returns
                      the new value and stale_rejected grows
  literal_guard       the same paraphrase for another id returns that row
  exclusion           `secret` is in exclude_tables: after a direct write the read returns
                      the NEW value; heliosdb_proxy_cache_excluded_total grows by 2
  skip_under_strip    a cached read, a direct write, then /*helios:cache=skip*/ before the
                      same read returns the NEW value (the plain read still returns the
                      cached one). Base-failing: the strip used to hide the skip.
  differential/<name> a corpus of statement pairs (A, B), each on its own fresh table
                      (`l3dc_NN`, plus the schemas `l3dc_s1`, `l3dc_s2` and `l3dc_NN`
                      holding tables of the same name with other rows): A with the hint
                      through the proxy, then
                      B with the hint through the proxy (served by the tiers whose hits
                      grew: L1, L2 or L3, else the backend), then B directly on the
                      backend. Nothing writes in between, so FAIL if B's rows or column
                      names (or error) through the proxy differ from the direct result,
                      whichever tier served it (a wrong row from L2 fails as well as one
                      from L3; L1 is per session and every statement runs on a new one,
                      and the proxy config has no [edge], so neither can serve B here: the
                      edge fingerprint has unit tests only); a pair marked `paraphrase`
                      must also BE an L3 hit; a pair marked `unmatched` must NOT be one
                      (a case or comment variant: same rows on PostgreSQL, but names and
                      comments are kept); a pair marked `different` must
                      also discriminate on this backend (A returns rows and B's direct
                      outcome differs from A's), or it covers nothing: when B then came
                      from the backend (no tier's hits grew) with the backend's own
                      result, the pair is PREMISE-GONE (printed, listed under
                      `premise_gone` in the evidence, never counted as a pass, not a
                      failure: the backend answers A and B alike now); if any tier
                      served B, or B through the proxy differs from the direct result,
                      it FAILs. The pairs are every wrong-row pair the review rounds found
                      against the earlier canonicalizers that PostgreSQL tells apart at
                      threshold 1.0 (a schema equal to the alias or the table's own name,
                      `1.e3` vs `1. e3`, a table-name qualifier under an alias, ...; the
                      C-07 literal kind pairs for L2), and their kin, plus legitimate
                      paraphrases the grammar must accept. The pairs only approximate
                      matching could merge run in phase 2. A shape whose wrong answer is
                      only a stale one (a relation the form hides, such as the comma join
                      `... AND i.order = 1, prices p`, whose writes would not retire the
                      entry) needs a write between A and B, which this corpus never does:
                      the unit tests cover those.
  endpoint_untouched  after the corpus (many hinted reads with unqualified columns) and two
                      more of its own (a store whose column is a slot, then a lookup that
                      has it as a candidate), at threshold 1.0 the embedding endpoint was
                      never called
Phase 2 - the drill adds semantic_similarity_threshold = 0.92 (semantic_match =
"canonical") and restarts the proxy:
  approximate         stub synonyms val~alt: after `SELECT val ... id = 3` is cached and
                      both names are embedded, `SELECT alt FROM kv AS k WHERE id = 3`
                      returns the cached val value (approximate_hits >= 1); only
                      unqualified columns are substituted (`k.alt` may be `alt(k)`)
  approximate_off     synonyms removed and the proxy restarted: the same sequence returns
                      the real alt value and no approximate hit
  hung_endpoint       stub hangs (10 s): on one session, a hinted store whose column `id`
                      has no embedding (its fetch hangs), then a hinted lookup whose only
                      candidate is that entry; each statement completes within
                      HUNG_READ_BOUND_SECS (0.25 s, half the 500 ms call timeout), so no
                      embedding call is awaited by a lookup or by a store (which would
                      delay the session's next statement); after the timeout the endpoint
                      is down; exact canonical hits continue; no new embedding request
                      inside the 3 s retry window
  recovery            stub answers again; after the window one hinted store probes it and
                      the endpoint is up
  approximate_refusal/<name>
                      the proxy restarted with the stub making every name below a synonym
                      of every other; per pair, on a fresh `l3da_NN`: both names embedded
                      through a helper table whose columns carry them, A with the hint,
                      then B with the hint, then B on the backend. A `refused` pair (an
                      unaliased whole-row reference, a qualified column that is attribute
                      notation, a DISTINCT or GROUP BY [ALL] statement: shapes that are
                      never a substitutable slot) FAILs if approximate_hits grows, if B's
                      outcome through the proxy differs from the backend's, or if A and B
                      read alike (a corpus defect); the `control` pair (val vs alt) must be
                      an approximate hit, so the setup is shown to fill a real slot.
Phase 3 - the proxy restarted in front of HeliosDB-Nano (semantic_match = "canonical",
threshold 1.0). Nano's startup server_version names HeliosDB, so the canonical tier must
turn itself off: it neither stores nor serves.
  nano/canonical_off  after the corpus: no semantic entry, no L3 hit, every hinted read
                      counted ineligible, and the proxy log has the one warning naming
                      cache.semantic_match and the node
  nano/<name>         the differential check above on Nano, for the pairs only Nano tells
                      apart: Nano sorts a lone ORDER BY name by the input column whenever
                      the table has one (PostgreSQL by the output column of that name),
                      labels an output column with its alias or column name as written
                      (PostgreSQL folds both to lower case), ends a block comment at its
                      first `*/` and a `--` comment at a newline only, rewrites `AS OF`
                      depending on its spacing, removes the first `SECURITY DEFINER` /
                      `SECURITY INVOKER` with exactly one space (`FROM l3dn_NNsecurity
                      definer` reads `l3dn_NN`), refuses a relation alias its parser
                      reserves (`sort`) and `trim.val`, re-reads the raw statement for
                      `AS OF` (a comment `was offline` fails the statement), and answers
                      `pg_settings` / `pg_tables` by matching the raw text (`name='x'`
                      keeps every row, `name = 'x'` one; `tablename='t'` has eight
                      columns, `tablename = 't'` five). Each `different` pair reads
                      alike on PostgreSQL, so it must discriminate here; a `distinct`
                      pair (A and B one text in two spellings) must also be served by no
                      tier. A `different` or `distinct` pair a newer Nano answers like
                      PostgreSQL (`order_by_alias_named_like_input` on Nano P4, which
                      sorts by the output alias) is PREMISE-GONE as above when B came
                      from the backend with its own result, and FAILs when any tier
                      served B or B through the proxy differs from the direct result;
                      the `unmatched` pairs (paraphrases on PostgreSQL) must not be
                      L3 hits. `alias_fast_path` is the canonical form's own class: an
                      alias decides between Nano's point-lookup fast path (the table as
                      written, the quoted "L3dn_NN") and its planner (folded, l3dn_NN).
Writes results (and /metrics snapshots, the `premise_gone` pairs and the `skipped`
phases) to --output (default DIR/cache-semantic-drill.json). Exit 1 if any case fails (a
PREMISE-GONE pair is neither a pass nor a failure).
"""
import argparse
import json
import os
import pathlib
import re
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

try:
    import psycopg
except ImportError:
    # Under `python3 -I` the user site-packages directory is not on the path; psycopg is
    # often a `pip install --user` package. Add it last, so nothing shadows the stdlib.
    import site

    sys.path.append(site.getusersitepackages())
    try:
        import psycopg
    except ImportError:
        sys.exit("cache-semantic-drill.py needs psycopg 3: python3 -m pip install 'psycopg[binary]'")

HERE = pathlib.Path(__file__).resolve().parent
PROXY_PORT = 6432
ADMIN = 9099
HINT = "/*helios:cache=semantic*/"
DEFAULT_BACKEND = "127.0.0.1:25433:bench:benchpass:benchdb"
# The proxy binds its listener and admin port with SO_REUSEPORT (as every
# scripts/regress/*.toml uses 6432 and 9099), so a stray proxy left on them does not make
# ours fail to start: the kernel splits connections and /metrics polls between the two.
# The drill refuses to start while anything answers on its ports, waits up to this many
# seconds for them to close between its own restarts, and takes a process as ready only
# once its own log (or, for the stub, its own pid on /count) says it is the one answering.
PORT_RELEASE_SECS = 10
# hung_endpoint: the slowest a hinted statement may take while the stub hangs. Well below
# the proxy config's semantic_embedding_timeout_ms (500), so an embedding call awaited on
# the read path (which lasts until that timeout) cannot pass.
HUNG_READ_BOUND_SECS = 0.25

# Differential corpus: (name, kind, A, B). `{t}` is the pair's own table, `{s1}` and `{s2}`
# two schemas holding a table of the same name with other rows (and `{s1}.amount`, a
# domain over int, `{s1}.c`, a collation from "C", `{s1}.<`, an int operator that compares
# like `>`, `{s1}.{t}_ids` / `{s2}.{t}_ids`, one-column id lists); `l3dc_total({t})` is a
# function of the row (n * 2), so `x.l3dc_total` is attribute notation for it. A
# `paraphrase` pair must be an L3 hit with the backend's own result; a `different` pair
# reads other rows, other column names, or fails on the backend, and must never be served
# from A's entry (it may be an L3 hit only with exactly the backend's result). A
# `different` pair must also discriminate (A returns rows, B's direct outcome differs);
# one that no longer does is reported as premise gone, not passed, when B came from the
# backend with the backend's result (run_corpus).
# Not in the corpus because PostgreSQL 16+ answers both sides alike, so they could never
# fail: `TREAT(n AS s.amount)` vs `TREAT(n AS amount)` (both 42883), `x.n precision` vs
# `n precision` (PRECISION needs AS: both 42601), `GROUP BY ALL f` vs `GROUP BY f`, and
# `1_000` vs `1000` in WHERE (both 1000). The unit tests refuse those shapes.
CORPUS = [
    # Legitimate paraphrases the grammar accepts. A qualified column is never served for a
    # bare one (it may be a function of the row), so each pair keeps the qualification; an
    # output label (alias, or else column name) keeps its spelling (HeliosDB-Nano does not
    # fold it), and an ORDER BY key is compared as written.
    ("alias_qualifier_case", "paraphrase",
     "SELECT {t}.val FROM {t} WHERE {t}.id = 1",
     "select x.val from {t} AS x where x.id = 1"),
    ("bare_case_alias", "paraphrase",
     "SELECT val FROM {t} WHERE id = 1",
     "select val from {t} AS x where id = 1"),
    ("qualifiers_everywhere", "paraphrase",
     "SELECT x.val, x.alt FROM {t} x WHERE x.n > 5 ORDER BY x.id DESC LIMIT 2",
     "SELECT {t}.val, {t}.alt FROM {t} WHERE {t}.n > 5 ORDER BY {t}.id DESC LIMIT 2"),
    ("aggregate_is_true", "paraphrase",
     "SELECT count(*) FROM {t} WHERE f IS TRUE",
     "select COUNT(*) from {t} t where f is true"),
    ("not_equal_spelling", "paraphrase",
     "SELECT val FROM {t} WHERE n != 5 AND s LIKE 'x%'",
     "SELECT val FROM {t} t WHERE n <> 5 AND s LIKE 'x%'"),
    ("order_by_input_under_alias", "paraphrase",
     "SELECT n AS amount FROM {t} ORDER BY n",
     "SELECT n AS amount FROM {t} t ORDER BY n"),
    # Names and comments are kept: the same rows on PostgreSQL, never one L3 entry.
    ("column_case_unmatched", "unmatched",
     "SELECT val FROM {t} WHERE id = 1",
     "SELECT val FROM {t} WHERE ID = 1"),
    ("order_by_case_unmatched", "unmatched",
     "SELECT n AS amount FROM {t} ORDER BY n",
     "SELECT n AS amount FROM {t} t ORDER BY N"),
    ("comment_unmatched", "unmatched",
     "SELECT val FROM {t} WHERE id = 1",
     "SELECT val FROM {t} AS x WHERE id = 1 /* c */"),
    ("order_by_swapped_aliases", "paraphrase",
     "SELECT x.val AS alt, x.alt AS val FROM {t} x ORDER BY x.alt LIMIT 2",
     "SELECT {t}.val AS alt, {t}.alt AS val FROM {t} ORDER BY {t}.alt LIMIT 2"),
    ("order_by_qualifier_spelling", "paraphrase",
     "SELECT x.val FROM {t} x ORDER BY x.val LIMIT 2",
     "SELECT {t}.val FROM {t} ORDER BY {t}.val LIMIT 2"),
    ("schema_with_alias", "paraphrase",
     "SELECT {t}.val FROM {s1}.{t} WHERE {t}.id BETWEEN 1 AND 3",
     "SELECT x.val FROM {s1}.{t} x WHERE x.id BETWEEN 1 AND 3"),
    ("aggregates", "paraphrase",
     "SELECT sum(n), max(id) FROM {t} WHERE id IN (1, 2, 3)",
     "SELECT SUM(n), MAX(id) FROM {t} AS t WHERE id IN (1,2,3)"),
    ("not_parenthesized", "paraphrase",
     "SELECT id FROM {t} WHERE NOT (n > 5 OR f IS NULL) ORDER BY id",
     "SELECT id FROM {t} t WHERE NOT ( n > 5 OR f IS NULL ) ORDER BY id"),
    # Pairs that read other rows: every wrong-row pair the review rounds found, and kin.
    ("limit_offset_swap", "different",
     "SELECT val FROM {t} ORDER BY id LIMIT 2 OFFSET 1",
     "SELECT val FROM {t} ORDER BY id LIMIT 1 OFFSET 2"),
    ("negative_literal", "different",
     "SELECT id FROM {t} WHERE n > -100 ORDER BY id",
     "SELECT id FROM {t} WHERE n > 100 ORDER BY id"),
    ("true_false", "different",
     "SELECT id FROM {t} WHERE f = TRUE ORDER BY id",
     "SELECT id FROM {t} WHERE f = FALSE ORDER BY id"),
    ("is_null_not_null", "different",
     "SELECT id FROM {t} WHERE alt IS NULL ORDER BY id",
     "SELECT id FROM {t} WHERE alt IS NOT NULL ORDER BY id"),
    ("lt_gt", "different",
     "SELECT id FROM {t} WHERE n < 5 ORDER BY id",
     "SELECT id FROM {t} WHERE n > 5 ORDER BY id"),
    ("eq_ne", "different",
     "SELECT id FROM {t} WHERE n = 5 ORDER BY id",
     "SELECT id FROM {t} WHERE n <> 5 ORDER BY id"),
    ("in_not_in", "different",
     "SELECT id FROM {t} WHERE id IN (1, 2) ORDER BY id",
     "SELECT id FROM {t} WHERE id NOT IN (1, 2) ORDER BY id"),
    ("asc_desc", "different",
     "SELECT id FROM {t} ORDER BY n ASC LIMIT 2",
     "SELECT id FROM {t} ORDER BY n DESC LIMIT 2"),
    ("min_max", "different",
     "SELECT min(n) FROM {t}",
     "SELECT max(n) FROM {t}"),
    ("swapped_predicates", "different",
     "SELECT id FROM {t} WHERE id = 1 AND n = 10",
     "SELECT id FROM {t} WHERE n = 1 AND id = 10"),
    # The same columns in the same order, the literal values swapped between them: a
    # canonicalizer that sorts or multiset-hashes the WHERE literals (or reorders AND
    # terms) merges each pair. The second is critic-1's tenant pair (`tenant_id = '1' AND
    # customer_id = 2` vs `tenant_id = '2' AND customer_id = 1`, a string and a number
    # swapped): merged, one tenant reads the other's row.
    ("where_value_swap", "different",
     "SELECT val FROM {t} WHERE id = 1 AND n = 10",
     "SELECT val FROM {t} WHERE id = 10 AND n = 1"),
    ("tenant_literal_swap", "different",
     "SELECT val FROM {t} WHERE tenant_id = '1' AND id = 2",
     "SELECT val FROM {t} WHERE tenant_id = '2' AND id = 1"),
    ("schema_swap", "different",
     "SELECT val FROM {s1}.{t} WHERE id = 1",
     "SELECT val FROM {s2}.{t} WHERE id = 1"),
    # Round-1 #1: a relation's schema dropped when it equals the alias or the bare name
    # (`kv.kv`, `s.kv s`) served the search_path table's rows, and its kin.
    ("schema_vs_search_path", "different",
     "SELECT val FROM {s1}.{t} WHERE id = 1",
     "SELECT val FROM {t} WHERE id = 1"),
    ("schema_equals_table_name", "different",
     "SELECT val FROM {t}.{t} WHERE id = 1",
     "SELECT val FROM {t} WHERE id = 1"),
    ("schema_equals_alias", "different",
     "SELECT {s1}.val FROM {s1}.{t} {s1} WHERE {s1}.id = 1",
     "SELECT val FROM {t} WHERE id = 1"),
    ("schema_equals_alias_qualified", "different",
     "SELECT {s1}.val FROM {s1}.{t} {s1} WHERE {s1}.id = 1",
     "SELECT {s1}.val FROM {t} {s1} WHERE {s1}.id = 1"),
    ("literal_case", "different",
     "SELECT id FROM {t} WHERE s = 'x1'",
     "SELECT id FROM {t} WHERE s = 'X1'"),
    ("comment_marker_in_literal", "different",
     "SELECT id FROM {t} WHERE s = '--' AND n = 1000",
     "SELECT id FROM {t} WHERE s = '--' AND n = 5"),
    # Round 2: one tokenizer read `1.e3` (1000) and `1. e3` (`1.` labelled `e3`) alike.
    ("trailing_dot_number", "different",
     "SELECT 1.e3 FROM {t} WHERE id = 1",
     "SELECT 1. e3 FROM {t} WHERE id = 1"),
    ("trailing_dot_number_where", "different",
     "SELECT id FROM {t} WHERE n = 1.e3",
     "SELECT id FROM {t} WHERE n = 1./**/e3"),
    ("underscore_number", "different",
     "SELECT id, 1_000 FROM {t} WHERE id = 1",
     "SELECT id, 1 _000 FROM {t} WHERE id = 1"),
    ("underscore_trailing_dot", "different",
     "SELECT id FROM {t} WHERE n = 1_0.e2",
     "SELECT id FROM {t} WHERE n = 1_0. e2"),
    # Round-3 #3: under an alias the table's own name is no qualifier (42P01), quoted
    # alias or not; one canonical form for both served A's rows.
    ("table_qualifier_under_alias", "different",
     "SELECT {t}.val FROM {t} WHERE id = 1 ORDER BY {t}.id",
     "SELECT {t}.val FROM {t} x WHERE x.id = 1 ORDER BY {t}.id"),
    ("table_qualifier_under_quoted_alias", "different",
     "SELECT {t}.val FROM {t} WHERE id = 1 ORDER BY {t}.id",
     'SELECT {t}.val FROM {t} "x" WHERE "x".id = 1 ORDER BY {t}.id'),
    # C-07 (the L2 key): a literal's kind, and kinds swapped between positions.
    ("string_vs_number_literal", "different",
     "SELECT '1' AS v FROM {t} WHERE id = 1",
     "SELECT 1 AS v FROM {t} WHERE id = 1"),
    ("literal_kind_swap", "different",
     "SELECT val FROM {t} WHERE id = '1' AND n = 10",
     "SELECT val FROM {t} WHERE id = 10 AND n = '1'"),
    ("whole_row_alias", "different",
     "SELECT q FROM {t} q WHERE id = 1",
     "SELECT q FROM {t} WHERE id = 1"),
    ("whole_row_table", "different",
     "SELECT {t} FROM {t} WHERE id = 1",
     "SELECT x.{t} FROM {t} x WHERE id = 1"),
    ("qualified_reserved_word", "different",
     "SELECT id FROM {t} x WHERE x.user = 'bench' ORDER BY id",
     "SELECT id FROM {t} WHERE user = 'bench' ORDER BY id"),
    # Round 3: a qualifier dropped before a word that ends a FROM list (or is reserved)
    # turned `x.limit` into the keyword `limit`: B is that folded spelling, with the same
    # literal, and a syntax error (42601) on the backend.
    ("qualified_from_end_word", "different",
     "SELECT x.val FROM {t} x WHERE x.limit = 1 ORDER BY x.id",
     "SELECT val FROM {t} WHERE limit = 1 ORDER BY id"),
    ("qualified_from_end_word_select", "different",
     "SELECT x.limit FROM {t} x WHERE x.id = 1",
     "SELECT limit FROM {t} WHERE id = 1"),
    ("order_by_input_vs_output", "different",
     "SELECT id::text AS id FROM {t} t ORDER BY t.id LIMIT 3",
     "SELECT id::text AS id FROM {t} ORDER BY id LIMIT 3"),
    ("order_by_swapped_aliases_qualified", "different",
     "SELECT val AS alt, alt AS val FROM {t} ORDER BY val LIMIT 2",
     "SELECT val AS alt, alt AS val FROM {t} ORDER BY {t}.val LIMIT 2"),
    ("distinct_on_output_name", "different",
     "SELECT DISTINCT ON (t.n) t.id::text AS n FROM {t} t ORDER BY t.n",
     "SELECT DISTINCT ON (n) id::text AS n FROM {t} ORDER BY n"),
    ("select_as_isnull", "different",
     "SELECT val AS isnull FROM {t} WHERE id = 1",
     "SELECT val isnull FROM {t} WHERE id = 1"),
    ("group_by_alias", "different",
     "SELECT n AS val, count(*) AS c FROM {t} GROUP BY n ORDER BY c, val",
     "SELECT n AS val, count(*) AS c FROM {t} GROUP BY val ORDER BY c, val"),
    ("group_by_distinct_vs_all", "different",
     "SELECT f, s, n, count(*) FROM {t} GROUP BY DISTINCT ROLLUP(f, s), ROLLUP(f, n) "
     "ORDER BY 1, 2, 3, 4",
     "SELECT f, s, n, count(*) FROM {t} GROUP BY ALL ROLLUP(f, s), ROLLUP(f, n) "
     "ORDER BY 1, 2, 3, 4"),
    ("cast_to_schema_type", "different",
     "SELECT CAST(n AS {s1}.amount) AS n FROM {t} {s1} WHERE id = 1",
     "SELECT CAST(n AS amount) AS n FROM {t} WHERE id = 1"),
    # The round-3 shapes: a schema named like the relation's alias, inside COLLATE and
    # OPERATOR(); folding the alias qualifier there turns `{s1}.c` into `c`.
    ("collate_alias_schema", "different",
     "SELECT id FROM {t} {s1} WHERE {s1}.s < 'b' COLLATE {s1}.c ORDER BY id",
     "SELECT id FROM {t} WHERE s < 'b' COLLATE c ORDER BY id"),
    ("operator_alias_schema", "different",
     "SELECT id FROM {t} {s1} WHERE {s1}.n OPERATOR({s1}.<) 6 ORDER BY id",
     "SELECT id FROM {t} WHERE n OPERATOR(<) 6 ORDER BY id"),
    ("operator_catalog", "different",
     "SELECT id FROM {t} WHERE n OPERATOR(pg_catalog.<) 6 ORDER BY id",
     "SELECT id FROM {t} WHERE n OPERATOR(pg_catalog.>) 6 ORDER BY id"),
    ("table_subquery", "different",
     "SELECT val FROM {t} WHERE id IN (TABLE {s1}.{t}_ids) ORDER BY id",
     "SELECT val FROM {t} WHERE id IN (TABLE {s2}.{t}_ids) ORDER BY id"),
    ("three_part_name", "different",
     "SELECT {s1}.{t}.val FROM {s1}.{t} WHERE id = 1",
     "SELECT val FROM {s2}.{t} WHERE id = 1"),
    # Attribute notation: `x.l3dc_total` is the call l3dc_total(x) (no such column), while
    # a bare `l3dc_total` is an error (42703).
    ("attribute_notation_select", "different",
     "SELECT x.l3dc_total FROM {t} x WHERE x.id = 1",
     "SELECT l3dc_total FROM {t} WHERE id = 1"),
    ("attribute_notation_where", "different",
     "SELECT x.id FROM {t} x WHERE x.l3dc_total > 15 ORDER BY x.id",
     "SELECT id FROM {t} WHERE l3dc_total > 15 ORDER BY id"),
]
CORPUS_SCHEMAS = ("l3dc_s1", "l3dc_s2")

# Approximate-refusal corpus (phase 2, threshold 0.92): (name, kind, A, B, names), each on a
# fresh `l3da_NN` (the differential fixture). The stub makes every name a synonym of every
# other (cosine 1.0), and before A and B run each of the pair's `names` is embedded through
# a legitimate slot (`SELECT <name> FROM {t}_emb WHERE id = k`, a helper table with a column
# per name), so B WOULD be filled from A's entry if the tier took B's column for a
# substitutable slot. A `refused` pair must not be an approximate hit, must return the
# backend's own result, and must discriminate (A's outcome differs from B's direct one); the
# `control` pair must be an approximate hit, or the setup proves nothing.
APPROX_CORPUS = [
    ("control", "control",
     "SELECT val FROM {t} WHERE id = 1",
     "SELECT alt FROM {t} WHERE id = 1", ("val", "alt")),
    # Round-3 #10: an unaliased whole-row reference reads the row, not a column `{t}`.
    ("whole_row_table", "refused",
     "SELECT val FROM {t} WHERE id = 1",
     "SELECT {t} FROM {t} WHERE id = 1", ("val", "{t}")),
    # Round 4: a qualified `x.l3dc_total` is the call l3dc_total(x) (n * 2), never a slot.
    ("attribute_notation", "refused",
     "SELECT val FROM {t} x WHERE id = 1",
     "SELECT x.l3dc_total FROM {t} x WHERE id = 1", ("val", "l3dc_total")),
    ("attribute_notation_qualified", "refused",
     "SELECT x.val FROM {t} x WHERE id = 1",
     "SELECT x.l3dc_total FROM {t} x WHERE id = 1", ("val", "l3dc_total")),
    # A DISTINCT or GROUP BY statement is never approximable: another column's distinct set
    # or groups, with another row count (round-3 #6 was GROUP BY ALL).
    ("distinct", "refused",
     "SELECT DISTINCT val FROM {t} WHERE id = 1",
     "SELECT DISTINCT alt FROM {t} WHERE id = 1", ("val", "alt")),
    ("group_by", "refused",
     "SELECT val, count(*) FROM {t} GROUP BY val, alt ORDER BY val, alt",
     "SELECT alt, count(*) FROM {t} GROUP BY val, alt ORDER BY val, alt", ("val", "alt")),
    ("group_by_all", "refused",
     "SELECT val, count(*) FROM {t} GROUP BY ALL val, alt ORDER BY val, alt",
     "SELECT alt, count(*) FROM {t} GROUP BY ALL val, alt ORDER BY val, alt", ("val", "alt")),
]

# Verbatim corpus (phase 0, the shipped default semantic_match = "verbatim", routing hints
# off): `{t}` is a fresh `l3dv_NN` and `{t}x` a second table with other rows.
VERBATIM_CORPUS = [
    # Removing the hint from the key would glue `{t}` and `x`: A reads {t} (alias x), B the
    # table {t}x, and both keys would be `SELECT val FROM {t}x WHERE id = 1`.
    ("hint_glue", "different",
     "SELECT val FROM {t}/*helios:cache=semantic*/x WHERE id = 1",
     "SELECT val FROM {t}x WHERE id = 1"),
    # Canonical paraphrases are other texts: never one verbatim entry.
    ("alias_paraphrase", "unmatched",
     "SELECT val FROM {t} WHERE id = 1",
     "SELECT val FROM {t} AS x WHERE id = 1"),
    ("keyword_case", "unmatched",
     "SELECT val FROM {t} WHERE id = 1",
     "select val from {t} where id = 1"),
    # On PostgreSQL the L2 key collapses whitespace, so B is an L2 hit on A's entry and L3
    # is never consulted: this pair covers L2 only (B must be served by L2 alone).
    ("whitespace", "l2",
     "SELECT val FROM {t} WHERE id = 1",
     "SELECT val FROM {t}  WHERE id = 1"),
]

# Nano corpus (phase 3), same rules, plus `distinct`: a `different` pair that no tier (L1,
# L2, L3) may serve at all; `{t}` is a fresh `l3dn_NN` with the columns id, val, alt, price
# and price_eur, where price and price_eur order the rows differently.
NANO_CORPUS = [
    # Nano: `ORDER BY price` sorts by the input column price, not by the output `price`
    # (price_eur). One entry with `ORDER BY price_eur` would serve price_eur's order.
    ("order_by_alias_named_like_input", "different",
     "SELECT price_eur AS price FROM {t} ORDER BY price LIMIT 1",
     "SELECT price_eur AS price FROM {t} ORDER BY price_eur LIMIT 1"),
    # Nano: `ORDER BY val` sorts by the input val; `{t}.alt` by the input alt.
    ("order_by_swapped_aliases", "different",
     "SELECT {t}.val AS alt, {t}.alt AS val FROM {t} ORDER BY val LIMIT 1",
     "SELECT {t}.val AS alt, {t}.alt AS val FROM {t} ORDER BY {t}.alt LIMIT 1"),
    # Nano labels these columns `VAL` and `Total`, as written.
    ("column_label_case", "different",
     "SELECT VAL FROM {t} WHERE id = 1",
     "select val from {t} AS x where id = 1"),
    ("alias_label_case", "different",
     "SELECT val AS Total FROM {t} WHERE id = 1",
     "SELECT val AS total FROM {t} AS x WHERE id = 1"),
    # The same pairs as one text in two spellings: no tier may serve B. The L2 and edge
    # keys used to upper-case every word, so B was an L2 hit with A's label.
    ("column_label_case_only", "distinct",
     "SELECT VAL FROM {t} WHERE id = 1",
     "SELECT val FROM {t} WHERE id = 1"),
    ("alias_label_case_only", "distinct",
     "SELECT val AS Total FROM {t} WHERE id = 1",
     "SELECT val AS total FROM {t} WHERE id = 1"),
    # Paraphrases on PostgreSQL: the canonical tier is off on Nano, so never L3 hits.
    ("qualifier_spelling", "unmatched",
     "SELECT x.val FROM {t} x WHERE x.id = 1",
     "SELECT {t}.val FROM {t} WHERE {t}.id = 1"),
    ("order_by_as_written", "unmatched",
     "SELECT price_eur AS price FROM {t} ORDER BY price_eur LIMIT 1",
     "select price_eur as price from {t} AS x order by price_eur limit 1"),
    # The canonical form drops the alias, which on Nano decides between the point-lookup
    # fast path (no alias: the table as written, the quoted "L3dn_NN") and the planner
    # (an alias: the name folded, l3dn_NN).
    ("alias_fast_path", "different",
     "SELECT * FROM {T} WHERE id = 1",
     "SELECT * FROM {T} AS p WHERE id = 1"),
    # Nano ends a block comment at its first `*/` (no nesting) and a `--` comment at `\n`
    # only: it runs `... WHERE id = 2`, and every row. The L2 key (and the L3 form) used to
    # read both as PostgreSQL does, the plain `... WHERE id = 1`.
    ("nested_block_comment", "distinct",
     "SELECT val FROM {t} /* /* */ WHERE id = 2 -- */ WHERE id = 1",
     "SELECT val FROM {t} WHERE id = 1"),
    ("line_comment_carriage_return", "distinct",
     "SELECT val FROM {t} --\rWHERE id = 1",
     "SELECT val FROM {t} WHERE id = 1"),
    # Nano's `AS OF` rewrite fires on `AS of` (one space), which then loses its `FROM`, and
    # not on `AS  of`: one L2 key and one L3 form used to serve both.
    ("as_of_spacing", "distinct",
     "SELECT val AS  of FROM {t} WHERE id = 1",
     "SELECT val AS of FROM {t} WHERE id = 1"),
    # Nano's parser refuses `sort` as a relation alias without AS, and `trim.val` (TRIM must
    # be TRIM(...)); the L3 form drops the alias and the qualifier's spelling.
    ("reserved_alias_without_as", "different",
     "SELECT val FROM {t} WHERE id = 1",
     "SELECT val FROM {t} sort WHERE id = 1"),
    ("unreadable_qualifier", "different",
     "SELECT x.val FROM {t} AS x WHERE x.id = 1",
     "SELECT trim.val FROM {t} AS trim WHERE trim.id = 1"),
    # Nano removes the first `SECURITY DEFINER` / `SECURITY INVOKER` (one space, any case,
    # where the words run on too) from the text: `FROM {t}security definer` reads the
    # table {t}. The L3 form drops the alias, so one form used to serve both tables' rows,
    # in either order (and with {t}security's TTL and invalidation for {t}'s rows).
    ("security_clause_alias", "different",
     "SELECT val FROM {t}security WHERE id = 1",
     "SELECT val FROM {t}security definer WHERE id = 1"),
    ("security_clause_alias_first", "different",
     "SELECT val FROM {t}security invoker WHERE id = 1",
     "SELECT val FROM {t}security WHERE id = 1"),
    # The same rewrite and the L2 key: one space (or a block comment, one space there)
    # fires it, two spaces or a tab do not; collapsed whitespace used to share one key.
    ("security_clause_spacing", "distinct",
     "SELECT val FROM {t}security definer WHERE id = 1",
     "SELECT val FROM {t}security  definer WHERE id = 1"),
    ("security_clause_comment", "distinct",
     "SELECT val FROM {t}security/**/invoker WHERE id = 1",
     "SELECT val FROM {t}security\tinvoker WHERE id = 1"),
    # Nano's planner searches the raw statement, comments included, for `AS OF` and
    # parses the clause after it: `was offline` is `AS OF` `FLINE …`, an error. The L2
    # key and the L3 form used to drop the comment, so B was served A's rows.
    ("comment_as_of", "distinct",
     "SELECT val FROM {t} WHERE id = 1",
     "SELECT val FROM {t} WHERE id = 1 /* was offline */"),
    # Nano answers pg_settings / pg_tables from a router that matches the raw text:
    # `name='max_connections'` is no predicate it knows (every canned row), `name =
    # 'max_connections'` one row; `tablename='{t}'` goes to the planner's eight-column
    # pg_tables, `tablename = '{t}'` to the five-column canned one. One L3 form used to
    # serve both of each pair.
    ("catalog_predicate_spacing", "distinct",
     "SELECT name, setting FROM pg_settings WHERE name='max_connections'",
     "SELECT name, setting FROM pg_settings WHERE name = 'max_connections'"),
    ("catalog_table_predicate_spacing", "distinct",
     "SELECT * FROM pg_tables WHERE tablename = '{t}'",
     "SELECT * FROM pg_tables WHERE tablename='{t}'"),
]


def parse_backend(text):
    """psycopg connection arguments of `host:port:user:password:database`."""
    host, port, user, password, database = text.split(":", 4)
    return {"host": host, "port": int(port), "user": user, "password": password, "dbname": database}


def http_json(url, timeout=5):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def port_answers(port):
    """Whether anything accepts a TCP connection on 127.0.0.1:`port`."""
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return True
    except OSError:
        return False


def wait_for(predicate, timeout, step=0.1):
    """Poll `predicate` until it is truthy or `timeout` seconds pass; its last value."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (OSError, urllib.error.URLError, KeyError, TypeError):
            pass
        time.sleep(step)
    return last


class Drill:
    def __init__(self, args):
        self.backend = parse_backend(os.environ.get("REGRESS_BACKEND", DEFAULT_BACKEND))
        self.nano = parse_backend(args.nano_backend) if args.nano_backend else None
        self.proxy_conn = dict(self.backend, host="127.0.0.1", port=PROXY_PORT)
        self.dir = pathlib.Path(args.dir or tempfile.mkdtemp(prefix="cache-semantic-drill-"))
        self.dir.mkdir(parents=True, exist_ok=True)
        self.binary = args.proxy_binary
        self.stub_port = args.stub_port
        self.control = self.dir / "stub-control.json"
        self.output = pathlib.Path(args.output) if args.output else self.dir / "cache-semantic-drill.json"
        self.results = []
        self.skipped = []
        self.premise_gone = []
        self.snapshots = {}
        self.proxy = None
        self.stub = None
        self.starts = 0
        self.corpus_tables = []
        self.helper_tables = []
        self.nano_tables = []

    # ------------------------------------------------------------ processes
    def set_stub(self, **settings):
        current = {"mode": "ok", "synonyms": [], "hang_secs": 10}
        if self.control.exists():
            current.update(json.loads(self.control.read_text()))
        current.update(settings)
        self.control.write_text(json.dumps(current))

    def stub_status(self):
        return http_json(f"http://127.0.0.1:{self.stub_port}/count")

    def stub_count(self):
        return self.stub_status()["requests"]

    def preflight(self):
        """Refuse to run while anything answers on the proxy, admin or stub port: a
        stray proxy shares 6432/9099 with ours (SO_REUSEPORT), a stray stub reads another
        control file, and either would answer the drill's reads and metric polls."""
        busy = [p for p in (PROXY_PORT, ADMIN, self.stub_port) if port_answers(p)]
        if busy:
            raise RuntimeError(
                f"127.0.0.1 ports {busy} already answer: stop the process holding them "
                "(a stray proxy or embed-stub) before running the drill"
            )

    def start_stub(self):
        self.set_stub(mode="ok", synonyms=[])
        log = open(self.dir / "embed-stub.log", "w")
        self.stub = subprocess.Popen(
            [
                sys.executable,
                "-I",
                str(HERE / "embed-stub.py"),
                "--port",
                str(self.stub_port),
                "--control",
                str(self.control),
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )

        def ours():
            # Ready only when OUR stub answers (its pid), with no request counted yet.
            status = self.stub_status()
            return (
                self.stub.poll() is None
                and status.get("pid") == self.stub.pid
                and status.get("requests") == 0
            )

        if wait_for(ours, 10) is not True or self.stub.poll() is not None:
            raise RuntimeError(f"embed-stub did not start (see {self.dir / 'embed-stub.log'})")

    def write_config(self, threshold=None, backend=None, name="proxy.toml",
                     semantic_match="canonical", routing_hints=True):
        """The drill's config: proxy-pg-cache-semantic.toml with its node rewritten to
        `backend`, `semantic_match` set (None keeps the shipped default, "verbatim"), the
        threshold if given, and [routing_hints] off unless `routing_hints`."""
        backend = backend or self.backend
        text = (HERE / "proxy-pg-cache-semantic.toml").read_text()
        text = re.sub(r'(?m)^host = ".*"$', f'host = "{backend["host"]}"', text)
        text = re.sub(r"(?m)^port = \d+$", f"port = {backend['port']}", text)
        if threshold is not None:
            text = text.replace(
                "[cache]\n", f"[cache]\nsemantic_similarity_threshold = {threshold}\n", 1
            )
            name = f"proxy-threshold-{threshold}.toml"
        if semantic_match is not None:
            text = text.replace("[cache]\n", f'[cache]\nsemantic_match = "{semantic_match}"\n', 1)
        if not routing_hints:
            section = "[routing_hints]\nenabled = true\n"
            if section not in text:
                raise RuntimeError("proxy-pg-cache-semantic.toml: [routing_hints] not found")
            text = text.replace(section, "[routing_hints]\nenabled = false\n", 1)
        path = self.dir / name
        path.write_text(text)
        return path

    def start_proxy(self, config):
        self.stop_proxy()
        # The previous proxy has exited; nothing else may answer on its ports.
        ports = (PROXY_PORT, ADMIN)
        if wait_for(lambda: not any(port_answers(p) for p in ports), PORT_RELEASE_SECS) is not True:
            raise RuntimeError(f"127.0.0.1 ports {list(ports)} still answer after the proxy stopped")
        self.starts += 1
        log = open(self.dir / f"proxy-{self.starts}.log", "w")
        env = dict(
            os.environ,
            HELIOS_EMBED_STUB_PORT=str(self.stub_port),
            RUST_LOG="heliosdb_proxy=info,helios::cache=debug",
            NO_COLOR="1",
        )
        self.proxy = subprocess.Popen(
            [self.binary, "--config", str(config)], stdout=log, stderr=subprocess.STDOUT, env=env
        )
        # Ready when this process logged both listeners and answers a query while it is
        # still running: a SELECT 1 alone may be answered by another process on the port.
        listening = (
            f"Proxy listening on 127.0.0.1:{PROXY_PORT}",
            f"Admin API listening on 127.0.0.1:{ADMIN}",
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self.proxy.poll() is not None:
                raise RuntimeError(f"proxy exited on startup (see {log.name})")
            try:
                text = pathlib.Path(log.name).read_text(errors="replace")
                if all(line in text for line in listening):
                    self.query("SELECT 1")
                    self.metrics()
                    if self.proxy.poll() is None:
                        return
            except (psycopg.Error, OSError, urllib.error.URLError):
                pass
            time.sleep(0.3)
        raise RuntimeError(f"proxy never became ready (see {log.name})")

    def stop_proxy(self):
        if self.proxy is not None and self.proxy.poll() is None:
            # No SIGTERM handler: the proxy exits at once, which is all a drill needs.
            self.proxy.terminate()
            try:
                self.proxy.wait(10)
            except subprocess.TimeoutExpired:
                self.proxy.kill()
                self.proxy.wait()
        self.proxy = None

    def stop_all(self):
        self.stop_proxy()
        if self.stub is not None and self.stub.poll() is None:
            self.stub.terminate()
            try:
                self.stub.wait(10)
            except subprocess.TimeoutExpired:
                self.stub.kill()
        self.stub = None

    # ------------------------------------------------------------ SQL and metrics
    def query(self, sql):
        """Rows of `sql` through the proxy, on a new session (simple protocol)."""
        with psycopg.connect(**self.proxy_conn, autocommit=True, connect_timeout=10) as conn:
            return conn.execute(sql).fetchall()

    def value(self, sql):
        rows = self.query(sql)
        return rows[0][0] if rows else None

    def proxy_exec(self, sql):
        with psycopg.connect(**self.proxy_conn, autocommit=True, connect_timeout=10) as conn:
            conn.execute(sql)

    def direct(self, sql, backend=None):
        """Run `sql` on the backend itself (default the PostgreSQL one): the proxy never
        sees it."""
        with psycopg.connect(**(backend or self.backend), autocommit=True, connect_timeout=10) as conn:
            conn.execute(sql)

    def metrics(self):
        return http_json(f"http://127.0.0.1:{ADMIN}/metrics")

    def query_cache(self):
        return self.metrics()["query_cache"]

    def semantic(self):
        return self.query_cache()["semantic"] or {}

    def l3_hits(self):
        return self.query_cache()["hits"]["l3"]

    def tier_hits(self):
        """The query cache's hits per tier: {"l1": n, "l2": n, "l3": n}."""
        return dict(self.query_cache()["hits"])

    def prom_value(self, name):
        text = http_json(f"http://127.0.0.1:{ADMIN}/metrics/prometheus")["text"]
        for line in text.splitlines():
            if line.startswith(name + " "):
                return int(float(line.split()[1]))
        return None

    def snapshot(self, label):
        try:
            self.snapshots[label] = {
                "metrics": self.metrics(),
                "prometheus": http_json(f"http://127.0.0.1:{ADMIN}/metrics/prometheus")["text"],
                "stub_requests": self.stub_count(),
            }
        except (OSError, urllib.error.URLError) as e:
            self.snapshots[label] = {"error": str(e)}

    def case(self, name, ok, **detail):
        self.results.append({"case": name, "pass": bool(ok), "detail": detail})
        print(f"{'PASS' if ok else 'FAIL'} {name} {json.dumps(detail, default=str)}", flush=True)

    def premise(self, name, **detail):
        """A pair that can no longer show a wrong row on this backend (it answers A and B
        alike now, or A returns no rows) and whose B came from the backend with the
        backend's own result: listed under `premise_gone`, never counted as a pass."""
        self.premise_gone.append({"case": name, "detail": detail})
        print(f"PREMISE-GONE {name} {json.dumps(detail, default=str)}", flush=True)

    # ------------------------------------------------------------ fixture
    def reset_tables(self):
        self.direct("DROP TABLE IF EXISTS kv")
        self.direct("DROP TABLE IF EXISTS secret")
        self.direct("CREATE TABLE kv (id int PRIMARY KEY, val text, alt text)")
        self.direct(
            "INSERT INTO kv VALUES (1, 'v1', 'a1'), (2, 'v2', 'a2'), (3, 'v3', 'a3'), (4, 'v4', 'a4')"
        )
        self.direct("CREATE TABLE secret (v int)")
        self.direct("INSERT INTO secret VALUES (1)")

    def drop_tables(self):
        try:
            self.direct("DROP TABLE IF EXISTS kv")
            self.direct("DROP TABLE IF EXISTS secret")
            for table in self.corpus_tables:
                # CASCADE: the table's l3dc_total function takes its row type.
                self.direct(f"DROP TABLE IF EXISTS public.{table} CASCADE")
                self.direct(f"DROP SCHEMA IF EXISTS {table} CASCADE")
            for table in self.helper_tables:
                self.direct(f"DROP TABLE IF EXISTS public.{table}")
            for schema in CORPUS_SCHEMAS:
                self.direct(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        except psycopg.Error as e:
            print(f"cleanup: {e}", file=sys.stderr)
        try:
            for table in self.nano_tables:
                self.direct(f"DROP TABLE IF EXISTS {table}", self.nano)
        except psycopg.Error as e:
            print(f"cleanup (nano): {e}", file=sys.stderr)

    # ------------------------------------------------------------ phase 1
    def exact_canonical(self):
        h0 = self.l3_hits()
        first = self.value(f"SELECT x.val FROM kv x WHERE x.id = 1 {HINT}")
        self.direct("UPDATE kv SET val = 'v1-direct' WHERE id = 1")
        paraphrase = self.value(f"SELECT t.val FROM kv AS t WHERE t.id = 1 {HINT}")
        h1 = self.l3_hits()
        self.case(
            "exact_canonical",
            first == "v1" and paraphrase == "v1" and h1 == h0 + 1,
            first=first,
            paraphrase=paraphrase,
            l3_hits=[h0, h1],
        )

    def leading_hint_strip(self):
        h0 = self.l3_hits()
        got = self.value(f"{HINT} SELECT kv.val FROM kv WHERE kv.id = 1")
        h1 = self.l3_hits()
        self.case("leading_hint_strip", got == "v1" and h1 == h0 + 1, got=got, l3_hits=[h0, h1])

    def c02_invalidation(self):
        s0 = self.query_cache()["stale_rejected"]
        self.proxy_exec("UPDATE kv SET val = 'v1-proxy' WHERE id = 1")
        got = self.value(f"SELECT t.val FROM kv t WHERE t.id = 1 {HINT}")
        s1 = self.query_cache()["stale_rejected"]
        self.case(
            "c02_invalidation", got == "v1-proxy" and s1 >= s0 + 1, got=got, stale_rejected=[s0, s1]
        )

    def literal_guard(self):
        got = self.value(f"SELECT t.val FROM kv AS t WHERE t.id = 2 {HINT}")
        self.case("literal_guard", got == "v2", got=got)

    def exclusion(self):
        name = "heliosdb_proxy_cache_excluded_total"
        x0 = self.prom_value(name)
        before = self.value("SELECT v FROM secret")
        self.direct("UPDATE secret SET v = 2")
        after = self.value("SELECT v FROM secret")
        # Server counters reach the admin API once a second.
        wait_for(lambda: (self.prom_value(name) or 0) >= (x0 or 0) + 2, 5)
        x1 = self.prom_value(name)
        self.case(
            "exclusion",
            before == 1 and after == 2 and x0 is not None and x1 is not None and x1 >= x0 + 2,
            before=before,
            after=after,
            excluded_total=[x0, x1],
        )

    def skip_under_strip(self):
        first = self.value("SELECT val FROM kv WHERE id = 3")
        self.direct("UPDATE kv SET val = 'v3-direct' WHERE id = 3")
        cached = self.value("SELECT val FROM kv WHERE id = 3")
        skipped = self.value("/*helios:cache=skip*/ SELECT val FROM kv WHERE id = 3")
        self.case(
            "skip_under_strip",
            first == "v3" and cached == "v3" and skipped == "v3-direct",
            first=first,
            cached=cached,
            skipped=skipped,
        )

    def endpoint_untouched(self):
        # A store with a slot column (a name it would embed), then a lookup with that
        # entry as its candidate (a name it would fetch): at threshold 1.0, neither.
        self.value(f"SELECT val FROM kv WHERE id = 2 {HINT}")
        self.value(f"SELECT alt FROM kv WHERE id = 2 {HINT}")
        semantic = self.semantic()
        requests = self.stub_count()
        self.case(
            "endpoint_untouched",
            requests == 0
            and semantic.get("embed_requests") == 0
            and semantic.get("endpoint") == "unknown",
            stub_requests=requests,
            semantic=semantic,
        )

    # ------------------------------------------------------------ differential corpus
    def outcome(self, sql, conn_args):
        """("rows", column names, rows) of `sql` on a new session to `conn_args` (the proxy
        or a backend), or ("error", SQLSTATE)."""
        try:
            with psycopg.connect(**conn_args, autocommit=True, connect_timeout=10) as conn:
                cur = conn.execute(sql)
                names = [d.name for d in cur.description] if cur.description else []
                return ("rows", names, [list(r) for r in cur.fetchall()])
        except psycopg.Error as e:
            return ("error", getattr(e, "sqlstate", None) or type(e).__name__)

    def corpus_setup(self):
        s1, s2 = CORPUS_SCHEMAS
        for schema in CORPUS_SCHEMAS:
            self.direct(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            self.direct(f"CREATE SCHEMA {schema}")
        self.direct(f"CREATE DOMAIN {s1}.amount AS int")
        # Objects named in a schema that is also a relation alias in the corpus.
        self.direct(f'CREATE COLLATION {s1}.c FROM "C"')
        self.direct(f"CREATE OPERATOR {s1}.< (LEFTARG = int, RIGHTARG = int, FUNCTION = int4gt)")

    def corpus_fixture(self, table):
        """A fresh `table` in public, in both corpus schemas and in a schema named like the
        table itself (`{t}.{t}`), each with its own rows."""
        columns = ('(id int PRIMARY KEY, val text, alt text, n int, f bool, s text, '
                   '"user" text, "limit" int, tenant_id text)')
        self.direct(f"DROP SCHEMA IF EXISTS {table} CASCADE")
        self.direct(f"CREATE SCHEMA {table}")
        self.corpus_tables.append(table)
        schemas = (("public", ""), (CORPUS_SCHEMAS[0], "s1-"), (CORPUS_SCHEMAS[1], "s2-"),
                   (table, "self-"))
        for schema, tag in schemas:
            name = f"{schema}.{table}"
            self.direct(f"DROP TABLE IF EXISTS {name} CASCADE")
            self.direct(f"CREATE TABLE {name} {columns}")
            self.direct(
                f"INSERT INTO {name} VALUES "
                f"(1, '{tag}v1', 'a9', 10, true, 'x1', 'alice', 3, '2'), "
                f"(2, '{tag}v2', 'a8', -100, false, 'X2', 'bench', 1, '1'), "
                f"(3, '{tag}v3', NULL, 1000, NULL, '--', 'carol', 2, '1'), "
                f"(10, NULL, 'a1', 5, true, 'a%b', 'dave', 1, '2')"
            )
        for schema, ids in ((CORPUS_SCHEMAS[0], "(1), (2)"), (CORPUS_SCHEMAS[1], "(3)")):
            self.direct(f"CREATE TABLE {schema}.{table}_ids (id int)")
            self.direct(f"INSERT INTO {schema}.{table}_ids VALUES {ids}")
        # A "computed field": x.l3dc_total is l3dc_total(x) by attribute notation.
        self.direct(
            f"CREATE FUNCTION public.l3dc_total(public.{table}) RETURNS int "
            "LANGUAGE sql IMMUTABLE AS 'SELECT $1.n * 2'"
        )

    def nano_fixture(self, table):
        """A fresh `table` on Nano, where price and price_eur order the rows differently,
        `{table}security`, the same columns with other values (Nano's SECURITY DEFINER
        removal turns `{table}security definer` into `{table}`), and the quoted
        `"{Table}"` (capitalized), with other values again (Nano's point-lookup fast path
        reads an unquoted `{Table}` as written, its planner folds it to `{table}`)."""
        quoted = '"' + table.capitalize() + '"'
        for name, tag in ((table, ""), (f"{table}security", "s"), (quoted, "q")):
            self.direct(f"DROP TABLE IF EXISTS {name}", self.nano)
            self.direct(
                f"CREATE TABLE {name} "
                "(id int PRIMARY KEY, val text, alt text, price int, price_eur int)",
                self.nano,
            )
            self.direct(
                f"INSERT INTO {name} VALUES "
                f"(1, '{tag}v1', 'a3', 10, 30), (2, '{tag}v2', 'a2', 20, 5), "
                f"(3, '{tag}v3', 'a1', 30, 20)",
                self.nano,
            )
            self.nano_tables.append(name)

    def verbatim_fixture(self, table):
        """Fresh `table` and `{table}x` (other rows), for the verbatim corpus."""
        for name, tag in ((table, ""), (f"{table}x", "x-")):
            self.direct(f"DROP TABLE IF EXISTS public.{name}")
            self.direct(f"CREATE TABLE public.{name} (id int PRIMARY KEY, val text)")
            self.direct(f"INSERT INTO public.{name} VALUES (1, '{tag}v1'), (2, '{tag}v2')")
            self.helper_tables.append(name)

    def verbatim(self):
        """Phase 0: the shipped default (semantic_match = "verbatim"), routing hints off."""
        self.start_proxy(
            self.write_config(name="proxy-verbatim.toml", semantic_match=None, routing_hints=False)
        )
        h0 = self.l3_hits()
        self.run_corpus("verbatim", VERBATIM_CORPUS, "l3dv", self.backend, self.verbatim_fixture)
        semantic = self.semantic()
        h1 = self.l3_hits()
        self.case(
            "verbatim_stores",
            semantic.get("entries", 0) >= 1 and h1 == h0,
            semantic=semantic,
            l3_hits=[h0, h1],
        )

    def differential(self):
        self.corpus_setup()
        self.run_corpus("differential", CORPUS, "l3dc", self.backend, self.corpus_fixture)

    def nano_differential(self):
        """Phase 3: the proxy in front of Nano, the NANO_CORPUS pairs."""
        if self.nano is None:
            self.skipped.append("nano")
            print("SKIP nano: no --nano-backend / REGRESS_NANO_BACKEND", flush=True)
            return
        self.proxy_conn = dict(self.nano, host="127.0.0.1", port=PROXY_PORT)
        self.start_proxy(self.write_config(backend=self.nano, name="proxy-nano.toml"))
        log = self.dir / f"proxy-{self.starts}.log"
        i0 = self.semantic().get("ineligible", 0)
        h0 = self.l3_hits()
        self.run_corpus("nano", NANO_CORPUS, "l3dn", self.nano, self.nano_fixture)
        semantic = self.semantic()
        h1 = self.l3_hits()
        text = log.read_text(errors="replace")
        warnings = [
            line for line in text.splitlines()
            if "WARN" in line and "cache.semantic_match" in line and "HeliosDB" in line
        ]
        # Every corpus statement runs hinted twice (A, B), except where the read gate
        # refuses it first (catalog reads, time-travel comments); at least the rest.
        self.case(
            "nano/canonical_off",
            semantic.get("entries", 0) == 0
            and h1 == h0
            and semantic.get("ineligible", 0) > i0
            and len(warnings) == 1,
            semantic=semantic,
            l3_hits=[h0, h1],
            ineligible=[i0, semantic.get("ineligible")],
            warnings=warnings,
        )

    def run_corpus(self, label, corpus, table_prefix, backend, fixture):
        s1, s2 = CORPUS_SCHEMAS
        for i, (name, kind, a, b) in enumerate(corpus):
            table = f"{table_prefix}_{i:02d}"
            fixture(table)
            names = dict(t=table, T=table.capitalize(), s1=s1, s2=s2)
            qa = a.format(**names)
            qb = b.format(**names)
            stored = self.outcome(f"{qa} {HINT}", self.proxy_conn)
            h0 = self.tier_hits()
            through = self.outcome(f"{qb} {HINT}", self.proxy_conn)
            h1 = self.tier_hits()
            served_by = [t for t in ("l1", "l2", "l3") if h1.get(t, 0) > h0.get(t, 0)]
            hit = "l3" in served_by
            direct = self.outcome(qb, backend)
            same = through == direct
            # A pair covers a wrong-row shape only if A is stored (rows) and B's real
            # outcome differs from it; otherwise serving A for B could not be seen.
            discriminating = stored[0] == "rows" and stored != direct
            if kind == "paraphrase":
                ok = hit and same
            elif kind == "unmatched":
                # The same rows here, but never one L3 entry.
                ok = same and not hit
            elif kind == "l2":
                # Served by L2 alone (its key folds the difference): L3 is never
                # consulted, so the pair says nothing about the L3 key.
                ok = same and served_by == ["l2"]
            elif kind == "distinct":
                # No tier may serve B from A's entry (nor from any other).
                ok = same and discriminating and not served_by
            else:
                # Nothing writes between the two calls: whichever tier served B (or
                # none), it must return what the backend returns.
                ok = same and discriminating
            detail = dict(
                kind=kind,
                l3_hit=hit,
                served_by=served_by or ["backend"],
                discriminating=discriminating,
                a=qa,
                b=qb,
                a_result=stored,
                b_through_proxy=through,
                b_direct=direct,
            )
            if kind in ("different", "distinct") and not discriminating:
                # The backend no longer tells A and B apart (e.g. Nano P4 sorts
                # order_by_alias_named_like_input by the output alias, as PostgreSQL
                # does), or A returns no rows: serving A's entry for B could not be
                # seen. Not a failure when B came from the backend with its own result,
                # and never a pass; any tier serving B, or another outcome, still FAILs.
                if same and not served_by:
                    reason = "a_and_b_alike" if stored == direct else "a_returned_no_rows"
                    self.premise(f"{label}/{name}", reason=reason, **detail)
                    continue
            self.case(f"{label}/{name}", ok, **detail)

    # ------------------------------------------------------------ phase 2
    def approximate_sequence(self):
        """Cache alt (id 4) and val (id 3), wait for both names' embeddings, write row 3
        directly, then read alt of row 3 through a paraphrase (only an unqualified column
        is substituted: a qualified `k.alt` may be the function call `alt(k)`)."""
        self.value(f"SELECT alt FROM kv WHERE id = 4 {HINT}")
        self.value(f"SELECT val FROM kv WHERE id = 3 {HINT}")
        embedded = wait_for(lambda: self.semantic().get("embeddings", 0) >= 2, 10)
        self.direct("UPDATE kv SET val = 'v3-direct', alt = 'a3-direct' WHERE id = 3")
        got = self.value(f"SELECT alt FROM kv AS k WHERE id = 3 {HINT}")
        return bool(embedded), got, self.semantic().get("approximate_hits")

    def approximate(self, config):
        self.set_stub(mode="ok", synonyms=[["val", "alt"]])
        self.reset_tables()
        self.start_proxy(config)
        embedded, got, hits = self.approximate_sequence()
        self.case(
            "approximate",
            embedded and got == "v3" and (hits or 0) >= 1,
            embedded=embedded,
            got=got,
            approximate_hits=hits,
        )

    def approximate_off(self, config):
        self.set_stub(mode="ok", synonyms=[])
        self.reset_tables()
        self.start_proxy(config)
        embedded, got, hits = self.approximate_sequence()
        self.case(
            "approximate_off",
            embedded and got == "a3-direct" and hits == 0,
            embedded=embedded,
            got=got,
            approximate_hits=hits,
        )

    def hung_endpoint(self):
        # Stored while the stub answers: a qualified column (no name to embed), for the
        # exact canonical hit below. `val` is remembered from approximate_off.
        self.value(f"SELECT x.id FROM kv x WHERE x.id = 2 {HINT}")
        self.set_stub(mode="hang", hang_secs=10)
        # One session: the store of the first statement schedules `id` (its call hangs);
        # the second statement's only candidate is that entry, so its lookup lacks `id`
        # too. A store that waited would delay the second statement, a lookup that waited
        # its own reply; the session's third statement follows the second's store.
        latencies = []
        with psycopg.connect(**self.proxy_conn, autocommit=True, connect_timeout=10) as conn:
            for sql in (
                f"SELECT id FROM kv WHERE id = 2 {HINT}",
                f"SELECT val FROM kv WHERE id = 2 {HINT}",
                "SELECT 1",
            ):
                started = time.monotonic()
                conn.execute(sql).fetchall()
                latencies.append(round(time.monotonic() - started, 3))
        slowest = max(latencies)
        down = wait_for(lambda: self.semantic().get("endpoint") == "down", 5)
        down_at = time.monotonic()
        requests_down = self.stub_count()
        h0 = self.l3_hits()
        exact = self.value(f"SELECT kv.id FROM kv WHERE kv.id = 2 {HINT}")
        h1 = self.l3_hits()
        # New names inside the retry window are not sent.
        self.value(f"SELECT id, val FROM kv WHERE id = 4 {HINT}")
        self.value(f"SELECT val, id FROM kv WHERE id = 1 {HINT}")
        inside_window = time.monotonic() - down_at < 3
        requests_window = self.stub_count()
        self.case(
            "hung_endpoint",
            slowest < HUNG_READ_BOUND_SECS
            and down is True
            and exact == 2
            and h1 == h0 + 1
            and inside_window
            and requests_window == requests_down,
            read_secs=latencies,
            down=down,
            exact=exact,
            l3_hits=[h0, h1],
            requests=[requests_down, requests_window],
            inside_window=inside_window,
        )
        return requests_window

    def recovery(self, requests_before):
        self.set_stub(mode="ok")
        time.sleep(4)
        self.value(f"SELECT alt, id FROM kv WHERE id = 4 {HINT}")
        up = wait_for(lambda: self.semantic().get("endpoint") == "up", 5)
        requests = self.stub_count()
        self.case(
            "recovery",
            up is True and requests > requests_before,
            up=up,
            requests=[requests_before, requests],
        )

    def approximate_refusal(self, config):
        """Phase 2: the APPROX_CORPUS pairs on a proxy restarted at threshold 0.92, with
        every pair's names synonyms of each other and embedded before A and B run."""
        tables = [f"l3da_{i:02d}" for i in range(len(APPROX_CORPUS))]
        names = {"val", "alt", "l3dc_total"}
        for (_, _, _, _, pair_names), table in zip(APPROX_CORPUS, tables):
            names.update(n.format(t=table) for n in pair_names)
        self.set_stub(mode="ok", synonyms=[sorted(names)])
        self.start_proxy(config)
        embedded = set()
        for (name, kind, a, b, pair_names), table in zip(APPROX_CORPUS, tables):
            self.corpus_fixture(table)
            words = [n.format(t=table) for n in pair_names]
            helper = f"{table}_emb"
            self.helper_tables.append(helper)
            self.direct(f"DROP TABLE IF EXISTS public.{helper}")
            self.direct(
                f"CREATE TABLE public.{helper} (id int PRIMARY KEY, "
                + ", ".join(f"{w} int" for w in words) + ")"
            )
            ids = range(1, len(words) + 1)
            rows = ", ".join("(" + ", ".join([str(k)] * (len(words) + 1)) + ")" for k in ids)
            self.direct(f"INSERT INTO public.{helper} VALUES {rows}")
            fresh = [w for w in words if w not in embedded]
            e0 = self.semantic().get("embeddings", 0)
            # One id per name: each slot gets its own entry (no candidate of another).
            for k, w in enumerate(words, start=1):
                self.outcome(f"SELECT {w} FROM {helper} WHERE id = {k} {HINT}", self.proxy_conn)
            ready = bool(
                wait_for(lambda: self.semantic().get("embeddings", 0) >= e0 + len(fresh), 10)
            )
            embedded.update(words)
            qa, qb = a.format(t=table), b.format(t=table)
            stored = self.outcome(f"{qa} {HINT}", self.proxy_conn)
            x0 = self.semantic().get("approximate_hits", 0)
            h0 = self.tier_hits()
            through = self.outcome(f"{qb} {HINT}", self.proxy_conn)
            x1 = self.semantic().get("approximate_hits", 0)
            h1 = self.tier_hits()
            served_by = [t for t in ("l1", "l2", "l3") if h1.get(t, 0) > h0.get(t, 0)]
            direct = self.outcome(qb, self.backend)
            discriminating = stored[0] == "rows" and stored != direct
            if kind == "control":
                ok = ready and x1 == x0 + 1
            else:
                ok = ready and x1 == x0 and through == direct and discriminating
            self.case(
                f"approximate_refusal/{name}",
                ok,
                kind=kind,
                embedded=ready,
                approximate_hits=[x0, x1],
                served_by=served_by or ["backend"],
                discriminating=discriminating,
                a=qa,
                b=qb,
                a_result=stored,
                b_through_proxy=through,
                b_direct=direct,
            )

    # ------------------------------------------------------------ run
    def run(self):
        self.preflight()
        self.start_stub()
        self.verbatim()
        self.snapshot("verbatim")

        self.reset_tables()
        self.start_proxy(self.write_config())
        self.exact_canonical()
        self.leading_hint_strip()
        self.c02_invalidation()
        self.literal_guard()
        self.exclusion()
        self.skip_under_strip()
        self.differential()
        self.endpoint_untouched()
        self.snapshot("phase1")

        approx = self.write_config(threshold=0.92)
        self.approximate(approx)
        self.snapshot("approximate")
        self.approximate_off(approx)
        requests = self.hung_endpoint()
        self.recovery(requests)
        self.approximate_refusal(approx)
        self.snapshot("phase2")

        self.nano_differential()
        if self.nano is not None:
            self.snapshot("nano")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--proxy-binary", required=True)
    parser.add_argument("--dir", help="work directory (configs, logs); default a new temp dir")
    parser.add_argument("--output", help="results JSON (default DIR/cache-semantic-drill.json)")
    parser.add_argument(
        "--stub-port",
        type=int,
        default=int(os.environ.get("HELIOS_EMBED_STUB_PORT", "18434")),
    )
    parser.add_argument(
        "--nano-backend",
        default=os.environ.get("REGRESS_NANO_BACKEND"),
        help="host:port:user:password:database of a disposable HeliosDB-Nano for phase 3 "
        "(default $REGRESS_NANO_BACKEND; without one phase 3 is skipped)",
    )
    args = parser.parse_args()
    drill = Drill(args)
    error = None
    try:
        drill.run()
    except Exception as e:  # report what ran, then fail
        error = f"{type(e).__name__}: {e}"
        print(f"ERROR {error}", file=sys.stderr, flush=True)
    finally:
        drill.stop_all()
        drill.drop_tables()
    failed = [r["case"] for r in drill.results if not r["pass"]]
    evidence = {
        "backend": {k: v for k, v in drill.backend.items() if k != "password"},
        "nano_backend": drill.nano and {k: v for k, v in drill.nano.items() if k != "password"},
        "results": drill.results,
        "failed": failed,
        "skipped": drill.skipped,
        "premise_gone": drill.premise_gone,
        "error": error,
        "snapshots": drill.snapshots,
        "dir": str(drill.dir),
    }
    drill.output.write_text(json.dumps(evidence, indent=2, default=str))
    skipped = f", skipped: {', '.join(drill.skipped)}" if drill.skipped else ""
    if drill.premise_gone:
        skipped += f", premise gone: {', '.join(p['case'] for p in drill.premise_gone)}"
    print(f"== cache-semantic drill: {len(drill.results) - len(failed)} passed, "
          f"{len(failed)} failed{', error' if error else ''}{skipped}; evidence {drill.output} ==")
    sys.exit(1 if failed or error else 0)


if __name__ == "__main__":
    main()
