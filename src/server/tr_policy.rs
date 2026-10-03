/// PostgreSQL built-in functions that have no side effects when re-executed
/// (TR-03). This is the whole basis for calling an interrupted read
/// re-executable on an unknown outcome: a read calling anything NOT listed here
/// — a user-defined function, `nextval`, `pg_notify`, `set_config`, advisory
/// locks, large-object or replication functions — may already have run once and
/// is never run again by the proxy. Nondeterminism (`random`, `now`, `pg_sleep`)
/// is deliberately allowed: an unknown-outcome autocommit read published nothing
/// to the client, so only side effects matter. Sorted; looked up by binary
/// search on the ASCII-lowercased call name.
pub(super) const TR_PURE_BUILTINS: &[&str] = &[
    "abs",
    "acos",
    "acosh",
    "age",
    "array_agg",
    "array_append",
    "array_cat",
    "array_dims",
    "array_fill",
    "array_length",
    "array_lower",
    "array_ndims",
    "array_position",
    "array_positions",
    "array_prepend",
    "array_remove",
    "array_replace",
    "array_to_json",
    "array_to_string",
    "array_upper",
    "ascii",
    "asin",
    "asinh",
    "atan",
    "atan2",
    "atanh",
    "avg",
    "bit_and",
    "bit_count",
    "bit_length",
    "bit_or",
    "bit_xor",
    "bool_and",
    "bool_or",
    "btrim",
    "cardinality",
    "cbrt",
    "ceil",
    "ceiling",
    "char_length",
    "character_length",
    "chr",
    "clock_timestamp",
    "coalesce",
    "col_description",
    "concat",
    "concat_ws",
    "convert",
    "convert_from",
    "convert_to",
    "corr",
    "cos",
    "cosh",
    "cot",
    "count",
    "covar_pop",
    "covar_samp",
    "cume_dist",
    "current_database",
    "current_query",
    "current_schema",
    "current_schemas",
    "current_setting",
    "date_bin",
    "date_part",
    "date_trunc",
    "decode",
    "degrees",
    "dense_rank",
    "div",
    "encode",
    "every",
    "exp",
    "extract",
    "factorial",
    "first_value",
    "floor",
    "format",
    "gcd",
    "gen_random_uuid",
    "generate_series",
    "generate_subscripts",
    "get_bit",
    "get_byte",
    "greatest",
    "has_column_privilege",
    "has_database_privilege",
    "has_function_privilege",
    "has_schema_privilege",
    "has_table_privilege",
    "inet_client_addr",
    "inet_client_port",
    "inet_server_addr",
    "inet_server_port",
    "initcap",
    "isfinite",
    "json_agg",
    "json_array_elements",
    "json_array_elements_text",
    "json_array_length",
    "json_build_array",
    "json_build_object",
    "json_each",
    "json_each_text",
    "json_extract_path",
    "json_extract_path_text",
    "json_object",
    "json_object_agg",
    "json_object_keys",
    "json_populate_record",
    "json_populate_recordset",
    "json_strip_nulls",
    "json_to_record",
    "json_to_recordset",
    "json_typeof",
    "jsonb_agg",
    "jsonb_array_elements",
    "jsonb_array_elements_text",
    "jsonb_array_length",
    "jsonb_build_array",
    "jsonb_build_object",
    "jsonb_each",
    "jsonb_each_text",
    "jsonb_extract_path",
    "jsonb_extract_path_text",
    "jsonb_insert",
    "jsonb_object",
    "jsonb_object_agg",
    "jsonb_object_keys",
    "jsonb_path_exists",
    "jsonb_path_match",
    "jsonb_path_query",
    "jsonb_path_query_array",
    "jsonb_path_query_first",
    "jsonb_populate_record",
    "jsonb_populate_recordset",
    "jsonb_pretty",
    "jsonb_set",
    "jsonb_set_lax",
    "jsonb_strip_nulls",
    "jsonb_to_record",
    "jsonb_to_recordset",
    "jsonb_typeof",
    "justify_days",
    "justify_hours",
    "justify_interval",
    "lag",
    "last_value",
    "lcm",
    "lead",
    "least",
    "left",
    "length",
    "ln",
    "localtime",
    "localtimestamp",
    "log",
    "log10",
    "lower",
    "lpad",
    "ltrim",
    "make_date",
    "make_interval",
    "make_time",
    "make_timestamp",
    "make_timestamptz",
    "max",
    "md5",
    "min",
    "mod",
    "mode",
    "nlevel",
    "now",
    "nth_value",
    "ntile",
    "nullif",
    "num_nonnulls",
    "num_nulls",
    "obj_description",
    "octet_length",
    "overlay",
    "parse_ident",
    "percent_rank",
    "percentile_cont",
    "percentile_disc",
    "pg_backend_pid",
    "pg_client_encoding",
    "pg_column_size",
    "pg_conf_load_time",
    "pg_current_wal_flush_lsn",
    "pg_current_wal_insert_lsn",
    "pg_current_wal_lsn",
    "pg_database_size",
    "pg_get_constraintdef",
    "pg_get_expr",
    "pg_get_functiondef",
    "pg_get_indexdef",
    "pg_get_userbyid",
    "pg_get_viewdef",
    "pg_has_role",
    "pg_indexes_size",
    "pg_is_in_recovery",
    "pg_last_wal_receive_lsn",
    "pg_last_wal_replay_lsn",
    "pg_last_xact_replay_timestamp",
    "pg_postmaster_start_time",
    "pg_relation_size",
    "pg_size_bytes",
    "pg_size_pretty",
    "pg_sleep",
    "pg_sleep_for",
    "pg_sleep_until",
    "pg_table_size",
    "pg_total_relation_size",
    "pg_typeof",
    "pi",
    "position",
    "power",
    "quote_ident",
    "quote_literal",
    "quote_nullable",
    "radians",
    "random",
    "rank",
    "regexp_count",
    "regexp_instr",
    "regexp_like",
    "regexp_match",
    "regexp_matches",
    "regexp_replace",
    "regexp_split_to_array",
    "regexp_split_to_table",
    "regexp_substr",
    "regr_avgx",
    "regr_avgy",
    "regr_count",
    "regr_intercept",
    "regr_r2",
    "regr_slope",
    "regr_sxx",
    "regr_sxy",
    "regr_syy",
    "repeat",
    "replace",
    "reverse",
    "right",
    "round",
    "row_number",
    "row_to_json",
    "rpad",
    "rtrim",
    "scale",
    "set_bit",
    "set_byte",
    "sha224",
    "sha256",
    "sha384",
    "sha512",
    "sign",
    "sin",
    "sinh",
    "split_part",
    "sqrt",
    "starts_with",
    "statement_timestamp",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "string_agg",
    "string_to_array",
    "string_to_table",
    "strpos",
    "substr",
    "substring",
    "sum",
    "tan",
    "tanh",
    "timeofday",
    "to_ascii",
    "to_char",
    "to_date",
    "to_hex",
    "to_json",
    "to_jsonb",
    "to_number",
    "to_timestamp",
    "to_tsquery",
    "to_tsvector",
    "transaction_timestamp",
    "translate",
    "trim",
    "trim_scale",
    "trunc",
    "unnest",
    "upper",
    "var_pop",
    "var_samp",
    "variance",
    "version",
    "width_bucket",
    "xmlagg",
];

/// SQL keywords that can be followed by `(` without being a function call.
pub(super) const TR_CALL_KEYWORDS: &[&str] = &[
    "all",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "between",
    "by",
    "case",
    "cast",
    "collate",
    "cross",
    "date",
    "desc",
    "distinct",
    "else",
    "end",
    "except",
    "exists",
    "filter",
    "first",
    "following",
    "for",
    "from",
    "full",
    "group",
    "having",
    "in",
    "inner",
    "intersect",
    "interval",
    "is",
    "join",
    "lateral",
    "least",
    "left",
    "limit",
    "natural",
    "not",
    "null",
    "nulls",
    "offset",
    "on",
    "or",
    "order",
    "outer",
    "over",
    "partition",
    "preceding",
    "range",
    "returning",
    "right",
    "row",
    "rows",
    "select",
    "some",
    "table",
    "then",
    "time",
    "timestamp",
    "unbounded",
    "union",
    "using",
    "values",
    "when",
    "where",
    "window",
    "with",
    "within",
];

/// Operator-extended read-eligibility policy (`tr_read_functions`).
#[derive(Debug, Default)]
pub struct TrReadPolicy {
    /// Lowercased extra function names treated as side-effect-free.
    pub(super) extra: std::collections::HashSet<String>,
}

impl TrReadPolicy {
    pub fn from_config(names: &[String]) -> Self {
        Self {
            extra: names.iter().map(|n| n.to_ascii_lowercase()).collect(),
        }
    }

    pub(super) fn allows_call(&self, name: &str) -> bool {
        // Names are ASCII identifiers by validation; fold without allocating
        // for the overwhelmingly common short case.
        let mut buf = [0u8; 64];
        let lower: &str = if name.len() <= buf.len() && name.is_ascii() {
            let b = &mut buf[..name.len()];
            b.copy_from_slice(name.as_bytes());
            b.make_ascii_lowercase();
            std::str::from_utf8(b).unwrap_or(name)
        } else {
            return self.extra.contains(&name.to_ascii_lowercase());
        };
        TR_CALL_KEYWORDS.binary_search(&lower).is_ok()
            || TR_PURE_BUILTINS.binary_search(&lower).is_ok()
            || self.extra.contains(lower)
    }
}

/// One deferred session-GUC change inside an explicit transaction (TR-04).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum GucOp {
    /// A `SET` statement to replay verbatim.
    Set(String),
    /// `RESET <name>`: forget the variable.
    Reset(String),
    /// `RESET ALL` / `DISCARD ALL`: forget every variable.
    ResetAll,
}

/// TR-06: a bounded, order-sensitive digest of the frames a client observed
/// from one response. Recorded per statement while a transaction is being
/// captured for replay; recomputed over the replacement backend's response at
/// replay time and compared, so a replay never continues a transaction whose
/// earlier results came from a different snapshot.
pub(super) struct Observation {
    pub(super) hasher: std::collections::hash_map::DefaultHasher,
    pub(super) bytes: usize,
    pub(super) cap: usize,
    pub(super) overflow: bool,
}

impl Observation {
    pub(super) fn new(cap: usize) -> Self {
        Self {
            hasher: std::collections::hash_map::DefaultHasher::new(),
            bytes: 0,
            cap,
            overflow: false,
        }
    }

    /// Frames that carry what the client sees of a result. Notices, parameter
    /// status and notifications are asynchronous and excluded; ReadyForQuery is
    /// the recorder's own bookkeeping.
    pub(super) fn observes(mtype: u8) -> bool {
        matches!(mtype, b'T' | b'D' | b'C' | b'I')
    }

    pub(super) fn note(&mut self, frame: &[u8]) {
        if self.overflow || !Self::observes(frame[0]) {
            return;
        }
        if self.bytes.saturating_add(frame.len()) > self.cap {
            self.overflow = true;
            return;
        }
        use std::hash::Hasher as _;
        self.hasher.write(frame);
        self.bytes += frame.len();
    }

    /// `None` when the response exceeded the budget (unverifiable); otherwise a
    /// non-zero digest (zero is reserved for "none recorded").
    pub(super) fn finish(&self) -> Option<u64> {
        use std::hash::Hasher as _;
        (!self.overflow).then(|| self.hasher.finish().max(1))
    }
}
