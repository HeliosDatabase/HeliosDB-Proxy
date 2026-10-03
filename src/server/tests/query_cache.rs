use super::ProxyServer;

#[test]
fn plain_selects_are_cacheable() {
    assert!(ProxyServer::is_cacheable_read_sql("select v from t"));
    assert!(ProxyServer::is_cacheable_read_sql(
        "  SELECT a, b FROM users WHERE id = 5"
    ));
}

#[test]
fn writes_and_non_selects_are_not_cacheable() {
    assert!(!ProxyServer::is_cacheable_read_sql(
        "insert into t values (1)"
    ));
    assert!(!ProxyServer::is_cacheable_read_sql("update t set v = 1"));
    assert!(!ProxyServer::is_cacheable_read_sql("show search_path"));
}

#[test]
fn locking_and_volatile_selects_are_not_cacheable() {
    assert!(!ProxyServer::is_cacheable_read_sql(
        "select * from t for update"
    ));
    assert!(!ProxyServer::is_cacheable_read_sql("select now()"));
    assert!(!ProxyServer::is_cacheable_read_sql("select random()"));
    assert!(!ProxyServer::is_cacheable_read_sql("select nextval('s')"));
    // set_config mutates GUCs + emits ParameterStatus — replaying
    // from cache would suppress the side effect.
    assert!(!ProxyServer::is_cacheable_read_sql(
        "select set_config('timezone', 'UTC', false) from t"
    ));
}

#[test]
fn multi_statement_strings_are_not_cacheable() {
    // Replaying `SELECT ...; UPDATE ...` would fabricate the
    // UPDATE's CommandComplete while executing nothing.
    assert!(!ProxyServer::is_cacheable_read_sql(
        "select v from t; update t set v = 1"
    ));
    assert!(!ProxyServer::is_cacheable_read_sql("select 1; select 2"));
    // A single trailing semicolon stays cacheable.
    assert!(ProxyServer::is_cacheable_read_sql("select v from t;"));
    assert!(ProxyServer::is_cacheable_read_sql("SELECT v FROM t ; "));
}

#[test]
fn literal_semicolon_or_into_over_rejects_by_design() {
    // G7 (accepted, safe-direction): the multi-statement and SELECT INTO
    // guards scan the RAW text, so a ';' or the word "into" inside a
    // string literal disqualifies an otherwise-cacheable SELECT. This
    // over-rejection is DELIBERATE and hit-rate-only — the raw scan is
    // the sole defense against multi-statement replay fabrication (a
    // literal-stripping pre-pass would misjudge `'x\'; UPDATE ...'` under
    // standard_conforming_strings and reopen that hole), and a real
    // SELECT INTO is protocol-indistinguishable from a plain SELECT.
    assert!(!ProxyServer::is_cacheable_read_sql(
        "select v from t where url = 'a;b=c'"
    ));
    assert!(!ProxyServer::is_cacheable_read_sql(
        "select v from t where body like '%go into space%'"
    ));
    // A real SELECT INTO (it creates a table) must never be cached.
    assert!(!ProxyServer::is_cacheable_read_sql(
        "select * into snapshot from t"
    ));
}

#[test]
fn select_into_is_not_cacheable() {
    // SELECT ... INTO creates a table (CREATE TABLE AS synonym);
    // a cache replay would silently skip the DDL.
    assert!(!ProxyServer::is_cacheable_read_sql(
        "select * into report_tmp from src"
    ));
    // Word-boundary: newline/tab-delimited INTO is caught too.
    assert!(!ProxyServer::is_cacheable_read_sql(
        "SELECT *\nINTO report_tmp\nFROM src"
    ));
    // ...but an identifier merely containing "into" is not.
    assert!(ProxyServer::is_cacheable_read_sql(
        "select into_total from t"
    ));
}

/// Regression for the single-lowercase-pass rewrite of the FOR
/// UPDATE/FOR SHARE + VOLATILE-token checks: every needle must still
/// be matched case-insensitively regardless of how the caller casts
/// the keyword, exactly as the old per-needle `contains_ci` scan did.
#[test]
fn locking_and_volatile_checks_stay_case_insensitive() {
    assert!(!ProxyServer::is_cacheable_read_sql(
        "select * from t FOR UPDATE"
    ));
    assert!(!ProxyServer::is_cacheable_read_sql(
        "select * from t For Update"
    ));
    assert!(!ProxyServer::is_cacheable_read_sql(
        "select * from t for share"
    ));
    assert!(!ProxyServer::is_cacheable_read_sql(
        "select * from t FOR SHARE"
    ));
    assert!(!ProxyServer::is_cacheable_read_sql("SELECT NOW()"));
    assert!(!ProxyServer::is_cacheable_read_sql(
        "select CURRENT_TIMESTAMP"
    ));
    assert!(!ProxyServer::is_cacheable_read_sql(
        "select GEN_RANDOM_UUID()"
    ));
    assert!(!ProxyServer::is_cacheable_read_sql(
        "SELECT Set_Config('timezone', 'UTC', false)"
    ));
}

/// Non-ASCII bytes in the SQL text must not panic the lowercasing
/// buffer (`to_ascii_lowercase` only touches ASCII bytes, so UTF-8
/// validity is preserved) and must not be case-folded — same
/// byte-for-byte semantics as the old `contains_ci`.
#[test]
fn non_ascii_sql_is_handled_safely() {
    assert!(ProxyServer::is_cacheable_read_sql(
        "select name from café where city = 'Zürich'"
    ));
    // A non-ASCII volatile-token lookalike must not be flagged —
    // "NÓW(" is not "now(" under byte-for-byte comparison.
    assert!(ProxyServer::is_cacheable_read_sql("select NÓW() from t"));
}
