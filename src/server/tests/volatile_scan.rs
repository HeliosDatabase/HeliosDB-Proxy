//! `[cache] extra_volatile_functions` and the shared volatile-function scan
//! (Sprinter 87c5caf119f8).

use super::*;
use crate::sql_text::VolatileScan;

#[test]
fn builtin_scan_matches_the_default_predicate() {
    let scan = VolatileScan::builtin();
    for sql in ["select now()", "select v from t", "select localtime"] {
        assert_eq!(
            ProxyServer::is_cacheable_read_sql_with(sql, scan),
            ProxyServer::is_cacheable_read_sql(sql),
            "{sql}"
        );
    }
}

#[test]
fn extra_volatile_names_and_prefixes_refuse_the_cache() {
    let scan = VolatileScan::new(["my_clock", "audit_"]);
    assert!(!ProxyServer::is_cacheable_read_sql_with(
        "select my_clock() from t",
        &scan
    ));
    // A prefix covers every function of that family, in any letter case.
    assert!(!ProxyServer::is_cacheable_read_sql_with(
        "SELECT AUDIT_NEXT_ID()",
        &scan
    ));
    assert!(ProxyServer::is_cacheable_read_sql_with(
        "select v from t",
        &scan
    ));
}

#[test]
fn extra_names_do_not_weaken_the_builtin_list() {
    let scan = VolatileScan::new(["my_clock"]);
    assert!(!ProxyServer::is_cacheable_read_sql_with(
        "select now()",
        &scan
    ));
    assert!(!ProxyServer::is_cacheable_read_sql_with(
        "select txid_current()",
        &scan
    ));
    assert!(!ProxyServer::is_cacheable_read_sql_with(
        "select 1 for update",
        &scan
    ));
}

#[test]
fn extra_names_are_matched_case_insensitively() {
    let scan = VolatileScan::new(["My_Clock"]);
    assert!(!ProxyServer::is_cacheable_read_sql_with(
        "select MY_CLOCK()",
        &scan
    ));
}
