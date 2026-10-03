use super::*;
use crate::routing::HintParser;

fn over(sql: &str) -> RouteOverride {
    let hints = HintParser::new().parse(sql);
    ProxyServer::hint_to_override(&hints)
}

#[test]
fn route_primary_maps_to_primary() {
    assert!(matches!(
        over("/*helios:route=primary*/ SELECT 1"),
        RouteOverride::Primary
    ));
}

#[test]
fn read_tier_targets_map_to_standby() {
    for t in ["standby", "sync", "semisync", "async", "local"] {
        assert!(
            matches!(
                over(&format!("/*helios:route={t}*/ SELECT 1")),
                RouteOverride::Standby
            ),
            "route={t} should map to Standby"
        );
    }
}

#[test]
fn any_and_vector_impose_no_constraint() {
    assert!(matches!(
        over("/*helios:route=any*/ SELECT 1"),
        RouteOverride::None
    ));
    assert!(matches!(
        over("/*helios:route=vector*/ SELECT 1"),
        RouteOverride::None
    ));
}

#[test]
fn node_hint_maps_to_node_and_wins_over_route() {
    // node= beats route= (precedence).
    match over("/*helios:node=pg-standby,route=primary*/ SELECT 1") {
        RouteOverride::Node(n) => assert_eq!(n, "pg-standby"),
        other => panic!("expected Node, got {other:?}"),
    }
}

#[test]
fn consistency_strong_forces_primary() {
    assert!(matches!(
        over("/*helios:consistency=strong*/ SELECT 1"),
        RouteOverride::Primary
    ));
}

#[test]
fn no_hint_yields_none() {
    assert!(matches!(over("SELECT 1"), RouteOverride::None));
}

// The core correctness fix: a leading hint comment must NOT hide the
// verb from write-detection. Raw classification misfires; classifying
// on the stripped SQL is correct.
#[test]
fn write_verb_classified_after_strip() {
    let parser = HintParser::new();
    let raw = "/*helios:route=primary*/ INSERT INTO t VALUES (1)";
    // Raw (unstripped) wrongly looks like a read because it starts
    // with the comment.
    assert!(!ProxyServer::is_write_query(raw));
    // Stripped is correctly a write.
    assert!(ProxyServer::is_write_query(&parser.strip(raw)));
}

#[test]
fn strip_removes_hint_comment() {
    let parser = HintParser::new();
    assert_eq!(
        parser.strip("/*helios:route=standby*/ SELECT 42"),
        "SELECT 42"
    );
}
