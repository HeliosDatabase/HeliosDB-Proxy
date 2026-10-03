use crate::analytics::{AnalyticsConfig, OrderBy, QueryAnalytics, QueryExecution};
use std::time::Duration;

#[test]
fn records_and_collapses_literals() {
    let a = QueryAnalytics::new(AnalyticsConfig::default());
    for n in [1, 2, 3] {
        a.record(QueryExecution::new(
            format!("select {n}"),
            Duration::from_millis(1),
        ));
    }
    let top = a.top_queries(OrderBy::Calls, 10);
    assert!(!top.is_empty(), "no fingerprints recorded");
    // The three literal variants collapse to one fingerprint (3 calls).
    assert!(
        top.iter().any(|s| s.calls >= 3),
        "literals did not collapse: {:?}",
        top.iter()
            .map(|s| (s.normalized.clone(), s.calls))
            .collect::<Vec<_>>()
    );
}
