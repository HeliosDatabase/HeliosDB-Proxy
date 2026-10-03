use crate::schema_routing::{QueryAnalyzer, SchemaRegistry};
use std::sync::Arc;

fn analyzer() -> QueryAnalyzer {
    QueryAnalyzer::new(Arc::new(SchemaRegistry::new()))
}

#[test]
fn aggregation_group_by_is_analytics() {
    let a = analyzer();
    assert!(a
        .analyze("select count(*) from orders group by region")
        .is_analytics());
}

#[test]
fn simple_point_query_is_not_analytics() {
    let a = analyzer();
    assert!(!a
        .analyze("select * from orders where id = 1")
        .is_analytics());
}
