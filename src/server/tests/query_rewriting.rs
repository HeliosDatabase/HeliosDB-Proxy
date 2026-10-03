use crate::rewriter::{QueryPattern, QueryRewriter, RewriteRule, RewriterConfig, Transformation};

fn rw_with_table_replace() -> QueryRewriter {
    let rw = QueryRewriter::new(RewriterConfig {
        enabled: true,
        ..Default::default()
    });
    rw.add_rule(
        RewriteRule::build("t")
            .pattern(QueryPattern::Table("a".to_string()))
            .transform(Transformation::ReplaceTable {
                from: "a".to_string(),
                to: "b".to_string(),
            })
            .build(),
    );
    rw
}

#[test]
fn matching_query_is_rewritten() {
    let res = rw_with_table_replace().rewrite("select * from a").unwrap();
    assert!(res.was_rewritten(), "rule did not fire");
    assert!(res.query().contains('b'), "rewritten: {}", res.query());
    assert!(
        !res.query().contains("from a"),
        "still references a: {}",
        res.query()
    );
}

#[test]
fn unmatched_query_is_unchanged() {
    let res = rw_with_table_replace()
        .rewrite("select * from other")
        .unwrap();
    assert!(!res.was_rewritten());
    assert_eq!(res.query(), "select * from other");
}
