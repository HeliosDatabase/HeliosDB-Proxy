use super::*;

fn context(id: u64) -> CacheContext {
    CacheContext {
        connection_id: Some(id),
        ..Default::default()
    }
}

async fn fill(cache: &QueryCache, sql: &str, id: u64, data: &'static [u8]) {
    cache
        .put(
            sql,
            &context(id),
            Bytes::from_static(data),
            1,
            Duration::ZERO,
        )
        .await;
}

#[tokio::test]
async fn l1_only_does_not_fill_or_consult_shared_tiers() {
    let cache = QueryCache::new(CacheConfig::default());
    let sql = "SELECT value FROM products /*helios:cache=l1*/";
    fill(&cache, sql, 1, b"local").await;
    assert!(matches!(
        cache.get(sql, &context(2)).await,
        CacheLookup::Miss
    ));
    let CacheLookup::Hit { result, level } = cache.get(sql, &context(1)).await else {
        panic!("local fill missing")
    };
    assert_eq!(level, CacheLevel::L1Hot);
    assert_eq!(result.data.as_ref(), b"local");
    assert_eq!(cache.stats().l2.hits, 0);
}

#[tokio::test]
async fn l2_only_never_creates_or_promotes_an_l1_entry() {
    let cache = QueryCache::new(CacheConfig::default());
    let sql = "SELECT value FROM products /*helios:cache=l2*/";
    fill(&cache, sql, 1, b"shared").await;
    for id in [1, 2, 1] {
        let CacheLookup::Hit { level, .. } = cache.get(sql, &context(id)).await else {
            panic!("shared fill missing")
        };
        assert_eq!(level, CacheLevel::L2Warm);
    }
    assert_eq!(cache.l1_cache_count(), 0);
}

#[tokio::test]
async fn l2_only_with_literal_predicate_and_explicit_ttls_stays_shared() {
    let mut config = CacheConfig::default();
    config.l1.ttl = Duration::from_secs(3600);
    config.l2.ttl = Duration::from_secs(3600);
    let cache = QueryCache::new(config);
    let sql = "SELECT id, name, email FROM accounts WHERE id = 42 AND status = 'active' /*helios:cache=l2*/";
    let conn = CacheContext {
        database: "bench".to_string(),
        user: Some("bench".to_string()),
        branch: None,
        connection_id: Some(1),
    };
    cache
        .put(sql, &conn, Bytes::from(vec![b'x'; 1024]), 1, Duration::ZERO)
        .await;
    assert_eq!(cache.l1_cache_count(), 0, "L2-only fill must not create L1");
    for id in [1, 2, 1] {
        let lookup_context = CacheContext {
            connection_id: Some(id),
            ..conn.clone()
        };
        let CacheLookup::Hit { level, .. } = cache.get(sql, &lookup_context).await else {
            panic!("shared fill missing")
        };
        assert_eq!(level, CacheLevel::L2Warm);
    }
    assert_eq!(cache.l1_cache_count(), 0, "L2-only reads must not promote");
}

#[tokio::test]
async fn declared_dependencies_are_retained_by_public_put() {
    let config = CacheConfig {
        default_ttl: Duration::from_secs(3600),
        l1: L1Config {
            ttl: Duration::from_secs(3600),
            ..Default::default()
        },
        l2: L2Config {
            ttl: Duration::from_secs(3600),
            ..Default::default()
        },
        ..Default::default()
    };
    let cache = QueryCache::new(config);
    let sql = "SELECT id, name, email FROM accounts WHERE id = 42 AND status = 'active' /*helios:cache_tables=prices*/";
    let conn = CacheContext {
        database: "bench".to_string(),
        user: Some("bench".to_string()),
        branch: None,
        connection_id: None,
    };
    cache
        .put(sql, &conn, Bytes::from(vec![b'x'; 1024]), 1, Duration::ZERO)
        .await;
    let CacheLookup::Hit { result, .. } = cache.get(sql, &conn).await else {
        panic!("declared dependency result missing")
    };
    assert!(result.tables.contains(&"accounts".to_string()));
    assert!(
        result.tables.contains(&"prices".to_string()),
        "tables={:?}",
        result.tables
    );
}

#[tokio::test]
async fn declarations_add_to_inferred_dependencies_and_the_fill_snapshot() {
    let cache = QueryCache::new(CacheConfig::default());
    let sql = "SELECT value FROM products /*helios:cache_tables=prices*/";
    let ctx = context(1);
    let (miss, prep) = cache.get_with_prep(sql, &ctx).await;
    assert!(matches!(miss, CacheLookup::Miss));
    cache
        .put_prepared(
            sql,
            &ctx,
            &prep,
            Bytes::from_static(b"rows"),
            1,
            Duration::ZERO,
        )
        .await;
    let CacheLookup::Hit { result, .. } = cache.get(sql, &ctx).await else {
        panic!("fill missing")
    };
    assert!(result.tables.contains(&"products".into()) && result.tables.contains(&"prices".into()));
    cache.mark_written(&["prices".into()]);
    assert!(matches!(cache.get(sql, &ctx).await, CacheLookup::Miss));
    let (_, prep) = cache.get_with_prep(sql, &ctx).await;
    cache.mark_written(&["products".into()]);
    cache
        .put_prepared(
            sql,
            &ctx,
            &prep,
            Bytes::from_static(b"raced"),
            1,
            Duration::ZERO,
        )
        .await;
    assert!(
        matches!(cache.get(sql, &ctx).await, CacheLookup::Miss),
        "inferred relation was removed by declaration"
    );
    let (_, prep) = cache.get_with_prep(sql, &ctx).await;
    cache.mark_written(&["prices".into()]);
    cache
        .put_prepared(
            sql,
            &ctx,
            &prep,
            Bytes::from_static(b"raced"),
            1,
            Duration::ZERO,
        )
        .await;
    assert!(
        matches!(cache.get(sql, &ctx).await, CacheLookup::Miss),
        "declared relation was absent from fetch snapshot"
    );
}

#[tokio::test]
async fn an_older_shared_entry_cannot_satisfy_new_dependencies() {
    let cache = QueryCache::new(CacheConfig::default());
    let plain = "SELECT value FROM products";
    let hinted = "SELECT value FROM products /*helios:cache_tables=prices*/";
    fill(&cache, plain, 1, b"old").await;
    assert!(matches!(
        cache.get(hinted, &context(2)).await,
        CacheLookup::Miss
    ));
    fill(&cache, hinted, 2, b"complete").await;
    let CacheLookup::Hit { result, .. } = cache.get(plain, &context(3)).await else {
        panic!("shared fill missing")
    };
    assert_eq!(result.data.as_ref(), b"complete");
    cache.mark_written(&["prices".into()]);
    assert!(matches!(
        cache.get(hinted, &context(2)).await,
        CacheLookup::Miss
    ));
    assert!(matches!(
        cache.get(plain, &context(3)).await,
        CacheLookup::Miss
    ));
}

#[tokio::test]
async fn missing_tiers_and_unreadable_declarations_bypass_capture() {
    let mut config = CacheConfig::default();
    config.l2.enabled = false;
    let cache = QueryCache::new(config);
    for sql in [
        "SELECT value FROM products /*helios:cache=l2*/",
        "SELECT value FROM products /*helios:cache_tables=prices$unreadable*/",
    ] {
        let (_, prep) = cache.get_with_prep(sql, &context(1)).await;
        assert!(!prep.storable(), "{sql}");
        fill(&cache, sql, 1, b"never stored").await;
        assert!(matches!(
            cache.get(sql, &context(1)).await,
            CacheLookup::Miss
        ));
    }
}

#[tokio::test]
async fn stripped_hints_keep_all_dependencies_and_the_strictest_policy() {
    let cache = QueryCache::new(CacheConfig::default());
    let forwarded = "SELECT value FROM products /*helios:cache_tables=stock,cache_ttl=60*/";
    let client = "SELECT value FROM products /*helios:cache_tables=prices,cache_ttl=5*/";
    let (_, prep) = cache
        .get_with_prep_hinted(forwarded, Some(client), &context(1))
        .await;
    cache
        .put_prepared(
            forwarded,
            &context(1),
            &prep,
            Bytes::from_static(b"rows"),
            1,
            Duration::ZERO,
        )
        .await;
    let CacheLookup::Hit { result, .. } = cache.get(forwarded, &context(1)).await else {
        panic!("fill missing")
    };
    for table in ["products", "prices", "stock"] {
        assert!(result.tables.contains(&table.to_string()));
    }
    assert!(result.ttl <= Duration::from_secs(5));
    let (_, prep) = cache
        .get_with_prep_hinted(
            "SELECT 1 /*helios:cache=l2*/",
            Some("SELECT 1 /*helios:cache=l1*/"),
            &context(1),
        )
        .await;
    assert!(
        !prep.storable(),
        "conflicting restrictions must not widen scope"
    );
}

#[test]
fn multiple_hint_comments_never_widen_scope_or_ttl() {
    let hint = parse_cache_hints(
        "SELECT 1 /*helios:cache=l1,cache_ttl=5*/ /*helios:cache=all,cache_ttl=60*/",
    );
    assert_eq!(hint.level, Some(hints::CacheLevelHint::L1Only));
    assert_eq!(hint.ttl, Some(Duration::from_secs(5)));
    let hint = parse_cache_hints("SELECT 1 /*helios:cache=l1*/ /*helios:cache=l2*/");
    assert!(hint.skip);
}

#[tokio::test]
async fn refreshing_l1_cannot_replace_the_shared_value() {
    let cache = QueryCache::new(CacheConfig::default());
    let plain = "SELECT value FROM products";
    let local = "SELECT value FROM products /*helios:cache=l1,cache_refresh*/";
    fill(&cache, plain, 1, b"shared").await;
    let (_, prep) = cache.get_with_prep(local, &context(2)).await;
    cache
        .put_prepared(
            local,
            &context(2),
            &prep,
            Bytes::from_static(b"local"),
            1,
            Duration::ZERO,
        )
        .await;
    let CacheLookup::Hit { result, level } = cache.get(plain, &context(3)).await else {
        panic!("shared value missing")
    };
    assert_eq!(level, CacheLevel::L2Warm);
    assert_eq!(result.data.as_ref(), b"shared");
}
