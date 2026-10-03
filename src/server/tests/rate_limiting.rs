use crate::rate_limit::{LimiterKey, RateLimitConfig, RateLimitResult, RateLimiter};

#[test]
fn burst_allows_then_denies() {
    // Mirror the wiring's config conversion: tiny bucket, reject on
    // exceed (the engine default).
    let cfg = RateLimitConfig {
        enabled: true,
        default_qps: 1,
        default_burst: 2,
        ..Default::default()
    };
    let limiter = RateLimiter::new(cfg);
    let key = LimiterKey::User("u".to_string());

    // The first `burst` checks are admitted.
    assert!(matches!(limiter.check(&key, 1), RateLimitResult::Allowed));
    assert!(matches!(limiter.check(&key, 1), RateLimitResult::Allowed));

    // Rapid over-burst checks must produce at least one hard denial.
    let mut denied = false;
    for _ in 0..5 {
        if matches!(limiter.check(&key, 1), RateLimitResult::Denied(_)) {
            denied = true;
        }
    }
    assert!(denied, "over-burst checks must yield a Denied verdict");
}

/// The per-session bucket key is resolved once and then reused: two
/// gate invocations must hand back the *same* memoized value, not a
/// freshly built one (the whole point of the cache — no key alloc, no
/// `variables` read lock, no metrics `format!` per query).
#[tokio::test]
async fn session_key_is_memoized_after_startup_params() {
    use crate::config::RateLimitKeyBy;

    let mut cfg = super::test_config();
    cfg.rate_limit.key_by = RateLimitKeyBy::User;

    let session = super::make_test_session();
    {
        let mut vars = session.variables.write().await;
        vars.insert("user".into(), "alice".into());
    }

    let first = super::ProxyServer::rate_limit_key(&session, &cfg).await;
    assert_eq!(first.as_ref().to_string(), "user:alice");
    drop(first);

    assert!(
        session.rate_limit_key.get().is_some(),
        "a resolvable key must be cached on the session"
    );

    // The second call must borrow the memoized value rather than
    // rebuild one.
    let second = super::ProxyServer::rate_limit_key(&session, &cfg).await;
    assert!(
        matches!(second, std::borrow::Cow::Borrowed(_)),
        "key was rebuilt instead of reused"
    );
    assert_eq!(second.as_ref().to_string(), "user:alice");
}

/// Before the startup parameters land the key must NOT be memoized —
/// otherwise a placeholder (`user:`) would be frozen for the whole
/// session. The pre-startup verdict is byte-identical to the old
/// recompute-every-time behavior.
#[tokio::test]
async fn key_is_not_memoized_before_startup_params() {
    use crate::config::RateLimitKeyBy;

    let mut cfg = super::test_config();
    cfg.rate_limit.key_by = RateLimitKeyBy::Database;

    let session = super::make_test_session();

    let early = super::ProxyServer::rate_limit_key(&session, &cfg).await;
    assert_eq!(early.as_ref().to_string(), "db:");
    assert!(
        matches!(early, std::borrow::Cow::Owned(_)),
        "a placeholder key must not be served from the cache"
    );
    drop(early);
    assert!(
        session.rate_limit_key.get().is_none(),
        "a placeholder key must never be cached"
    );

    {
        let mut vars = session.variables.write().await;
        vars.insert("database".into(), "shop".into());
    }

    let later = super::ProxyServer::rate_limit_key(&session, &cfg).await;
    assert_eq!(later.as_ref().to_string(), "db:shop");
    drop(later);
    assert!(session.rate_limit_key.get().is_some());
}

/// Keying dimensions that do not read session variables are cached on
/// the very first call, and render exactly as before.
#[tokio::test]
async fn variable_free_keys_are_cached_immediately() {
    use crate::config::RateLimitKeyBy;

    for (key_by, expected) in [
        (RateLimitKeyBy::Global, "global"),
        (RateLimitKeyBy::ClientIp, "ip:127.0.0.1"),
    ] {
        let mut cfg = super::test_config();
        cfg.rate_limit.key_by = key_by;

        let session = super::make_test_session();
        let key = super::ProxyServer::rate_limit_key(&session, &cfg).await;
        assert_eq!(key.as_ref().to_string(), expected);
        drop(key);
        assert!(session.rate_limit_key.get().is_some());
    }
}

#[test]
fn distinct_keys_have_independent_buckets() {
    let cfg = RateLimitConfig {
        enabled: true,
        default_qps: 1,
        default_burst: 1,
        ..Default::default()
    };
    let limiter = RateLimiter::new(cfg);
    // Each user gets its own bucket: both first checks are admitted.
    assert!(matches!(
        limiter.check(&LimiterKey::User("a".to_string()), 1),
        RateLimitResult::Allowed
    ));
    assert!(matches!(
        limiter.check(&LimiterKey::User("b".to_string()), 1),
        RateLimitResult::Allowed
    ));
}
