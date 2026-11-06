//! Startup `database` conventions shared by `[[hba]]` admission, tenant
//! identification (`[multi_tenancy] identify_by = "database"`) and the
//! per-tenant caps, so that every check resolves a session's database the
//! same way.
//!
//! HeliosDB-Nano selects a branch per connection with `database=<db>@<branch>`.
//! Nano resolves the whole value first (a reserved name or a tenant), and only
//! when that fails splits it at the FIRST `@`. The helpers here follow the
//! same rule.

/// Split a startup database name `<base>@<branch>` at its FIRST `@`.
///
/// A name without `@`, or with an empty base or an empty branch, has no
/// branch: the result is `(database, None)`.
pub fn split_branch(database: &str) -> (&str, Option<&str>) {
    match database.split_once('@') {
        Some((base, branch)) if !base.is_empty() && !branch.is_empty() => (base, Some(branch)),
        _ => (database, None),
    }
}

/// Whether an `[[hba]]` rule's `database` field admits the startup database.
///
/// `all` matches every database. A rule without `@` matches the full startup
/// value or its base (the part before the first `@`), so `prod` covers
/// `prod@feature`. A rule containing `@` (e.g. `prod@feature`) matches only
/// the exact full value.
pub fn hba_database_matches(rule: &str, database: &str) -> bool {
    if rule == "all" || rule == database {
        return true;
    }
    if rule.contains('@') {
        return false;
    }
    split_branch(database).0 == rule
}

/// The tenant a startup packet names when `identify_by = "database"`.
///
/// `database` is the startup `database` parameter; when it is absent,
/// PostgreSQL uses the user name, so `user` is used instead. If the whole
/// value is a known tenant (`is_known`) it is the tenant, otherwise its base
/// before the first `@` is. The result may still be unknown: callers check it
/// against their tenant list.
pub fn tenant_from_startup<'a>(
    database: Option<&'a str>,
    user: Option<&'a str>,
    is_known: impl Fn(&str) -> bool,
) -> Option<&'a str> {
    let value = database.or(user)?;
    if is_known(value) {
        return Some(value);
    }
    Some(split_branch(value).0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_branch_splits_at_the_first_at() {
        assert_eq!(split_branch("prod"), ("prod", None));
        assert_eq!(split_branch("prod@feat"), ("prod", Some("feat")));
        assert_eq!(split_branch("prod@a@b"), ("prod", Some("a@b")));
        // Empty base or empty branch: no branch.
        assert_eq!(split_branch("@feat"), ("@feat", None));
        assert_eq!(split_branch("prod@"), ("prod@", None));
        assert_eq!(split_branch(""), ("", None));
    }

    #[test]
    fn hba_rule_without_at_matches_full_value_or_base() {
        assert!(hba_database_matches("prod", "prod"));
        assert!(hba_database_matches("prod", "prod@b"));
        assert!(!hba_database_matches("prod", "prodx@b"));
        assert!(!hba_database_matches("prod", "other@prod"));
        assert!(!hba_database_matches("prod", "prod2"));
    }

    #[test]
    fn hba_rule_with_at_matches_only_the_exact_value() {
        assert!(hba_database_matches("prod@b", "prod@b"));
        assert!(!hba_database_matches("prod@b", "prod@c"));
        assert!(!hba_database_matches("prod@b", "prod"));
    }

    #[test]
    fn hba_all_matches_everything() {
        assert!(hba_database_matches("all", "prod@b"));
        assert!(hba_database_matches("all", ""));
    }

    #[test]
    fn tenant_is_the_base_unless_the_full_value_is_known() {
        let known = |t: &str| t == "acme" || t == "acme@b";
        assert_eq!(
            tenant_from_startup(Some("acme@b"), Some("u"), known),
            Some("acme@b")
        );
        assert_eq!(
            tenant_from_startup(Some("acme@c"), Some("u"), known),
            Some("acme")
        );
        assert_eq!(tenant_from_startup(Some("acme"), None, known), Some("acme"));
        // No database: the user name, as PostgreSQL does.
        assert_eq!(
            tenant_from_startup(None, Some("acme@x"), known),
            Some("acme")
        );
        assert_eq!(tenant_from_startup(None, None, known), None);
    }
}
