//! The query cache on the extended protocol (`[cache] extended_protocol`).
//!
//! A batch the cache may answer has one of two exact shapes, each ended by
//! `Sync` and run outside a transaction block:
//!
//! - `Parse` (unnamed, the one the promotion holds ahead of the batch),
//!   `Bind`, an optional `Describe` of the portal, `Execute`
//! - `Bind` of a named statement the session prepared earlier, an optional
//!   `Describe` of the portal, `Execute`
//!
//! with the unnamed portal throughout and no row limit on the `Execute`.
//! Anything else (several statements before one `Sync`, a named portal, a
//! row limit, a `Describe` of the statement, a `Close`, a `Bind` of the
//! unnamed statement without its `Parse`) goes to the backend as before.
//!
//! An entry is the backend's whole response to that batch, so it is keyed by
//! everything the bytes depend on beside the statement text and the session
//! scope the simple protocol's entries already carry: which of the two
//! shapes it is and whether the portal is described (the frames present),
//! the parameter types the `Parse` declares, and the `Bind` after its two
//! names: parameter format codes, parameter values, result format codes.
//! Those are hashed into the entry's namespace ([`CacheableBatch::namespace`]),
//! which every shared tier keys on; the per-connection exact-text tier (L1)
//! keys on the text alone and is not used.
//!
//! A hit runs nothing on the backend, but the client was told its `Parse`
//! succeeded: the connection the batch was routed to is given that unnamed
//! statement first when it holds another, so a later `Bind` of it finds it.

use super::*;
use sha2::{Digest, Sha256};

/// A batch the query cache may answer (see the module's shapes).
pub(super) struct CacheableBatch<'a> {
    /// The statement's text.
    pub(super) sql: &'a str,
    /// The signature of the unnamed `Parse` held ahead of the batch; `None`
    /// when the batch binds a named statement.
    pub(super) unnamed_sig: Option<&'a bytes::Bytes>,
    /// Everything of the batch the response depends on beside `sql`.
    identity: Sha256,
}

use super::frames::{next_frontend_frame as frame, split_cstring as cstring};

impl<'a> CacheableBatch<'a> {
    /// The cacheable reading of `batch` (the frames up to and including the
    /// `Sync`), `unnamed` being the unnamed `Parse` held ahead of it and its
    /// signature, and `registry` the session's named statements (their
    /// `Parse` messages). `None` for any other shape.
    pub(super) fn of(
        batch: &'a [u8],
        unnamed: Option<&'a (bytes::Bytes, bytes::Bytes)>,
        registry: &'a HashMap<String, bytes::Bytes>,
    ) -> Option<Self> {
        let mut rest = batch;
        let (b'B', bind) = frame(&mut rest)? else {
            return None;
        };
        let (portal, bind) = cstring(bind)?;
        let (statement, bind_tail) = cstring(bind)?;
        if !portal.is_empty() {
            return None;
        }
        let parse: &[u8] = match unnamed {
            Some((parse, _)) if statement.is_empty() => &parse[..],
            None if !statement.is_empty() => {
                &registry.get(std::str::from_utf8(statement).ok()?)?[..]
            }
            _ => return None,
        };
        let mut next = frame(&mut rest)?;
        let described = next == (b'D', &b"P\0"[..]);
        if described {
            next = frame(&mut rest)?;
        }
        // The unnamed portal, no row limit.
        if next != (b'E', &[0, 0, 0, 0, 0][..]) {
            return None;
        }
        if frame(&mut rest)? != (b'S', &[][..]) || !rest.is_empty() {
            return None;
        }
        // The Parse: its name, the statement text, the declared types.
        let (_, parse_body) = cstring(parse.get(5..)?)?;
        let (sql, types) = cstring(parse_body)?;
        let sql = std::str::from_utf8(sql).ok()?;
        let mut identity = Sha256::new();
        identity.update(b"helios-extended-read-v1");
        identity.update([u8::from(unnamed.is_some()), u8::from(described)]);
        identity.update((types.len() as u64).to_be_bytes());
        identity.update(types);
        identity.update((bind_tail.len() as u64).to_be_bytes());
        identity.update(bind_tail);
        Some(Self {
            sql,
            unnamed_sig: unnamed.map(|(_, sig)| sig),
            identity,
        })
    }

    /// The cache namespace of this batch's entry under the session scope
    /// `scope` (the namespace a simple query of the session would use).
    pub(super) fn namespace(&self, scope: &[u8; 32]) -> [u8; 32] {
        let mut hash = self.identity.clone();
        hash.update(scope);
        hash.finalize().into()
    }
}

/// What the cache made of a batch before it reached the backend.
pub(super) enum ExtendedLookup {
    /// Not a read the cache answers or stores: forward it as any batch.
    Bypass,
    /// Answered from the cache; the bytes written to the client.
    Hit(u64),
    /// The scope proof refused the request and the client was told so (the
    /// bytes written): nothing more reaches the backend for this batch.
    Rejected(u64),
    /// Not cached: forward the batch, capture its response and store it.
    Miss(Box<ExtendedFill>),
}

/// The client and the backend connection its batch was routed to (`node`),
/// idle at a cycle boundary.
pub(super) struct CacheConn<'a> {
    pub(super) client: &'a mut ClientStream,
    pub(super) backend: &'a mut BackendConn,
    pub(super) session: &'a Arc<ClientSession>,
    pub(super) state: &'a Arc<ServerState>,
    pub(super) node: &'a str,
}

/// What a missed lookup carries to the store.
pub(super) struct ExtendedFill {
    context: crate::cache::CacheContext,
    prep: crate::cache::QueryPrep,
    /// The session scope the lookup was keyed under.
    scope: [u8; 32],
}

impl ProxyServer {
    /// Whether the session may have a batch answered by the query cache at
    /// this point, from its state alone: checked before the batch is read
    /// ([`CacheableBatch::of`]), so the batches of a transaction block cost
    /// nothing here.
    pub(super) fn extended_cache_open(
        session: &ClientSession,
        state: &ServerState,
        config: &ProxyConfig,
    ) -> bool {
        state.query_cache.is_some()
            && config.cache.extended_protocol
            && !session.in_transaction.load(Ordering::Relaxed)
            && !session.cache_ineligible.load(Ordering::Relaxed)
    }

    /// Whether the query cache may look up `batch`'s statement at all: the
    /// conditions a simple query's lookup has, none of which reads the
    /// backend.
    pub(super) fn extended_cache_candidate(
        sql: &str,
        session: &ClientSession,
        state: &ServerState,
        config: &ProxyConfig,
    ) -> bool {
        if !Self::extended_cache_open(session, state, config) {
            return false;
        }
        if !(config.cache.trust_backend_read_effects
            || crate::replay_sql::literal_scalar_select(sql))
        {
            return false;
        }
        let mut facts = StmtFacts::new(sql);
        !facts.is_write()
            && facts.is_cacheable_read(&state.volatile_scan)
            && !Self::cache_refuses(state, sql)
    }

    /// Give the backend the unnamed statement `sig` describes (a `Parse`
    /// payload after its empty name) out of band: one `Parse` and `Flush`,
    /// its `ParseComplete` read here.
    async fn establish_unnamed(io: &mut CacheConn<'_>, sig: &bytes::Bytes) -> Result<()> {
        let mut parse = Vec::with_capacity(6 + sig.len());
        parse.push(b'P');
        parse.extend_from_slice(&((4 + 1 + sig.len()) as u32).to_be_bytes());
        parse.push(0);
        parse.extend_from_slice(sig);
        Self::finish_pending_backend_frames(io.client, io.backend, io.session, io.state).await?;
        Self::reprepare_statement(
            &mut io.backend.stream,
            &parse,
            io.state.limits.reprepare_timeout,
            io.state.limits.max_backend_frame_bytes,
        )
        .await?;
        io.backend.unnamed_sig = Some(sig.clone());
        Ok(())
    }

    /// The session scope of the backend for a cached read
    /// ([`Self::cache_backend_namespace`]), leaving the client's unnamed
    /// statement as it found it: the proof may run a simple Query, which
    /// destroys it, and `keep` (the signature recorded before) is then
    /// established again.
    async fn extended_scope(
        io: &mut CacheConn<'_>,
        keep: Option<&bytes::Bytes>,
    ) -> Result<super::cache_scope::BackendCacheScope> {
        let scope =
            Self::cache_backend_namespace(io.client, io.backend, io.session, io.state, io.node)
                .await?;
        if let Some(sig) = keep {
            if io.backend.unnamed_sig.is_none()
                && !matches!(
                    scope,
                    super::cache_scope::BackendCacheScope::RejectedLost(_)
                )
            {
                Self::establish_unnamed(io, sig).await?;
            }
        }
        Ok(scope)
    }

    /// Look `plan` up before its batch reaches the backend. `registered`:
    /// the batch and the unnamed `Parse` held ahead of it, when the journal
    /// capture follows the session's batches. `Err` is a failure of the
    /// backend connection before anything of the batch was delivered.
    pub(super) async fn extended_cache_lookup(
        io: &mut CacheConn<'_>,
        plan: &CacheableBatch<'_>,
        registered: Option<(&[u8], Option<&[u8]>)>,
    ) -> Result<ExtendedLookup> {
        use super::cache_scope::BackendCacheScope;
        let Some(cache) = io.state.query_cache.as_ref() else {
            return Ok(ExtendedLookup::Bypass);
        };
        // The proof may destroy the connection's unnamed statement. The
        // batch's own unnamed Parse replaces it anyway; a batch of a named
        // statement must leave it, which needs its signature.
        if plan.unnamed_sig.is_none() && io.backend.unnamed_untracked {
            return Ok(ExtendedLookup::Bypass);
        }
        let keep = match plan.unnamed_sig {
            Some(_) => None,
            None => io.backend.unnamed_sig.clone(),
        };
        let scope = match Self::extended_scope(io, keep.as_ref()).await? {
            BackendCacheScope::Verified(scope) => scope,
            BackendCacheScope::Unsupported => return Ok(ExtendedLookup::Bypass),
            BackendCacheScope::Rejected(response) => {
                // The batch never reached the backend: this is its whole
                // answer (an error, then ReadyForQuery).
                tokio::time::timeout(
                    io.state.limits.client_write_timeout,
                    io.client.write_all(&response),
                )
                .await
                .map_err(|_| ProxyError::Network("Client write timeout".to_string()))?
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
                Self::tally_frames(io.session, &response);
                Self::note_ready_for_query(io.session, b'I', true);
                return Ok(ExtendedLookup::Rejected(response.len() as u64));
            }
            BackendCacheScope::RejectedLost(response) => {
                let _ = tokio::time::timeout(
                    io.state.limits.client_write_timeout,
                    io.client.write_all(&response),
                )
                .await;
                return Err(ProxyError::Connection(
                    "backend scope rejected the request before its response boundary was lost"
                        .to_string(),
                ));
            }
        };
        let Some(mut context) = Self::cacheable_read_ctx(io.session, true).await else {
            return Ok(ExtendedLookup::Bypass);
        };
        // The exact-text tier keys on the statement text alone.
        context.connection_id = None;
        let namespace = plan.namespace(&scope);
        let (lookup, prep) = cache.get_scoped(plan.sql, None, &context, &namespace).await;
        let crate::cache::CacheLookup::Hit { result, level } = lookup else {
            return Ok(if prep.storable() {
                ExtendedLookup::Miss(Box::new(ExtendedFill {
                    context,
                    prep,
                    scope,
                }))
            } else {
                ExtendedLookup::Bypass
            });
        };
        tracing::debug!(target: "helios::cache", level = %level, "cache hit (extended protocol)");
        // The client's unnamed statement is now this batch's.
        if let Some(sig) = plan.unnamed_sig {
            if io.backend.unnamed_sig.as_ref() != Some(sig) {
                Self::establish_unnamed(io, sig).await?;
            }
            io.backend.unnamed_untracked = false;
        }
        // The capture follows the session's statements and portals through
        // the batch as if the backend had run it (a read: nothing is armed).
        if let Some((batch, held)) = registered {
            Self::journal_register_batch(io.session, batch, held, true, false);
        }
        io.client
            .write_all(&result.data)
            .await
            .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
        Self::tally_frames(io.session, &result.data);
        Self::note_ready_for_query(io.session, b'I', false);
        Self::journal_observe_status(io.session, io.state).await;
        Ok(ExtendedLookup::Hit(result.data.len() as u64))
    }

    /// Store a missed batch's response (`body`, complete and error-free)
    /// once the backend's scope is proven unchanged since the lookup. `Err`:
    /// the connection was lost to the proof (the client has its answer).
    pub(super) async fn extended_cache_store(
        io: &mut CacheConn<'_>,
        plan: &CacheableBatch<'_>,
        fill: &ExtendedFill,
        body: bytes::Bytes,
        rows: usize,
    ) -> Result<()> {
        let Some(cache) = io.state.query_cache.as_ref() else {
            return Ok(());
        };
        // The proof may destroy the unnamed statement the batch left.
        if io.backend.unnamed_untracked {
            return Ok(());
        }
        let keep = io.backend.unnamed_sig.clone();
        match Self::extended_scope(io, keep.as_ref()).await? {
            super::cache_scope::BackendCacheScope::Verified(scope) if scope == fill.scope => {
                cache
                    .put_prepared(
                        plan.sql,
                        &fill.context,
                        &fill.prep,
                        body,
                        rows,
                        Duration::ZERO,
                    )
                    .await;
                Ok(())
            }
            super::cache_scope::BackendCacheScope::RejectedLost(_) => Err(ProxyError::Connection(
                "backend scope proof lost its response boundary".to_string(),
            )),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wire(tag: u8, payload: &[u8]) -> Vec<u8> {
        let mut frame = vec![tag];
        frame.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    fn parse(name: &str, sql: &str, types: &[u32]) -> bytes::Bytes {
        let mut p = name.as_bytes().to_vec();
        p.push(0);
        p.extend_from_slice(sql.as_bytes());
        p.push(0);
        p.extend_from_slice(&(types.len() as u16).to_be_bytes());
        for oid in types {
            p.extend_from_slice(&oid.to_be_bytes());
        }
        bytes::Bytes::from(wire(b'P', &p))
    }

    /// A Bind of `statement` into `portal`: one parameter in `format` with
    /// `value`, one result column in `result`.
    fn bind(portal: &str, statement: &str, format: u16, value: &[u8], result: u16) -> Vec<u8> {
        let mut p = portal.as_bytes().to_vec();
        p.push(0);
        p.extend_from_slice(statement.as_bytes());
        p.push(0);
        p.extend_from_slice(&1u16.to_be_bytes());
        p.extend_from_slice(&format.to_be_bytes());
        p.extend_from_slice(&1u16.to_be_bytes());
        p.extend_from_slice(&(value.len() as u32).to_be_bytes());
        p.extend_from_slice(value);
        p.extend_from_slice(&1u16.to_be_bytes());
        p.extend_from_slice(&result.to_be_bytes());
        wire(b'B', &p)
    }

    fn execute(portal: &str, max_rows: u32) -> Vec<u8> {
        let mut p = portal.as_bytes().to_vec();
        p.push(0);
        p.extend_from_slice(&max_rows.to_be_bytes());
        wire(b'E', &p)
    }

    fn describe(kind: u8, name: &str) -> Vec<u8> {
        let mut p = vec![kind];
        p.extend_from_slice(name.as_bytes());
        p.push(0);
        wire(b'D', &p)
    }

    fn sync() -> Vec<u8> {
        wire(b'S', &[])
    }

    const SQL: &str = "SELECT v FROM kv WHERE id = $1";

    fn held(sql: &str, types: &[u32]) -> (bytes::Bytes, bytes::Bytes) {
        let msg = parse("", sql, types);
        let sig = msg.slice(6..);
        (msg, sig)
    }

    fn registry() -> HashMap<String, bytes::Bytes> {
        HashMap::from([("s1".to_string(), parse("s1", SQL, &[23]))])
    }

    #[test]
    fn the_two_shapes_are_cacheable() {
        let unnamed = held(SQL, &[23]);
        let registry = registry();
        for described in [false, true] {
            let mut tail = Vec::new();
            if described {
                tail.extend(describe(b'P', ""));
            }
            tail.extend(execute("", 0));
            tail.extend(sync());
            let batch = [bind("", "", 0, b"7", 0), tail.clone()].concat();
            let plan = CacheableBatch::of(&batch, Some(&unnamed), &registry).expect("unnamed");
            assert_eq!(plan.sql, SQL);
            assert_eq!(plan.unnamed_sig, Some(&unnamed.1));
            let batch = [bind("", "s1", 0, b"7", 0), tail].concat();
            let plan = CacheableBatch::of(&batch, None, &registry).expect("named");
            assert_eq!(plan.sql, SQL);
            assert_eq!(plan.unnamed_sig, None);
        }
    }

    #[test]
    fn any_other_shape_is_not() {
        let unnamed = held(SQL, &[23]);
        let registry = registry();
        let b = || bind("", "s1", 0, b"7", 0);
        let cases: Vec<(&str, Vec<u8>, bool)> = vec![
            (
                "a named portal",
                [bind("p", "s1", 0, b"7", 0), execute("p", 0), sync()].concat(),
                false,
            ),
            (
                "a row limit",
                [b(), execute("", 10), sync()].concat(),
                false,
            ),
            (
                "an unknown statement",
                [bind("", "s2", 0, b"7", 0), execute("", 0), sync()].concat(),
                false,
            ),
            (
                "the unnamed statement without its Parse",
                [bind("", "", 0, b"7", 0), execute("", 0), sync()].concat(),
                false,
            ),
            (
                "a named statement behind a held Parse",
                [b(), execute("", 0), sync()].concat(),
                true,
            ),
            (
                "a Describe of the statement",
                [b(), describe(b'S', "s1"), execute("", 0), sync()].concat(),
                false,
            ),
            (
                "a Describe of another portal",
                [b(), describe(b'P', "p"), execute("", 0), sync()].concat(),
                false,
            ),
            (
                "two executions",
                [b(), execute("", 0), b(), execute("", 0), sync()].concat(),
                false,
            ),
            (
                "a Close",
                [b(), execute("", 0), wire(b'C', b"P\0"), sync()].concat(),
                false,
            ),
            ("no Sync", [b(), execute("", 0)].concat(), false),
            (
                "a Flush",
                [b(), execute("", 0), wire(b'H', &[])].concat(),
                false,
            ),
            (
                "bytes after the Sync",
                [b(), execute("", 0), sync(), sync()].concat(),
                false,
            ),
            (
                "a Parse in the batch",
                [parse("s3", SQL, &[]).to_vec(), b(), execute("", 0), sync()].concat(),
                false,
            ),
            ("no Execute", [b(), sync()].concat(), false),
            ("nothing", Vec::new(), false),
        ];
        for (what, batch, with_held) in cases {
            let held = with_held.then_some(&unnamed);
            assert!(
                CacheableBatch::of(&batch, held, &registry).is_none(),
                "{what}"
            );
        }
        // Cut anywhere, the batch is not one.
        let whole = [b(), describe(b'P', ""), execute("", 0), sync()].concat();
        assert!(CacheableBatch::of(&whole, None, &registry).is_some());
        for cut in 0..whole.len() {
            assert!(
                CacheableBatch::of(&whole[..cut], None, &registry).is_none(),
                "{cut}"
            );
        }
    }

    /// Everything the response bytes depend on separates two entries.
    #[test]
    fn the_namespace_separates_what_the_response_depends_on() {
        let registry = HashMap::from([
            ("s1".to_string(), parse("s1", SQL, &[23])),
            ("same".to_string(), parse("same", SQL, &[23])),
            ("typed".to_string(), parse("typed", SQL, &[20])),
            ("untyped".to_string(), parse("untyped", SQL, &[])),
        ]);
        let unnamed = held(SQL, &[23]);
        let scope = [7u8; 32];
        let tail = [execute("", 0), sync()].concat();
        let named = |statement: &str, format: u16, value: &[u8], result: u16, described: bool| {
            let mut batch = bind("", statement, format, value, result);
            if described {
                batch.extend(describe(b'P', ""));
            }
            batch.extend(&tail);
            CacheableBatch::of(&batch, None, &registry)
                .expect("cacheable")
                .namespace(&scope)
        };
        let base = named("s1", 0, b"7", 0, false);
        // The statement's name is not part of the response.
        assert_eq!(base, named("same", 0, b"7", 0, false));
        let mut seen = vec![base];
        for (what, namespace) in [
            ("the parameter value", named("s1", 0, b"8", 0, false)),
            ("the parameter format", named("s1", 1, b"7", 0, false)),
            ("the result format", named("s1", 0, b"7", 1, false)),
            ("the declared type", named("typed", 0, b"7", 0, false)),
            ("no declared type", named("untyped", 0, b"7", 0, false)),
            ("the portal's description", named("s1", 0, b"7", 0, true)),
            (
                "a binary value of the same bytes",
                named("s1", 1, b"8", 0, false),
            ),
            ("a longer value", named("s1", 0, b"77", 0, false)),
        ] {
            assert!(!seen.contains(&namespace), "{what}");
            seen.push(namespace);
        }
        // The unnamed shape's response opens with ParseComplete.
        let batch = [bind("", "", 0, b"7", 0), tail.clone()].concat();
        let with_parse = CacheableBatch::of(&batch, Some(&unnamed), &registry)
            .expect("cacheable")
            .namespace(&scope);
        assert!(!seen.contains(&with_parse));
        // Another session scope is another entry, and never the scope itself.
        let other = CacheableBatch::of(&batch, Some(&unnamed), &registry)
            .expect("cacheable")
            .namespace(&[8u8; 32]);
        assert_ne!(with_parse, other);
        assert_ne!(with_parse, scope);
    }
}
