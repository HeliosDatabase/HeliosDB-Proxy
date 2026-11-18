//! PostgreSQL's legacy `FunctionCall` message (`F`, the fastpath interface):
//! which calls the proxy forwards (`function_call` in `proxy.toml`) and how.
//!
//! A `FunctionCall` names a function by OID and carries binary arguments. It
//! is answered like a simple `Query`: an optional `ErrorResponse` or
//! `FunctionCallResponse`, then `ReadyForQuery`. The client loop therefore
//! runs it through the simple-query request arm (cycle and COPY bookkeeping,
//! the tenant's active-query permit, fault recovery, pooling) as a request
//! whose SQL text is unknown, and this module supplies what differs: the
//! frame check, the decision by OID, and the forward itself.

use super::*;
use crate::config::FunctionCallMode;

/// PostgreSQL's built-in large-object functions a client reaches through the
/// fastpath interface (libpq `lo_*`, JDBC `LargeObject`) that write nothing:
/// `lo_open`, `lo_close`, `loread`, `lo_lseek`, `lo_tell`, `lo_lseek64`,
/// `lo_tell64`. Catalog OIDs (`pg_proc.dat`), the same in every PostgreSQL
/// release.
const LARGE_OBJECT_DESCRIPTOR_OIDS: [u32; 7] = [952, 953, 954, 956, 958, 3170, 3171];

/// The ones that change large-object data: `lo_create`, `lowrite`,
/// `lo_creat`, `lo_unlink`, `lo_truncate`, `lo_truncate64`.
const LARGE_OBJECT_WRITE_OIDS: [u32; 6] = [715, 955, 957, 964, 1004, 3172];

/// What the proxy does with one `FunctionCall`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FunctionCallClass {
    /// Answered `0A000` before any backend.
    Refused,
    /// A large-object call that changes nothing outside its transaction's
    /// descriptors.
    Descriptor,
    /// A large-object call that changes large-object data.
    LargeObjectWrite,
    /// Any other function (`function_call = "any"`): it may write any table,
    /// change any setting, take any lock.
    Unknown,
}

impl FunctionCallClass {
    /// The decision for function `oid` under `mode`.
    pub(super) fn of(mode: FunctionCallMode, oid: u32) -> Self {
        match mode {
            FunctionCallMode::Refuse => Self::Refused,
            FunctionCallMode::Any => Self::Unknown,
            FunctionCallMode::LargeObjects => {
                if LARGE_OBJECT_DESCRIPTOR_OIDS.contains(&oid) {
                    Self::Descriptor
                } else if LARGE_OBJECT_WRITE_OIDS.contains(&oid) {
                    Self::LargeObjectWrite
                } else {
                    Self::Refused
                }
            }
        }
    }

    /// The decision for a `FunctionCall` message under `mode`; a message
    /// that is not a well-formed `FunctionCall` is refused.
    pub(super) fn of_message(mode: FunctionCallMode, msg: &Message) -> Self {
        function_call_oid(&msg.payload).map_or(Self::Refused, |oid| Self::of(mode, oid))
    }
}

/// The function OID of a `FunctionCall` payload, when the whole payload is
/// well formed: the OID, the argument format codes, the arguments (each a
/// length, `-1` for NULL, and that many bytes) and the result format code,
/// with nothing after it.
pub(super) fn function_call_oid(payload: &[u8]) -> Option<u32> {
    fn int16(rest: &mut &[u8]) -> Option<u16> {
        let (head, tail) = rest.split_first_chunk::<2>()?;
        *rest = tail;
        Some(u16::from_be_bytes(*head))
    }
    fn int32(rest: &mut &[u8]) -> Option<u32> {
        let (head, tail) = rest.split_first_chunk::<4>()?;
        *rest = tail;
        Some(u32::from_be_bytes(*head))
    }
    let mut rest = payload;
    let oid = int32(&mut rest)?;
    let formats = usize::from(int16(&mut rest)?);
    rest = rest.get(formats.checked_mul(2)?..)?;
    for _ in 0..int16(&mut rest)? {
        // A length of -1 is a NULL argument with no bytes.
        let len = int32(&mut rest)? as i32;
        if len >= 0 {
            rest = rest.get(len as usize..)?;
        } else if len != -1 {
            return None;
        }
    }
    int16(&mut rest)?;
    rest.is_empty().then_some(oid)
}

impl ProxyServer {
    /// The SQL text of a simple-protocol request: a `Query`'s, when it is
    /// UTF-8. A `FunctionCall` has none (its payload is binary).
    pub(super) fn simple_request_sql(msg: &Message) -> Option<&str> {
        (msg.msg_type == MessageType::Query)
            .then(|| crate::protocol::query_text(&msg.payload))
            .flatten()
    }

    /// Forward a `FunctionCall` the client loop validated and stream its
    /// response to the client, ending at ReadyForQuery. Returns like
    /// [`Self::forward_simple_query`]: `(Some(node), bytes)` once a backend
    /// answered, `(None, bytes)` when the call was refused before any
    /// backend (the configured mode, the rate limiter, an open circuit), and
    /// `Err` with `fwd.fault` filled on a backend fault.
    ///
    /// The call always runs on the primary, or on the connection holding the
    /// cycle it continues. Once it is delivered its transaction is never
    /// replayed (the journal holds no text for it) and an explicit
    /// transaction's journal is marked incomplete; a call that may have
    /// written retires every cached result.
    pub(super) async fn forward_function_call(
        fwd: ForwardCtx<'_>,
        msg: &Message,
    ) -> Result<(Option<String>, u64)> {
        let ForwardCtx {
            client,
            conns,
            current_node,
            reads,
            session,
            state,
            config,
            fault,
            continues_cycle,
        } = fwd;
        let class = FunctionCallClass::of_message(config.function_call, msg);
        if class == FunctionCallClass::Refused {
            let reason = match config.function_call {
                FunctionCallMode::Refuse => {
                    "legacy FunctionCall messages are refused (function_call = \"refuse\"); use SQL instead"
                }
                _ => {
                    "this function cannot be called through the legacy FunctionCall message \
                     (function_call = \"large-objects\" forwards the large-object functions only); \
                     use SQL instead"
                }
            };
            return Self::answer_before_backend(
                client,
                session,
                Self::create_error_response("0A000", reason),
            )
            .await;
        }
        // Without the limiter nothing charges a continuation.
        #[cfg(not(feature = "rate-limiting"))]
        let _ = continues_cycle.map(|cycle| cycle.runs);
        // As for a simple Query: a new request is checked, one continuing a
        // cycle the backend holds open is paced.
        #[cfg(feature = "rate-limiting")]
        let limited = match continues_cycle {
            None => Self::rate_limit_check(session, state, config, || false).await,
            Some(cycle) => {
                if cycle.runs {
                    Self::rate_limit_pace(session, state, config).await;
                }
                None
            }
        };
        #[cfg(feature = "rate-limiting")]
        if let Some(resp) = limited {
            return Self::answer_before_backend(client, session, resp).await;
        }

        #[cfg(feature = "lag-routing")]
        if config.lag_routing.enabled {
            *session.last_write_at.write().await = Some(std::time::Instant::now());
        }
        reads.note_write(session);
        let target = match continues_cycle {
            Some(cycle) => {
                if let Some(e) = cycle.lost(conns, fault) {
                    return Err(e);
                }
                cycle.node.to_string()
            }
            None => {
                Self::choose_target_node(
                    true,
                    None,
                    current_node,
                    ReadRoute::Stay,
                    session,
                    state,
                    config,
                )
                .await?
                .0
            }
        };
        tracing::debug!(target: "helios::routing", node = %target, ?class, "routed function call");

        // A half-open circuit's probe holds its slot until this call is done.
        #[cfg(feature = "circuit-breaker")]
        let _probe = match continues_cycle {
            Some(_) => None,
            None => match Self::circuit_admit(state, &target) {
                Ok(probe) => probe,
                Err(resp) => return Self::answer_before_backend(client, session, resp).await,
            },
        };

        if let Err(e) = Self::ensure_conn(conns, &target, session, config, state).await {
            Self::record_backend_failure(state, &target, &e.to_string());
            BackendFault::set(fault, &target, FaultPhase::NotDelivered, &e);
            return Err(e);
        }
        let backend = conns.get_mut(&target).expect("just ensured");
        let unknown = class == FunctionCallClass::Unknown;
        // What an arbitrary function left on the connection is not known.
        #[cfg(feature = "pool-modes")]
        if unknown {
            backend.dirty = true;
        }
        let backend_err = match tokio::time::timeout(
            state.limits.backend_write_timeout,
            backend.stream.write_all(&msg.encode()),
        )
        .await
        {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(format!("Backend write error: {}", e)),
            Err(_) => Some("Backend write timeout".to_string()),
        };
        if let Some(msg) = backend_err {
            let e = ProxyError::Network(msg);
            Self::record_backend_failure(state, &target, &e.to_string());
            BackendFault::set(fault, &target, FaultPhase::NotDelivered, &e);
            Self::drop_undelivered(conns, &target, continues_cycle.is_some(), session, state).await;
            return Err(e);
        }

        // Delivered: the journal has no text to run again.
        session.tr_replay_tainted.store(true, Ordering::Relaxed);
        let writes = unknown || class == FunctionCallClass::LargeObjectWrite;
        if writes && (config.tr_enabled || Self::cache_needs_capture(state)) {
            session
                .journal
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .mark_open_incomplete(
                    "a legacy FunctionCall's effects are not captured by the journal",
                );
        }
        // An arbitrary function may have written any table: every cached
        // result is retired at its ReadyForQuery, and again at the commit of
        // a transaction it leaves open (`journal_observe_sync`).
        #[cfg(feature = "query-cache")]
        if unknown && state.table_generations.is_some() {
            session.cache_unseen_write.store(true, Ordering::Relaxed);
        }
        if unknown {
            session.reads_pinned.store(true, Ordering::Relaxed);
        }

        match Self::stream_until_ready_buffered(
            client,
            &mut backend.stream,
            &mut backend.response_buf,
            session,
            state,
            0,
        )
        .await
        {
            Ok(sent) => {
                #[cfg(feature = "circuit-breaker")]
                Self::circuit_record(state, &target, true, "");
                // The edge cache follows the journal's commits; a call
                // outside a transaction has none.
                #[cfg(feature = "edge-proxy")]
                if unknown && config.edge.enabled {
                    Self::edge_invalidate_write(state, config, Vec::new()).await;
                }
                Ok((Some(target), sent))
            }
            Err(e) => {
                Self::record_backend_failure(state, &target, &e.to_string());
                BackendFault::set_response(fault, &target, &e);
                Self::abandon_conn(conns, &target, session, state).await;
                Err(e.error)
            }
        }
    }

    /// Answer a request before any backend with `response` (an
    /// ErrorResponse) and ReadyForQuery carrying the backend's own
    /// transaction status, the last one relayed: nothing reached it, so a
    /// transaction open there stays 'T' or 'E'.
    async fn answer_before_backend(
        client: &mut ClientStream,
        session: &ClientSession,
        mut response: Vec<u8>,
    ) -> Result<(Option<String>, u64)> {
        response.extend_from_slice(&Self::create_ready_for_query(
            session.last_rfq_status.load(Ordering::Relaxed),
        ));
        client
            .write_all(&response)
            .await
            .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
        Ok((None, response.len() as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `FunctionCall` payload: `oid`, one binary format code, `args`, a
    /// binary result.
    fn payload(oid: u32, args: &[Option<&[u8]>]) -> Vec<u8> {
        let mut p = oid.to_be_bytes().to_vec();
        p.extend_from_slice(&1u16.to_be_bytes());
        p.extend_from_slice(&1u16.to_be_bytes());
        p.extend_from_slice(&(args.len() as u16).to_be_bytes());
        for arg in args {
            match arg {
                Some(bytes) => {
                    p.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                    p.extend_from_slice(bytes);
                }
                None => p.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        p.extend_from_slice(&1u16.to_be_bytes());
        p
    }

    #[test]
    fn a_well_formed_payload_yields_its_oid() {
        assert_eq!(function_call_oid(&payload(952, &[])), Some(952));
        let args: [Option<&[u8]>; 3] = [Some(&[0, 0, 0, 7]), None, Some(&[])];
        assert_eq!(function_call_oid(&payload(955, &args)), Some(955));
        // No format codes at all (every argument is text).
        assert_eq!(
            function_call_oid(&[0, 0, 3, 184, 0, 0, 0, 0, 0, 0]),
            Some(952)
        );
    }

    #[test]
    fn a_malformed_payload_yields_none() {
        let good = payload(955, &[Some(&[1, 2, 3]), None]);
        for cut in 0..good.len() {
            assert_eq!(function_call_oid(&good[..cut]), None, "cut at {cut}");
        }
        let mut trailing = good.clone();
        trailing.push(0);
        assert_eq!(function_call_oid(&trailing), None);
        // An argument length below -1.
        let mut negative = payload(955, &[None]);
        let at = negative.len() - 6;
        negative[at..at + 4].copy_from_slice(&(-2i32).to_be_bytes());
        assert_eq!(function_call_oid(&negative), None);
        // An argument longer than the payload.
        let mut long = payload(955, &[Some(&[1])]);
        let at = long.len() - 7;
        long[at..at + 4].copy_from_slice(&u32::MAX.to_be_bytes()[..]);
        assert_eq!(function_call_oid(&long), None);
    }

    #[test]
    fn the_mode_decides_by_oid() {
        use FunctionCallClass::*;
        for oid in LARGE_OBJECT_DESCRIPTOR_OIDS {
            assert_eq!(
                FunctionCallClass::of(FunctionCallMode::LargeObjects, oid),
                Descriptor
            );
        }
        for oid in LARGE_OBJECT_WRITE_OIDS {
            assert_eq!(
                FunctionCallClass::of(FunctionCallMode::LargeObjects, oid),
                LargeObjectWrite
            );
        }
        // `lo_import` / `lo_export` read and write server files, and
        // `pg_advisory_lock(bigint)` takes a session lock.
        for oid in [764, 765, 2880, 0, u32::MAX] {
            assert_eq!(
                FunctionCallClass::of(FunctionCallMode::LargeObjects, oid),
                Refused
            );
            assert_eq!(FunctionCallClass::of(FunctionCallMode::Any, oid), Unknown);
        }
        for oid in [952, 955, 2880] {
            assert_eq!(
                FunctionCallClass::of(FunctionCallMode::Refuse, oid),
                Refused
            );
        }
        assert!(LARGE_OBJECT_DESCRIPTOR_OIDS
            .iter()
            .all(|oid| !LARGE_OBJECT_WRITE_OIDS.contains(oid)));
    }

    #[test]
    fn only_a_query_has_sql_text() {
        let query = Message {
            msg_type: MessageType::Query,
            payload: bytes::BytesMut::from(&b"select 1\0"[..]),
        };
        assert_eq!(ProxyServer::simple_request_sql(&query), Some("select 1"));
        // A FunctionCall of OID 952 starts with a NUL byte, which would read
        // as an empty query.
        let call = Message {
            msg_type: MessageType::FunctionCall,
            payload: bytes::BytesMut::from(&payload(952, &[])[..]),
        };
        assert_eq!(ProxyServer::simple_request_sql(&call), None);
    }
}
