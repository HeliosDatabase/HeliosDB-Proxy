//! Extended-protocol `SET` tracking (Sprinter d3a4c04c28be).
//!
//! JDBC, Npgsql and tokio-postgres send a `SET` as its own extended-protocol
//! cycle: Parse, Bind, Execute, Sync. Such a cycle is tracked like a
//! simple-protocol `SET` (offloaded reads mirror it, transaction replay
//! restores it) when it is a *lone* SET: exactly one Bind and one Execute of
//! the same portal, of a SET-like statement, idle before and after, not split
//! by a Flush. Any other cycle that binds a SET-like statement, or after which
//! the proxy's record of the session's prepared statements may no longer match
//! the backend's, keeps the session's reads on its home node.

/// What a cycle boundary means for the session's tracked settings.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum ExtSetDecision {
    /// Nothing changed that the proxy needs to know about.
    Nothing,
    /// A lone SET-like statement ran: track it like the simple protocol.
    Track(String),
    /// The proxy cannot vouch for the session's settings: keep its reads home.
    Pin,
}

/// A Flush or Sync boundary of the cycle, with what the response showed.
#[derive(Debug, Clone, Copy)]
pub(super) struct ExtSetBoundary {
    /// A Sync (else a Flush).
    pub(super) sync: bool,
    /// The boundary's batch reached a backend.
    pub(super) forwarded: bool,
    /// The transaction status before the cycle and in its ReadyForQuery
    /// (Sync only).
    pub(super) idle_before: bool,
    pub(super) idle_after: bool,
    /// The response carried an error (Sync only).
    pub(super) failed: bool,
}

/// One extended-protocol cycle (Sync to Sync), as far as SETs are concerned.
#[derive(Debug, Default)]
pub(super) struct ExtSetCycle {
    binds: u32,
    execs: u32,
    /// The SET-like statement the cycle's first Bind bound, and that Bind's
    /// portal.
    set: Option<(String, Vec<u8>)>,
    /// The cycle is not a lone SET: a SET-like statement bound together with
    /// another statement, an Execute of a portal other than the bound one,
    /// or a Bind of a statement the proxy has no record of.
    mixed: bool,
    /// The cycle Parsed a named statement that is SET-like or replaced one:
    /// if the Parse failed (the backend keeps the old definition) or never
    /// reached the backend, the proxy's record of it is wrong.
    named_set_parsed: bool,
    /// Likewise for the unnamed statement; a failed unnamed Parse leaves no
    /// unnamed statement behind, so only a cycle that never reached the
    /// backend matters.
    unnamed_set_parsed: bool,
    /// A Flush sent part of the cycle already.
    flushed: bool,
}

impl ExtSetCycle {
    /// A Parse: `named` statement or the unnamed one; `set_like` its new
    /// SQL; `replaced_set_like` the definition it replaces.
    pub(super) fn on_parse(&mut self, named: bool, set_like: bool, replaced_set_like: bool) {
        if set_like || replaced_set_like {
            if named {
                self.named_set_parsed = true;
            } else {
                self.unnamed_set_parsed = true;
            }
        }
    }

    /// A Bind of `portal` to a statement that is SET-like (`Some(Some(sql))`),
    /// another known statement (`Some(None)`), or one the proxy has no record
    /// of (`None`).
    pub(super) fn on_bind(&mut self, portal: &[u8], bound: Option<Option<String>>) {
        self.binds += 1;
        match bound {
            None => self.mixed = true,
            Some(Some(sql)) if self.binds == 1 => self.set = Some((sql, portal.to_vec())),
            Some(Some(_)) => self.mixed = true,
            Some(None) if self.set.is_some() => self.mixed = true,
            Some(None) => {}
        }
    }

    /// An Execute of `portal`.
    pub(super) fn on_execute(&mut self, portal: &[u8]) {
        self.execs += 1;
        if self.set.as_ref().is_some_and(|(_, p)| p != portal) {
            self.mixed = true;
        }
    }

    /// The cycle reached a Flush or Sync boundary. A Sync ends the cycle.
    pub(super) fn at_boundary(&mut self, b: ExtSetBoundary) -> ExtSetDecision {
        let binds_set = self.set.is_some() || self.mixed;
        let decision = if !b.sync {
            // A cycle split by a Flush is never a lone SET, and the SET it
            // carries may already be running.
            if (b.forwarded && binds_set)
                || (!b.forwarded && (self.named_set_parsed || self.unnamed_set_parsed))
            {
                ExtSetDecision::Pin
            } else {
                ExtSetDecision::Nothing
            }
        } else if (self.named_set_parsed && (!b.forwarded || b.failed))
            || (self.unnamed_set_parsed && !b.forwarded)
        {
            ExtSetDecision::Pin
        } else if !b.forwarded || !binds_set {
            ExtSetDecision::Nothing
        } else if !self.mixed
            && self.binds == 1
            && self.execs == 1
            && !self.flushed
            && b.idle_before
            && b.idle_after
        {
            match self.set.take() {
                Some((sql, _)) if !b.failed => ExtSetDecision::Track(sql),
                _ => ExtSetDecision::Nothing,
            }
        } else {
            ExtSetDecision::Pin
        };
        if b.sync {
            *self = Self::default();
        } else {
            self.flushed = true;
        }
        decision
    }
}
