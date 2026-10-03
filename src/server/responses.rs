use super::*;

impl ProxyServer {
    /// Create PostgreSQL error response message
    pub(super) fn create_error_response(code: &str, message: &str) -> Vec<u8> {
        Self::create_severity_response("ERROR", code, message)
    }

    /// Create a PostgreSQL `ErrorResponse` with `FATAL` severity — the severity
    /// PostgreSQL itself uses for a connection it is about to close (e.g.
    /// `53300 too_many_connections`, `57P05 idle_session_timeout`). Drivers
    /// (pgx, npgsql, JDBC) key on `FATAL` to mark the connection dead; with
    /// `ERROR` they try to keep using it and then hit a bare EOF.
    pub(super) fn create_fatal_response(code: &str, message: &str) -> Vec<u8> {
        Self::create_severity_response("FATAL", code, message)
    }

    pub(super) fn create_severity_response(severity: &str, code: &str, message: &str) -> Vec<u8> {
        // One allocation, no HashMap + four Strings per error frame (O-05).
        // Framing is byte-identical to `ErrorResponse { fields }.encode()
        // .encode()`: 'E' tag, u32 length (payload + 4), fields S/V/C/M each
        // followed by NUL, then the payload terminator.
        let mut payload = Vec::with_capacity(16 + severity.len() * 2 + code.len() + message.len());
        for (field, value) in [
            ('S', severity),
            ('V', severity),
            ('C', code),
            ('M', message),
        ] {
            payload.push(field as u8);
            payload.extend_from_slice(value.as_bytes());
            payload.push(0);
        }
        payload.push(0);

        let mut frame = Vec::with_capacity(payload.len() + 5);
        frame.push(b'E');
        frame.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
        frame.extend_from_slice(&payload);
        frame
    }

    /// Create a `ReadyForQuery` frame with the given transaction-status byte
    /// (`b'I'` = idle, `b'T'` = in transaction, `b'E'` = failed transaction).
    pub(super) fn create_ready_for_query(status: u8) -> Vec<u8> {
        let mut payload = BytesMut::with_capacity(1);
        payload.put_u8(status);
        Message::new(MessageType::ReadyForQuery, payload)
            .encode()
            .to_vec()
    }

    /// Synthesise a full PostgreSQL simple-query response from a cached
    /// payload produced by a plugin's `PreQueryResult::Cached`.
    ///
    /// # Payload format
    ///
    /// The plugin is expected to serialise a JSON document of the form:
    ///
    /// ```json
    /// {
    ///   "columns": [
    ///     {"name": "id",    "oid": 23},
    ///     {"name": "email", "oid": 25}
    ///   ],
    ///   "rows": [
    ///     ["1", "alice@example.com"],
    ///     ["2", null]
    ///   ]
    /// }
    /// ```
    ///
    /// `oid` is the PostgreSQL type OID (`23` = int4, `25` = text,
    /// `20` = int8, `16` = bool, `1184` = timestamptz, etc.). Row values
    /// are strings in text format; `null` encodes a SQL NULL. The type
    /// OID is advisory — pgwire clients accept `25` (text) universally
    /// and cast as needed.
    ///
    /// # Returned bytes
    ///
    /// One concatenated PostgreSQL wire response:
    ///
    /// ```text
    /// RowDescription (T) + DataRow (D) × N + CommandComplete (C: "SELECT N")
    ///                    + ReadyForQuery (Z: idle)
    /// ```
    ///
    /// Returns an error on malformed JSON; the caller falls back to
    /// backend forwarding.
    #[cfg(feature = "wasm-plugins")]
    pub(super) fn synthesise_cached_response(bytes: &[u8]) -> Result<Vec<u8>> {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct CachedPayload {
            columns: Vec<ColumnDef>,
            rows: Vec<Vec<Option<String>>>,
        }

        #[derive(Deserialize)]
        struct ColumnDef {
            name: String,
            #[serde(default = "default_text_oid")]
            oid: u32,
        }

        fn default_text_oid() -> u32 {
            25 // text
        }

        let payload: CachedPayload = serde_json::from_slice(bytes)
            .map_err(|e| ProxyError::Protocol(format!("invalid cached payload JSON: {}", e)))?;

        if payload.columns.is_empty() {
            return Err(ProxyError::Protocol(
                "cached payload must declare at least one column".to_string(),
            ));
        }

        let mut reply = Vec::new();

        // RowDescription (tag 'T')
        let mut rd = BytesMut::new();
        rd.put_u16(payload.columns.len() as u16);
        for col in &payload.columns {
            rd.extend_from_slice(col.name.as_bytes());
            rd.put_u8(0); // cstring terminator
            rd.put_i32(0); // tableOID (unknown)
            rd.put_i16(0); // columnNumber (unknown)
            rd.put_u32(col.oid);
            rd.put_i16(-1); // typeLen (unspecified)
            rd.put_i32(-1); // typeMod (unspecified)
            rd.put_i16(0); // format code: text
        }
        reply.extend_from_slice(&Message::new(MessageType::RowDescription, rd).encode());

        // DataRow (tag 'D') per row
        let column_count = payload.columns.len();
        for row in &payload.rows {
            if row.len() != column_count {
                return Err(ProxyError::Protocol(format!(
                    "cached row has {} values but {} columns are declared",
                    row.len(),
                    column_count
                )));
            }
            let mut dr = BytesMut::new();
            dr.put_u16(row.len() as u16);
            for value in row {
                match value {
                    Some(s) => {
                        dr.put_i32(s.len() as i32);
                        dr.extend_from_slice(s.as_bytes());
                    }
                    None => {
                        dr.put_i32(-1); // NULL sentinel
                    }
                }
            }
            reply.extend_from_slice(&Message::new(MessageType::DataRow, dr).encode());
        }

        // CommandComplete (tag 'C')
        let tag = format!("SELECT {}", payload.rows.len());
        let mut cc = BytesMut::new();
        cc.extend_from_slice(tag.as_bytes());
        cc.put_u8(0);
        reply.extend_from_slice(&Message::new(MessageType::CommandComplete, cc).encode());

        // ReadyForQuery (tag 'Z', status 'I' idle)
        reply.extend_from_slice(&Self::create_ready_for_query(b'I'));

        Ok(reply)
    }
}
