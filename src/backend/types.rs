//! PostgreSQL type OIDs and text-format value encoding/decoding.
//!
//! This is intentionally narrow — it covers the seven common OIDs needed
//! for TR-management queries (`pg_is_in_recovery`, `pg_last_wal_replay_lsn`,
//! failover status, tenant quota config, etc.). Binary format is out of
//! scope; everything rides the simple-query text path.

use super::error::{BackendError, BackendResult};
use chrono::{DateTime, FixedOffset};

/// PostgreSQL type OIDs the backend client understands.
///
/// These line up with `pg_type.oid` values in system catalogs. Clients
/// that receive other OIDs fall back to returning the raw UTF-8 string
/// — callers that need strict typing should check the OID and refuse
/// to interpret unfamiliar ones.
pub mod oid {
    pub const BOOL: u32 = 16;
    pub const INT8: u32 = 20;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const FLOAT8: u32 = 701;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const NUMERIC: u32 = 1700;
    /// PG_LSN — used by WAL-position queries on replicas.
    pub const PG_LSN: u32 = 3220;
}

/// A single column's text-format value as received from the backend.
///
/// Held as a `Cow<str>` view into the row bytes; callers typically turn
/// it into a typed Rust value via the `as_<type>` helpers below.
#[derive(Debug, Clone, PartialEq)]
pub enum TextValue {
    /// The column is SQL NULL.
    Null,
    /// Raw UTF-8 bytes as sent by the server.
    Text(String),
}

impl TextValue {
    /// Return `true` if this value is SQL NULL.
    pub fn is_null(&self) -> bool {
        matches!(self, TextValue::Null)
    }

    /// Borrow the underlying string if not NULL.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            TextValue::Null => None,
            TextValue::Text(s) => Some(s.as_str()),
        }
    }

    /// Consume and return the inner `String`, or `None` for NULL.
    pub fn into_string(self) -> Option<String> {
        match self {
            TextValue::Null => None,
            TextValue::Text(s) => Some(s),
        }
    }

    /// Decode as `bool`. PostgreSQL text format is `"t"` or `"f"`.
    pub fn as_bool(&self, column: &str) -> BackendResult<Option<bool>> {
        match self {
            TextValue::Null => Ok(None),
            TextValue::Text(s) => match s.as_str() {
                "t" | "true" | "TRUE" => Ok(Some(true)),
                "f" | "false" | "FALSE" => Ok(Some(false)),
                other => Err(BackendError::ParseValue {
                    column: column.to_string(),
                    reason: format!("expected bool ('t'|'f'), got {:?}", other),
                }),
            },
        }
    }

    /// Decode as `i64` (covers INT4 and INT8 at the wire level — text
    /// format is the same).
    pub fn as_i64(&self, column: &str) -> BackendResult<Option<i64>> {
        match self {
            TextValue::Null => Ok(None),
            TextValue::Text(s) => {
                s.parse::<i64>()
                    .map(Some)
                    .map_err(|e| BackendError::ParseValue {
                        column: column.to_string(),
                        reason: format!("i64: {}", e),
                    })
            }
        }
    }

    /// Decode as `f64` (FLOAT8 / double precision).
    pub fn as_f64(&self, column: &str) -> BackendResult<Option<f64>> {
        match self {
            TextValue::Null => Ok(None),
            TextValue::Text(s) => {
                s.parse::<f64>()
                    .map(Some)
                    .map_err(|e| BackendError::ParseValue {
                        column: column.to_string(),
                        reason: format!("f64: {}", e),
                    })
            }
        }
    }

    /// Decode as `DateTime<FixedOffset>` (TIMESTAMPTZ). PG text format
    /// with a timezone offset: `2026-04-24 12:34:56.789+00`.
    pub fn as_timestamptz(&self, column: &str) -> BackendResult<Option<DateTime<FixedOffset>>> {
        match self {
            TextValue::Null => Ok(None),
            TextValue::Text(s) => {
                // PG emits a space between date and time by default; RFC3339
                // wants a 'T'. Support either.
                let normalised = if s.contains(' ') && !s.contains('T') {
                    s.replacen(' ', "T", 1)
                } else {
                    s.clone()
                };
                // Append minutes to a bare hour offset: "+00" -> "+00:00".
                let normalised = if let Some(idx) = normalised.rfind(['+', '-']) {
                    let off = &normalised[idx + 1..];
                    if off.len() == 2 && off.bytes().all(|b| b.is_ascii_digit()) {
                        format!("{}:00", normalised)
                    } else {
                        normalised
                    }
                } else {
                    normalised
                };
                DateTime::parse_from_rfc3339(&normalised)
                    .map(Some)
                    .map_err(|e| BackendError::ParseValue {
                        column: column.to_string(),
                        reason: format!("timestamptz {:?}: {}", s, e),
                    })
            }
        }
    }

    /// Decode as a textual pg_lsn (e.g. `"0/16B3758"`). We leave LSN
    /// arithmetic to callers — string form is what `pg_last_wal_*_lsn()`
    /// returns in text format, and the natural lex order on these
    /// strings matches WAL ordering for positions in the same
    /// timeline.
    pub fn as_pg_lsn(&self, column: &str) -> BackendResult<Option<String>> {
        match self {
            TextValue::Null => Ok(None),
            TextValue::Text(s) => {
                // Validate shape: H[H..]/H[H..]
                if let Some((hi, lo)) = s.split_once('/') {
                    let hex_ok =
                        |p: &str| !p.is_empty() && p.bytes().all(|b| b.is_ascii_hexdigit());
                    if hex_ok(hi) && hex_ok(lo) {
                        return Ok(Some(s.clone()));
                    }
                }
                Err(BackendError::ParseValue {
                    column: column.to_string(),
                    reason: format!("pg_lsn {:?}: expected 'H/H' hex pair", s),
                })
            }
        }
    }

    /// Decode as `NUMERIC` — text is PG's canonical form (e.g. "3.1415").
    /// We return the raw string; callers that need arithmetic can route
    /// to `rust_decimal` or parse further.
    pub fn as_numeric(&self, column: &str) -> BackendResult<Option<String>> {
        match self {
            TextValue::Null => Ok(None),
            TextValue::Text(s) => {
                // Reject only obviously malformed values — a single optional
                // sign, digits, optional single dot, optional exponent.
                let bytes = s.as_bytes();
                let mut i = 0;
                if bytes.first().is_some_and(|&b| b == b'+' || b == b'-') {
                    i += 1;
                }
                let mut saw_digit = false;
                let mut saw_dot = false;
                while i < bytes.len() {
                    let b = bytes[i];
                    if b.is_ascii_digit() {
                        saw_digit = true;
                    } else if b == b'.' && !saw_dot {
                        saw_dot = true;
                    } else if (b == b'e' || b == b'E') && saw_digit {
                        // Exponent form — stop validating shape, accept.
                        saw_digit = true;
                        break;
                    } else if s.eq_ignore_ascii_case("NaN") {
                        return Ok(Some("NaN".to_string()));
                    } else {
                        return Err(BackendError::ParseValue {
                            column: column.to_string(),
                            reason: format!("numeric {:?}", s),
                        });
                    }
                    i += 1;
                }
                if saw_digit {
                    Ok(Some(s.clone()))
                } else {
                    Err(BackendError::ParseValue {
                        column: column.to_string(),
                        reason: format!("numeric {:?}: no digits", s),
                    })
                }
            }
        }
    }
}

/// Encode a Rust value as a PostgreSQL text-format parameter.
///
/// The backend client substitutes parameters into simple-query SQL by
/// properly-quoting literals; we do not use the extended protocol here.
/// This function produces the already-quoted literal (e.g. `'alice'::text`
/// for a string, `42` for an i64).
///
/// Implementations are deliberately tight — enough to serialise the
/// argument values TR-management queries need. Callers with richer
/// types should extend this set.
pub fn encode_literal(v: &ParamValue) -> String {
    match v {
        ParamValue::Null => "NULL".to_string(),
        ParamValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        ParamValue::Int(i) => i.to_string(),
        ParamValue::Float(f) => {
            // Match PG conventions: `NaN` and `Infinity` are unquoted
            // identifiers inside numeric context, but for parameters we
            // pass them via the text format cast.
            if f.is_nan() {
                "'NaN'::float8".to_string()
            } else if f.is_infinite() {
                if *f > 0.0 {
                    "'Infinity'::float8".to_string()
                } else {
                    "'-Infinity'::float8".to_string()
                }
            } else {
                format!("{:?}", f) // {:?} preserves precision round-trip
            }
        }
        ParamValue::Text(s) => {
            // PG simple-query string literal: wrap in single quotes,
            // escape embedded single quotes by doubling.
            let mut out = String::with_capacity(s.len() + 2);
            out.push('\'');
            for ch in s.chars() {
                if ch == '\'' {
                    out.push_str("''");
                } else {
                    out.push(ch);
                }
            }
            out.push('\'');
            out
        }
        ParamValue::Lsn(s) => format!("'{}'::pg_lsn", s),
    }
}

/// Minimal parameter-value enum covering the seven supported OIDs.
///
/// Kept intentionally small — this is for TR-management queries, not
/// general-purpose query execution.
#[derive(Debug, Clone, PartialEq)]
pub enum ParamValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Lsn(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_text_value_bool() {
        let t = TextValue::Text("t".to_string());
        assert_eq!(t.as_bool("x").unwrap(), Some(true));
        let f = TextValue::Text("f".to_string());
        assert_eq!(f.as_bool("x").unwrap(), Some(false));
        let n = TextValue::Null;
        assert_eq!(n.as_bool("x").unwrap(), None);
        let bad = TextValue::Text("maybe".to_string());
        assert!(bad.as_bool("x").is_err());
    }

    #[test]
    fn test_text_value_i64() {
        assert_eq!(
            TextValue::Text("42".to_string()).as_i64("x").unwrap(),
            Some(42)
        );
        assert_eq!(
            TextValue::Text("-1".to_string()).as_i64("x").unwrap(),
            Some(-1)
        );
        assert!(TextValue::Text("abc".to_string()).as_i64("x").is_err());
    }

    #[test]
    fn test_text_value_f64() {
        assert_eq!(
            TextValue::Text("3.14".to_string()).as_f64("x").unwrap(),
            Some(3.14)
        );
        assert!(TextValue::Text("oops".to_string()).as_f64("x").is_err());
    }

    #[test]
    fn test_text_value_timestamptz_pg_format() {
        let v = TextValue::Text("2026-04-24 12:34:56.789+00".to_string());
        let parsed = v.as_timestamptz("ts").unwrap().expect("some");
        assert_eq!(
            parsed.to_rfc3339().starts_with("2026-04-24T12:34:56.789"),
            true
        );
    }

    #[test]
    fn test_text_value_timestamptz_rfc3339() {
        let v = TextValue::Text("2026-04-24T12:34:56+02:00".to_string());
        assert!(v.as_timestamptz("ts").unwrap().is_some());
    }

    #[test]
    fn test_text_value_pg_lsn_roundtrip() {
        assert_eq!(
            TextValue::Text("0/16B3758".to_string())
                .as_pg_lsn("x")
                .unwrap(),
            Some("0/16B3758".to_string())
        );
        assert!(TextValue::Text("nope".to_string()).as_pg_lsn("x").is_err());
        assert!(TextValue::Text("/abc".to_string()).as_pg_lsn("x").is_err());
    }

    #[test]
    fn test_text_value_numeric_accepts_valid() {
        for s in ["0", "1", "-42", "3.14", "+1.0", "1e10", "-2.5E-3", "NaN"] {
            assert!(
                TextValue::Text(s.to_string())
                    .as_numeric("x")
                    .unwrap()
                    .is_some(),
                "should accept {:?}",
                s
            );
        }
    }

    #[test]
    fn test_text_value_numeric_rejects_invalid() {
        for s in ["", "abc", "1..2", "-", "+"] {
            assert!(
                TextValue::Text(s.to_string()).as_numeric("x").is_err(),
                "should reject {:?}",
                s
            );
        }
    }

    #[test]
    fn test_encode_literal_null_bool_int() {
        assert_eq!(encode_literal(&ParamValue::Null), "NULL");
        assert_eq!(encode_literal(&ParamValue::Bool(true)), "TRUE");
        assert_eq!(encode_literal(&ParamValue::Bool(false)), "FALSE");
        assert_eq!(encode_literal(&ParamValue::Int(-7)), "-7");
    }

    #[test]
    fn test_encode_literal_text_escapes_single_quote() {
        assert_eq!(
            encode_literal(&ParamValue::Text("a'b".to_string())),
            "'a''b'"
        );
        assert_eq!(
            encode_literal(&ParamValue::Text("plain".to_string())),
            "'plain'"
        );
    }

    #[test]
    fn test_encode_literal_lsn() {
        assert_eq!(
            encode_literal(&ParamValue::Lsn("0/16B3758".to_string())),
            "'0/16B3758'::pg_lsn"
        );
    }

    #[test]
    fn test_encode_literal_float_special() {
        assert_eq!(
            encode_literal(&ParamValue::Float(f64::NAN)),
            "'NaN'::float8"
        );
        assert_eq!(
            encode_literal(&ParamValue::Float(f64::INFINITY)),
            "'Infinity'::float8"
        );
        assert_eq!(
            encode_literal(&ParamValue::Float(f64::NEG_INFINITY)),
            "'-Infinity'::float8"
        );
    }
}
