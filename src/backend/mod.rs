//! Backend PostgreSQL client used by TR-management code paths.
//!
//! `ProxyServer::route_and_forward` still forwards client frames to
//! backends without ever parsing or originating SQL — that zero-copy
//! path remains untouched. This module is the *other* direction: the
//! proxy itself acts as a PG client for internal management queries
//! (health checks, `pg_is_in_recovery()`, `pg_promote()`, WAL-position
//! probes, failover replay, session-state restoration).
//!
//! Design goals:
//!
//! * **No `tokio-postgres`.** We reuse `crate::protocol` for every
//!   wire frame so the crate has exactly one PG protocol
//!   implementation.
//! * **TLS-capable.** `tls::negotiate` performs the `SSLRequest`
//!   dance and wraps the TCP stream in `tokio-rustls` when accepted.
//! * **Text format only.** All parameter values are interpolated as
//!   quoted SQL literals before send; extended-protocol
//!   Parse/Bind/Execute is deliberately out of scope for internal
//!   management queries.
//! * **Narrow type surface.** Seven OIDs — BOOL, INT4, INT8, TEXT,
//!   FLOAT8, TIMESTAMPTZ, NUMERIC — plus PG_LSN for WAL positions.
//!
//! # Example
//!
//! ```no_run
//! use heliosdb_proxy::backend::{BackendClient, BackendConfig, tls, TlsMode};
//! use std::time::Duration;
//!
//! # async fn f() -> heliosdb_proxy::backend::BackendResult<()> {
//! let cfg = BackendConfig {
//!     host: "primary.db.internal".into(),
//!     port: 5432,
//!     user: "postgres".into(),
//!     password: Some("secret".into()),
//!     database: Some("app".into()),
//!     application_name: Some("helios-health".into()),
//!     tls_mode: TlsMode::Prefer,
//!     connect_timeout: Duration::from_secs(5),
//!     query_timeout: Duration::from_secs(5),
//!     tls_config: tls::default_client_config(),
//! };
//! let mut c = BackendClient::connect(&cfg).await?;
//! let in_recovery = c.query_scalar("SELECT pg_is_in_recovery()").await?;
//! # Ok(())
//! # }
//! ```

pub mod auth;
pub mod client;
pub mod error;
pub mod stream;
pub mod tls;
pub mod types;

pub use client::{BackendClient, BackendConfig, ColumnMeta, QueryResult};
pub use error::{BackendError, BackendResult};
pub use stream::Stream;
pub use tls::TlsMode;
pub use types::{encode_literal, oid, ParamValue, TextValue};
