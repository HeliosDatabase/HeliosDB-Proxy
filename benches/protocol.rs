//! PG-wire Protocol Benchmarks
//!
//! Measures the per-query hot path that every client frame and backend
//! response flows through: message decode (framing), message encode
//! (serialization), and zero-copy query-text extraction. Each group runs
//! over three payload sizes — a trivial `SELECT 1`, a ~60-char `WHERE`
//! query, and a deterministically-built ~1 KiB `IN (...)` statement — so a
//! regression shows up as both a per-call delta and a bytes/sec throughput
//! change. Feature-free: only the always-public `protocol` API is exercised,
//! so the bench compiles under every feature set.

use bytes::{BufMut, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use heliosdb_proxy::protocol::{
    contains_ci, query_text, starts_with_ci, AuthRequest, BindMessage, CommandComplete,
    ErrorResponse, Message, MessageType, ParseMessage, ProtocolCodec,
};

/// A trivial single-value query.
const SHORT_SQL: &str = "SELECT 1";

/// A ~60-char point-lookup with a `WHERE` predicate.
const MEDIUM_SQL: &str = "SELECT * FROM users WHERE id = 42 AND status = 'active' LIMIT 10";

/// Build a deterministic ~1 KiB SQL statement (a long `IN (...)` list) so the
/// decode/encode/scan path is exercised on a realistically large frame. The
/// output is byte-for-byte identical across runs.
fn kilobyte_sql() -> String {
    let mut sql = String::from("SELECT * FROM events WHERE id IN (");
    let mut i = 0u32;
    while sql.len() < 1024 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&i.to_string());
        i += 1;
    }
    sql.push(')');
    sql
}

/// A `Query` message payload: NUL-terminated SQL, as it appears on the wire.
fn payload_for(sql: &str) -> BytesMut {
    let mut payload = BytesMut::with_capacity(sql.len() + 1);
    payload.extend_from_slice(sql.as_bytes());
    payload.extend_from_slice(b"\0"); // Query payload is NUL-terminated SQL
    payload
}

/// A fully-framed `Query` message ('Q' tag + length + NUL-terminated SQL).
fn wire_for(sql: &str) -> BytesMut {
    let codec = ProtocolCodec::new();
    codec.encode_message(&Message::new(MessageType::Query, payload_for(sql)))
}

/// Decode a framed message from a buffer (consumes/advances the buffer, so
/// each iteration clones a fresh copy of the pre-built wire bytes).
fn bench_decode(c: &mut Criterion) {
    let codec = ProtocolCodec::new();
    let kb = kilobyte_sql();
    let cases: [(&str, &str); 3] = [
        ("short_select", SHORT_SQL),
        ("medium_where", MEDIUM_SQL),
        ("kilobyte_in_list", kb.as_str()),
    ];

    let mut group = c.benchmark_group("protocol/decode_message");
    for (name, sql) in cases {
        let wire = wire_for(sql);
        group.throughput(Throughput::Bytes(wire.len() as u64));
        group.bench_function(name, |b| {
            b.iter(|| {
                let mut buf = wire.clone();
                black_box(codec.decode_message(&mut buf).unwrap());
            });
        });
    }
    group.finish();
}

/// Encode a message to framed wire bytes.
fn bench_encode(c: &mut Criterion) {
    let codec = ProtocolCodec::new();
    let kb = kilobyte_sql();
    let cases: [(&str, &str); 3] = [
        ("short_select", SHORT_SQL),
        ("medium_where", MEDIUM_SQL),
        ("kilobyte_in_list", kb.as_str()),
    ];

    let mut group = c.benchmark_group("protocol/encode_message");
    for (name, sql) in cases {
        let msg = Message::new(MessageType::Query, payload_for(sql));
        group.throughput(Throughput::Bytes(codec.encode_message(&msg).len() as u64));
        group.bench_function(name, |b| {
            b.iter(|| {
                black_box(codec.encode_message(black_box(&msg)));
            });
        });
    }
    group.finish();
}

/// Extract the SQL text out of a `Query` payload without copying.
fn bench_query_text(c: &mut Criterion) {
    let kb = kilobyte_sql();
    let cases: [(&str, &str); 3] = [
        ("short_select", SHORT_SQL),
        ("medium_where", MEDIUM_SQL),
        ("kilobyte_in_list", kb.as_str()),
    ];

    let mut group = c.benchmark_group("protocol/query_text");
    for (name, sql) in cases {
        let payload = payload_for(sql);
        let bytes: &[u8] = &payload;
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_function(name, |b| {
            b.iter(|| {
                black_box(query_text(black_box(bytes)));
            });
        });
    }
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────
// Connection-setup framing: startup / SSLRequest / CancelRequest
// ─────────────────────────────────────────────────────────────────────

/// Build a v3 startup frame (`length` + version + NUL-delimited key/value pairs
/// + terminator), as it appears on the wire before the first byte of auth.
fn startup_wire(params: &[(&str, &str)]) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_u32(196608); // protocol version 3.0
    for (k, v) in params {
        body.extend_from_slice(k.as_bytes());
        body.put_u8(0);
        body.extend_from_slice(v.as_bytes());
        body.put_u8(0);
    }
    body.put_u8(0); // final NUL terminating the parameter list
    let mut wire = BytesMut::with_capacity(body.len() + 4);
    wire.put_u32((body.len() + 4) as u32); // length counts itself
    wire.extend_from_slice(&body);
    wire
}

/// An 8-byte SSLRequest frame (length + magic).
fn ssl_request_wire() -> BytesMut {
    let mut wire = BytesMut::with_capacity(8);
    wire.put_u32(8);
    wire.put_u32(80877103);
    wire
}

/// A 16-byte CancelRequest frame (length + magic + pid + key).
fn cancel_request_wire() -> BytesMut {
    let mut wire = BytesMut::with_capacity(16);
    wire.put_u32(16);
    wire.put_u32(80877102);
    wire.put_u32(4242);
    wire.put_u32(133_742);
    wire
}

/// Decode the startup frame the pre-auth handler sees on every new connection.
/// `decode_startup` consumes the buffer, so each iteration clones a fresh copy.
fn bench_decode_startup(c: &mut Criterion) {
    let codec = ProtocolCodec::new();
    let startup = startup_wire(&[
        ("user", "bench_user"),
        ("database", "app_db"),
        ("application_name", "heliosdb-bench"),
        ("client_encoding", "UTF8"),
    ]);
    let ssl = ssl_request_wire();
    let cancel = cancel_request_wire();

    let mut group = c.benchmark_group("protocol/decode_startup");
    let cases: [(&str, &BytesMut); 3] = [
        ("startup_params", &startup),
        ("ssl_request", &ssl),
        ("cancel_request", &cancel),
    ];
    for (name, wire) in cases {
        group.throughput(Throughput::Bytes(wire.len() as u64));
        group.bench_function(name, |b| {
            b.iter(|| {
                let mut buf = wire.clone();
                black_box(codec.decode_startup(&mut buf).unwrap());
            });
        });
    }
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────
// Extended-query relay parsers: Parse / Bind
// ─────────────────────────────────────────────────────────────────────

/// A `Parse` payload: statement name + query text + parameter type OIDs.
fn parse_payload() -> BytesMut {
    let mut p = BytesMut::new();
    p.extend_from_slice(b"stmt_users\0");
    p.extend_from_slice(b"SELECT * FROM users WHERE id = $1 AND status = $2\0");
    p.put_u16(2);
    p.put_u32(23); // int4
    p.put_u32(25); // text
    p
}

/// A `Bind` payload: portal + statement + formats + two parameter values +
/// result formats (exercises the zero-copy `split_to().freeze()` value path).
fn bind_payload() -> BytesMut {
    let mut p = BytesMut::new();
    p.put_u8(0); // portal "" (unnamed)
    p.extend_from_slice(b"stmt_users\0");
    p.put_u16(1); // one parameter format code...
    p.put_i16(1); // ...binary
    p.put_u16(2); // two parameter values
    let v1 = b"42";
    p.put_i32(v1.len() as i32);
    p.extend_from_slice(v1);
    let v2 = b"active";
    p.put_i32(v2.len() as i32);
    p.extend_from_slice(v2);
    p.put_u16(1); // one result format code...
    p.put_i16(0); // ...text
    p
}

/// Extended-query-protocol relay parsers not covered by the Query-frame bench.
/// Both `parse` consume their `BytesMut`, so each iteration clones a fresh copy.
fn bench_extended_parse(c: &mut Criterion) {
    let parse_p = parse_payload();
    let bind_p = bind_payload();

    let mut group = c.benchmark_group("protocol/extended_parse");

    group.throughput(Throughput::Bytes(parse_p.len() as u64));
    group.bench_function("parse_message", |b| {
        b.iter(|| {
            black_box(ParseMessage::parse(parse_p.clone()).unwrap());
        });
    });

    group.throughput(Throughput::Bytes(bind_p.len() as u64));
    group.bench_function("bind_message", |b| {
        b.iter(|| {
            black_box(BindMessage::parse(bind_p.clone()).unwrap());
        });
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────
// Backend-response relay parsers: ErrorResponse / CommandComplete / AuthRequest
// ─────────────────────────────────────────────────────────────────────

/// An `ErrorResponse` payload: a set of `(field-code, value)` pairs + terminator.
fn error_payload() -> BytesMut {
    let mut p = BytesMut::new();
    let fields: [(u8, &str); 4] = [
        (b'S', "ERROR"),
        (b'C', "23505"),
        (
            b'M',
            "duplicate key value violates unique constraint \"users_pkey\"",
        ),
        (b'D', "Key (id)=(1) already exists."),
    ];
    for (code, value) in fields {
        p.put_u8(code);
        p.extend_from_slice(value.as_bytes());
        p.put_u8(0);
    }
    p.put_u8(0); // terminating NUL field code
    p
}

/// A `CommandComplete` payload carrying a NUL-terminated command tag.
fn command_complete_payload(tag: &str) -> BytesMut {
    let mut p = BytesMut::new();
    p.extend_from_slice(tag.as_bytes());
    p.put_u8(0);
    p
}

/// An `AuthenticationSASL` payload: type 10 + one mechanism + list terminator.
fn auth_sasl_payload() -> BytesMut {
    let mut p = BytesMut::new();
    p.put_i32(10);
    p.extend_from_slice(b"SCRAM-SHA-256\0");
    p.put_u8(0); // empty cstring terminates the mechanism list
    p
}

/// An `AuthenticationMD5Password` payload: type 5 + 4-byte salt.
fn auth_md5_payload() -> BytesMut {
    let mut p = BytesMut::new();
    p.put_i32(5);
    p.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
    p
}

/// Backend-response framing: the error-field map, the command-tag + rows-affected
/// parse, and the SASL/MD5 auth decode — all on the per-response relay hot path.
fn bench_backend_response(c: &mut Criterion) {
    let err = error_payload();
    let cc = command_complete_payload("INSERT 0 1");
    let sasl = auth_sasl_payload();
    let md5 = auth_md5_payload();

    let mut group = c.benchmark_group("protocol/backend_response");

    group.throughput(Throughput::Bytes(err.len() as u64));
    group.bench_function("error_response", |b| {
        b.iter(|| {
            black_box(ErrorResponse::parse(err.clone()).unwrap());
        });
    });

    group.throughput(Throughput::Bytes(cc.len() as u64));
    group.bench_function("command_complete", |b| {
        b.iter(|| {
            let parsed = CommandComplete::parse(cc.clone()).unwrap();
            black_box(parsed.rows_affected());
        });
    });

    group.throughput(Throughput::Bytes(sasl.len() as u64));
    group.bench_function("auth_sasl", |b| {
        b.iter(|| {
            black_box(AuthRequest::parse(sasl.clone()).unwrap());
        });
    });

    group.throughput(Throughput::Bytes(md5.len() as u64));
    group.bench_function("auth_md5", |b| {
        b.iter(|| {
            black_box(AuthRequest::parse(md5.clone()).unwrap());
        });
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────
// Per-frame tag dispatch + allocation-free case-insensitive helpers
// ─────────────────────────────────────────────────────────────────────

/// `MessageType::from_tag` dispatch and the `starts_with_ci` / `contains_ci`
/// helpers that underpin `TransactionEvent::detect`.
fn bench_tag_dispatch(c: &mut Criterion) {
    let mut group = c.benchmark_group("protocol/tag_dispatch");

    let tags: [u8; 8] = [b'Q', b'P', b'B', b'E', b'C', b'R', b'Z', b'X'];
    group.throughput(Throughput::Elements(tags.len() as u64));
    group.bench_function("from_tag", |b| {
        b.iter(|| {
            for &tag in &tags {
                black_box(MessageType::from_tag(black_box(tag)));
            }
        });
    });

    let sql = "select * from accounts where id = 1";
    group.bench_function("starts_with_ci", |b| {
        b.iter(|| black_box(starts_with_ci(black_box(sql), "SELECT")));
    });
    group.bench_function("contains_ci", |b| {
        b.iter(|| black_box(contains_ci(black_box(sql), "TRANSACTION")));
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_decode,
    bench_encode,
    bench_query_text,
    bench_decode_startup,
    bench_extended_parse,
    bench_backend_response,
    bench_tag_dispatch,
);
criterion_main!(benches);
