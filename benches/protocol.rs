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

use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use heliosdb_proxy::protocol::{query_text, Message, MessageType, ProtocolCodec};

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

criterion_group!(benches, bench_decode, bench_encode, bench_query_text);
criterion_main!(benches);
