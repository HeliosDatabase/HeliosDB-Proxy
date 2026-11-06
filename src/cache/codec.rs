//! L2 payload compression (`[cache] l2_compression`).
//!
//! The idea of DistribCache's marker-byte codec, on the query cache's own
//! entries (nothing is shared with `src/distribcache`). A payload of at least
//! `compression_min_bytes` is kept as a blob: one marker byte naming the
//! codec, then the codec's frame. A payload the codec does not shrink, or a
//! shorter one, is kept raw, as received, with no marker, so the raw form
//! costs nothing. Which form an entry holds is recorded next to it
//! ([`L2Payload`]): decoding never guesses from the bytes, and a blob whose
//! marker is unknown or whose frame does not decode to exactly the recorded
//! length is refused ([`decode`]), so the caller treats it as a miss.
//!
//! Decoding allocates at most the recorded raw length: neither codec's own
//! size field is trusted.

use std::cell::RefCell;

use bytes::Bytes;

use crate::config::CacheCompression;

/// Marker byte of an LZ4 block blob.
const MARKER_LZ4: u8 = 0x01;
/// Marker byte of a zstd frame blob.
const MARKER_ZSTD: u8 = 0x02;

/// How L2 stores payloads: the codec, the smallest payload it applies to and
/// the zstd level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L2Codec {
    /// The codec; `None` keeps every payload raw.
    pub compression: CacheCompression,
    /// Smallest payload, in bytes, that is compressed.
    pub min_bytes: usize,
    /// zstd level of [`CacheCompression::Zstd`].
    pub zstd_level: i32,
}

/// A payload in the form L2 retains it: the raw bytes, or a blob (marker
/// byte and frame) that decodes to `raw_len` bytes.
#[derive(Debug, Clone)]
pub struct L2Payload {
    bytes: Bytes,
    /// The decoded length when `bytes` is a compressed blob; `None` when raw.
    raw_len: Option<usize>,
}

impl L2Payload {
    /// A payload kept as received.
    pub fn raw(bytes: Bytes) -> Self {
        Self {
            bytes,
            raw_len: None,
        }
    }

    /// The bytes L2 keeps: the raw payload or the compressed blob.
    pub fn stored(&self) -> &Bytes {
        &self.bytes
    }

    /// The decoded length when the payload is a compressed blob, `None`
    /// when it is raw.
    pub fn raw_len(&self) -> Option<usize> {
        self.raw_len
    }

    /// Whether the payload is a compressed blob.
    pub fn is_compressed(&self) -> bool {
        self.raw_len.is_some()
    }

    /// Split into the stored bytes and, for a blob, its decoded length.
    pub(crate) fn into_parts(self) -> (Bytes, Option<usize>) {
        (self.bytes, self.raw_len)
    }
}

impl L2Codec {
    /// Whether a payload of `len` bytes is compressed at all.
    pub fn applies(&self, len: usize) -> bool {
        self.compression != CacheCompression::None && len >= self.min_bytes
    }

    /// The form L2 keeps `data` in: a compressed blob when the codec applies
    /// and the blob is smaller than `data`, else `data` itself (no copy).
    /// CPU-bound: zstd of a large payload takes milliseconds.
    pub fn encode(&self, data: &Bytes) -> L2Payload {
        if !self.applies(data.len()) {
            return L2Payload::raw(data.clone());
        }
        let blob = match self.compression {
            CacheCompression::None => None,
            CacheCompression::Lz4 => lz4_blob(data),
            CacheCompression::Zstd => zstd_blob(data, self.zstd_level),
        };
        match blob {
            Some(blob) if blob.len() < data.len() => L2Payload {
                bytes: Bytes::from(blob),
                raw_len: Some(data.len()),
            },
            _ => L2Payload::raw(data.clone()),
        }
    }
}

/// `data` as an LZ4 blob: the marker, then one LZ4 block (no size prefix:
/// the entry records the raw length). `None` if LZ4 runs out of room, which
/// its own worst-case bound rules out.
fn lz4_blob(data: &[u8]) -> Option<Vec<u8>> {
    let mut blob = vec![0u8; 1 + lz4_flex::block::get_maximum_output_size(data.len())];
    blob[0] = MARKER_LZ4;
    let written = lz4_flex::block::compress_into(data, &mut blob[1..]).ok()?;
    blob.truncate(1 + written);
    Some(blob)
}

/// `data` as a zstd blob at `level`: the marker, then one zstd frame; `None`
/// when zstd refuses (it does not for a valid level).
fn zstd_blob(data: &[u8], level: i32) -> Option<Vec<u8>> {
    let mut blob = vec![0u8; 1 + zstd::zstd_safe::compress_bound(data.len())];
    blob[0] = MARKER_ZSTD;
    let written = zstd::bulk::compress_to_buffer(data, &mut blob[1..], level).ok()?;
    blob.truncate(1 + written);
    Some(blob)
}

thread_local! {
    /// One zstd decompression context per thread, reused across hits:
    /// creating one per hit costs about as much as decoding a small payload.
    static ZSTD_DECODER: RefCell<Option<zstd::bulk::Decompressor<'static>>> =
        const { RefCell::new(None) };
}

/// The raw payload of `blob`, which decodes to `raw_len` bytes; `None` when
/// the marker is unknown, the frame is corrupt or its length differs.
pub fn decode(blob: &[u8], raw_len: usize) -> Option<Vec<u8>> {
    let (&marker, frame) = blob.split_first()?;
    let raw = match marker {
        MARKER_LZ4 => {
            let mut out = vec![0u8; raw_len];
            let written = lz4_flex::block::decompress_into(frame, &mut out).ok()?;
            out.truncate(written);
            out
        }
        MARKER_ZSTD => ZSTD_DECODER.with(|cell| {
            let mut slot = cell.try_borrow_mut().ok()?;
            if slot.is_none() {
                *slot = Some(zstd::bulk::Decompressor::new().ok()?);
            }
            // `capacity` bounds the output: a frame claiming more fails.
            slot.as_mut()?.decompress(frame, raw_len).ok()
        })?,
        _ => return None,
    };
    (raw.len() == raw_len).then_some(raw)
}

/// At least `bytes` bytes of PostgreSQL DataRow frames: an int, a name, a
/// status and a timestamp per row, as text (the repetitive payload cached
/// reads carry). For tests across the cache module.
#[cfg(test)]
pub(crate) fn data_rows(bytes: usize) -> Bytes {
    let mut out = Vec::with_capacity(bytes + 128);
    let mut i = 0u32;
    while out.len() < bytes {
        let id = i.to_string();
        let name = format!("user_{i:06}");
        let fields: [&[u8]; 4] = [
            id.as_bytes(),
            name.as_bytes(),
            b"active",
            b"2026-11-06 12:00:00+00",
        ];
        let body: usize = fields.iter().map(|f| 4 + f.len()).sum();
        out.push(b'D');
        out.extend_from_slice(&((4 + 2 + body) as u32).to_be_bytes());
        out.extend_from_slice(&(fields.len() as u16).to_be_bytes());
        for f in fields {
            out.extend_from_slice(&(f.len() as u32).to_be_bytes());
            out.extend_from_slice(f);
        }
        i += 1;
    }
    Bytes::from(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bytes no codec shrinks (an xorshift stream).
    fn noise(len: usize) -> Bytes {
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        Bytes::from(
            (0..len)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x as u8
                })
                .collect::<Vec<u8>>(),
        )
    }

    fn codec(compression: CacheCompression) -> L2Codec {
        L2Codec {
            compression,
            min_bytes: 4096,
            zstd_level: 3,
        }
    }

    #[test]
    fn round_trip_for_every_codec() {
        let data = data_rows(64 * 1024);
        let raw = codec(CacheCompression::None).encode(&data);
        assert!(!raw.is_compressed());
        assert_eq!(raw.stored(), &data);
        assert_eq!(
            raw.stored().as_ptr(),
            data.as_ptr(),
            "the raw form is not copied"
        );
        for (compression, marker) in [
            (CacheCompression::Lz4, MARKER_LZ4),
            (CacheCompression::Zstd, MARKER_ZSTD),
        ] {
            let p = codec(compression).encode(&data);
            assert!(p.is_compressed(), "{compression:?}");
            assert_eq!(p.raw_len(), Some(data.len()));
            assert_eq!(p.stored()[0], marker);
            assert!(
                p.stored().len() * 3 < data.len(),
                "{compression:?}: DataRows compress well ({} of {})",
                p.stored().len(),
                data.len()
            );
            let back = decode(p.stored(), data.len()).unwrap();
            assert_eq!(back, data, "{compression:?}");
        }
    }

    /// Only payloads of at least `min_bytes` are compressed.
    #[test]
    fn the_threshold_is_inclusive() {
        for compression in [CacheCompression::Lz4, CacheCompression::Zstd] {
            let c = codec(compression);
            let below = data_rows(8192).slice(..c.min_bytes - 1);
            let at = data_rows(8192).slice(..c.min_bytes);
            assert!(!c.applies(below.len()) && c.applies(at.len()));
            assert!(!c.encode(&below).is_compressed(), "{compression:?} below");
            let p = c.encode(&at);
            assert!(p.is_compressed(), "{compression:?} at the threshold");
            assert_eq!(decode(p.stored(), at.len()).unwrap(), at);
        }
        assert!(!codec(CacheCompression::None).applies(usize::MAX));
    }

    /// A payload the codec does not shrink is kept raw, uncopied.
    #[test]
    fn an_incompressible_payload_stays_raw() {
        let data = noise(64 * 1024);
        for compression in [CacheCompression::Lz4, CacheCompression::Zstd] {
            let p = codec(compression).encode(&data);
            assert!(!p.is_compressed(), "{compression:?}");
            assert_eq!(p.stored().as_ptr(), data.as_ptr());
        }
    }

    /// A blob with an unknown marker, a damaged frame or another length
    /// never decodes.
    #[test]
    fn a_corrupt_blob_does_not_decode() {
        let data = data_rows(16 * 1024);
        for compression in [CacheCompression::Lz4, CacheCompression::Zstd] {
            let blob = codec(compression).encode(&data).stored().to_vec();
            for marker in [0x00, 0x03, 0xff] {
                let mut bad = blob.clone();
                bad[0] = marker;
                assert!(decode(&bad, data.len()).is_none(), "marker {marker:#x}");
            }
            // The other codec's marker on this frame.
            let mut swapped = blob.clone();
            swapped[0] = if blob[0] == MARKER_LZ4 {
                MARKER_ZSTD
            } else {
                MARKER_LZ4
            };
            assert!(decode(&swapped, data.len()).is_none(), "{compression:?}");
            assert!(decode(&blob[..blob.len() / 2], data.len()).is_none());
            assert!(
                decode(&blob, data.len() - 1).is_none(),
                "longer than recorded"
            );
            assert!(
                decode(&blob, data.len() + 1).is_none(),
                "shorter than recorded"
            );
            assert!(decode(&[], data.len()).is_none());
            assert!(decode(&blob[..1], data.len()).is_none());
        }
    }

    /// Every zstd level the config accepts round-trips.
    #[test]
    fn every_accepted_zstd_level_round_trips() {
        let data = data_rows(8192);
        for level in crate::config::CACHE_ZSTD_LEVELS {
            let c = L2Codec {
                zstd_level: level,
                ..codec(CacheCompression::Zstd)
            };
            let p = c.encode(&data);
            assert!(p.is_compressed(), "level {level}");
            assert_eq!(decode(p.stored(), data.len()).unwrap(), data);
        }
    }
}
