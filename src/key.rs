//! Exact 16-byte keys for IDs.
//!
//! An ID of at most 15 bytes is stored inline: its bytes big-endian and
//! zero-padded in the upper 120 bits, its length in the lowest 8 bits. Comparing
//! two inline keys as `(hi, lo)` is then exactly byte-wise lexicographic order,
//! embedded NUL bytes included (`"a"` < `"a\0"` < `"a\0b"`), and key equality is
//! exactly byte equality.
//!
//! A longer ID is interned in a global, thread-safe overflow store: the low 8 bits
//! are `0xFF`, bits 8.. hold the store index, and `hi` still holds the first 8
//! bytes (big-endian), which keeps the cheap `hi` comparison order-correct. Equal
//! long IDs always get the same index, so equality stays a plain key comparison.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

pub const P0: u64 = 0xa076_1d64_78bd_642f;
pub const P1: u64 = 0xe703_7ed1_a0b4_28db;

#[inline(always)]
pub fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128).wrapping_mul(b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

const OVF_TAG: u64 = 0xFF;
const OVF_SHARD_BITS: u32 = 6;
const OVF_SHARDS: usize = 1 << OVF_SHARD_BITS;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Debug)]
#[repr(C)]
pub struct Key16 {
    pub hi: u64,
    pub lo: u64,
}

#[inline(always)]
fn be_mask(nbytes: usize) -> u64 {
    // Keeps the top `nbytes` (0..=8) bytes of a big-endian word.
    if nbytes == 0 {
        0
    } else {
        !0u64 << (64 - 8 * nbytes)
    }
}

impl Key16 {
    pub const EMPTY: Key16 = Key16 { hi: 0, lo: 0 };

    #[inline(always)]
    pub fn new(s: &[u8]) -> Key16 {
        let n = s.len();
        if n <= 15 {
            let p = s.as_ptr();
            let (w0, w1);
            if n == 0 {
                // Empty slices may carry a dangling pointer.
                return Key16::EMPTY;
            }
            if (p as usize & 4095) <= 4096 - 16 {
                // 16-byte read stays inside the page that holds `s[0]`.
                unsafe {
                    w0 = u64::from_be((p as *const u64).read_unaligned());
                    w1 = u64::from_be((p.add(8) as *const u64).read_unaligned());
                }
            } else {
                let mut buf = [0u8; 16];
                buf[..n].copy_from_slice(s);
                w0 = u64::from_be_bytes(buf[0..8].try_into().unwrap());
                w1 = u64::from_be_bytes(buf[8..16].try_into().unwrap());
            }
            let hi = w0 & be_mask(n.min(8));
            let lo = (w1 & be_mask(n.saturating_sub(8))) | n as u64;
            Key16 { hi, lo }
        } else {
            Key16::overflow(s)
        }
    }

    #[cold]
    fn overflow(s: &[u8]) -> Key16 {
        let idx = overflow_store().intern(s);
        let hi = u64::from_be_bytes(s[0..8].try_into().unwrap());
        Key16 { hi, lo: (idx << 8) | OVF_TAG }
    }

    #[inline(always)]
    pub fn is_inline(&self) -> bool {
        self.lo & 0xFF != OVF_TAG
    }

    /// Two-round multiply-fold hash of the 128-bit key.
    #[inline(always)]
    pub fn hash(&self, seed: u64) -> u64 {
        mum(mum(self.hi ^ seed ^ P0, self.lo ^ P1) ^ seed, P1 ^ P0)
    }

    /// The ID bytes; inline keys are decoded into `buf`.
    #[inline]
    pub fn as_slice<'a>(&self, buf: &'a mut [u8; 16]) -> &'a [u8] {
        if self.is_inline() {
            buf[0..8].copy_from_slice(&self.hi.to_be_bytes());
            buf[8..16].copy_from_slice(&self.lo.to_be_bytes());
            &buf[..(self.lo & 0xFF) as usize]
        } else {
            overflow_store().get(self.lo >> 8)
        }
    }

    /// Exact byte-wise lexicographic comparison of the two IDs.
    #[inline(always)]
    pub fn cmp_bytes(&self, other: &Key16) -> Ordering {
        if self.hi != other.hi {
            return self.hi.cmp(&other.hi);
        }
        if self.is_inline() && other.is_inline() {
            return self.lo.cmp(&other.lo);
        }
        self.cmp_slow(other)
    }

    #[cold]
    fn cmp_slow(&self, other: &Key16) -> Ordering {
        if self == other {
            return Ordering::Equal;
        }
        let (mut a, mut b) = ([0u8; 16], [0u8; 16]);
        self.as_slice(&mut a).cmp(other.as_slice(&mut b))
    }
}

// ---------------------------------------------------------------------------

struct OvfShard {
    map: HashMap<&'static [u8], u64>,
    strs: Vec<&'static [u8]>,
}

pub struct OverflowStore {
    shards: Vec<Mutex<OvfShard>>,
}

pub fn overflow_store() -> &'static OverflowStore {
    static S: OnceLock<OverflowStore> = OnceLock::new();
    S.get_or_init(|| OverflowStore {
        shards: (0..OVF_SHARDS).map(|_| Mutex::new(OvfShard { map: HashMap::new(), strs: Vec::new() })).collect(),
    })
}

impl OverflowStore {
    fn intern(&self, s: &[u8]) -> u64 {
        let sh = (crate::hash::hash(s, P1) >> (64 - OVF_SHARD_BITS)) as usize;
        let mut g = self.shards[sh].lock().unwrap();
        if let Some(&i) = g.map.get(s) {
            return i;
        }
        let owned: &'static [u8] = Box::leak(s.to_vec().into_boxed_slice());
        let local = g.strs.len() as u64;
        assert!(local < (1 << (56 - OVF_SHARD_BITS)), "overflow store full");
        let idx = (local << OVF_SHARD_BITS) | sh as u64;
        g.strs.push(owned);
        g.map.insert(owned, idx);
        idx
    }

    fn get(&self, idx: u64) -> &'static [u8] {
        let sh = (idx as usize) & (OVF_SHARDS - 1);
        self.shards[sh].lock().unwrap().strs[(idx >> OVF_SHARD_BITS) as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt(k: &Key16) -> Vec<u8> {
        let mut b = [0u8; 16];
        k.as_slice(&mut b).to_vec()
    }

    #[test]
    fn nul_ordering_is_bytewise() {
        let ids: Vec<&[u8]> = vec![
            b"", b"\0", b"\0\0", b"a", b"a\0", b"a\0\0", b"a\0b", b"a\x01", b"ab", b"b", b"\xff",
            b"aaaaaaa", b"aaaaaaaa", b"aaaaaaaa\0", b"aaaaaaaa\0\0\0\0\0\0\0", b"aaaaaaab",
            b"aaaaaaaaaaaaaaa", b"aaaaaaaaaaaaaaaa", b"aaaaaaaaaaaaaaa\0", b"aaaaaaaaaaaaaab",
            b"aaaaaaaaaaaaaaaaaaaaaaaaaa", b"aaaaaaaaaaaaaaaaaaaaaaaaab", b"aaaaaaab\0\0\0\0\0\0\0\0\0\0",
            b"zzzzzzzzzzzzzzzzzzzz", b"U1", b"u1", b" u1", b"u1 ",
        ];
        let keys: Vec<Key16> = ids.iter().map(|s| Key16::new(s)).collect();
        for (i, a) in ids.iter().enumerate() {
            assert_eq!(rt(&keys[i]), a.to_vec(), "roundtrip {a:?}");
            for (j, b) in ids.iter().enumerate() {
                assert_eq!(keys[i].cmp_bytes(&keys[j]), a.cmp(b), "{a:?} vs {b:?}");
                assert_eq!(keys[i] == keys[j], a == b, "eq {a:?} vs {b:?}");
                if a.len() <= 15 && b.len() <= 15 {
                    // Inline keys: plain (hi, lo) order is byte order.
                    assert_eq!(keys[i].cmp(&keys[j]), a.cmp(b));
                }
            }
        }
    }

    #[test]
    fn overflow_is_exact_and_interned() {
        let a = vec![b'x'; 300];
        let mut b = a.clone();
        b[299] = b'y';
        let ka = Key16::new(&a);
        let kb = Key16::new(&b);
        assert!(!ka.is_inline() && !kb.is_inline());
        assert_ne!(ka, kb);
        assert_eq!(ka, Key16::new(&a.clone()));
        assert_eq!(rt(&ka), a);
        assert_eq!(ka.cmp_bytes(&kb), Ordering::Less);
        // Concurrent interning returns one index per distinct string.
        let ks: Vec<Vec<Key16>> = std::thread::scope(|s| {
            let hs: Vec<_> = (0..8)
                .map(|_| s.spawn(|| (0..500).map(|i| Key16::new(format!("long-identifier-{i:08}").as_bytes())).collect()))
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        for v in &ks[1..] {
            assert_eq!(v, &ks[0]);
        }
    }

    #[test]
    fn page_edge_reads() {
        // Keys built from slices ending exactly at a page boundary.
        let mut buf = vec![0u8; 3 * 4096];
        let base = (buf.as_ptr() as usize + 4095) & !4095;
        let off = base - buf.as_ptr() as usize + 4096;
        for n in 0..=15 {
            for (i, c) in buf[off - n..off].iter_mut().enumerate() {
                *c = b'a' + i as u8;
            }
            let s = &buf[off - n..off];
            assert_eq!(rt(&Key16::new(s)), s.to_vec());
        }
    }

    #[test]
    fn hash_distinct_keys_never_merge() {
        // Equality never depends on the hash: distinct IDs with identical
        // 64-bit hashes (forced here by comparing only keys) stay distinct.
        let mut seen = std::collections::HashSet::new();
        for i in 0..200_000u32 {
            let s = format!("u{i:x}");
            assert!(seen.insert(Key16::new(s.as_bytes())));
        }
        // Shard spread of the hash is reasonable.
        let mut cnt = [0u32; 256];
        for i in 0..256_000u32 {
            cnt[(Key16::new(format!("e{i}").as_bytes()).hash(7) >> 56) as usize] += 1;
        }
        assert!(cnt.iter().all(|&c| c > 700 && c < 1300), "{cnt:?}");
    }
}
