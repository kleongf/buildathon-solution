//! Exact 8-byte keys for IDs (`Key8`).
//!
//! Any ID of length 1..=10 using the 63-character sorted ASCII alphabet
//! `0-9`, `A-Z`, `_`, `a-z` is packed inline into 60 big-endian 6-bit slots
//! (codes 1..=63; 0 = end-of-string padding; bit 63 = 0). Because the 6-bit
//! codes are strictly monotonically increasing in ASCII order and 0 < 1,
//! comparing two inline `Key8` values as plain `u64` is **100% identical**
//! to byte-wise lexicographic comparison, and equality is plain `u64` equality.
//!
//! Any longer ID (>10 bytes) or ID containing any byte outside `[0-9A-Za-z_]`
//! (embedded NULs, hyphens, Unicode, spaces, etc.) is interned in a global,
//! thread-safe sharded `OverflowStore` with bit 63 = 1 (`OVF_BIT`). Equal
//! overflow IDs always receive the exact same index, so equality remains a
//! single `u64` comparison everywhere, while `cmp_bytes` falls back to exact
//! byte slice comparison whenever either operand has `OVF_BIT` set.

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

const OVF_BIT: u64 = 1u64 << 63;
const OVF_SHARD_BITS: u32 = 6;
const OVF_SHARDS: usize = 1 << OVF_SHARD_BITS;

/// 6-bit order-preserving ASCII encoding table:
/// 1..=10: '0'..='9' (48..=57)
/// 11..=36: 'A'..='Z' (65..=90)
/// 37: '_' (95)
/// 38..=63: 'a'..='z' (97..=122)
/// 0xFF: invalid (triggers overflow store)
const ENC6: [u8; 256] = {
    let mut t = [0xFFu8; 256];
    let mut c = b'0';
    while c <= b'9' {
        t[c as usize] = (c - b'0') + 1;
        c += 1;
    }
    let mut c = b'A';
    while c <= b'Z' {
        t[c as usize] = (c - b'A') + 11;
        c += 1;
    }
    t[b'_' as usize] = 37;
    let mut c = b'a';
    while c <= b'z' {
        t[c as usize] = (c - b'a') + 38;
        c += 1;
    }
    t
};

const DEC6: [u8; 64] = {
    let mut t = [0u8; 64];
    let mut i = 0u8;
    while i < 10 {
        t[(i + 1) as usize] = b'0' + i;
        i += 1;
    }
    let mut i = 0u8;
    while i < 26 {
        t[(i + 11) as usize] = b'A' + i;
        i += 1;
    }
    t[37] = b'_';
    let mut i = 0u8;
    while i < 26 {
        t[(i + 38) as usize] = b'a' + i;
        i += 1;
    }
    t
};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Debug)]
#[repr(transparent)]
pub struct Key8(pub u64);

impl Key8 {
    pub const EMPTY: Key8 = Key8(0);

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    #[inline(always)]
    pub fn is_inline(&self) -> bool {
        (self.0 & OVF_BIT) == 0
    }

    #[inline(always)]
    pub fn new(s: &[u8]) -> Key8 {
        let n = s.len();
        if n == 9 {
            let p = s.as_ptr();
            unsafe {
                let c0 = *ENC6.get_unchecked(*p as usize);
                let c1 = *ENC6.get_unchecked(*p.add(1) as usize);
                let c2 = *ENC6.get_unchecked(*p.add(2) as usize);
                let c3 = *ENC6.get_unchecked(*p.add(3) as usize);
                let c4 = *ENC6.get_unchecked(*p.add(4) as usize);
                let c5 = *ENC6.get_unchecked(*p.add(5) as usize);
                let c6 = *ENC6.get_unchecked(*p.add(6) as usize);
                let c7 = *ENC6.get_unchecked(*p.add(7) as usize);
                let c8 = *ENC6.get_unchecked(*p.add(8) as usize);
                if ((c0 | c1 | c2 | c3 | c4 | c5 | c6 | c7 | c8) & 0xC0) == 0 {
                    let hi = ((c0 as u64) << 54)
                        | ((c1 as u64) << 48)
                        | ((c2 as u64) << 42)
                        | ((c3 as u64) << 36)
                        | ((c4 as u64) << 30);
                    let lo = ((c5 as u64) << 24)
                        | ((c6 as u64) << 18)
                        | ((c7 as u64) << 12)
                        | ((c8 as u64) << 6);
                    return Key8(hi | lo);
                }
            }
            return Key8::overflow(s);
        }
        if n == 8 {
            let p = s.as_ptr();
            unsafe {
                let c0 = *ENC6.get_unchecked(*p as usize);
                let c1 = *ENC6.get_unchecked(*p.add(1) as usize);
                let c2 = *ENC6.get_unchecked(*p.add(2) as usize);
                let c3 = *ENC6.get_unchecked(*p.add(3) as usize);
                let c4 = *ENC6.get_unchecked(*p.add(4) as usize);
                let c5 = *ENC6.get_unchecked(*p.add(5) as usize);
                let c6 = *ENC6.get_unchecked(*p.add(6) as usize);
                let c7 = *ENC6.get_unchecked(*p.add(7) as usize);
                if ((c0 | c1 | c2 | c3 | c4 | c5 | c6 | c7) & 0xC0) == 0 {
                    let hi = ((c0 as u64) << 54)
                        | ((c1 as u64) << 48)
                        | ((c2 as u64) << 42)
                        | ((c3 as u64) << 36);
                    let lo = ((c4 as u64) << 30)
                        | ((c5 as u64) << 24)
                        | ((c6 as u64) << 18)
                        | ((c7 as u64) << 12);
                    return Key8(hi | lo);
                }
            }
            return Key8::overflow(s);
        }
        if n == 0 {
            return Key8::EMPTY;
        }
        if n <= 10 {
            let p = s.as_ptr();
            let mut acc = 0u64;
            let mut bad = 0u8;
            let mut i = 0usize;
            while i < n {
                let c = unsafe { *ENC6.get_unchecked(*p.add(i) as usize) };
                bad |= c;
                acc = (acc << 6) | (c as u64);
                i += 1;
            }
            if (bad & 0xC0) == 0 {
                return Key8(acc << ((10 - n) * 6));
            }
        }
        Key8::overflow(s)
    }

    #[cold]
    fn overflow(s: &[u8]) -> Key8 {
        let idx = overflow_store().intern(s);
        Key8(OVF_BIT | idx)
    }

    /// Single-multiply wyhash-grade 64-bit hash with full avalanche across all 64 bits.
    #[inline(always)]
    pub fn hash(&self, seed: u64) -> u64 {
        mum(self.0 ^ seed ^ P0, self.0.wrapping_add(P1) ^ seed)
    }

    /// The ID bytes; inline keys are decoded into `buf`.
    #[inline(always)]
    pub fn as_slice<'a>(&self, buf: &'a mut [u8; 16]) -> &'a [u8] {
        if self.is_inline() {
            let mut v = self.0 << 4;
            let mut len = 0usize;
            while v != 0 {
                let code = (v >> 58) as usize;
                unsafe { *buf.get_unchecked_mut(len) = *DEC6.get_unchecked(code) };
                len += 1;
                v <<= 6;
            }
            unsafe { buf.get_unchecked(..len) }
        } else {
            overflow_store().get(self.0 & !OVF_BIT)
        }
    }

    /// Exact byte-wise lexicographic comparison of the two IDs.
    #[inline(always)]
    pub fn cmp_bytes(&self, other: &Key8) -> Ordering {
        if ((self.0 | other.0) & OVF_BIT) == 0 {
            return self.0.cmp(&other.0);
        }
        self.cmp_slow(other)
    }

    #[cold]
    fn cmp_slow(&self, other: &Key8) -> Ordering {
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
        assert!(local < (1 << (62 - OVF_SHARD_BITS)), "overflow store full");
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

    fn rt(k: &Key8) -> Vec<u8> {
        let mut b = [0u8; 16];
        k.as_slice(&mut b).to_vec()
    }

    #[test]
    fn nul_ordering_is_bytewise() {
        let ids: Vec<&[u8]> = vec![
            b"", b"\0", b"\0\0", b"a", b"a\0", b"a\0\0", b"a\0b", b"a\x01", b"ab", b"b", b"\xff",
            b"0", b"9", b"A", b"Z", b"_", b"z", b"c_1", b"c_1958", b"C_1", b"p13649",
            b"e42huhoug", b"uduffvac", b"u8w84o3t0", b"u8w84o3t01", b"u8w84o3t012",
            b"aaaaaaa", b"aaaaaaaa", b"aaaaaaaa\0", b"aaaaaaaa\0\0\0\0\0\0\0", b"aaaaaaab",
            b"aaaaaaaaaa", b"aaaaaaaaab", b"aaaaaaaaaaa", b"aaaaaaaaaaaaaaa", b"aaaaaaaaaaaaaaaa",
            b"aaaaaaaaaaaaaaaaaaaaaaaaaa", b"aaaaaaaaaaaaaaaaaaaaaaaaab", b"aaaaaaab\0\0\0\0\0\0\0\0\0\0",
            b"zzzzzzzzzzzzzzzzzzzz", b"U1", b"u1", b" u1", b"u1 ", b"u-1", b"u_1",
        ];
        let keys: Vec<Key8> = ids.iter().map(|s| Key8::new(s)).collect();
        for (i, a) in ids.iter().enumerate() {
            assert_eq!(rt(&keys[i]), a.to_vec(), "roundtrip {a:?}");
            for (j, b) in ids.iter().enumerate() {
                assert_eq!(keys[i].cmp_bytes(&keys[j]), a.cmp(b), "{a:?} vs {b:?}");
                assert_eq!(keys[i] == keys[j], a == b, "eq {a:?} vs {b:?}");
                if keys[i].is_inline() && keys[j].is_inline() {
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
        let ka = Key8::new(&a);
        let kb = Key8::new(&b);
        assert!(!ka.is_inline() && !kb.is_inline());
        assert_ne!(ka, kb);
        assert_eq!(ka, Key8::new(&a.clone()));
        assert_eq!(rt(&ka), a);
        assert_eq!(ka.cmp_bytes(&kb), Ordering::Less);
        // Concurrent interning returns one index per distinct string.
        let ks: Vec<Vec<Key8>> = std::thread::scope(|s| {
            let hs: Vec<_> = (0..8)
                .map(|_| s.spawn(|| (0..500).map(|i| Key8::new(format!("long-identifier-{i:08}").as_bytes())).collect()))
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        for v in &ks[1..] {
            assert_eq!(v, &ks[0]);
        }
    }

    #[test]
    fn page_edge_reads() {
        let mut buf = vec![0u8; 3 * 4096];
        let base = (buf.as_ptr() as usize + 4095) & !4095;
        let off = base - buf.as_ptr() as usize + 4096;
        for n in 0..=15 {
            for (i, c) in buf[off - n..off].iter_mut().enumerate() {
                *c = b'a' + i as u8;
            }
            let s = &buf[off - n..off];
            assert_eq!(rt(&Key8::new(s)), s.to_vec());
        }
    }

    #[test]
    fn hash_distinct_keys_never_merge() {
        let mut seen = std::collections::HashSet::new();
        for i in 0..200_000u32 {
            let s = format!("u{i:x}");
            assert!(seen.insert(Key8::new(s.as_bytes())));
        }
        let mut cnt = [0u32; 256];
        for i in 0..256_000u32 {
            cnt[(Key8::new(format!("e{i}").as_bytes()).hash(7) >> 56) as usize] += 1;
        }
        assert!(cnt.iter().all(|&c| c > 700 && c < 1300), "{cnt:?}");
    }
}
