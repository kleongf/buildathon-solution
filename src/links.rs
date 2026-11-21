//! Linked-ID resolution.
//!
//! `build_links` interns every linked ID exactly (`Key16`), joins components with a
//! lock-free union-find whose roots are always the byte-wise smallest ID, and stores
//! only the IDs whose person differs from themselves in a compact `LinkTable`
//! (exact `Key16` -> person `Key16`). A 2 MB blocked Bloom filter over those IDs
//! lets unlinked/root IDs skip the DRAM table probe, and `Resolver` adds a 64-entry
//! direct-mapped per-thread cache (whale IDs) plus a prefetch delay ring.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};

use crate::arena::{ShardSet, Scatter};
use crate::key::Key16;
use crate::{csv, make_chunks, chunk_target, par, populate, release_chunk, SyncPtr};

pub const SEED_L: u64 = 0x9e37_79b9_7f4a_7c15;
pub const SEED_A: u64 = SEED_L;

const REGION_BITS: u32 = 8;
const REGIONS: usize = 1 << REGION_BITS;
/// Link-side shards for interning (each holds at least one 2 MB block).
const SIDE_BITS: u32 = 6;
const SIDE_SHARDS: usize = 1 << SIDE_BITS;

#[derive(Clone, Copy)]
#[repr(C, align(32))]
struct LEnt {
    key: Key16,
    person: Key16,
}

const LEMPTY: LEnt = LEnt { key: Key16::EMPTY, person: Key16::EMPTY };

pub struct LinkTable {
    tab: Vec<LEnt>,
    rbits: u32, // log2 slots per region
    bloom: Vec<u64>,
    bloom_mask: usize,
}

#[inline(always)]
fn bloom_word(h: u64, mask: usize) -> usize {
    ((h >> 32) as usize) & mask
}
#[inline(always)]
fn bloom_bits(h: u64) -> u64 {
    (1u64 << (h & 63)) | (1u64 << ((h >> 6) & 63))
}

#[inline(always)]
pub fn prefetch(p: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_T0)
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, readonly, preserves_flags))
    }
}

impl LinkTable {
    #[inline(always)]
    fn slot(&self, h: u64) -> usize {
        (((h >> (64 - REGION_BITS)) as usize) << self.rbits) | (h as usize & ((1 << self.rbits) - 1))
    }

    #[inline(always)]
    pub fn maybe(&self, h: u64) -> bool {
        let b = bloom_bits(h);
        unsafe { *self.bloom.get_unchecked(bloom_word(h, self.bloom_mask)) & b == b }
    }

    #[inline(always)]
    pub fn prefetch(&self, h: u64) {
        prefetch(unsafe { self.tab.as_ptr().add(self.slot(h)) } as *const u8);
    }

    /// Person of `uid` if it is a linked, non-root ID.
    #[inline(always)]
    pub fn lookup(&self, h: u64, uid: Key16) -> Option<Key16> {
        let rmask = (1usize << self.rbits) - 1;
        let base = ((h >> (64 - REGION_BITS)) as usize) << self.rbits;
        let mut i = h as usize & rmask;
        loop {
            let e = unsafe { self.tab.get_unchecked(base + i) };
            if e.key == uid {
                return Some(e.person);
            }
            if e.key.lo == 0 {
                return None;
            }
            i = (i + 1) & rmask;
        }
    }

    #[cfg(test)]
    pub fn resolve(&self, uid: Key16) -> Key16 {
        let h = uid.hash(SEED_L);
        if !self.maybe(h) {
            return uid;
        }
        self.lookup(h, uid).unwrap_or(uid)
    }
}

// ---------------------------------------------------------------------------
// per-thread resolver: L1 direct-mapped cache + Bloom + prefetch ring

#[derive(Clone, Copy)]
struct CEnt {
    uid: Key16,
    person: Key16,
    phash: u64,
}

const CACHE: usize = 128;
/// Items between the Bloom-word prefetch and the Bloom test.
const RA: usize = 8;
/// Items between the table-slot prefetch and the table probe.
const RB: usize = 16;

type Item<R> = (R, Key16, u64);

/// Two-stage software pipeline: push -> (cache hit: emit) -> prefetch Bloom word ->
/// RA items later: Bloom test (negative: emit uid itself) -> prefetch table slot ->
/// RB items later: exact table probe -> emit. Output order differs from input order.
pub struct Resolver<'a, R: Copy> {
    lt: &'a LinkTable,
    cache: [CEnt; CACHE],
    a: [Item<R>; RA],
    ah: usize,
    at: usize,
    b: [Item<R>; RB],
    bh: usize,
    bt: usize,
}

impl<'a, R: Copy> Resolver<'a, R> {
    pub fn new(lt: &'a LinkTable, zero: R) -> Self {
        Resolver {
            lt,
            cache: [CEnt { uid: Key16::EMPTY, person: Key16::EMPTY, phash: 0 }; CACHE],
            a: [(zero, Key16::EMPTY, 0); RA],
            ah: 0,
            at: 0,
            b: [(zero, Key16::EMPTY, 0); RB],
            bh: 0,
            bt: 0,
        }
    }

    /// Resolves `uid` to (person, person hash) and eventually calls `emit` for it.
    #[inline(always)]
    pub fn push(&mut self, item: R, uid: Key16, emit: &mut impl FnMut(R, Key16, u64)) {
        let h = uid.hash(SEED_L);
        let c = unsafe { self.cache.get_unchecked((h as usize) & (CACHE - 1)) };
        if c.uid == uid {
            emit(item, c.person, c.phash);
            return;
        }
        prefetch(unsafe { self.lt.bloom.as_ptr().add(bloom_word(h, self.lt.bloom_mask)) } as *const u8);
        if self.at - self.ah == RA {
            let x = self.a[self.ah % RA];
            self.ah += 1;
            self.stage_b(x, emit);
        }
        self.a[self.at % RA] = (item, uid, h);
        self.at += 1;
    }

    #[inline(always)]
    fn stage_b(&mut self, x: Item<R>, emit: &mut impl FnMut(R, Key16, u64)) {
        let (item, uid, h) = x;
        if !self.lt.maybe(h) {
            self.cache[(h as usize) & (CACHE - 1)] = CEnt { uid, person: uid, phash: h };
            emit(item, uid, h);
            return;
        }
        self.lt.prefetch(h);
        if self.bt - self.bh == RB {
            let y = self.b[self.bh % RB];
            self.bh += 1;
            self.finish_one(y, emit);
        }
        self.b[self.bt % RB] = x;
        self.bt += 1;
    }

    #[inline(always)]
    fn finish_one(&mut self, y: Item<R>, emit: &mut impl FnMut(R, Key16, u64)) {
        let (item, uid, h) = y;
        let (person, phash) = match self.lt.lookup(h, uid) {
            Some(p) => (p, p.hash(SEED_A)),
            None => (uid, h),
        };
        self.cache[(h as usize) & (CACHE - 1)] = CEnt { uid, person, phash };
        emit(item, person, phash);
    }

    pub fn drain(&mut self, emit: &mut impl FnMut(R, Key16, u64)) {
        while self.ah < self.at {
            let x = self.a[self.ah % RA];
            self.ah += 1;
            self.stage_b(x, emit);
        }
        while self.bh < self.bt {
            let y = self.b[self.bh % RB];
            self.bh += 1;
            self.finish_one(y, emit);
        }
        (self.ah, self.at, self.bh, self.bt) = (0, 0, 0, 0);
    }
}

// ---------------------------------------------------------------------------
// build

#[derive(Clone, Copy)]
#[repr(C)]
struct LSide {
    key: Key16,
    ep: u64, // thread << 40 | endpoint slot
}

pub fn build_links(files: &[&'static [u8]], threads: usize) -> LinkTable {
    let chunks = make_chunks(files, chunk_target(files, threads).min(1 << 20));
    let next = AtomicUsize::new(0);
    let sides: ShardSet<LSide> = ShardSet::new_shared(SIDE_SHARDS);
    // Per thread: number of edges.
    let edges: Vec<usize> = par(threads, |t| {
        let mut w = Scatter::new(&sides);
        let mut ne = 0u64;
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            if i >= chunks.len() {
                break;
            }
            let d = chunks[i].data;
            populate(d);
            csv::scan::<2, _>(d, |f| {
                let (a, b) = (f[0], f[1]);
                if a.is_empty() || b.is_empty() {
                    return;
                }
                for (side, k) in [a, b].into_iter().enumerate() {
                    let key = Key16::new(k);
                    let s = (key.hash(SEED_L) >> (64 - SIDE_BITS)) as usize;
                    w.push(s, LSide { key, ep: ((t as u64) << 40) | (ne * 2 + side as u64) });
                }
                ne += 1;
            });
            release_chunk(d);
        }
        w.finish();
        ne as usize
    });

    // Intern per shard; record each endpoint's (shard, local node).
    // Every slot in `eps` is written once before being read, so skip single-threaded zero-fill.
    let mut eps: Vec<Vec<u64>> = edges
        .iter()
        .map(|&e| {
            let mut v = Vec::with_capacity(e * 2);
            unsafe { v.set_len(e * 2) };
            v
        })
        .collect();
    let ep_ptrs: Vec<SyncPtr> = eps.iter_mut().map(|v| SyncPtr(v.as_mut_ptr())).collect();
    let next = AtomicUsize::new(0);
    let shard_keys: Vec<Vec<(usize, Vec<Key16>)>> = par(threads, |_| {
        let mut res = Vec::new();
        let mut tab: Vec<u32> = Vec::new();
        loop {
            let s = next.fetch_add(1, Ordering::Relaxed);
            if s >= SIDE_SHARDS {
                break;
            }
            let segs = sides.take(s);
            let n = segs.len();
            let cap = (n * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            tab.clear();
            tab.resize(cap, 0);
            let mut keys: Vec<Key16> = Vec::new();
            for seg in segs.iter() {
                for side in seg {
                    let mut i = side.key.hash(SEED_L) as usize & mask;
                    let node;
                    loop {
                        let e = tab[i];
                        if e == 0 {
                            node = keys.len() as u32;
                            keys.push(side.key);
                            tab[i] = node + 1;
                            break;
                        }
                        if keys[(e - 1) as usize] == side.key {
                            node = e - 1;
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                    let ep = ep_ptrs[(side.ep >> 40) as usize].0;
                    unsafe { *ep.add((side.ep & ((1 << 40) - 1)) as usize) = ((s as u64) << 32) | node as u64 };
                }
            }
            segs.free();
            res.push((s, keys));
        }
        res
    });
    sides.free_all();
    let mut per_shard: Vec<Vec<Key16>> = (0..SIDE_SHARDS).map(|_| Vec::new()).collect();
    for (s, k) in shard_keys.into_iter().flatten() {
        per_shard[s] = k;
    }
    let mut base = vec![0u32; SIDE_SHARDS + 1];
    for s in 0..SIDE_SHARDS {
        base[s + 1] = base[s] + per_shard[s].len() as u32;
    }
    let n = base[SIDE_SHARDS] as usize;
    let mut keys: Vec<Key16> = Vec::with_capacity(n);
    unsafe { keys.set_len(n) };
    let mut parent: Vec<AtomicU32> = Vec::with_capacity(n);
    unsafe { parent.set_len(n) };
    {
        let kp = SyncPtr(keys.as_mut_ptr() as *mut u64);
        let pp = SyncPtr(parent.as_mut_ptr() as *mut u64);
        let kp = &kp;
        let pp = &pp;
        let per_shard = &per_shard;
        let base = &base;
        let step_s = SIDE_SHARDS.div_ceil(threads).max(1);
        par(threads, |t| {
            let kdst = kp.0 as *mut Key16;
            let pdst = pp.0 as *mut AtomicU32;
            for s in (t * step_s).min(SIDE_SHARDS)..((t + 1) * step_s).min(SIDE_SHARDS) {
                let b = base[s] as usize;
                let src = &per_shard[s];
                if !src.is_empty() {
                    unsafe {
                        std::ptr::copy_nonoverlapping(src.as_ptr(), kdst.add(b), src.len());
                    }
                }
                for i in b..b + src.len() {
                    unsafe { pdst.add(i).write(AtomicU32::new(i as u32)) };
                }
            }
        });
    }
    drop(per_shard);
    let keys = &keys;
    let parent = &parent;
    let less = |a: u32, b: u32| keys[a as usize].cmp_bytes(&keys[b as usize]) == std::cmp::Ordering::Less;
    let find = |mut x: u32| -> u32 {
        loop {
            let p = parent[x as usize].load(Ordering::Relaxed);
            if p == x {
                return x;
            }
            let g = parent[p as usize].load(Ordering::Relaxed);
            if g != p {
                let _ = parent[x as usize].compare_exchange_weak(p, g, Ordering::Relaxed, Ordering::Relaxed);
            }
            x = p;
        }
    };
    {
        const JOB: usize = 1 << 16;
        let jobs: Vec<(usize, usize)> =
            eps.iter().enumerate().flat_map(|(t, v)| (0..v.len()).step_by(JOB).map(move |o| (t, o))).collect();
        let next = AtomicUsize::new(0);
        let gid = |v: u64| base[(v >> 32) as usize] + (v as u32);
        par(threads, |_| loop {
            let j = next.fetch_add(1, Ordering::Relaxed);
            if j >= jobs.len() {
                break;
            }
            let (t, o) = jobs[j];
            let v = &eps[t][o..(o + JOB).min(eps[t].len())];
            for e in v.chunks_exact(2) {
                let (mut a, mut b) = (gid(e[0]), gid(e[1]));
                loop {
                    a = find(a);
                    b = find(b);
                    if a == b {
                        break;
                    }
                    let (lo, hi) = if less(a, b) { (a, b) } else { (b, a) };
                    if parent[hi as usize]
                        .compare_exchange(hi, lo, Ordering::Relaxed, Ordering::Relaxed)
                        .is_ok()
                    {
                        break;
                    }
                }
            }
        });
    }
    drop(eps);

    // Non-root nodes, grouped by table region (per thread, per region).
    let step = n.div_ceil(threads).max(1);
    let groups: Vec<Vec<Vec<u64>>> = par(threads, |t| {
        let mut g: Vec<Vec<u64>> = (0..REGIONS).map(|_| Vec::new()).collect();
        for x in (t * step).min(n)..((t + 1) * step).min(n) {
            let r = find(x as u32);
            if r != x as u32 {
                let h = keys[x].hash(SEED_L);
                g[(h >> (64 - REGION_BITS)) as usize].push(((r as u64) << 32) | x as u64);
            }
        }
        g
    });
    let mut counts = vec![0usize; REGIONS];
    for g in &groups {
        for (r, v) in g.iter().enumerate() {
            counts[r] += v.len();
        }
    }
    let total_nr: usize = counts.iter().sum();
    let bloom_words = ((total_nr * 12 / 64).next_power_of_two()).clamp(1 << 10, 1 << 18);
    let bloom_mask = bloom_words - 1;
    let maxc = counts.iter().copied().max().unwrap_or(0);
    let rbits = ((maxc + maxc / 2 + 1).next_power_of_two().max(4)).trailing_zeros();
    let mut tab = vec![LEMPTY; REGIONS << rbits];
    #[cfg(target_os = "linux")]
    if tab.len() * std::mem::size_of::<LEnt>() >= (2 << 20) {
        unsafe {
            libc::madvise(
                tab.as_mut_ptr() as *mut libc::c_void,
                tab.len() * std::mem::size_of::<LEnt>(),
                libc::MADV_HUGEPAGE,
            );
        }
    }
    let bloom: Vec<AtomicU64> = (0..bloom_words).map(|_| AtomicU64::new(0)).collect();
    {
        let tp = SyncPtr(tab.as_mut_ptr() as *mut u64);
        let tp = &tp;
        let bloom = &bloom;
        let groups = &groups;
        let next = AtomicUsize::new(0);
        par(threads, |_| loop {
            let r = next.fetch_add(1, Ordering::Relaxed);
            if r >= REGIONS {
                break;
            }
            let tab = tp.0 as *mut LEnt;
            let rmask = (1usize << rbits) - 1;
            for g in groups {
                for &v in &g[r] {
                    let (root, x) = ((v >> 32) as usize, v as u32 as usize);
                    let key = keys[x];
                    let h = key.hash(SEED_L);
                    bloom[bloom_word(h, bloom_mask)].fetch_or(bloom_bits(h), Ordering::Relaxed);
                    let mut i = h as usize & rmask;
                    unsafe {
                        while (*tab.add((r << rbits) + i)).key.lo != 0 {
                            i = (i + 1) & rmask;
                        }
                        *tab.add((r << rbits) + i) = LEnt { key, person: keys[root] };
                    }
                }
            }
        });
    }
    LinkTable { tab, rbits, bloom: bloom.into_iter().map(|a| a.into_inner()).collect(), bloom_mask }
}

/// Fast zero-sync single-threaded link-table builder (retained for reference/tests).
#[allow(dead_code)]
pub fn build_links_single(files: &[&'static [u8]]) -> LinkTable {
    let total_bytes: usize = files.iter().map(|d| d.len()).sum();
    let est = (total_bytes / 18).max(64);
    let mut cap = (est * 2).next_power_of_two().max(256);
    let mut tab: Vec<u32> = vec![0; cap];
    let mut mask = cap - 1;
    let mut keys: Vec<Key16> = Vec::with_capacity(est);
    let mut parent: Vec<u32> = Vec::with_capacity(est);

    #[inline(always)]
    fn find_seq(parent: &mut [u32], mut x: u32) -> u32 {
        loop {
            let p = unsafe { *parent.get_unchecked(x as usize) };
            if p == x {
                return x;
            }
            let g = unsafe { *parent.get_unchecked(p as usize) };
            unsafe { *parent.get_unchecked_mut(x as usize) = g };
            x = g;
        }
    }

    #[cold]
    fn grow(tab: &mut Vec<u32>, cap: &mut usize, mask: &mut usize, keys: &[Key16]) {
        *cap *= 2;
        *mask = *cap - 1;
        tab.clear();
        tab.resize(*cap, 0);
        let m = *mask;
        for (id, k) in keys.iter().enumerate() {
            let mut i = k.hash(SEED_L) as usize & m;
            while unsafe { *tab.get_unchecked(i) } != 0 {
                i = (i + 1) & m;
            }
            unsafe { *tab.get_unchecked_mut(i) = id as u32 + 1 };
        }
    }

    for &d in files {
        let start = match d.iter().position(|&c| c == b'\n') {
            Some(i) => i + 1,
            None => continue,
        };
        let body = &d[start..];
        if body.is_empty() {
            continue;
        }
        populate(body);
        csv::scan::<2, _>(body, |f| {
            let (fa, fb) = (f[0], f[1]);
            if fa.is_empty() || fb.is_empty() {
                return;
            }
            let mut intern = |k: Key16| -> u32 {
                if keys.len() * 2 >= cap {
                    grow(&mut tab, &mut cap, &mut mask, &keys);
                }
                let mut i = k.hash(SEED_L) as usize & mask;
                loop {
                    let e = unsafe { *tab.get_unchecked(i) };
                    if e == 0 {
                        let id = keys.len() as u32;
                        keys.push(k);
                        parent.push(id);
                        unsafe { *tab.get_unchecked_mut(i) = id + 1 };
                        return id;
                    }
                    let id = e - 1;
                    if unsafe { *keys.get_unchecked(id as usize) } == k {
                        return id;
                    }
                    i = (i + 1) & mask;
                }
            };
            let ia = intern(Key16::new(fa));
            let ib = intern(Key16::new(fb));
            let a = find_seq(&mut parent, ia);
            let b = find_seq(&mut parent, ib);
            if a != b {
                if keys[a as usize].cmp_bytes(&keys[b as usize]) == std::cmp::Ordering::Less {
                    unsafe { *parent.get_unchecked_mut(b as usize) = a };
                } else {
                    unsafe { *parent.get_unchecked_mut(a as usize) = b };
                }
            }
        });
        release_chunk(body);
    }
    drop(tab);

    let n = keys.len();
    let mut counts = [0usize; REGIONS];
    let mut non_roots: Vec<(u32, u32, u64)> = Vec::with_capacity(n / 2 + 16);
    for x in 0..n {
        let r = find_seq(&mut parent, x as u32);
        if r != x as u32 {
            let h = unsafe { keys.get_unchecked(x) }.hash(SEED_L);
            counts[(h >> (64 - REGION_BITS)) as usize] += 1;
            non_roots.push((x as u32, r, h));
        }
    }
    drop(parent);

    let total_nr = non_roots.len();
    let bloom_words = ((total_nr * 12 / 64).next_power_of_two()).clamp(1 << 10, 1 << 18);
    let bloom_mask = bloom_words - 1;
    let maxc = counts.iter().copied().max().unwrap_or(0);
    let rbits = ((maxc + maxc / 2 + 1).next_power_of_two().max(4)).trailing_zeros();
    let mut ltab = vec![LEMPTY; REGIONS << rbits];
    #[cfg(target_os = "linux")]
    if ltab.len() * std::mem::size_of::<LEnt>() >= (2 << 20) {
        unsafe {
            libc::madvise(
                ltab.as_mut_ptr() as *mut libc::c_void,
                ltab.len() * std::mem::size_of::<LEnt>(),
                libc::MADV_HUGEPAGE,
            );
        }
    }
    let mut bloom = vec![0u64; bloom_words];
    let rmask = (1usize << rbits) - 1;
    let lptr = ltab.as_mut_ptr();
    for &(x, root, h) in &non_roots {
        unsafe {
            *bloom.get_unchecked_mut(bloom_word(h, bloom_mask)) |= bloom_bits(h);
            let r = (h >> (64 - REGION_BITS)) as usize;
            let base = r << rbits;
            let mut i = h as usize & rmask;
            while (*lptr.add(base + i)).key.lo != 0 {
                i = (i + 1) & rmask;
            }
            *lptr.add(base + i) = LEnt {
                key: *keys.get_unchecked(x as usize),
                person: *keys.get_unchecked(root as usize),
            };
        }
    }
    LinkTable { tab: ltab, rbits, bloom, bloom_mask }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_min_name_resolution() {
        // Chain of 1000 hops in shuffled order, NUL/length-only differences, long IDs.
        let mut lines = String::from("user_id_a,user_id_b\n");
        let mut order: Vec<usize> = (0..999).collect();
        let mut x = 0x9e37_79b9u64;
        for i in (1..order.len()).rev() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            order.swap(i, (x % (i as u64 + 1)) as usize);
        }
        for &i in &order {
            lines.push_str(&format!("n{:04},n{:04}\n", i + 1, i));
        }
        lines.push_str("a\0,a\0b\nb,a\0\0\n");
        let long_a = "L".repeat(15) + "zz";
        let long_b = "L".repeat(15) + "ab";
        lines.push_str(&format!("{long_a},{long_b}\nM,{long_a}\nq,q\n"));
        let data: &'static [u8] = Box::leak(lines.into_bytes().into_boxed_slice());
        for mode in [0usize, 1, 4] {
            let lt = if mode == 0 { build_links_single(&[data]) } else { build_links(&[data], mode) };
            for i in 0..1000 {
                assert_eq!(lt.resolve(Key16::new(format!("n{i:04}").as_bytes())), Key16::new(b"n0000"));
            }
            assert_eq!(lt.resolve(Key16::new(b"a\0b")), Key16::new(b"a\0"));
            assert_eq!(lt.resolve(Key16::new(b"b")), Key16::new(b"a\0\0"));
            assert_eq!(lt.resolve(Key16::new(b"a")), Key16::new(b"a"));
            assert_eq!(lt.resolve(Key16::new(long_a.as_bytes())), Key16::new(long_b.as_bytes()));
            assert_eq!(lt.resolve(Key16::new(b"M")), Key16::new(long_b.as_bytes()));
            assert_eq!(lt.resolve(Key16::new(b"q")), Key16::new(b"q"));
            assert_eq!(lt.resolve(Key16::new(b"unlinked")), Key16::new(b"unlinked"));
        }
    }
}
