//! Shopping Sessions pipeline.
//!
//! Stages: init (manifest, products, campaigns, mmaps) -> links (exact Key16
//! interning, lock-free union-find, LinkTable + Bloom) -> clicks (parse, validate,
//! resolve person, scatter to person shards) -> events (parse, validate, scatter a
//! compact RawEv to event_id shards) -> dedup (per event_id shard: exact dedup,
//! resolve person, scatter PEv to person shards) -> solve (per person shard:
//! sessionize, attribute, assist, aggregate) -> write Parquet.
//!
//! All shard data lives in pooled 2 MB blocks (`arena`), written through
//! write-combining staging lines, and is returned to the pool as soon as each
//! shard is consumed; the pool is released in parallel before exit.

mod arena;
mod csv;
mod hash;
mod key;
mod links;
mod parquet;
mod ts;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use arena::{ShardSet, Scatter, Segs};
use hash::hash;
use key::Key16;
use links::{LinkTable, Resolver, SEED_A};
use parquet::{Column, Kind};
use ts::{parse_f1_fast, parse_price, parse_ts, DAY_MS};

const GAP_MS: i64 = 1_800_000;
const SEVEN_DAYS_MS: i64 = 7 * DAY_MS;
const LATE_MS: i64 = DAY_MS;

const SEED_ID: u64 = 0x4b33_a62e_d433_d4a3;
const SEED_MAP: u64 = 0x8bb8_4b93_962e_acc9;

const ID_BITS: u32 = 8;
const ID_SHARDS: usize = 1 << ID_BITS;
const P_BITS: u32 = 8;
const P_SHARDS: usize = 1 << P_BITS;

pub(crate) struct SyncPtr(pub *mut u64);
unsafe impl Sync for SyncPtr {}
unsafe impl Send for SyncPtr {}

// ---------------------------------------------------------------------------
// mmap

struct Mmap {
    ptr: *const u8,
    len: usize,
}

impl Mmap {
    fn open(path: &Path) -> std::io::Result<Mmap> {
        use std::os::unix::io::AsRawFd;
        let f = std::fs::File::open(path)?;
        let len = f.metadata()?.len() as usize;
        if len == 0 {
            return Ok(Mmap { ptr: std::ptr::NonNull::<u8>::dangling().as_ptr(), len: 0 });
        }
        let p = unsafe {
            libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_PRIVATE, f.as_raw_fd(), 0)
        };
        if p == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Mmap { ptr: p as *const u8, len })
    }
    fn data(&self) -> &'static [u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            let name = e.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".csv") && p.is_file() {
                v.push(p);
            }
        }
    }
    v.sort();
    v
}

fn map_dir(dir: &Path) -> Vec<&'static [u8]> {
    list_csv(dir)
        .iter()
        .map(|p| {
            // Mappings live for the whole process (no Drop / munmap).
            Mmap::open(p).unwrap_or_else(|e| panic!("mmap {}: {e}", p.display())).data()
        })
        .collect()
}

#[derive(Clone, Copy)]
pub(crate) struct Chunk {
    pub data: &'static [u8],
}

/// Splits every file's body (after its header line) into newline-aligned chunks.
pub(crate) fn make_chunks(files: &[&'static [u8]], target: usize) -> Vec<Chunk> {
    let mut out = Vec::new();
    for &d in files {
        let start = match d.iter().position(|&c| c == b'\n') {
            Some(i) => i + 1,
            None => continue,
        };
        let mut s = start;
        while s < d.len() {
            let mut e = (s + target).min(d.len());
            if e < d.len() {
                e = match d[e..].iter().position(|&c| c == b'\n') {
                    Some(i) => e + i + 1,
                    None => d.len(),
                };
            }
            out.push(Chunk { data: &d[s..e] });
            s = e;
        }
    }
    // Largest first for better balance.
    out.sort_by_key(|c| std::cmp::Reverse(c.data.len()));
    out
}

pub(crate) fn chunk_target(files: &[&'static [u8]], threads: usize) -> usize {
    let total: usize = files.iter().map(|d| d.len()).sum();
    (total / (threads * 24)).clamp(256 << 10, 16 << 20)
}

pub(crate) fn par<R: Send>(threads: usize, f: impl Fn(usize) -> R + Sync) -> Vec<R> {
    std::thread::scope(|s| {
        let f = &f;
        let hs: Vec<_> = (0..threads).map(|t| s.spawn(move || f(t))).collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

#[inline(always)]
pub(crate) fn populate(d: &[u8]) {
    #[cfg(target_os = "linux")]
    unsafe {
        // MADV_POPULATE_READ (Linux 5.14+); harmless EINVAL elsewhere.
        let page = 4096usize;
        let a = (d.as_ptr() as usize) & !(page - 1);
        let e = d.as_ptr() as usize + d.len();
        libc::madvise(a as *mut libc::c_void, e - a, 22);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = d;
}

/// Drops the page-table entries of a fully parsed input chunk (whole pages only),
/// spreading the unmap work over the parse threads instead of process exit. The
/// mapping stays valid: a later access just re-reads the page cache.
#[inline(always)]
pub(crate) fn release_chunk(d: &[u8]) {
    #[cfg(target_os = "linux")]
    unsafe {
        let page = 4096usize;
        let a = (d.as_ptr() as usize + page - 1) & !(page - 1);
        let e = (d.as_ptr() as usize + d.len()) & !(page - 1);
        if e > a {
            libc::madvise(a as *mut libc::c_void, e - a, libc::MADV_DONTNEED);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = d;
}

// ---------------------------------------------------------------------------
// small string maps (products, campaigns)

struct StrMap {
    slots: Vec<(u64, &'static [u8], u32)>,
    mask: usize,
}

impl StrMap {
    fn new(n: usize) -> StrMap {
        let cap = (n * 2).next_power_of_two().max(16);
        StrMap { slots: vec![(0, &[][..], u32::MAX); cap], mask: cap - 1 }
    }
    fn insert(&mut self, k: &'static [u8], v: u32) {
        let h = hash(k, SEED_MAP);
        let mut i = h as usize & self.mask;
        loop {
            let s = &mut self.slots[i];
            if s.2 == u32::MAX {
                *s = (h, k, v);
                return;
            }
            if s.0 == h && s.1 == k {
                return; // first wins (keys are unique anyway)
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    fn get(&self, k: &[u8]) -> Option<u32> {
        let h = hash(k, SEED_MAP);
        let mut i = h as usize & self.mask;
        loop {
            let s = &self.slots[i];
            if s.2 == u32::MAX {
                return None;
            }
            if s.0 == h && s.1 == k {
                return Some(s.2);
            }
            i = (i + 1) & self.mask;
        }
    }
}

fn read_static(path: &Path) -> &'static [u8] {
    match std::fs::read(path) {
        Ok(v) => Box::leak(v.into_boxed_slice()),
        Err(_) => &[],
    }
}

fn body(d: &'static [u8]) -> &'static [u8] {
    match d.iter().position(|&c| c == b'\n') {
        Some(i) => &d[i + 1..],
        None => &[],
    }
}

// ---------------------------------------------------------------------------
// records

const TS_INVALID: u32 = u32::MAX;

/// One parsed event line of any validity (48 bytes), sharded by event_id.
/// `ingest` = biased epoch ms << 8 | category bits 8..16; `price` = price | category
/// bits 0..8 << 24. `ts == TS_INVALID` marks a copy that only takes part in dedup.
#[derive(Clone, Copy)]
#[repr(C)]
struct RawEv {
    eid: Key16,
    uid: Key16,
    ingest: u64,
    ts: u32,
    price: u32,
}

const INGEST_BIAS: i64 = 1 << 55;

#[inline(always)]
fn enc_ingest(ms: Option<i64>, cat: u32) -> u64 {
    let v = match ms {
        Some(ms) => (ms.clamp(-INGEST_BIAS, INGEST_BIAS - 2) + INGEST_BIAS) as u64,
        None => (1u64 << 56) - 1,
    };
    (v << 8) | (cat >> 8) as u64
}

/// A surviving event, sharded by person (24 bytes). `pc` is 0 for non-purchases;
/// otherwise price << 8 | category, or (with more than 256 categories) 1 + an
/// index into the dedup threads' side tables.
#[derive(Clone, Copy)]
#[repr(C)]
struct PEv {
    person: Key16,
    ts: u32,
    pc: u32,
}

/// A valid click, sharded by person (48 bytes).
#[derive(Clone, Copy)]
#[repr(C)]
struct Click {
    person: Key16,
    cid: Key16,
    phash: u64,
    ts: u32,
    camp: u32,
}

/// A person-shard event inside solve (32 bytes): `key` = person-hash low 32 bits
/// << 32 | ts, so small buckets sort on one u64.
#[derive(Clone, Copy)]
#[repr(C)]
struct SEv {
    key: u64,
    person: Key16,
    pc: u32,
    _pad: u32,
}

const _: () = assert!(std::mem::size_of::<RawEv>() == 48);
const _: () = assert!(std::mem::size_of::<PEv>() == 24);
const _: () = assert!(std::mem::size_of::<Click>() == 48);

struct Ctx {
    ws: i64,
    base: i64,
    we: i64,
    day0: i64,
    ndays: usize,
    products: StrMap,
    unknown_cat: u32,
    cat_names: Vec<&'static [u8]>,
    wide: bool,
    camps: StrMap,
    camp_ids: Vec<&'static [u8]>,
    camp_channels: Vec<&'static [u8]>,
    links: LinkTable,
}

/// Dedup-table slot: a short tag of the event_id hash plus 1 + record index.
trait Slot: Copy {
    fn make(h: u64, me: u32) -> Self;
    fn is_empty(self) -> bool;
    fn tag_eq(self, h: u64) -> bool;
    fn idx(self) -> u32;
}

impl Slot for u32 {
    #[inline(always)]
    fn make(h: u64, me: u32) -> u32 {
        (((h >> 24) as u32) << 24) | (me + 1)
    }
    #[inline(always)]
    fn is_empty(self) -> bool {
        self == 0
    }
    #[inline(always)]
    fn tag_eq(self, h: u64) -> bool {
        (self >> 24) == ((h >> 24) as u32 & 0xFF)
    }
    #[inline(always)]
    fn idx(self) -> u32 {
        (self & 0xFF_FFFF) - 1
    }
}

impl Slot for u64 {
    #[inline(always)]
    fn make(h: u64, me: u32) -> u64 {
        (((h >> 24) as u32 as u64) << 32) | (me as u64 + 1)
    }
    #[inline(always)]
    fn is_empty(self) -> bool {
        self == 0
    }
    #[inline(always)]
    fn tag_eq(self, h: u64) -> bool {
        (self >> 32) == (h >> 24) as u32 as u64
    }
    #[inline(always)]
    fn idx(self) -> u32 {
        self as u32 - 1
    }
}

/// Exact event_id dedup of one shard: marks in `loser` (bit per record index
/// `seg << 9 | rec`) every copy that is not the one with the smallest ingest_ts
/// (the first one read on a tie). Tag matches are confirmed on the full Key16.
fn mark_losers<S: Slot + Default + Clone>(segs: &Segs<RawEv>, table: &mut Vec<S>, cap: usize, loser: &mut [u64]) {
    let mask = cap - 1;
    table.clear();
    table.resize(cap, S::default());
    let rec = |idx: u32| -> &RawEv { unsafe { segs.seg((idx >> 9) as usize).get_unchecked((idx & 511) as usize) } };
    for si in 0..segs.segs.len() {
        for (ri, r) in segs.seg(si).iter().enumerate() {
            let h = r.eid.hash(SEED_ID);
            let me = ((si << 9) | ri) as u32;
            let mut i = h as usize & mask;
            loop {
                let slot = unsafe { *table.get_unchecked(i) };
                if slot.is_empty() {
                    table[i] = S::make(h, me);
                    break;
                }
                if slot.tag_eq(h) {
                    let oi = slot.idx();
                    let o = rec(oi);
                    if o.eid == r.eid {
                        if (r.ingest >> 8) < (o.ingest >> 8) {
                            loser[(oi >> 6) as usize] |= 1 << (oi & 63);
                            table[i] = S::make(h, me);
                        } else {
                            loser[(me >> 6) as usize] |= 1 << (me & 63);
                        }
                        break;
                    }
                }
                i = (i + 1) & mask;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// solve

#[derive(Clone, Copy, Default)]
struct CampAcc {
    sessions: u32,
    purchasing: u32,
    assisted: u32,
    revenue: u64,
}

#[derive(Clone, Copy)]
struct URow {
    person: Key16,
    revenue: u64,
    day: i32,
    sessions: u32,
    events: u32,
    top_cat: i32,
}

#[derive(Clone, Copy)]
struct Sess {
    start: i64,
    rev: u64,
    day: u32,
    purch: bool,
}

type Side = Vec<(u32, u16)>;

struct Worker<'a> {
    ctx: &'a Ctx,
    side: &'a [Side],
    k1: usize,
    cnt: Vec<u32>,
    since: Vec<u32>,
    acc: Vec<CampAcc>,
    cat_rev: Vec<u64>,
    touched: Vec<u32>,
    sess: Vec<Sess>,
    day_first: Vec<u32>,
    rows: Vec<URow>,
}

impl<'a> Worker<'a> {
    fn new(ctx: &'a Ctx, side: &'a [Side]) -> Worker<'a> {
        let k1 = ctx.camp_ids.len();
        Worker {
            ctx,
            side,
            k1,
            cnt: vec![0; k1],
            since: vec![0; k1],
            acc: vec![CampAcc::default(); k1 * ctx.ndays],
            cat_rev: vec![0; ctx.cat_names.len()],
            touched: Vec::new(),
            sess: Vec::new(),
            day_first: vec![0; ctx.ndays + 1],
            rows: Vec::new(),
        }
    }

    #[inline(always)]
    fn price_cat(&self, pc: u32) -> (u64, usize) {
        if !self.ctx.wide {
            ((pc >> 8) as u64, (pc & 0xFF) as usize)
        } else {
            let v = pc - 1;
            let (p, c) = self.side[(v >> 24) as usize][(v & 0xFF_FFFF) as usize];
            (p as u64, c as usize)
        }
    }

    #[inline]
    fn flush_row(&mut self, person: Key16, day: u32, ns: u32, ne: u32, rev: u64) {
        let mut top = -1i32;
        let mut best = 0u64;
        for &c in &self.touched {
            let v = self.cat_rev[c as usize];
            if v > best || (v == best && (c as i32) < top) {
                best = v;
                top = c as i32;
            }
            self.cat_rev[c as usize] = 0;
        }
        self.touched.clear();
        self.rows.push(URow {
            person,
            day: (self.ctx.day0 + day as i64) as i32,
            sessions: ns,
            events: ne,
            revenue: rev,
            top_cat: top,
        });
    }

    #[inline]
    fn close(&mut self, camp: usize, a: u32, b: u32) {
        if a >= b {
            return;
        }
        let first = self.sess[a as usize].day as usize;
        let last = self.sess[b as usize - 1].day as usize;
        for d in first..=last {
            let lo = a.max(self.day_first[d]);
            let hi = b.min(self.day_first[d + 1]);
            if hi > lo {
                self.acc[d * self.k1 + camp].assisted += hi - lo;
            }
        }
    }

    /// One person: events sorted by ts, clicks sorted by (ts, click_id).
    fn person(&mut self, person: Key16, evs: &[SEv], clicks: &[Click]) {
        self.sess.clear();
        let day0ms = self.ctx.day0 * DAY_MS - self.ctx.base;
        let mut cur_day = u32::MAX;
        let (mut ns, mut ne, mut rev) = (0u32, 0u32, 0u64);
        let mut prev = i64::MIN;
        for e in evs {
            let ts = e.key as u32 as i64;
            if self.sess.is_empty() || ts - prev > GAP_MS {
                let day = ((ts - day0ms) / DAY_MS) as u32;
                if day != cur_day {
                    if cur_day != u32::MAX {
                        self.flush_row(person, cur_day, ns, ne, rev);
                    }
                    cur_day = day;
                    ns = 0;
                    ne = 0;
                    rev = 0;
                }
                self.sess.push(Sess { start: ts, rev: 0, day, purch: false });
                ns += 1;
            }
            prev = ts;
            ne += 1;
            if e.pc != 0 {
                let (p, c) = self.price_cat(e.pc);
                let s = self.sess.last_mut().unwrap();
                s.rev += p;
                s.purch = true;
                rev += p;
                if self.cat_rev[c] == 0 {
                    self.touched.push(c as u32);
                }
                self.cat_rev[c] += p;
            }
        }
        self.flush_row(person, cur_day, ns, ne, rev);

        let k1 = self.k1;
        if clicks.is_empty() {
            for s in &self.sess {
                let a = &mut self.acc[s.day as usize * k1];
                a.sessions += 1;
                a.purchasing += s.purch as u32;
                a.revenue += s.rev;
            }
            return;
        }
        // day_first[d] = first session index with day >= d
        let nsess = self.sess.len();
        {
            let mut j = 0usize;
            for d in 0..=self.ctx.ndays {
                while j < nsess && (self.sess[j].day as usize) < d {
                    j += 1;
                }
                self.day_first[d] = j as u32;
            }
        }
        let n = clicks.len();
        let (mut cl, mut cr) = (0usize, 0usize);
        let mut latest_ts = i64::MIN;
        let mut latest_camp = 0usize;
        for j in 0..nsess {
            let s = self.sess[j];
            while cr < n && clicks[cr].ts as i64 <= s.start {
                let c = &clicks[cr];
                if c.ts as i64 != latest_ts {
                    latest_ts = c.ts as i64;
                    latest_camp = c.camp as usize;
                }
                let k = c.camp as usize;
                self.cnt[k] += 1;
                if self.cnt[k] == 1 {
                    self.since[k] = j as u32;
                }
                cr += 1;
            }
            let lo = s.start - SEVEN_DAYS_MS;
            while cl < cr && (clicks[cl].ts as i64) < lo {
                let k = clicks[cl].camp as usize;
                self.cnt[k] -= 1;
                if self.cnt[k] == 0 {
                    let a = self.since[k];
                    self.close(k, a, j as u32);
                }
                cl += 1;
            }
            let credited = if cl < cr { latest_camp } else { 0 };
            let a = &mut self.acc[s.day as usize * k1 + credited];
            a.sessions += 1;
            a.purchasing += s.purch as u32;
            a.revenue += s.rev;
        }
        for i in cl..cr {
            let k = clicks[i].camp as usize;
            if self.cnt[k] > 0 {
                self.cnt[k] = 0;
                let a = self.since[k];
                self.close(k, a, nsess as u32);
            }
        }
    }

    /// One person shard, bucketed by person hash; within a bucket events are sorted
    /// by (pkey, ts) and clicks by (pkey, ts, click_id). Persons group exactly.
    fn shard(&mut self, evs: &[SEv], ec: &[u32], cls: &[Click], cc: &[u32]) {
        for b in 0..NB {
            let ev = &evs[ec[b] as usize..ec[b + 1] as usize];
            let cl = &cls[cc[b] as usize..cc[b + 1] as usize];
            let (mut i, mut ci) = (0usize, 0usize);
            while i < ev.len() {
                let p = ev[i].person;
                let pk = sev_pk(&ev[i]);
                let mut j = i + 1;
                while j < ev.len() && ev[j].person == p {
                    j += 1;
                }
                while ci < cl.len() && click_pk(&cl[ci]) < pk {
                    ci += 1;
                }
                let mut cj = ci;
                while cj < cl.len() && cl[cj].person == p {
                    cj += 1;
                }
                self.person(p, &ev[i..j], &cl[ci..cj]);
                i = j;
                ci = cj;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// bucket scatter (counting sort on the person-hash bits just below the shard bits)

const NB_BITS: u32 = 14;
const NB: usize = 1 << NB_BITS;

#[inline(always)]
fn bucket_of(phash: u64) -> usize {
    ((phash >> (64 - P_BITS - NB_BITS)) as usize) & (NB - 1)
}

/// Copies all records of `segs` into `out` (transformed by `make`, which also gets
/// the record's person hash) grouped by bucket; afterwards bucket b is
/// `out[counts[b]..counts[b + 1]]`. `hs` caches each record's person hash.
fn bucket_scatter<T: Copy, U: Copy>(
    segs: &Segs<T>,
    phash: impl Fn(&T) -> u64,
    make: impl Fn(&T, u64) -> U,
    counts: &mut Vec<u32>,
    hs: &mut Vec<u64>,
    out: &mut Vec<U>,
) {
    counts.clear();
    counts.resize(NB + 1, 0);
    hs.clear();
    for v in segs.iter() {
        for x in v {
            let h = phash(x);
            hs.push(h);
            counts[bucket_of(h)] += 1;
        }
    }
    let mut sum = 0u32;
    for i in 0..NB {
        let c = counts[i];
        counts[i] = sum;
        sum += c;
    }
    counts[NB] = sum;
    let n = sum as usize;
    out.clear();
    out.reserve(n);
    let o = out.as_mut_ptr();
    let mut k = 0usize;
    for v in segs.iter() {
        for x in v {
            unsafe {
                let h = *hs.get_unchecked(k);
                let c = counts.get_unchecked_mut(bucket_of(h));
                o.add(*c as usize).write(make(x, h));
                *c += 1;
            }
            k += 1;
        }
    }
    unsafe { out.set_len(n) };
    // Cursors now hold bucket ends; shift to starts.
    for i in (1..=NB).rev() {
        counts[i] = counts[i - 1];
    }
    counts[0] = 0;
}

/// Stable LSD radix sort of `v` by a 32-bit key (3 passes of 11 bits; passes in
/// which every key has the same digit are skipped).
fn radix_by_u32<T: Copy>(v: &mut [T], tmp: &mut Vec<T>, key: impl Fn(&T) -> u32) {
    let n = v.len();
    tmp.clear();
    tmp.reserve(n);
    unsafe { tmp.set_len(n) };
    let mut cnt = [[0u32; 2048]; 3];
    for x in v.iter() {
        let k = key(x);
        cnt[0][(k & 2047) as usize] += 1;
        cnt[1][((k >> 11) & 2047) as usize] += 1;
        cnt[2][(k >> 22) as usize] += 1;
    }
    let mut in_v = true;
    for (pass, c) in cnt.iter_mut().enumerate() {
        if c.iter().any(|&x| x as usize == n) {
            continue;
        }
        let mut sum = 0u32;
        for x in c.iter_mut() {
            let t = *x;
            *x = sum;
            sum += t;
        }
        let sh = pass as u32 * 11;
        let (src, dst): (&[T], &mut [T]) = if in_v { (&*v, &mut tmp[..]) } else { (&tmp[..], &mut *v) };
        // SAFETY of the split borrow above: `v` and `tmp` are distinct buffers.
        let dst = dst.as_mut_ptr();
        for x in src {
            let d = ((key(x) >> sh) & 2047) as usize;
            unsafe { dst.add(c[d] as usize).write(*x) };
            c[d] += 1;
        }
        in_v = !in_v;
    }
    if !in_v {
        v.copy_from_slice(&tmp[..n]);
    }
}

/// Person order used throughout solve: (low 32 bits of the person hash, exact key).
type PKey = (u32, Key16);

/// Sorts a big bucket (whale / shared-device persons) by (pkey, ts, tie): radix sort
/// on ts, a stable split by person, then a tie sort of equal-(person, ts) runs.
fn sort_big<T: Copy>(
    sl: &mut [T],
    tmp: &mut Vec<T>,
    pk: impl Fn(&T) -> PKey,
    ts: impl Fn(&T) -> u32,
    tie: Option<fn(&T, &T) -> std::cmp::Ordering>,
) {
    radix_by_u32(sl, tmp, &ts);
    let mut ps: Vec<PKey> = Vec::new();
    for x in sl.iter() {
        let p = pk(x);
        if !ps.contains(&p) {
            ps.push(p);
            if ps.len() > 64 {
                break;
            }
        }
    }
    if ps.len() > 64 {
        // Many persons share this bucket: a stable sort by person keeps ts order.
        sl.sort_by_key(|x| pk(x));
    } else if ps.len() > 1 {
        ps.sort_unstable();
        let mut cnt = vec![0usize; ps.len() + 1];
        for x in sl.iter() {
            cnt[ps.binary_search(&pk(x)).unwrap() + 1] += 1;
        }
        for i in 1..=ps.len() {
            cnt[i] += cnt[i - 1];
        }
        tmp.clear();
        tmp.extend_from_slice(sl);
        for x in tmp.iter() {
            let r = ps.binary_search(&pk(x)).unwrap();
            sl[cnt[r]] = *x;
            cnt[r] += 1;
        }
    }
    if let Some(t) = tie {
        let mut i = 0;
        while i < sl.len() {
            let mut j = i + 1;
            while j < sl.len() && ts(&sl[j]) == ts(&sl[i]) && pk(&sl[j]) == pk(&sl[i]) {
                j += 1;
            }
            if j - i > 1 {
                sl[i..j].sort_unstable_by(t);
            }
            i = j;
        }
    }
}

#[inline(always)]
fn sev_pk(e: &SEv) -> PKey {
    ((e.key >> 32) as u32, e.person)
}

/// Sorts one event bucket by (pkey, ts). Small buckets sort on the single u64 key
/// (ph32, ts); two persons sharing ph32 in one bucket are detected exactly and the
/// bucket is re-sorted by the full (ph32, person, ts).
fn sort_events(sl: &mut [SEv], tmp: &mut Vec<SEv>) {
    if sl.len() < 2 {
        return;
    }
    if sl.len() > 256 {
        sort_big(sl, tmp, sev_pk, |e| e.key as u32, None);
        return;
    }
    sl.sort_unstable_by_key(|e| e.key);
    let collide = sl.windows(2).any(|w| (w[0].key >> 32) == (w[1].key >> 32) && w[0].person != w[1].person);
    if collide {
        sl.sort_unstable_by_key(|e| (e.key >> 32, e.person, e.key as u32));
    }
}

#[inline(always)]
fn click_pk(c: &Click) -> PKey {
    (c.phash as u32, c.person)
}

/// Sorts one click bucket by (pkey, ts, click_id).
fn sort_clicks(sl: &mut [Click], tmp: &mut Vec<Click>) {
    if sl.len() < 2 {
        return;
    }
    if sl.len() > 256 {
        sort_big(sl, tmp, click_pk, |c| c.ts, Some(|a: &Click, b: &Click| a.cid.cmp_bytes(&b.cid)));
        return;
    }
    sl.sort_unstable_by(|a, b| (click_pk(a), a.ts).cmp(&(click_pk(b), b.ts)).then_with(|| a.cid.cmp_bytes(&b.cid)));
}

// ---------------------------------------------------------------------------

fn peak_rss_mb() -> f64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let v = ru.ru_maxrss as f64;
    if cfg!(target_os = "macos") {
        v / (1024.0 * 1024.0)
    } else {
        v / 1024.0
    }
}

fn manifest_value(m: &str, key: &str) -> i64 {
    let pat = format!("\"{key}\"");
    let i = m.find(&pat).unwrap_or_else(|| panic!("manifest missing {key}")) + pat.len();
    let rest = &m[i..];
    let rest = &rest[rest.find(':').expect("manifest colon") + 1..];
    let q = rest.find('"').expect("manifest quote") + 1;
    let e = rest[q..].find('"').expect("manifest quote") + q;
    parse_ts(rest[q..e].as_bytes()).unwrap_or_else(|| panic!("bad manifest {key}"))
}

fn main() {
    let t0 = Instant::now();
    let mut marks: Vec<(&str, f64)> = Vec::new();
    let mut last = t0;
    let mut mark = |name: &'static str, marks: &mut Vec<(&str, f64)>| {
        let now = Instant::now();
        marks.push((name, (now - last).as_secs_f64() * 1000.0));
        last = now;
    };

    let args: Vec<String> = std::env::args().collect();
    let env = |k: &str| std::env::var(k).ok().filter(|s| !s.is_empty());
    let data_dir = PathBuf::from(args.get(1).cloned().or_else(|| env("DATA_DIR")).unwrap_or_else(|| "/data".into()));
    let out_dir =
        PathBuf::from(args.get(2).cloned().or_else(|| env("OUTPUT_DIR")).unwrap_or_else(|| "/output".into()));
    let threads = env("NCPU")
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8))
        .clamp(1, 128);

    // ---- init
    let manifest = std::fs::read_to_string(data_dir.join("manifest.json")).expect("read manifest.json");
    let ws = manifest_value(&manifest, "window_start");
    let we = manifest_value(&manifest, "window_end");
    assert!(we - ws + SEVEN_DAYS_MS < u32::MAX as i64, "event window too long");
    let day0 = ws.div_euclid(DAY_MS);
    let ndays = if we > ws { ((we - 1).div_euclid(DAY_MS) - day0 + 1) as usize } else { 1 };

    let pdata = read_static(&data_dir.join("products.csv"));
    let mut prod_rows: Vec<(&'static [u8], &'static [u8])> = Vec::new();
    csv::scan::<2, _>(body(pdata), |f| prod_rows.push((f[0], f[1])));
    let mut cat_names: Vec<&'static [u8]> = prod_rows.iter().map(|r| r.1).collect();
    cat_names.push(b"unknown");
    cat_names.sort_unstable();
    cat_names.dedup();
    assert!(cat_names.len() < u16::MAX as usize);
    let mut catmap = StrMap::new(cat_names.len());
    for (i, c) in cat_names.iter().enumerate() {
        catmap.insert(c, i as u32);
    }
    let unknown_cat = catmap.get(b"unknown").unwrap();
    let mut products = StrMap::new(prod_rows.len());
    for (p, c) in &prod_rows {
        products.insert(p, catmap.get(c).unwrap());
    }

    let cdata = read_static(&data_dir.join("campaigns.csv"));
    let mut camp_ids: Vec<&'static [u8]> = vec![b"organic"];
    let mut camp_channels: Vec<&'static [u8]> = vec![b"organic"];
    let mut camp_rows: Vec<(&'static [u8], &'static [u8])> = Vec::new();
    csv::scan::<2, _>(body(cdata), |f| camp_rows.push((f[0], f[1])));
    let mut camps = StrMap::new(camp_rows.len());
    for (id, ch) in camp_rows {
        if camps.get(id).is_none() {
            camps.insert(id, camp_ids.len() as u32);
            camp_ids.push(id);
            camp_channels.push(ch);
        }
    }

    let link_files = map_dir(&data_dir.join("links"));
    let click_files = map_dir(&data_dir.join("clicks"));
    let event_files = map_dir(&data_dir.join("events"));
    mark("init", &mut marks);

    // ---- links
    let links = links::build_links(&link_files, threads);
    mark("links", &mut marks);

    let wide = cat_names.len() > 256;
    let ctx = Ctx {
        ws,
        base: ws - SEVEN_DAYS_MS,
        we,
        day0,
        ndays,
        products,
        unknown_cat,
        cat_names,
        wide,
        camps,
        camp_ids,
        camp_channels,
        links,
    };
    let ctx = &ctx;

    // ---- clicks: parse, validate, resolve person, scatter by person
    let cws = ws - SEVEN_DAYS_MS;
    let chunks = make_chunks(&click_files, chunk_target(&click_files, threads));
    let click_set: ShardSet<Click> = ShardSet::new(P_SHARDS);
    let next = AtomicUsize::new(0);
    par(threads, |_| {
        let mut w = Scatter::new(&click_set);
        let zero = Click { person: Key16::EMPTY, cid: Key16::EMPTY, phash: 0, ts: 0, camp: 0 };
        let mut res = Resolver::new(&ctx.links, zero);
        let mut emit = |mut c: Click, person: Key16, phash: u64| {
            c.person = person;
            c.phash = phash;
            w.push((phash >> (64 - P_BITS)) as usize, c);
        };
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            if i >= chunks.len() {
                break;
            }
            let d = chunks[i].data;
            populate(d);
            csv::scan::<4, _>(d, |f| {
                let (cid, uid) = (f[0], f[1]);
                if cid.is_empty() || uid.is_empty() {
                    return;
                }
                let Some(camp) = ctx.camps.get(f[2]) else { return };
                let ts = match parse_ts(f[3]) {
                    Some(t) if t >= cws && t < ctx.we => t,
                    _ => return,
                };
                let c = Click { person: Key16::EMPTY, cid: Key16::new(cid), phash: 0, ts: (ts - ctx.base) as u32, camp };
                res.push(c, Key16::new(uid), &mut emit);
            });
            release_chunk(d);
        }
        res.drain(&mut emit);
        w.finish();
    });
    mark("clicks", &mut marks);

    // ---- events: parse + validate, scatter one RawEv per line by event_id
    let chunks = make_chunks(&event_files, chunk_target(&event_files, threads));
    let id_set: ShardSet<RawEv> = ShardSet::new(ID_SHARDS);
    let next = AtomicUsize::new(0);
    par(threads, |_| {
        let mut w = Scatter::new(&id_set);
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            if i >= chunks.len() {
                break;
            }
            let d = chunks[i].data;
            populate(d);
            csv::scan::<7, _>(d, |f| {
                let eid = f[0];
                if eid.is_empty() {
                    return;
                }
                let ingest = parse_f1_fast(f[3]);
                let ek = Key16::new(eid);
                let mut r = RawEv { eid: ek, uid: Key16::EMPTY, ingest: enc_ingest(ingest, 0), ts: TS_INVALID, price: 0 };
                'v: {
                    let Some(ing) = ingest else { break 'v };
                    let uid = f[1];
                    if uid.is_empty() || f[5].is_empty() {
                        break 'v;
                    }
                    let ts = match parse_ts(f[2]) {
                        Some(t) if t >= ctx.ws && t < ctx.we => t,
                        _ => break 'v,
                    };
                    let (mut price, mut cat) = (0u32, 0u32);
                    match f[4] {
                        b"view" | b"cart" => {}
                        b"purchase" => {
                            price = parse_price(f[6]);
                            if price == 0 {
                                break 'v;
                            }
                            cat = ctx.products.get(f[5]).unwrap_or(ctx.unknown_cat);
                        }
                        _ => break 'v,
                    }
                    if ing - ts > LATE_MS {
                        break 'v;
                    }
                    r.uid = Key16::new(uid);
                    r.ts = (ts - ctx.base) as u32;
                    r.price = price | ((cat & 0xFF) << 24);
                    r.ingest = enc_ingest(ingest, cat);
                }
                w.push((ek.hash(SEED_ID) >> (64 - ID_BITS)) as usize, r);
            });
            release_chunk(d);
        }
        w.finish();
    });
    mark("events", &mut marks);

    // ---- dedup (exact event_id) fused with person resolution and person scatter
    let ev_set: ShardSet<PEv> = ShardSet::new(P_SHARDS);
    let next = AtomicUsize::new(0);
    let side: Vec<Side> = par(threads, |tid| {
        let mut w = Scatter::new(&ev_set);
        let zero = PEv { person: Key16::EMPTY, ts: 0, pc: 0 };
        let mut res = Resolver::new(&ctx.links, zero);
        let mut emit = |mut e: PEv, person: Key16, phash: u64| {
            e.person = person;
            w.push((phash >> (64 - P_BITS)) as usize, e);
        };
        let mut side: Side = Vec::new();
        let (mut t32, mut t64): (Vec<u32>, Vec<u64>) = (Vec::new(), Vec::new());
        let mut loser: Vec<u64> = Vec::new();
        loop {
            let s = next.fetch_add(1, Ordering::Relaxed);
            if s >= ID_SHARDS {
                break;
            }
            let segs = id_set.take(s);
            let nseg = segs.segs.len();
            assert!(nseg < (1 << 22), "event_id shard too large");
            let n = segs.len();
            let cap = (n + n / 2).next_power_of_two().max(16);
            loser.clear();
            loser.resize((nseg << 9).div_ceil(64), 0);
            if nseg < (1 << 15) - 1 {
                mark_losers(&segs, &mut t32, cap, &mut loser);
            } else {
                mark_losers(&segs, &mut t64, cap, &mut loser);
            }
            for si in 0..nseg {
                for (ri, r) in segs.seg(si).iter().enumerate() {
                    let me = (si << 9) | ri;
                    if r.ts == TS_INVALID || (loser[me >> 6] >> (me & 63)) & 1 != 0 {
                        continue;
                    }
                    let price = r.price & 0xFF_FFFF;
                    let pc = if price == 0 {
                        0
                    } else if !ctx.wide {
                        (price << 8) | (r.price >> 24)
                    } else {
                        let cat = ((r.ingest & 0xFF) << 8) as u32 | (r.price >> 24);
                        assert!(side.len() < (1 << 24) && tid < 128);
                        side.push((price, cat as u16));
                        (((tid as u32) << 24) | (side.len() as u32 - 1)) + 1
                    };
                    res.push(PEv { person: Key16::EMPTY, ts: r.ts, pc }, r.uid, &mut emit);
                }
            }
            segs.free();
        }
        res.drain(&mut emit);
        w.finish();
        side
    });
    mark("dedup", &mut marks);

    // ---- solve
    let mut order: Vec<(usize, usize)> =
        (0..P_SHARDS).map(|p| (p, ev_set.count(p) * 2 + click_set.count(p))).collect();
    order.sort_by_key(|x| std::cmp::Reverse(x.1));
    let side = &side;
    let next = AtomicUsize::new(0);
    let results: Vec<(Vec<URow>, Vec<CampAcc>)> = par(threads, |_| {
        let mut w = Worker::new(ctx, side);
        let mut evs: Vec<SEv> = Vec::new();
        let mut cls: Vec<Click> = Vec::new();
        let (mut ec, mut cc, mut hs) = (Vec::new(), Vec::new(), Vec::new());
        let (mut etmp, mut ctmp): (Vec<SEv>, Vec<Click>) = (Vec::new(), Vec::new());
        loop {
            let k = next.fetch_add(1, Ordering::Relaxed);
            if k >= P_SHARDS {
                break;
            }
            let p = order[k].0;
            let es = ev_set.take(p);
            bucket_scatter(
                &es,
                |e: &PEv| e.person.hash(SEED_A),
                |e: &PEv, h: u64| SEv { key: (h << 32) | e.ts as u64, person: e.person, pc: e.pc, _pad: 0 },
                &mut ec,
                &mut hs,
                &mut evs,
            );
            es.free();
            for b in 0..NB {
                sort_events(&mut evs[ec[b] as usize..ec[b + 1] as usize], &mut etmp);
            }
            let cs = click_set.take(p);
            bucket_scatter(&cs, |c: &Click| c.phash, |c: &Click, _| *c, &mut cc, &mut hs, &mut cls);
            cs.free();
            for b in 0..NB {
                sort_clicks(&mut cls[cc[b] as usize..cc[b + 1] as usize], &mut ctmp);
            }
            w.shard(&evs, &ec, &cls, &cc);
        }
        (std::mem::take(&mut w.rows), std::mem::take(&mut w.acc))
    });
    mark("solve", &mut marks);

    // ---- write (pooled blocks are released in parallel meanwhile)
    let ud = out_dir.join("user_daily");
    let cd = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("create user_daily");
    std::fs::create_dir_all(&cd).expect("create campaign_daily");
    let k1 = ctx.camp_ids.len();
    let mut rows_list: Vec<Vec<URow>> = Vec::with_capacity(results.len());
    let mut accs: Vec<Vec<CampAcc>> = Vec::with_capacity(results.len());
    for (r, a) in results {
        rows_list.push(r);
        accs.push(a);
    }
    const MAX_ROWS_PER_FILE: usize = 2_000_000;
    let mut jobs: Vec<&[URow]> = Vec::new();
    for r in &rows_list {
        for c in r.chunks(MAX_ROWS_PER_FILE) {
            jobs.push(c);
        }
    }
    let write_user = |idx: usize, rows: &[URow]| {
        let mut c_user = Column::new("user_id", Kind::Str, rows.len());
        let mut c_day = Column::new("day", Kind::Date, rows.len());
        let mut c_s = Column::new("sessions", Kind::I64, rows.len());
        let mut c_e = Column::new("events", Kind::I64, rows.len());
        let mut c_r = Column::new("revenue_cents", Kind::I64, rows.len());
        let mut c_t = Column::new("top_category", Kind::OptStr, rows.len());
        let mut buf = [0u8; 16];
        for r in rows {
            c_user.push_str(r.person.as_slice(&mut buf));
            c_day.push_date(r.day);
            c_s.push_i64(r.sessions as i64);
            c_e.push_i64(r.events as i64);
            c_r.push_i64(r.revenue as i64);
            c_t.push_opt_str(if r.top_cat >= 0 { Some(ctx.cat_names[r.top_cat as usize]) } else { None });
        }
        parquet::write_file(&ud.join(format!("part-{idx:05}.parquet")), &[c_user, c_day, c_s, c_e, c_r, c_t])
            .expect("write user_daily");
    };
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let freer = s.spawn(|| arena::release_all(threads));
        let h = s.spawn(|| {
            // campaign_daily reduce + write
            let mut tot = vec![CampAcc::default(); k1 * ctx.ndays];
            for a in &accs {
                for (t, x) in tot.iter_mut().zip(a) {
                    t.sessions += x.sessions;
                    t.purchasing += x.purchasing;
                    t.assisted += x.assisted;
                    t.revenue += x.revenue;
                }
            }
            let n = tot.iter().filter(|x| x.sessions > 0 || x.assisted > 0).count();
            let mut c_id = Column::new("campaign_id", Kind::Str, n);
            let mut c_ch = Column::new("channel", Kind::Str, n);
            let mut c_day = Column::new("day", Kind::Date, n);
            let mut c_s = Column::new("sessions", Kind::I64, n);
            let mut c_p = Column::new("purchasing_sessions", Kind::I64, n);
            let mut c_r = Column::new("revenue_cents", Kind::I64, n);
            let mut c_a = Column::new("assisted_sessions", Kind::I64, n);
            for d in 0..ctx.ndays {
                for k in 0..k1 {
                    let x = &tot[d * k1 + k];
                    if x.sessions == 0 && x.assisted == 0 {
                        continue;
                    }
                    c_id.push_str(ctx.camp_ids[k]);
                    c_ch.push_str(ctx.camp_channels[k]);
                    c_day.push_date((ctx.day0 + d as i64) as i32);
                    c_s.push_i64(x.sessions as i64);
                    c_p.push_i64(x.purchasing as i64);
                    c_r.push_i64(x.revenue as i64);
                    c_a.push_i64(x.assisted as i64);
                }
            }
            parquet::write_file(&cd.join("part-00000.parquet"), &[c_id, c_ch, c_day, c_s, c_p, c_r, c_a])
                .expect("write campaign_daily");
        });
        let nw = threads.min(jobs.len()).max(1);
        let hs: Vec<_> = (0..nw)
            .map(|_| {
                s.spawn(|| loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= jobs.len() {
                        break;
                    }
                    write_user(i, jobs[i]);
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        h.join().unwrap();
        freer.join().unwrap();
    });
    if jobs.is_empty() {
        write_user(0, &[]);
    }
    mark("write", &mut marks);

    let total = (Instant::now() - t0).as_secs_f64() * 1000.0;
    let mut line = format!("[timing] total={total:.1}ms");
    for (k, v) in &marks {
        line.push_str(&format!(" {k}={v:.1}ms"));
    }
    line.push_str(&format!(" threads={threads} simd={} peak_rss_mb={:.1}", csv::simd_backend(), peak_rss_mb()));
    eprintln!("{line}");
}
