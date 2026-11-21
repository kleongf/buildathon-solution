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
#[allow(dead_code)]
mod hash;
mod key;
mod links;
mod parquet;
mod ts;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use arena::{ShardSet, Scatter, Segs};
use key::Key16;
use links::{prefetch, LinkTable, Resolver, SEED_A};
use parquet::{Column, Kind};
use ts::{parse_price, parse_ts, DateCache, DAY_MS};

const GAP_MS: i64 = 1_800_000;
const SEVEN_DAYS_MS: i64 = 7 * DAY_MS;
const LATE_MS: i64 = DAY_MS;

const SEED_ID: u64 = 0x4b33_a62e_d433_d4a3;
const SEED_MAP: u64 = 0x8bb8_4b93_962e_acc9;

const ID_BITS: u32 = 9;
const ID_SHARDS: usize = 1 << ID_BITS;
const P_BITS: u32 = 9;
const P_SHARDS: usize = 1 << P_BITS;

const MAX_ROWS_PER_FILE: usize = 2_000_000;

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
    #[cfg(all(target_os = "linux", not(test)))]
    unsafe {
        let page = 4096usize;
        let a = (d.as_ptr() as usize + page - 1) & !(page - 1);
        let e = (d.as_ptr() as usize + d.len()) & !(page - 1);
        if e > a {
            libc::madvise(a as *mut libc::c_void, e - a, libc::MADV_DONTNEED);
        }
    }
    #[cfg(any(not(target_os = "linux"), test))]
    let _ = d;
}

// ---------------------------------------------------------------------------
// inline Key16 string maps (products, campaigns) — zero pointer chase to CSV

struct KeyMap {
    slots: Vec<(Key16, u32)>,
    mask: usize,
}

impl KeyMap {
    fn new(n: usize) -> KeyMap {
        let cap = (n * 2).next_power_of_two().max(16);
        KeyMap { slots: vec![(Key16::EMPTY, u32::MAX); cap], mask: cap - 1 }
    }
    fn insert(&mut self, k: &[u8], v: u32) {
        let key = Key16::new(k);
        let mut i = key.hash(SEED_MAP) as usize & self.mask;
        loop {
            let s = &mut self.slots[i];
            if s.1 == u32::MAX {
                *s = (key, v);
                return;
            }
            if s.0 == key {
                return; // first wins (keys are unique anyway)
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, k: &[u8]) -> Option<u32> {
        let key = Key16::new(k);
        let mut i = key.hash(SEED_MAP) as usize & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s.1 == u32::MAX {
                return None;
            }
            if s.0 == key {
                return Some(s.1);
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
const INGEST_OOB_BASE: u32 = 0xF000_0000;
const INGEST_NONE: u32 = u32::MAX;

/// One parsed event line of any validity (48 bytes), sharded by event_id.
/// `ingest` = ms offset from `base` (< INGEST_OOB_BASE), or `INGEST_OOB_BASE + idx`
/// into `oob_ingest`, or `INGEST_NONE`. `ehash` = low 32 bits of `eid.hash(SEED_ID)`
/// so `dedup` never re-hashes `eid`.
#[derive(Clone, Copy)]
#[repr(C)]
struct RawEv {
    eid: Key16,
    uid: Key16,
    ts: u32,
    price: u32,
    ingest: u32,
    ehash: u32,
}

#[inline(always)]
fn enc_ingest_fast(ms: Option<i64>, base: i64, oob: &Mutex<Vec<i64>>) -> u32 {
    match ms {
        Some(m) => {
            let d = m.wrapping_sub(base);
            if d >= 0 && (d as u64) < INGEST_OOB_BASE as u64 {
                d as u32
            } else {
                enc_ingest_oob(m, oob)
            }
        }
        None => INGEST_NONE,
    }
}

#[cold]
fn enc_ingest_oob(m: i64, oob: &Mutex<Vec<i64>>) -> u32 {
    let mut g = oob.lock().unwrap();
    let idx = g.len() as u32;
    g.push(m);
    INGEST_OOB_BASE + idx
}

#[inline(always)]
fn ingest_less(a: u32, b: u32, base: i64, oob: &[i64]) -> bool {
    if (a | b) < INGEST_OOB_BASE {
        a < b
    } else {
        ingest_less_slow(a, b, base, oob)
    }
}

#[cold]
fn ingest_less_slow(a: u32, b: u32, base: i64, oob: &[i64]) -> bool {
    let dec = |v: u32| -> (u8, i64) {
        if v == INGEST_NONE {
            (1, i64::MAX)
        } else if v >= INGEST_OOB_BASE {
            (0, oob[(v - INGEST_OOB_BASE) as usize])
        } else {
            (0, base + v as i64)
        }
    };
    dec(a) < dec(b)
}

/// A surviving event, sharded by person (24 bytes). `pc` is 0 for non-purchases;
/// otherwise price << 8 | category, or (with more than 256 categories) 1 + an
/// index into the wide side table.
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

/// Compact 8-byte event inside solve packed as `(ts << 32) | pc` for single-register sorting.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
struct Ev8(u64);

impl Ev8 {
    #[inline(always)]
    fn new(ts: u32, pc: u32) -> Ev8 {
        Ev8(((ts as u64) << 32) | pc as u64)
    }
    #[inline(always)]
    fn ts(self) -> u32 {
        (self.0 >> 32) as u32
    }
    #[inline(always)]
    fn pc(self) -> u32 {
        self.0 as u32
    }
}

/// Compact 24-byte click inside solve (2x smaller than Click).
#[derive(Clone, Copy)]
#[repr(C)]
struct Cl24 {
    cid: Key16,
    ts: u32,
    camp: u32,
}

const _: () = assert!(std::mem::size_of::<RawEv>() == 48);
const _: () = assert!(std::mem::size_of::<PEv>() == 24);
const _: () = assert!(std::mem::size_of::<Click>() == 48);
const _: () = assert!(std::mem::size_of::<Ev8>() == 8);
const _: () = assert!(std::mem::size_of::<Cl24>() == 24);

struct Ctx {
    ws: i64,
    base: i64,
    we: i64,
    day0: i64,
    ndays: usize,
    dates: DateCache,
    products: KeyMap,
    unknown_cat: u32,
    cat_names: Vec<&'static [u8]>,
    wide: bool,
    camps: KeyMap,
    camp_ids: Vec<&'static [u8]>,
    camp_channels: Vec<&'static [u8]>,
    links: LinkTable,
}

/// Dedup-table slot: a short tag of the event_id hash plus 1 + record index.
trait Slot: Copy {
    fn make(h: u32, me: u32) -> Self;
    fn is_empty(self) -> bool;
    fn tag_eq(self, h: u32) -> bool;
    fn idx(self) -> u32;
}

impl Slot for u32 {
    #[inline(always)]
    fn make(h: u32, me: u32) -> u32 {
        (h & 0xFF00_0000) | (me + 1)
    }
    #[inline(always)]
    fn is_empty(self) -> bool {
        self == 0
    }
    #[inline(always)]
    fn tag_eq(self, h: u32) -> bool {
        (self & 0xFF00_0000) == (h & 0xFF00_0000)
    }
    #[inline(always)]
    fn idx(self) -> u32 {
        (self & 0xFF_FFFF) - 1
    }
}

impl Slot for u64 {
    #[inline(always)]
    fn make(h: u32, me: u32) -> u64 {
        ((h as u64) << 32) | (me as u64 + 1)
    }
    #[inline(always)]
    fn is_empty(self) -> bool {
        self == 0
    }
    #[inline(always)]
    fn tag_eq(self, h: u32) -> bool {
        (self >> 32) as u32 == h
    }
    #[inline(always)]
    fn idx(self) -> u32 {
        self as u32 - 1
    }
}

/// Exact event_id dedup of one shard: marks in `loser` (bit per record index
/// `seg << 9 | rec`) every copy that is not the one with the smallest ingest_ts
/// (the first one read on a tie). Uses cached `ehash` + software prefetching.
fn mark_losers<S: Slot + Default + Clone>(
    segs: &Segs<RawEv>,
    table: &mut Vec<S>,
    cap: usize,
    loser: &mut [u64],
    base: i64,
    oob: &[i64],
) {
    let mask = cap - 1;
    table.clear();
    table.resize(cap, S::default());
    let tptr = table.as_mut_ptr();
    let rec = |idx: u32| -> &RawEv { unsafe { segs.seg((idx >> 9) as usize).get_unchecked((idx & 511) as usize) } };
    const PF: usize = 16;
    for si in 0..segs.segs.len() {
        let seg = segs.seg(si);
        let slen = seg.len();
        for ri in 0..slen {
            if ri + PF < slen {
                let fh = unsafe { seg.get_unchecked(ri + PF).ehash };
                prefetch(unsafe { tptr.add(fh as usize & mask) } as *const u8);
            }
            let r = unsafe { seg.get_unchecked(ri) };
            let h = r.ehash;
            let me = ((si << 9) | ri) as u32;
            let mut i = h as usize & mask;
            loop {
                let slot = unsafe { *tptr.add(i) };
                if slot.is_empty() {
                    unsafe { *tptr.add(i) = S::make(h, me) };
                    break;
                }
                if slot.tag_eq(h) {
                    let oi = slot.idx();
                    let o = rec(oi);
                    if o.eid == r.eid {
                        if ingest_less(r.ingest, o.ingest, base, oob) {
                            loser[(oi >> 6) as usize] |= 1 << (oi & 63);
                            unsafe { *tptr.add(i) = S::make(h, me) };
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
struct Sess {
    start: i64,
    rev: u64,
    day: u32,
    purch: bool,
}

fn new_user_cols(cap: usize) -> [Column; 6] {
    [
        Column::new("user_id", Kind::Str, cap),
        Column::new("day", Kind::Date, cap),
        Column::new("sessions", Kind::I64, cap),
        Column::new("events", Kind::I64, cap),
        Column::new("revenue_cents", Kind::I64, cap),
        Column::new("top_category", Kind::OptStr, cap),
    ]
}

struct Worker<'a> {
    ctx: &'a Ctx,
    wide_store: &'a [(u32, u16)],
    k1: usize,
    cnt: Vec<u32>,
    since: Vec<u32>,
    acc: Vec<CampAcc>,
    cat_rev: Vec<u64>,
    touched: Vec<u32>,
    sess: Vec<Sess>,
    day_first: Vec<u32>,
    cols: [Column; 6],
    done_cols: Vec<[Column; 6]>,
}

impl<'a> Worker<'a> {
    fn new(ctx: &'a Ctx, wide_store: &'a [(u32, u16)]) -> Worker<'a> {
        let k1 = ctx.camp_ids.len();
        Worker {
            ctx,
            wide_store,
            k1,
            cnt: vec![0; k1],
            since: vec![0; k1],
            acc: vec![CampAcc::default(); k1 * ctx.ndays],
            cat_rev: vec![0; ctx.cat_names.len()],
            touched: Vec::new(),
            sess: Vec::new(),
            day_first: vec![0; ctx.ndays + 1],
            cols: new_user_cols(32_768),
            done_cols: Vec::new(),
        }
    }

    #[inline(always)]
    fn price_cat(&self, pc: u32) -> (u64, usize) {
        if !self.ctx.wide {
            ((pc >> 8) as u64, (pc & 0xFF) as usize)
        } else {
            let (p, c) = unsafe { *self.wide_store.get_unchecked((pc - 1) as usize) };
            (p as u64, c as usize)
        }
    }

    #[inline(always)]
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
        if self.cols[0].len() == MAX_ROWS_PER_FILE {
            let prev = std::mem::replace(&mut self.cols, new_user_cols(32_768));
            self.done_cols.push(prev);
        }
        let mut buf = [0u8; 16];
        self.cols[0].push_str(person.as_slice(&mut buf));
        self.cols[1].push_date((self.ctx.day0 + day as i64) as i32);
        self.cols[2].push_i64(ns as i64);
        self.cols[3].push_i64(ne as i64);
        self.cols[4].push_i64(rev as i64);
        self.cols[5].push_opt_str(if top >= 0 { Some(self.ctx.cat_names[top as usize]) } else { None });
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
    fn person(&mut self, person: Key16, evs: &[Ev8], clicks: &[Cl24]) {
        self.sess.clear();
        let day0ms = self.ctx.day0 * DAY_MS - self.ctx.base;
        let mut day_idx = 0u32;
        let mut next_day_ms = day0ms + DAY_MS;
        let mut cur_day = u32::MAX;
        let (mut ns, mut ne, mut rev) = (0u32, 0u32, 0u64);
        let mut prev = i64::MIN;
        for &e in evs {
            let ts = e.ts() as i64;
            if self.sess.is_empty() || ts - prev > GAP_MS {
                if ts >= next_day_ms {
                    while ts >= next_day_ms {
                        day_idx += 1;
                        next_day_ms += DAY_MS;
                    }
                }
                let day = day_idx;
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
            let pc = e.pc();
            if pc != 0 {
                let (p, c) = self.price_cat(pc);
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

}

// ---------------------------------------------------------------------------
// per-shard direct person interning + compact L2 solve (8B Ev8 + 24B Cl24)

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

#[inline(always)]
fn sort_person_events(sl: &mut [Ev8], tmp: &mut Vec<Ev8>) {
    let n = sl.len();
    if n < 2 {
        return;
    }
    if n == 2 {
        unsafe {
            let a = *sl.get_unchecked(0);
            let b = *sl.get_unchecked(1);
            if a.0 > b.0 {
                *sl.get_unchecked_mut(0) = b;
                *sl.get_unchecked_mut(1) = a;
            }
        }
        return;
    }
    if n <= 24 {
        for i in 1..n {
            let x = unsafe { *sl.get_unchecked(i) };
            let mut j = i;
            while j > 0 {
                let prev = unsafe { *sl.get_unchecked(j - 1) };
                if prev.0 <= x.0 {
                    break;
                }
                unsafe { *sl.get_unchecked_mut(j) = prev };
                j -= 1;
            }
            unsafe { *sl.get_unchecked_mut(j) = x };
        }
        return;
    }
    if n > 256 {
        radix_by_u32(sl, tmp, |e| e.ts());
    } else {
        sl.sort_unstable();
    }
}

#[inline(always)]
fn sort_person_clicks(sl: &mut [Cl24], tmp: &mut Vec<Cl24>) {
    let n = sl.len();
    if n < 2 {
        return;
    }
    if n > 256 {
        radix_by_u32(sl, tmp, |c| c.ts);
        let mut i = 0;
        while i < n {
            let mut j = i + 1;
            while j < n && sl[j].ts == sl[i].ts {
                j += 1;
            }
            if j - i > 1 {
                sl[i..j].sort_unstable_by(|a, b| a.cid.cmp_bytes(&b.cid));
            }
            i = j;
        }
    } else {
        sl.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.cid.cmp_bytes(&b.cid)));
    }
}

/// Scratch buffers reused across all shards handled by one solve worker.
struct SolveScratch {
    ht: Vec<u32>,
    ht_mask: usize,
    pkeys: Vec<Key16>,
    ev_off: Vec<u32>,
    ev_cur: Vec<u32>,
    ev_pids: Vec<u32>,
    evs: Vec<Ev8>,
    etmp: Vec<Ev8>,
    cl_off: Vec<u32>,
    cl_cur: Vec<u32>,
    cl_pids: Vec<u32>,
    cls: Vec<Cl24>,
    ctmp: Vec<Cl24>,
}

impl SolveScratch {
    fn new() -> Self {
        SolveScratch {
            ht: Vec::new(),
            ht_mask: 0,
            pkeys: Vec::new(),
            ev_off: Vec::new(),
            ev_cur: Vec::new(),
            ev_pids: Vec::new(),
            evs: Vec::new(),
            etmp: Vec::new(),
            cl_off: Vec::new(),
            cl_cur: Vec::new(),
            cl_pids: Vec::new(),
            cls: Vec::new(),
            ctmp: Vec::new(),
        }
    }

    fn reset_ht(&mut self, cap: usize) {
        self.ht.clear();
        self.ht.resize(cap, 0);
        self.ht_mask = cap - 1;
    }

    #[inline(always)]
    fn intern(&mut self, person: Key16) -> u32 {
        // Grow if load > 50%
        if self.pkeys.len() * 2 >= self.ht.len() {
            self.grow_ht();
        }
        let mask = self.ht_mask;
        let mut i = person.hash(SEED_A) as usize & mask;
        loop {
            let slot = unsafe { *self.ht.get_unchecked(i) };
            if slot == 0 {
                let pid = self.pkeys.len() as u32;
                self.pkeys.push(person);
                self.ev_off.push(1);
                unsafe { *self.ht.get_unchecked_mut(i) = pid + 1 };
                return pid;
            }
            let pid = slot - 1;
            if unsafe { *self.pkeys.get_unchecked(pid as usize) } == person {
                unsafe { *self.ev_off.get_unchecked_mut(pid as usize) += 1 };
                return pid;
            }
            i = (i + 1) & mask;
        }
    }

    #[cold]
    fn grow_ht(&mut self) {
        let new_cap = (self.ht.len() * 2).max(256);
        self.reset_ht(new_cap);
        let mask = self.ht_mask;
        for (pid, &pk) in self.pkeys.iter().enumerate() {
            let mut i = pk.hash(SEED_A) as usize & mask;
            while self.ht[i] != 0 {
                i = (i + 1) & mask;
            }
            self.ht[i] = pid as u32 + 1;
        }
    }

    #[inline(always)]
    fn lookup(&self, person: Key16, phash: u64) -> Option<u32> {
        let mask = self.ht_mask;
        let mut i = phash as usize & mask;
        loop {
            let slot = unsafe { *self.ht.get_unchecked(i) };
            if slot == 0 {
                return None;
            }
            let pid = slot - 1;
            if unsafe { *self.pkeys.get_unchecked(pid as usize) } == person {
                return Some(pid);
            }
            i = (i + 1) & mask;
        }
    }

    fn run_shard(&mut self, es: Segs<PEv>, cs: Segs<Click>, w: &mut Worker<'_>) {
        let nev = es.len();
        if nev == 0 {
            es.free();
            cs.free();
            return;
        }
        // Estimate ~1 person per 12 events (safe initial load, grows automatically if needed)
        let init_cap = ((nev / 6).next_power_of_two()).max(256);
        self.reset_ht(init_cap);
        self.pkeys.clear();
        self.ev_off.clear();
        self.ev_pids.clear();
        self.ev_pids.reserve(nev);

        // Pass 1 over events: intern exact Key16 persons in L1/L2 hash table
        for seg in es.iter() {
            for e in seg {
                let pid = self.intern(e.person);
                self.ev_pids.push(pid);
            }
        }
        let np = self.pkeys.len();

        // Prefix sums for events
        let mut sum = 0u32;
        self.ev_cur.clear();
        self.ev_cur.reserve(np);
        for c in &mut self.ev_off {
            let cnt = *c;
            *c = sum;
            self.ev_cur.push(sum);
            sum += cnt;
        }
        self.ev_off.push(sum);

        // Pass 2 over events: scatter compact 8-byte Ev8 into L2-resident buffer
        self.evs.clear();
        self.evs.reserve(nev);
        let eo = self.evs.as_mut_ptr();
        let mut k = 0usize;
        for seg in es.iter() {
            for e in seg {
                unsafe {
                    let pid = *self.ev_pids.get_unchecked(k) as usize;
                    let dst = self.ev_cur.get_unchecked_mut(pid);
                    eo.add(*dst as usize).write(Ev8::new(e.ts, e.pc));
                    *dst += 1;
                }
                k += 1;
            }
        }
        unsafe { self.evs.set_len(nev) };
        es.free();

        // Clicks: lookup in the same L1/L2 person table; drop clicks for persons with 0 events!
        let ncl_raw = cs.len();
        self.cl_off.clear();
        self.cl_off.resize(np + 1, 0);
        self.cl_pids.clear();
        self.cl_pids.reserve(ncl_raw);
        let mut ncl_kept = 0usize;
        if ncl_raw > 0 {
            for seg in cs.iter() {
                for c in seg {
                    if let Some(pid) = self.lookup(c.person, c.phash) {
                        self.cl_pids.push(pid);
                        unsafe { *self.cl_off.get_unchecked_mut(pid as usize) += 1 };
                        ncl_kept += 1;
                    } else {
                        self.cl_pids.push(u32::MAX);
                    }
                }
            }
        }

        self.cls.clear();
        if ncl_kept > 0 {
            let mut csum = 0u32;
            self.cl_cur.clear();
            self.cl_cur.reserve(np);
            for i in 0..np {
                let cnt = self.cl_off[i];
                self.cl_off[i] = csum;
                self.cl_cur.push(csum);
                csum += cnt;
            }
            self.cl_off[np] = csum;
            self.cls.reserve(ncl_kept);
            let co = self.cls.as_mut_ptr();
            let mut ck = 0usize;
            for seg in cs.iter() {
                for c in seg {
                    let pid = unsafe { *self.cl_pids.get_unchecked(ck) };
                    ck += 1;
                    if pid != u32::MAX {
                        unsafe {
                            let dst = self.cl_cur.get_unchecked_mut(pid as usize);
                            co.add(*dst as usize).write(Cl24 { cid: c.cid, ts: c.ts, camp: c.camp });
                            *dst += 1;
                        }
                    }
                }
            }
            unsafe { self.cls.set_len(ncl_kept) };
        }
        cs.free();

        // Process each person cleanly in L1/L2 cache
        for pid in 0..np {
            let es0 = self.ev_off[pid] as usize;
            let es1 = self.ev_off[pid + 1] as usize;
            sort_person_events(&mut self.evs[es0..es1], &mut self.etmp);
            let cls_slice: &[Cl24] = if ncl_kept > 0 {
                let cs0 = self.cl_off[pid] as usize;
                let cs1 = self.cl_off[pid + 1] as usize;
                if cs1 > cs0 {
                    sort_person_clicks(&mut self.cls[cs0..cs1], &mut self.ctmp);
                }
                &self.cls[cs0..cs1]
            } else {
                &[]
            };
            w.person(self.pkeys[pid], &self.evs[es0..es1], cls_slice);
        }
    }
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
    let mut catmap = KeyMap::new(cat_names.len());
    for (i, c) in cat_names.iter().enumerate() {
        catmap.insert(c, i as u32);
    }
    let unknown_cat = catmap.get(b"unknown").unwrap();
    let mut products = KeyMap::new(prod_rows.len());
    for (p, c) in &prod_rows {
        products.insert(p, catmap.get(c).unwrap());
    }

    let cdata = read_static(&data_dir.join("campaigns.csv"));
    let mut camp_ids: Vec<&'static [u8]> = vec![b"organic"];
    let mut camp_channels: Vec<&'static [u8]> = vec![b"organic"];
    let mut camp_rows: Vec<(&'static [u8], &'static [u8])> = Vec::new();
    csv::scan::<2, _>(body(cdata), |f| camp_rows.push((f[0], f[1])));
    let mut camps = KeyMap::new(camp_rows.len());
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
        dates: DateCache::new(ws, we),
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
    let click_set: ShardSet<Click> = ShardSet::new_shared(P_SHARDS);
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
                let ts = match ctx.dates.parse_ts(f[3]) {
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
    let oob_ingest: Mutex<Vec<i64>> = Mutex::new(Vec::new());
    let wide_store: Mutex<Vec<(u32, u16)>> = Mutex::new(Vec::new());
    let next = AtomicUsize::new(0);
    par(threads, |_| {
        let mut w = Scatter::new(&id_set);
        let oob_ref = &oob_ingest;
        let wide_ref = &wide_store;
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
                let ingest = ctx.dates.parse_f1_ingest(f[3]);
                let ek = Key16::new(eid);
                let eh = ek.hash(SEED_ID);
                let ing_u32 = enc_ingest_fast(ingest, ctx.base, oob_ref);
                let mut r = RawEv {
                    eid: ek,
                    uid: Key16::EMPTY,
                    ts: TS_INVALID,
                    price: 0,
                    ingest: ing_u32,
                    ehash: eh as u32,
                };
                'v: {
                    let Some(ing) = ingest else { break 'v };
                    let uid = f[1];
                    if uid.is_empty() || f[5].is_empty() {
                        break 'v;
                    }
                    let ts = match ctx.dates.parse_ts(f[2]) {
                        Some(t) if t >= ctx.ws && t < ctx.we => t,
                        _ => break 'v,
                    };
                    let mut price_enc = 0u32;
                    match f[4] {
                        b"view" | b"cart" => {}
                        b"purchase" => {
                            let price = parse_price(f[6]);
                            if price == 0 {
                                break 'v;
                            }
                            let cat = ctx.products.get(f[5]).unwrap_or(ctx.unknown_cat);
                            price_enc = if !ctx.wide {
                                (price << 8) | cat
                            } else {
                                let mut g = wide_ref.lock().unwrap();
                                g.push((price, cat as u16));
                                g.len() as u32
                            };
                        }
                        _ => break 'v,
                    }
                    if ing - ts > LATE_MS {
                        break 'v;
                    }
                    r.uid = Key16::new(uid);
                    r.ts = (ts - ctx.base) as u32;
                    r.price = price_enc;
                }
                w.push((eh >> (64 - ID_BITS)) as usize, r);
            });
            release_chunk(d);
        }
        w.finish();
    });
    let oob_ingest = oob_ingest.into_inner().unwrap();
    let wide_store = wide_store.into_inner().unwrap();
    mark("events", &mut marks);

    // ---- dedup (exact event_id) fused with person resolution and person scatter
    let ev_set: ShardSet<PEv> = ShardSet::new_shared(P_SHARDS);
    let next = AtomicUsize::new(0);
    let oob_slice = &oob_ingest[..];
    par(threads, |_| {
        let mut w = Scatter::new(&ev_set);
        let zero = PEv { person: Key16::EMPTY, ts: 0, pc: 0 };
        let mut res = Resolver::new(&ctx.links, zero);
        let mut emit = |mut e: PEv, person: Key16, phash: u64| {
            e.person = person;
            w.push((phash >> (64 - P_BITS)) as usize, e);
        };
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
                mark_losers(&segs, &mut t32, cap, &mut loser, ctx.base, oob_slice);
            } else {
                mark_losers(&segs, &mut t64, cap, &mut loser, ctx.base, oob_slice);
            }
            for si in 0..nseg {
                for (ri, r) in segs.seg(si).iter().enumerate() {
                    let me = (si << 9) | ri;
                    if r.ts == TS_INVALID || (loser[me >> 6] >> (me & 63)) & 1 != 0 {
                        continue;
                    }
                    res.push(PEv { person: Key16::EMPTY, ts: r.ts, pc: r.price }, r.uid, &mut emit);
                }
            }
            segs.free();
        }
        res.drain(&mut emit);
        w.finish();
    });
    id_set.free_all();
    mark("dedup", &mut marks);

    // ---- solve
    let mut order: Vec<(usize, usize)> =
        (0..P_SHARDS).map(|p| (p, ev_set.count(p) * 2 + click_set.count(p))).collect();
    order.sort_by_key(|x| std::cmp::Reverse(x.1));
    let wide_slice = &wide_store[..];
    let next = AtomicUsize::new(0);
    let results: Vec<(Vec<[Column; 6]>, Vec<CampAcc>)> = par(threads, |_| {
        let mut w = Worker::new(ctx, wide_slice);
        let mut sc = SolveScratch::new();
        loop {
            let k = next.fetch_add(1, Ordering::Relaxed);
            if k >= P_SHARDS {
                break;
            }
            let p = order[k].0;
            let es = ev_set.take(p);
            let cs = click_set.take(p);
            sc.run_shard(es, cs, &mut w);
        }
        if w.cols[0].len() > 0 {
            let last = std::mem::replace(&mut w.cols, new_user_cols(0));
            w.done_cols.push(last);
        }
        (std::mem::take(&mut w.done_cols), std::mem::take(&mut w.acc))
    });
    ev_set.free_all();
    click_set.free_all();
    mark("solve", &mut marks);

    // ---- write (pooled blocks are released in parallel meanwhile)
    let ud = out_dir.join("user_daily");
    let cd = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("create user_daily");
    std::fs::create_dir_all(&cd).expect("create campaign_daily");
    let k1 = ctx.camp_ids.len();
    let mut jobs: Vec<[Column; 6]> = Vec::new();
    let mut accs: Vec<Vec<CampAcc>> = Vec::with_capacity(results.len());
    for (chunks, a) in results {
        jobs.extend(chunks);
        accs.push(a);
    }
    let jobs = &jobs;
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
                    parquet::write_file(&ud.join(format!("part-{i:05}.parquet")), &jobs[i])
                        .expect("write user_daily");
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
        let empty = new_user_cols(0);
        parquet::write_file(&ud.join("part-00000.parquet"), &empty).expect("write user_daily");
    }
    mark("write", &mut marks);

    let total = (Instant::now() - t0).as_secs_f64() * 1000.0;
    let mut line = format!("[timing] total={total:.1}ms");
    for (k, v) in &marks {
        line.push_str(&format!(" {k}={v:.1}ms"));
    }
    line.push_str(&format!(" threads={threads} simd={} peak_rss_mb={:.1}", csv::simd_backend(), peak_rss_mb()));
    eprintln!("{line}");
    std::process::exit(0);
}
