//! 2 MB pooled block arena, chained per-shard segment storage, and a
//! write-combining scatter writer.
//!
//! * `pool`: process-wide free list of 2 MB-aligned anonymous blocks (THP hinted on
//!   Linux). Blocks are reused across phases and released in parallel before exit.
//! * `ShardSet<T>`: `S` shards of fixed-size records. Each shard owns a chain of
//!   blocks carved into `SEG`-byte segments; a writer thread claims one segment of
//!   a shard at a time, so no shard can ever overflow (it just chains more blocks).
//! * `Scatter<T>`: per-thread writer with one 64-byte-aligned staging area per shard.
//!   When a staging area fills (64 B, or 192 B for 24/48-byte records) it is copied
//!   to the shard's segment with non-temporal stores (AVX2 on x86_64).

use std::marker::PhantomData;
use std::sync::Mutex;

pub const BLOCK: usize = 2 << 20;
/// Segment size: a multiple of 192 and 64 (so 16/24/32/48-byte records and
/// staging flushes never straddle a segment).
pub const SEG: usize = 85 * 192; // 16320
pub const SEGS_PER_BLOCK: usize = BLOCK / SEG; // 128
const SLAB_BLOCKS: usize = 16;

// ---------------------------------------------------------------------------
// block pool

struct PoolInner {
    free: Vec<usize>,
    slabs: Vec<(usize, usize)>,
}

static POOL: Mutex<PoolInner> = Mutex::new(PoolInner { free: Vec::new(), slabs: Vec::new() });

fn map_slab(inner: &mut PoolInner) {
    let want = SLAB_BLOCKS * BLOCK;
    let len = want + BLOCK;
    let p = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANON,
            -1,
            0,
        )
    };
    assert!(p != libc::MAP_FAILED, "mmap block slab failed");
    let start = p as usize;
    let aligned = (start + BLOCK - 1) & !(BLOCK - 1);
    unsafe {
        if aligned > start {
            libc::munmap(start as *mut libc::c_void, aligned - start);
        }
        let end = aligned + want;
        if start + len > end {
            libc::munmap(end as *mut libc::c_void, start + len - end);
        }
        #[cfg(target_os = "linux")]
        libc::madvise(aligned as *mut libc::c_void, want, libc::MADV_HUGEPAGE);
    }
    inner.slabs.push((aligned, want));
    for i in (0..SLAB_BLOCKS).rev() {
        inner.free.push(aligned + i * BLOCK);
    }
}

pub fn get_block() -> *mut u8 {
    let mut g = POOL.lock().unwrap();
    if g.free.is_empty() {
        map_slab(&mut g);
    }
    g.free.pop().unwrap() as *mut u8
}

pub fn put_blocks(blocks: &[usize]) {
    if blocks.is_empty() {
        return;
    }
    POOL.lock().unwrap().free.extend_from_slice(blocks);
}

/// Bytes of address space mapped by the pool.
#[cfg(test)]
pub fn mapped_bytes() -> usize {
    POOL.lock().unwrap().slabs.iter().map(|s| s.1).sum()
}

/// Returns every pooled page to the kernel, split across `threads` threads, so
/// process exit does not free them on one core. All blocks must be back in the pool.
pub fn release_all(threads: usize) {
    let slabs: Vec<(usize, usize)> = {
        let mut g = POOL.lock().unwrap();
        g.free.clear();
        std::mem::take(&mut g.slabs)
    };
    let pieces: Vec<(usize, usize)> =
        slabs.iter().flat_map(|&(a, len)| (0..len / BLOCK).map(move |i| (a + i * BLOCK, BLOCK))).collect();
    if pieces.is_empty() {
        return;
    }
    let threads = threads.clamp(1, pieces.len());
    std::thread::scope(|s| {
        for t in 0..threads {
            let pieces = &pieces;
            s.spawn(move || {
                for &(a, len) in pieces.iter().skip(t).step_by(threads) {
                    // MADV_DONTNEED only takes the mmap lock for reading, so the
                    // threads zap their blocks concurrently.
                    #[cfg(target_os = "linux")]
                    unsafe {
                        libc::madvise(a as *mut libc::c_void, len, libc::MADV_DONTNEED)
                    };
                    #[cfg(not(target_os = "linux"))]
                    unsafe {
                        libc::munmap(a as *mut libc::c_void, len)
                    };
                }
            });
        }
    });
}

// ---------------------------------------------------------------------------
// shard set

#[derive(Clone, Copy)]
pub struct Seg {
    pub ptr: usize,
    pub n: u32,
}

struct ShardState {
    blocks: Vec<usize>,
    next_seg: usize, // segments used in the last block
    segs: Vec<Seg>,
}

pub struct ShardSet<T> {
    shards: Vec<Mutex<ShardState>>,
    _t: PhantomData<T>,
}

unsafe impl<T> Sync for ShardSet<T> {}
unsafe impl<T> Send for ShardSet<T> {}

/// One shard's records, as returned by `ShardSet::take`.
pub struct Segs<T> {
    pub segs: Vec<Seg>,
    blocks: Vec<usize>,
    _t: PhantomData<T>,
}

unsafe impl<T> Send for Segs<T> {}

impl<T: Copy> Segs<T> {
    pub fn len(&self) -> usize {
        self.segs.iter().map(|s| s.n as usize).sum()
    }
    #[inline(always)]
    pub fn seg(&self, i: usize) -> &[T] {
        let s = self.segs[i];
        unsafe { std::slice::from_raw_parts(s.ptr as *const T, s.n as usize) }
    }
    pub fn iter(&self) -> impl Iterator<Item = &[T]> + '_ {
        (0..self.segs.len()).map(move |i| self.seg(i))
    }
    /// Returns this shard's blocks to the pool.
    pub fn free(self) {
        put_blocks(&self.blocks);
    }
}

impl<T: Copy> ShardSet<T> {
    pub fn new(n: usize) -> ShardSet<T> {
        assert!(SEG % std::mem::size_of::<T>() == 0);
        ShardSet {
            shards: (0..n)
                .map(|_| Mutex::new(ShardState { blocks: Vec::new(), next_seg: SEGS_PER_BLOCK, segs: Vec::new() }))
                .collect(),
            _t: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.shards.len()
    }

    /// Records the finished segment `done` (if any) and claims a fresh one.
    fn swap_seg(&self, s: usize, done: Option<Seg>, want_new: bool) -> usize {
        let mut g = self.shards[s].lock().unwrap();
        if let Some(d) = done {
            if d.n > 0 {
                g.segs.push(d);
            }
        }
        if !want_new {
            return 0;
        }
        if g.next_seg == SEGS_PER_BLOCK {
            let b = get_block() as usize;
            g.blocks.push(b);
            g.next_seg = 0;
        }
        let p = *g.blocks.last().unwrap() + g.next_seg * SEG;
        g.next_seg += 1;
        p
    }

    /// Number of records currently stored in shard `s` (all writers finished).
    pub fn count(&self, s: usize) -> usize {
        self.shards[s].lock().unwrap().segs.iter().map(|x| x.n as usize).sum()
    }

    /// Removes shard `s`'s records (all writers must have finished).
    pub fn take(&self, s: usize) -> Segs<T> {
        let mut g = self.shards[s].lock().unwrap();
        g.next_seg = SEGS_PER_BLOCK;
        Segs { segs: std::mem::take(&mut g.segs), blocks: std::mem::take(&mut g.blocks), _t: PhantomData }
    }
}

// ---------------------------------------------------------------------------
// scatter writer

const fn stage_bytes(sz: usize) -> usize {
    if 64 % sz == 0 {
        64
    } else if 192 % sz == 0 {
        192
    } else {
        panic!("unsupported record size")
    }
}

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct Line([u8; 64]);

#[derive(Clone, Copy)]
struct Cur {
    ptr: usize, // current segment (0 = none)
    off: u32,   // bytes written into it
    fill: u32,  // records in staging
}

pub struct Scatter<'a, T: Copy> {
    set: &'a ShardSet<T>,
    stage: Vec<Line>,
    cur: Vec<Cur>,
    nt: bool,
}

impl<'a, T: Copy> Scatter<'a, T> {
    const SZ: usize = std::mem::size_of::<T>();
    const STAGE: usize = stage_bytes(std::mem::size_of::<T>());
    const LINES: usize = Self::STAGE / 64;
    const PER: u32 = (Self::STAGE / std::mem::size_of::<T>()) as u32;

    pub fn new(set: &'a ShardSet<T>) -> Scatter<'a, T> {
        let n = set.len();
        Scatter {
            set,
            stage: vec![Line([0; 64]); n * Self::LINES],
            cur: vec![Cur { ptr: 0, off: 0, fill: 0 }; n],
            nt: nt_enabled(),
        }
    }

    #[inline(always)]
    pub fn push(&mut self, s: usize, x: T) {
        unsafe {
            let c = self.cur.get_unchecked_mut(s);
            let st = (self.stage.as_mut_ptr().add(s * Self::LINES)) as *mut T;
            st.add(c.fill as usize).write(x);
            c.fill += 1;
            if c.fill == Self::PER {
                self.flush(s);
            }
        }
    }

    #[inline(never)]
    fn flush(&mut self, s: usize) {
        let c = self.cur[s];
        let (mut ptr, mut off) = (c.ptr, c.off as usize);
        if ptr == 0 || off == SEG {
            let done = if ptr == 0 { None } else { Some(Seg { ptr, n: (SEG / Self::SZ) as u32 }) };
            ptr = self.set.swap_seg(s, done, true);
            off = 0;
        }
        let src = unsafe { self.stage.as_ptr().add(s * Self::LINES) } as *const u8;
        unsafe { copy_stage(self.nt, (ptr + off) as *mut u8, src, Self::STAGE) };
        self.cur[s] = Cur { ptr, off: (off + Self::STAGE) as u32, fill: 0 };
    }

    /// Writes out every partial staging area and publishes all segments.
    pub fn finish(mut self) {
        for s in 0..self.cur.len() {
            let c = self.cur[s];
            let (mut ptr, mut off) = (c.ptr, c.off as usize);
            if c.fill > 0 {
                if ptr == 0 || off == SEG {
                    let done = if ptr == 0 { None } else { Some(Seg { ptr, n: (SEG / Self::SZ) as u32 }) };
                    ptr = self.set.swap_seg(s, done, true);
                    off = 0;
                }
                let bytes = c.fill as usize * Self::SZ;
                unsafe {
                    let src = self.stage.as_ptr().add(s * Self::LINES) as *const u8;
                    std::ptr::copy_nonoverlapping(src, (ptr + off) as *mut u8, bytes);
                }
                off += bytes;
            }
            if ptr != 0 {
                self.set.swap_seg(s, Some(Seg { ptr, n: (off / Self::SZ) as u32 }), false);
            }
            self.cur[s] = Cur { ptr: 0, off: 0, fill: 0 };
        }
        sfence();
        self.stage = Vec::new();
    }
}

fn nt_enabled() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        crate::csv::simd_backend() == "avx2"
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        false
    }
}

#[inline(always)]
fn sfence() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_sfence()
    }
}

/// Copies `bytes` (a multiple of 64) from 64-byte-aligned `src` to 64-byte-aligned `dst`.
#[inline(always)]
unsafe fn copy_stage(nt: bool, dst: *mut u8, src: *const u8, bytes: usize) {
    #[cfg(target_arch = "x86_64")]
    {
        if nt {
            stream_avx2(dst, src, bytes);
            return;
        }
    }
    let _ = nt;
    std::ptr::copy_nonoverlapping(src, dst, bytes);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx,avx2")]
unsafe fn stream_avx2(dst: *mut u8, src: *const u8, bytes: usize) {
    use core::arch::x86_64::*;
    let mut i = 0;
    while i < bytes {
        let a = _mm256_load_si256(src.add(i) as *const __m256i);
        let b = _mm256_load_si256(src.add(i + 32) as *const __m256i);
        _mm256_stream_si256(dst.add(i) as *mut __m256i, a);
        _mm256_stream_si256(dst.add(i + 32) as *mut __m256i, b);
        i += 64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, PartialEq, Debug)]
    #[repr(C)]
    struct R48 {
        a: u64,
        b: u64,
        c: u64,
        d: u64,
        e: u64,
        f: u64,
    }
    #[derive(Clone, Copy, PartialEq, Debug)]
    #[repr(C)]
    struct R24 {
        a: u64,
        b: u64,
        c: u64,
    }

    #[test]
    fn chained_blocks_whale_shard() {
        // One shard receives ~10 MB (5+ blocks) from 4 threads; nothing is lost.
        let set: ShardSet<R48> = ShardSet::new(256);
        let per_thread = 60_000u64;
        std::thread::scope(|s| {
            for t in 0..4u64 {
                let set = &set;
                s.spawn(move || {
                    let mut w = Scatter::new(set);
                    for i in 0..per_thread {
                        let shard = if i % 10 < 9 { 7 } else { (i % 256) as usize };
                        let v = t * 1_000_000 + i;
                        w.push(shard, R48 { a: v, b: v + 1, c: v + 2, d: v + 3, e: v + 4, f: shard as u64 });
                    }
                    w.finish();
                });
            }
        });
        let mut seen = Vec::new();
        for sh in 0..256 {
            let segs = set.take(sh);
            assert_eq!(segs.len(), segs.iter().map(|x| x.len()).sum::<usize>());
            for seg in segs.iter() {
                for r in seg {
                    assert_eq!(r.f, sh as u64);
                    assert_eq!((r.b, r.c, r.d, r.e), (r.a + 1, r.a + 2, r.a + 3, r.a + 4));
                    seen.push(r.a);
                }
            }
            if sh == 7 {
                assert!(segs.blocks.len() >= 5, "whale shard chained {} blocks", segs.blocks.len());
            }
            segs.free();
        }
        seen.sort_unstable();
        let mut exp: Vec<u64> = (0..4u64).flat_map(|t| (0..per_thread).map(move |i| t * 1_000_000 + i)).collect();
        exp.sort_unstable();
        assert_eq!(seen, exp);

        // Freed blocks are reused by the next set.
        let before = mapped_bytes();
        let set2: ShardSet<R24> = ShardSet::new(4);
        let mut w = Scatter::new(&set2);
        for i in 0..100_000u64 {
            w.push((i % 3) as usize, R24 { a: i, b: !i, c: i ^ 5 });
        }
        w.finish();
        let mut n = 0;
        for sh in 0..4 {
            let segs = set2.take(sh);
            for seg in segs.iter() {
                for r in seg {
                    assert_eq!((r.b, r.c, r.a % 3), (!r.a, r.a ^ 5, sh as u64));
                    n += 1;
                }
            }
            segs.free();
        }
        assert_eq!(n, 100_000);
        assert_eq!(mapped_bytes(), before);
    }
}
