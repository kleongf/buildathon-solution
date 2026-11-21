# buildercomp native Rust candidate

Imported from the `buildercomp` workspace (`opt-loop` branch). It is an independent native Rust pipeline with no shared code with `candidates/rust` or Alex. Its only dependency is `libc`.

| What | Commit | Status |
|---|---|---|
| `src/`, `Cargo.*`, `Dockerfile` | `d7e4761` (iter 2) | **Last benchmarked build.** 20M and 60M golden output MATCH (torture 7/7 and fixtures 36/36 were last recorded on iter 1) |
| `patches/iter4-a4f1c35.patch` | `a4f1c35` (iter 4) | Latest code. Committed but **not yet benchmarked or validated**; don't submit it untested |
| `patches/iter3-1af1f07-reverted.patch` | `1af1f07` (iter 3) | Rejected: no significant gain at 20M, regression at 60M. Kept for reference |

Apply iter 4 from the repo root:

```sh
git apply --directory=candidates/buildercomp candidates/buildercomp/patches/iter4-a4f1c35.patch
```

## Headline numbers (scorer-style Docker, 8 CPU / 24 GB, linux/amd64 Xeon VM)

| | 20M wall median | 60M wall | Peak RSS (20M) |
|---|---:|---:|---:|
| Alex `2d9954b2` (same rig) | 1952 ms | 5757 ms | – |
| **buildercomp iter 2** | **1684 ms** (−13.7% vs Alex) | **5056 ms** (−12.2%) | 2.45 GB |

Timing runs `docker run` with the scorer flags (read-only filesystem, no network, `--cpus 8 --memory 24g`, user 1000), timed from the container's `StartedAt` to `FinishedAt` via `docker inspect`. Each comparison is paired AB/BA with a bootstrap 95% confidence interval. See `scripts/scorer_time.sh`.

These figures are **not** directly comparable to this repo's `research.benchmark` or AWS numbers, because the host and timing boundary differ. Re-measure under this repo's harness before promoting.

## Layout

- `src/csv.rs`: 64-byte AVX2 CSV scanner (quote-aware)
- `src/ts.rs`: F1/F2/F3 timestamp parsing with an L1-sized calendar lookup table
- `src/links.rs`: union-find over linked user IDs, a Bloom filter sized to fit L3, and a 128-entry cache for whale users
- `src/key.rs`, `src/hash.rs`: exact 16-byte ID keys and hashing
- `src/arena.rs`: pooled arenas on 2 MB hugepages
- `src/main.rs`: fused parse → dedup → sessionize → attribute pipeline, using non-temporal-store scatter into 256 person shards
- `src/parquet.rs`: minimal Parquet writer
- `scripts/`: scorer-exact Docker timer (`scorer_time.sh`), torture and fuzz differential tests, and a Python reference oracle
