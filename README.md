# buildercomp native Rust candidate

Auto-synced from the `buildercomp` workspace by `scripts/sync_kleong.py`. It is an independent native Rust pipeline with no shared code with `candidates/rust` or Alex; its only dependency is `libc`.

**Current source: iter 5 (`1306817`)**, the newest kept build: 20M wall median **1443.4 ms**, -26.1% vs Alex's 1952 ms on the same rig. Notes: delta_wall=-57.9ms (-3.86%) ci95=[-73.2,-45.4]ms; 60M 4425->4199ms (-226ms, RSS 5314->4653MB MATCH); write 47->14ms (-69%), dedup 287->236ms (-51ms, 60M -219ms), links 114->102ms, RSS -663MB; NOTE clicks regressed +41ms (20M) / +135ms (60M) — diagnosing immediately!

Rejected experiments are kept under `patches/`. Older kept builds are in this directory's git history.

## Every iteration (20M, scorer-style Docker, 8 CPU / 24 GB, linux/amd64 Xeon VM)

| Iter | Commit | Wall median | Verdict | Change |
|---|---|---:|---|---|
| 0 | `a427bc5` | 2702.1 ms | BASELINE | v1-baseline |
| 1 | `3e5a056` | 2501.8 ms | WIN | 64B AVX2 CSV + 620B L1 calendar LUT + unrolled 13-digit F3 + fast F1 ingest_ts |
| 2 | `d7e4761` | 1683.9 ms | WIN | Wave 1B: 2MB hugepage arenas + NT-store scatter + Key16 + L3 Bloom/L1 cache + fused dedup |
| 3 | `1af1f07` | 1657.1 ms | REVERT | shared-block arena + P_SHARDS=128 + 32B PEv + NB=2048 + single-seed Bloom + exit(0) |
| 4 | `e29a052` | 1516.1 ms | WIN | per-shard 32KB person interning + 8B Ev8 + 24B Cl24 solve, DateCache L1 timestamps, AVX2 edge-quote fast path, single-seed Bloom/whale cache, exit(0) |
| 5 | `1306817` | 1443.4 ms | WIN | hybrid shared ShardSet, parallel links init, KeyMap, ID/P_SHARDS=512 + ehash/prefetch dedup, direct Parquet Column emit + zero-copy write, zero-div solve + packed u64 Ev8 sort |

Alex `2d9954b2` on the same rig: 1952 ms at 20M, 5757 ms at 60M. Timing runs `docker run` with the scorer flags, timed from the container's `StartedAt` to `FinishedAt`, with paired AB/BA comparisons and a 95% confidence interval (`scripts/scorer_time.sh`). These numbers are **not** directly comparable to this repo's `research.benchmark` or AWS numbers; re-measure before promoting.

Findings and next experiments: [docs/buildercomp-findings.md](../../docs/buildercomp-findings.md).
