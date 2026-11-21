#!/usr/bin/env python3
"""
Multi-seed differential fuzz tester.
Generates diverse synthetic datasets (including a Feb 29, 2028 leap-year window),
caches reference_oracle.py expected outputs, and compares the candidate binary
using the official compare.py.

Usage: fuzz_diff.py <CANDIDATE_BIN>
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PY = str(ROOT / ".venv" / "bin" / "python")
GEN = str(ROOT / "tools" / "generator" / "target" / "release" / "generate")
ORACLE = str(ROOT / "scripts" / "reference_oracle.py")
COMPARE = str(ROOT / "starters" / "starter-duckdb" / "scripts" / "compare.py")
CACHE_DIR = ROOT / "local" / "fuzz-cache"


def find_leap_2028_seed() -> int:
    # Check cached marker first
    marker = CACHE_DIR / "leap_seed.txt"
    if marker.is_file():
        return int(marker.read_text().strip())
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    probe_dir = CACHE_DIR / "_probe"
    for seed in range(2, 500):
        if probe_dir.exists():
            shutil.rmtree(probe_dir)
        subprocess.run(
            [GEN, "--seed", str(seed), "--events", "1000", "--clicks", "250", "--output", str(probe_dir)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        m = json.loads((probe_dir / "manifest.json").read_text())
        ws, we = m["window_start"], m["window_end"]
        # Feb 29, 2028 is inside [ws, we) if ws <= 2028-02-29T00:00:00Z < we
        if ws <= "2028-02-29T00:00:00Z" < we:
            shutil.rmtree(probe_dir)
            marker.write_text(f"{seed}\n")
            return seed
    raise RuntimeError("Could not find a Feb 29, 2028 seed in 2..500")


def ensure_case(name: str, seed: int, events: int, clicks: int) -> tuple[Path, Path]:
    case_dir = CACHE_DIR / name
    data_dir = case_dir / "data"
    oracle_dir = case_dir / "oracle"
    if not (data_dir / "manifest.json").is_file():
        data_dir.mkdir(parents=True, exist_ok=True)
        print(f"[fuzz] generating {name} (seed={seed}, events={events}, clicks={clicks})...", flush=True)
        subprocess.run(
            [GEN, "--seed", str(seed), "--events", str(events), "--clicks", str(clicks), "--output", str(data_dir)],
            check=True,
        )
    if not (oracle_dir / "user_daily" / "part-000.parquet").is_file():
        if oracle_dir.exists():
            shutil.rmtree(oracle_dir)
        print(f"[fuzz] running reference oracle on {name}...", flush=True)
        subprocess.run([PY, ORACLE, str(data_dir), str(oracle_dir)], check=True)
    return data_dir, oracle_dir


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: fuzz_diff.py <CANDIDATE_BIN>", file=sys.stderr)
        return 2
    candidate = sys.argv[1]
    leap_seed = find_leap_2028_seed()
    cases = [
        ("seed_7_50k", 7, 50_000, 15_000),
        ("seed_1337_100k", 1337, 100_000, 30_000),
        (f"seed_{leap_seed}_leap2028_100k", leap_seed, 100_000, 30_000),
    ]
    all_ok = True
    for name, seed, events, clicks in cases:
        data_dir, oracle_dir = ensure_case(name, seed, events, clicks)
        cand_out = CACHE_DIR / name / "candidate_out"
        if cand_out.exists():
            shutil.rmtree(cand_out)
        cand_out.mkdir(parents=True, exist_ok=True)
        m = json.loads((data_dir / "manifest.json").read_text())
        print(f"[fuzz] testing {name} ({m['window_start']} .. {m['window_end']})", flush=True)
        p = subprocess.run([candidate, str(data_dir), str(cand_out)])
        if p.returncode != 0:
            print(f"[fuzz] FAIL {name}: exit {p.returncode}", file=sys.stderr)
            all_ok = False
            continue
        c = subprocess.run([PY, COMPARE, str(cand_out), str(oracle_dir)])
        if c.returncode != 0:
            print(f"[fuzz] FAIL {name}: output mismatch", file=sys.stderr)
            all_ok = False
    print("[fuzz] ALL PASSED" if all_ok else "[fuzz] FAILED")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
