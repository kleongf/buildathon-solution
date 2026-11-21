#!/usr/bin/env python3
"""
Adversarial Tier-0 torture suite for Shopping Sessions.
Exercises edge cases that SIMD scanners, fast timestamp LUTs, inline u128 keys,
pooled arenas, and multi-threaded dedup/attribution commonly break:
  1. SIMD lane/tail boundary offsets (0..=64) across 2-, 4-, 7-field CSVs,
     CRLF, unmatched quotes, quoted empty fields, no trailing newline, empty files
  2. Header-only empty dataset (must emit valid empty Parquet with exact schema)
  3. ID lengths 1, 14, 15, 16, 17, 64, 256 bytes + UTF-8 + spaces + common prefixes
  4. Cross-user dedup across files: malformed earliest vs invalid earliest vs valid later
  5. Calendar/timestamp torture: leap 2028-02-29, non-leap 2027-02-29, Dec->Jan year
     boundary (click lookback into prior year), years 0000/0001/9999, F3 i64::MAX/+1
  6. Attribution + link torture: 1000-hop link chain, 10%-whale skew, equal-ts click
     tie-breaks across linked aliases, assist-only campaign, expiry & re-entry,
     exact 30m/30m+1ms, 7d/7d+1ms, 24h/24h+1s, midnight-crossing purchase
  7. Determinism across NCPU=1 and NCPU=8

Usage: torture_test.py <CANDIDATE_BIN>
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PY = str(ROOT / ".venv" / "bin" / "python")
ORACLE = str(ROOT / "scripts" / "reference_oracle.py")
COMPARE = str(ROOT / "starters" / "starter-duckdb" / "scripts" / "compare.py")


def write_dataset(root: Path, ws: str, we: str, products: list[tuple[str, str]],
                  campaigns: list[tuple[str, str]], links_files: dict[str, bytes],
                  clicks_files: dict[str, bytes], events_files: dict[str, bytes]) -> None:
    for d in ("links", "clicks", "events"):
        (root / d).mkdir(parents=True, exist_ok=True)
    (root / "manifest.json").write_text(json.dumps({"window_start": ws, "window_end": we}))
    (root / "products.csv").write_bytes(
        b"product_id,category\n" + b"".join(f"{p},{c}\n".encode() for p, c in products)
    )
    (root / "campaigns.csv").write_bytes(
        b"campaign_id,channel\n" + b"".join(f"{c},{ch}\n".encode() for c, ch in campaigns)
    )
    for name, body in links_files.items():
        (root / "links" / name).write_bytes(body)
    for name, body in clicks_files.items():
        (root / "clicks" / name).write_bytes(body)
    for name, body in events_files.items():
        (root / "events" / name).write_bytes(body)


def case_empty(d: Path) -> None:
    # Completely empty products.csv & campaigns.csv (header-only catalogs) + empty data
    write_dataset(
        d,
        "2026-10-01T00:00:00Z",
        "2026-10-15T00:00:00Z",
        [],
        [],
        {"empty.csv": b"user_id_a,user_id_b\n", "zero.csv": b""},
        {"empty.csv": b"click_id,user_id,campaign_id,ts\n"},
        {"empty.csv": b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n"},
    )


def case_nul_and_huge_digits(d: Path) -> None:
    # 10,000-digit leading-zero timestamps & prices + 10,000-digit overflow + empty catalogs + embedded NUL IDs
    zeros = "0" * 10_000
    nines = "9" * 10_000
    ev = (
        b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n"
        + f"e_lz_ts,u_lz,{zeros}1791071890000,2026-10-03T23:59:00Z,purchase,p1,{zeros}50\n".encode()
        + f"e_ovf,u_lz,{nines},2026-10-03T23:59:00Z,view,p1,\n".encode()
        + b"e_cat_unk,u_lz,2026-10-04T01:00:00Z,2026-10-04T01:01:00Z,purchase,p_missing,123\n"
        + b"e_n1,a,2026-10-04T02:00:00Z,2026-10-04T02:01:00Z,view,p1,\n"
        + b"e_n2,a\x00,2026-10-04T02:00:00Z,2026-10-04T02:01:00Z,view,p1,\n"
        + b"e_n3,a\x00b,2026-10-04T02:00:00Z,2026-10-04T02:01:00Z,view,p1,\n"
        + b"e_n4,u_lnk_nul,2026-10-04T02:00:00Z,2026-10-04T02:01:00Z,view,p1,\n"
    )
    lk = (
        b"user_id_a,user_id_b\n"
        b"u_lnk_nul,A\x00min\n"
    )
    write_dataset(
        d,
        "2026-10-01T00:00:00Z",
        "2026-10-15T00:00:00Z",
        [],
        [],
        {"part-0.csv": lk},
        {"part-0.csv": b"click_id,user_id,campaign_id,ts\n"},
        {"part-0.csv": ev},
    )


def case_simd_and_id_lengths(d: Path) -> None:
    # Build lines so commas, quotes, \n, \r hit every byte offset 0..=64
    ev_lines: list[bytes] = [b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n"]
    cl_lines: list[bytes] = [b"click_id,user_id,campaign_id,ts\n"]
    lk_lines: list[bytes] = [b"user_id_a,user_id_b\n"]

    # Vary prefix/user_id lengths across 1..65 + 256 bytes, including UTF-8 & spaces
    id_lengths = list(range(1, 66)) + [128, 256]
    for idx, n in enumerate(id_lengths):
        # Common prefix of 'u' + 'a'*(n-2) + digit to stress prefix comparisons & u128 overflow
        if n == 1:
            uid = f"{idx % 10}"
        elif n == 2:
            uid = f"u{idx % 10}"
        else:
            uid = "u" + ("a" * (n - 3)) + f"{idx % 90 + 10}"
        eid = f"e_{idx:03d}_" + ("x" * (idx % 20))
        q = '"' if (idx % 2 == 0) else ""
        ts = f"2026-10-03T10:{idx % 60:02d}:00Z"
        ing = f"2026-10-03T10:{idx % 60:02d}:30Z"
        etype = "purchase" if (idx % 3 == 0) else "view"
        price = "0250" if etype == "purchase" else ""
        ev_lines.append(f"{q}{eid}{q},{q}{uid}{q},{q}{ts}{q},{q}{ing}{q},{q}{etype}{q},p1,{q}{price}{q}\n".encode())
        if idx % 2 == 0:
            cid = f"k_{idx:03d}_" + ("z" * (idx % 18))
            cl_lines.append(f"{q}{cid}{q},{q}{uid}{q},c_1,2026-10-03T09:00:00Z\n".encode())
        if idx % 4 == 0 and n >= 4:
            alias = "A" + uid[1:]  # uppercase sorts before lowercase, unused alias
            lk_lines.append(f"{q}{uid}{q},{q}{alias}{q}\n".encode())

    # UTF-8, spaces, malformed lines, unmatched quotes followed by valid rows, CRLF, quoted empty fields
    ev_lines += [
        b"e_utf8,u_\xc3\xa9\xe2\x9c\x93,2026-10-05T12:00:00Z,2026-10-05T12:01:00Z,purchase,p1,500\n",
        b"\"e_sp\",\" u_sp \",\"2026-10-05T12:00:00Z\",\"2026-10-05T12:01:00Z\",\"view\",\"p1\",\"\"\n",
        b"\"unmatched_quote,u1,2026-10-05T12:00:00Z,2026-10-05T12:01:00Z,view,p1,\n",
        b"e_after_bad,u_after,2026-10-05T12:00:00Z,2026-10-05T12:01:00Z,view,p1,\n",
        b"e_crlf_view,u_cr,2026-10-05T12:00:00Z,2026-10-05T12:01:00Z,view,p1,\r\n",
        b"e_crlf_purch,u_cr,2026-10-05T12:00:00Z,2026-10-05T12:01:00Z,purchase,p1,1000\r\n",
        b"\n",
        b"e_no_nl,u_nonl,2026-10-06T08:00:00Z,2026-10-06T08:01:00Z,purchase,p2,999",  # NO trailing newline
    ]
    cl_lines += [
        b"k_crlf,u_cr,c_1,2026-10-05T11:00:00Z\r\n",
        b"\"k_bad,u_after,c_1,2026-10-05T11:00:00Z\n",
        b"k_nonl,u_nonl,c_2,2026-10-06T07:59:59Z",  # NO trailing newline
    ]
    lk_lines += [
        b"\"u_bad_link,u_other\n",
        b"u_nonl,u_nonl_alias",  # NO trailing newline
    ]

    write_dataset(
        d,
        "2026-10-01T00:00:00Z",
        "2026-10-15T00:00:00Z",
        [("p1", "books"), ("p2", "shoes")],
        [("c_1", "search"), ("c_2", "social")],
        {"part-0.csv": b"".join(lk_lines)},
        {"part-0.csv": b"".join(cl_lines)},
        {"part-0.csv": b"".join(ev_lines)},
    )


def case_cross_user_dedup_and_year_boundary(d: Path) -> None:
    # Window crosses Dec 2027 -> Jan 2028 AND includes leap day check in another case
    # Click window starts 2027-12-21T00:00:00Z (crosses into 2027!)
    ev0 = (
        b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n"
        # e_inv: earliest copy (10:00) is well-formed but invalid ts (2027-02-29 non-leap) on u_wrong
        b"e_inv,u_wrong,2027-02-29T10:00:00Z,2027-12-30T10:00:00Z,view,p1,\n"
        # e_mal: earliest copy (09:59) is MALFORMED (dropped at step 1), so later valid copy MUST survive!
        b"e_mal,u_wrong,2027-12-30T10:00:00Z,2027-12-30T09:59:00Z,view,p1\n"
        # e_swap: earliest valid copy is on u_orig (10:01), later copy on u_rev (10:05)
        b"e_swap,u_rev,2027-12-30T10:00:00Z,2027-12-30T10:05:00Z,purchase,p1,700\n"
        # Bad years + F3 overflow + exact 24h vs 24h+1s + ingest_ts < ts
        b"e_y0,u_ts,0000-01-01T00:00:00Z,2027-12-30T10:00:00Z,view,p1,\n"
        b"e_y1,u_ts,0001-01-01T00:00:00Z,2027-12-30T10:00:00Z,view,p1,\n"
        b"e_y9,u_ts,9999-12-31T23:59:59Z,2027-12-30T10:00:00Z,view,p1,\n"
        b"e_f3_max,u_ts,9223372036854775807,2027-12-30T10:00:00Z,view,p1,\n"
        b"e_f3_ovf,u_ts,9223372036854775808,2027-12-30T10:00:00Z,view,p1,\n"
        b"e_24h_ok,u_late,2027-12-30T10:00:00Z,2027-12-31T10:00:00Z,view,p1,\n"
        b"e_24h_bad,u_late,2027-12-30T10:00:00Z,2027-12-31T10:00:01Z,view,p1,\n"
        b"e_early_ing,u_late,2028-01-01T00:05:00Z,2028-01-01T00:04:55Z,purchase,p2,300\n"
        # Midnight-crossing session across YEAR boundary! Starts 2027-12-31T23:50Z, purchase 2028-01-01T00:10Z
        b"e_nye1,u_nye,2027-12-31T23:50:00Z,2027-12-31T23:51:00Z,view,p1,\n"
        b"e_nye2,u_nye,2028-01-01T02:10:00+02:00,2028-01-01T00:11:00Z,purchase,p2,1200\n"
        # Exact 30m and 30m+1ms boundaries (1830330000000 = 2028-01-01T09:00:00Z)
        b"e_g1,u_gap,1830330000000,2028-01-01T09:01:00Z,view,p1,\n"
        b"e_g2,u_gap,1830331800000,2028-01-01T09:31:00Z,view,p1,\n"
        b"e_g3,u_gap,1830333600001,2028-01-01T10:01:00Z,view,p1,\n"
    )
    ev1 = (
        b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n"
        b"e_inv,u_rescued,2027-12-30T10:00:00Z,2027-12-30T10:05:00Z,view,p1,\n"
        b"e_mal,u_mal_ok,2027-12-30T10:00:00Z,2027-12-30T10:05:00Z,purchase,p1,400\n"
        b"e_swap,u_orig,2027-12-30T10:00:00Z,2027-12-30T10:01:00Z,purchase,p1,700\n"
    )
    cl0 = (
        b"click_id,user_id,campaign_id,ts\n"
        # Click in 2027-12-21T00:00:00Z (exact lower bound window_start - 7d for ws=2027-12-28)
        b"k_lb_ok,u_nye,c_1,2027-12-21T00:00:00Z\n"
        b"k_lb_bad,u_nye,c_2,2027-12-20T23:59:59Z\n"
        # Equal-ts click tie-break on linked aliases + duplicate click with different quoting
        b"k_tie_b,u_nye_a2,c_2,2027-12-31T23:50:00Z\n"
        b"\"k_tie_b\",\"u_nye_a2\",\"c_2\",\"2027-12-31T23:50:00Z\"\n"
        b"k_tie_a,u_nye,c_1,2027-12-31T23:50:00Z\n"
        # Assist-only campaign + expiry & re-entry
        b"k_assist,u_nye,c_3,2027-12-25T00:00:00Z\n"
        b"k_re1,u_gap,c_1,2027-12-25T09:00:00Z\n"
        b"k_re2,u_gap,c_1,2028-01-01T09:30:00Z\n"
    )
    lk0 = (
        b"user_id_a,user_id_b\n"
        b"u_nye,u_nye_a2\n"
    )
    write_dataset(
        d,
        "2027-12-28T00:00:00Z",
        "2028-01-11T00:00:00Z",
        [("p1", "books"), ("p2", "apparel")],
        [("c_1", "search"), ("c_2", "social"), ("c_3", "email")],
        {"part-0.csv": lk0},
        {"part-0.csv": cl0},
        {"part-0.csv": ev0, "part-1.csv": ev1},
    )


def case_leap_chain_and_whale(d: Path) -> None:
    # Leap window holding 2028-02-29 + 1000-hop chain + 10%-whale skew
    lk_lines = [b"user_id_a,user_id_b\n"]
    for i in range(1000):
        lk_lines.append(f"ch_{i:04d},ch_{i+1:04d}\n".encode())
    # Unused smallest alias attached at the very end of the 1000-hop chain
    lk_lines.append(b"ch_1000,AA_min_chain\n")

    ev_lines = [b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n"]
    cl_lines = [b"click_id,user_id,campaign_id,ts\n"]

    # Chain events on leap day 2028-02-29 and across Feb 28 -> Feb 29 -> Mar 1
    ev_lines.append(b"ec0,ch_0000,2028-02-28T23:55:00Z,2028-02-28T23:56:00Z,view,p1,\n")
    ev_lines.append(b"ec1,ch_0500,2028-02-29T00:10:00Z,2028-02-29T00:11:00Z,purchase,p1,500\n")
    ev_lines.append(b"ec2,ch_1000,2028-02-29T12:00:00Z,2028-02-29T12:01:00Z,purchase,p2,500\n")
    ev_lines.append(b"ec3,ch_0777,2028-03-01T12:00:00Z,2028-03-01T12:01:00Z,view,p1,\n")
    cl_lines.append(b"kc1,ch_0999,c_1,2028-02-29T11:59:59Z\n")

    # Whale user: 2,500 events (>10% of dataset) + 200 clicks across many campaigns
    base_ms = 1835136000000  # 2028-02-27T00:00:00Z
    for i in range(2500):
        ts = base_ms + i * 60_000  # 1 min apart -> big sessions + multiple days
        ing_s = (ts // 1000) + 10
        # Use F3 for ts, valid F1 for ingest_ts
        import datetime
        ing = datetime.datetime.fromtimestamp(ing_s, tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        et = "purchase" if (i % 10 == 0) else "view"
        pr = str((i % 50) * 100 + 100) if et == "purchase" else ""
        ev_lines.append(f"ew_{i},u_whale,{ts},{ing},{et},p{(i % 2) + 1},{pr}\n".encode())
    for i in range(200):
        ts = base_ms + i * 700_000
        cl_lines.append(f"kw_{i},u_whale,c_{(i % 5) + 1},{ts}\n".encode())

    # 15,000 ordinary events across 300 users
    for i in range(15000):
        u = f"u_ord_{i % 300}"
        ts = base_ms + (i % 500) * 120_000
        ev_lines.append(f"eo_{i},{u},{ts},2028-02-28T12:00:00Z,view,p1,\n".encode())

    write_dataset(
        d,
        "2028-02-25T00:00:00Z",
        "2028-03-10T00:00:00Z",
        [("p1", "books"), ("p2", "apparel")],
        [(f"c_{i}", "search") for i in range(1, 6)],
        {"part-0.csv": b"".join(lk_lines)},
        {"part-0.csv": b"".join(cl_lines)},
        {"part-0.csv": b"".join(ev_lines)},
    )


def case_wide_categories_and_overflow_ids(d: Path) -> None:
    # >256 categories (wide price/category side-table path) with top-category ties,
    # long (>15 B) IDs sharing 15-byte prefixes as users, link names and click IDs,
    # NUL-suffixed IDs, and equal-ts click tie-breaks decided past byte 15.
    import random

    rnd = random.Random(1234)
    cats = [f"cat_{i:04d}" for i in range(600)]
    rnd.shuffle(cats)
    products = [(f"p{i}", cats[i % len(cats)]) for i in range(1500)]
    pre = "longprefix_0123"  # exactly 15 bytes
    users = [f"{pre}{s}" for s in ("a", "b", "aa", "\x00", "zz_tail_very_long_" + "x" * 40)]
    users += ["short", "short\x00", "short\x00\x00", "s"]
    links = [
        f"{users[0]},{users[1]}\n",           # long-long link: name is users[0]
        f"{users[2]},short\x00\n",            # name "longprefix_0123aa" vs "short\x00" -> long one
        f"short\x00\x00,{users[3]}\n",        # name "longprefix_0123\x00"
        f"{users[4]},s\n",                    # name "longprefix_0123zz..." vs "s" -> long one
    ]
    camps = [(f"c_{i}", ch) for i, ch in enumerate(["search", "social", "email", "display"], 1)]
    ev_lines = [b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n"]
    eid = 0
    for u in users:
        for k in range(40):
            day = 1 + (k % 13)
            hh = (k * 5) % 24
            ts = f"2026-10-{day:02d}T{hh:02d}:{(k * 7) % 60:02d}:00Z"
            ing = f"2026-10-{day:02d}T{hh:02d}:{(k * 7) % 60:02d}:30Z"
            p = rnd.randrange(len(products))
            if k % 3 == 0:
                # equal prices so category ties are broken byte-wise
                line = f"e{eid},{u},{ts},{ing},purchase,p{p},{100 if k % 2 else 250}\n"
            else:
                line = f"e{eid},{u},{ts},{ing},view,p{p},\n"
            ev_lines.append(line.encode())
            eid += 1
    cl_lines = [b"click_id,user_id,campaign_id,ts\n"]
    for i, u in enumerate(users):
        for k in range(6):
            day = 1 + (k * 2) % 13
            ts = f"2026-10-{day:02d}T00:00:00Z"
            # same user, same ts, click ids differing only after byte 15
            cl_lines.append(f"{pre}{'b' if k % 2 else 'a'}{i}_{k},{u},c_{1 + (k % 4)},{ts}\n".encode())
            cl_lines.append(f"{pre}{'a' if k % 2 else 'b'}{i}_{k},{u},c_{1 + ((k + 1) % 4)},{ts}\n".encode())
    write_dataset(
        d,
        "2026-10-01T00:00:00Z",
        "2026-10-15T00:00:00Z",
        products,
        camps,
        {"part-0.csv": ("user_id_a,user_id_b\n" + "".join(links)).encode()},
        {"part-0.csv": b"".join(cl_lines)},
        {"part-0.csv": b"".join(ev_lines)},
    )


def run_case(bin_path: str, name: str, builder) -> bool:
    with tempfile.TemporaryDirectory(prefix=f"torture_{name}_") as td:
        base = Path(td)
        data = base / "data"
        oracle_out = base / "oracle"
        builder(data)
        subprocess.run([PY, ORACLE, str(data), str(oracle_out)], check=True)

        for ncpu in ("1", "8"):
            cand_out = base / f"cand_ncpu_{ncpu}"
            cand_out.mkdir()
            env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(cand_out), NCPU=ncpu)
            p = subprocess.run([bin_path, str(data), str(cand_out)], env=env, capture_output=True, text=True)
            if p.returncode != 0:
                print(f"[torture] FAIL {name} (NCPU={ncpu}): exit {p.returncode}\n{p.stderr}", file=sys.stderr)
                return False
            if p.stdout:
                print(f"[torture] FAIL {name} (NCPU={ncpu}): non-empty stdout ({len(p.stdout)} B)", file=sys.stderr)
                return False
            cmp_p = subprocess.run([PY, COMPARE, str(cand_out), str(oracle_out)], capture_output=True, text=True)
            if cmp_p.returncode != 0:
                print(f"[torture] FAIL {name} (NCPU={ncpu}):\n{cmp_p.stdout}\n{cmp_p.stderr}", file=sys.stderr)
                return False
        print(f"[torture] ok   {name} (NCPU=1 & NCPU=8, stdout=0B)")
        return True


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: torture_test.py <CANDIDATE_BIN>", file=sys.stderr)
        return 2
    bin_path = sys.argv[1]
    cases = [
        ("empty_dataset", case_empty),
        ("huge_digits_empty_catalogs", case_nul_and_huge_digits),
        ("simd_and_id_lengths", case_simd_and_id_lengths),
        ("cross_user_dedup_year_boundary", case_cross_user_dedup_and_year_boundary),
        ("leap_1000hop_whale", case_leap_chain_and_whale),
        ("wide_categories_overflow_ids", case_wide_categories_and_overflow_ids),
    ]
    ok = True
    for name, fn in cases:
        if not run_case(bin_path, name, fn):
            ok = False
    print("[torture] ALL PASSED" if ok else "[torture] FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
