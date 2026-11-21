#!/usr/bin/env python3
"""
100% spec-faithful reference implementation of Databricks Shopping Sessions
for differential fuzz testing on fresh seeds.

Usage: reference_oracle.py [DATA_DIR] [OUTPUT_DIR]
Also honors DATA_DIR and OUTPUT_DIR env vars.
"""
from __future__ import annotations

import calendar
import datetime
import json
import os
import sys
sys.set_int_max_str_digits(0)
from collections import defaultdict
from pathlib import Path

import polars as pl

DAY_MS = 86_400_000
SEVEN_DAYS_MS = 7 * DAY_MS
THIRTY_MIN_MS = 1_800_000
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def is_ascii_digits(b: bytes) -> bool:
    return len(b) > 0 and all(48 <= c <= 57 for c in b)


def parse_csv_line(raw: bytes, expected_fields: int) -> list[bytes] | None:
    if not raw:
        return None
    parts = raw.split(b",")
    if len(parts) != expected_fields:
        return None
    out: list[bytes] = []
    for p in parts:
        if len(p) >= 2 and p[0] == 34 and p[-1] == 34:
            inner = p[1:-1]
            if b'"' in inner:
                return None
            out.append(inner)
        else:
            if b'"' in p:
                return None
            out.append(p)
    return out


def parse_two_digits(b: bytes, i: int) -> int | None:
    c0, c1 = b[i], b[i + 1]
    if 48 <= c0 <= 57 and 48 <= c1 <= 57:
        return (c0 - 48) * 10 + (c1 - 48)
    return None


def parse_four_digits(b: bytes, i: int) -> int | None:
    v = 0
    for k in range(i, i + 4):
        c = b[k]
        if not (48 <= c <= 57):
            return None
        v = v * 10 + (c - 48)
    return v


def parse_calendar_prefix(b: bytes) -> int | None:
    # YYYY-MM-DDTHH:MM:SS (19 bytes)
    if b[4] != 45 or b[7] != 45 or b[10] != 84 or b[13] != 58 or b[16] != 58:
        return None
    year = parse_four_digits(b, 0)
    month = parse_two_digits(b, 5)
    day = parse_two_digits(b, 8)
    hour = parse_two_digits(b, 11)
    minute = parse_two_digits(b, 14)
    second = parse_two_digits(b, 17)
    if None in (year, month, day, hour, minute, second):
        return None
    assert year is not None and month is not None and day is not None
    assert hour is not None and minute is not None and second is not None
    if not (1 <= month <= 12):
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
        return None
    # Handle leap year accurately for any 4-digit year (0001..9999)
    if year == 0:
        return None
    max_day = calendar.monthrange(year, month)[1]
    if not (1 <= day <= max_day):
        return None
    dt = datetime.datetime(year, month, day, hour, minute, second, tzinfo=datetime.timezone.utc)
    return int((dt - EPOCH).total_seconds()) * 1000


def parse_timestamp(b: bytes) -> int | None:
    n = len(b)
    if n == 0:
        return None
    # F3: ASCII digits only (epoch ms, leading zeros allowed)
    if is_ascii_digits(b):
        # Python int handles arbitrary length; out-of-window values will be filtered by window check
        return int(b.decode("ascii"))
    # F1: YYYY-MM-DDTHH:MM:SSZ (len 20)
    if n == 20 and b[19] == 90:
        return parse_calendar_prefix(b)
    # F2: YYYY-MM-DDTHH:MM:SS(+|-)HH:MM (len 25)
    if n == 25 and (b[19] == 43 or b[19] == 45) and b[22] == 58:
        base = parse_calendar_prefix(b)
        if base is None:
            return None
        oh = parse_two_digits(b, 20)
        om = parse_two_digits(b, 23)
        if oh is None or om is None or oh > 23 or om > 59:
            return None
        offset_ms = (oh * 60 + om) * 60_000
        if b[19] == 43:  # '+'
            return base - offset_ms
        return base + offset_ms
    return None


def iter_csv_data_lines(path: Path):
    data = path.read_bytes()
    if not data:
        return
    # Split on \n ONLY (never strip \r)
    lines = data.split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    # Skip header row
    for line in lines[1:]:
        yield line


class UnionFind:
    def __init__(self) -> None:
        self.parent: dict[bytes, bytes] = {}

    def find(self, x: bytes) -> bytes:
        root = x
        while self.parent.get(root, root) != root:
            root = self.parent[root]
        cur = x
        while self.parent.get(cur, cur) != root:
            nxt = self.parent[cur]
            self.parent[cur] = root
            cur = nxt
        return root

    def union(self, a: bytes, b: bytes) -> None:
        if a not in self.parent:
            self.parent[a] = a
        if b not in self.parent:
            self.parent[b] = b
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        # Keep byte-wise smallest as root!
        if ra < rb:
            self.parent[rb] = ra
        else:
            self.parent[ra] = rb

    def canonical(self, x: bytes) -> bytes:
        if x not in self.parent:
            return x
        return self.find(x)


def run(data_dir: Path, out_dir: Path) -> None:
    manifest = json.loads((data_dir / "manifest.json").read_text("utf-8"))
    window_start = parse_timestamp(manifest["window_start"].encode("utf-8"))
    window_end = parse_timestamp(manifest["window_end"].encode("utf-8"))
    assert window_start is not None and window_end is not None
    click_window_start = window_start - SEVEN_DAYS_MS

    # Products lookup
    products: dict[bytes, str] = {}
    for line in iter_csv_data_lines(data_dir / "products.csv"):
        fields = parse_csv_line(line, 2)
        if fields is not None:
            products[fields[0]] = fields[1].decode("utf-8")

    # Campaigns lookup
    campaigns: dict[bytes, str] = {}
    for line in iter_csv_data_lines(data_dir / "campaigns.csv"):
        fields = parse_csv_line(line, 2)
        if fields is not None:
            campaigns[fields[0]] = fields[1].decode("utf-8")

    # 1. Links
    uf = UnionFind()
    links_dir = data_dir / "links"
    if links_dir.is_dir():
        for p in sorted(links_dir.glob("*.csv")):
            for line in iter_csv_data_lines(p):
                fields = parse_csv_line(line, 2)
                if fields is None:
                    continue
                a, b = fields
                if not a or not b:
                    continue
                uf.union(a, b)

    # 2. Events: parse -> dedup on event_id by smallest ingest_ts -> validate -> late filter
    best_events: dict[bytes, tuple[int, list[bytes]]] = {}
    events_dir = data_dir / "events"
    if events_dir.is_dir():
        for p in sorted(events_dir.glob("*.csv")):
            for line in iter_csv_data_lines(p):
                fields = parse_csv_line(line, 7)
                if fields is None:
                    continue
                event_id = fields[0]
                if not event_id:
                    continue
                ingest_ts = parse_timestamp(fields[3])
                assert ingest_ts is not None
                prev = best_events.get(event_id)
                if prev is None or ingest_ts < prev[0]:
                    best_events[event_id] = (ingest_ts, fields)

    person_events: dict[bytes, list[tuple[int, int, str, int]]] = defaultdict(list)
    for ingest_ts, fields in best_events.values():
        _, user_id, ts_b, _, etype_b, prod_b, price_b = fields
        if not user_id or not prod_b:
            continue
        ts = parse_timestamp(ts_b)
        if ts is None or not (window_start <= ts < window_end):
            continue
        if etype_b == b"view":
            is_purchase = 0
            price = 0
        elif etype_b == b"cart":
            is_purchase = 0
            price = 0
        elif etype_b == b"purchase":
            if not is_ascii_digits(price_b):
                continue
            price = int(price_b.decode("ascii"))
            if not (1 <= price <= 10_000_000):
                continue
            is_purchase = 1
        else:
            continue
        if ingest_ts - ts > DAY_MS:
            continue
        cat = products.get(prod_b, "unknown")
        person = uf.canonical(user_id)
        person_events[person].append((ts, is_purchase, cat, price))

    del best_events

    # 3. Clicks: parse -> dedup on click_id -> validate
    seen_clicks: set[bytes] = set()
    person_clicks: dict[bytes, list[tuple[int, bytes, bytes]]] = defaultdict(list)
    clicks_dir = data_dir / "clicks"
    if clicks_dir.is_dir():
        for p in sorted(clicks_dir.glob("*.csv")):
            for line in iter_csv_data_lines(p):
                fields = parse_csv_line(line, 4)
                if fields is None:
                    continue
                click_id, user_id, camp_id, ts_b = fields
                if not click_id or click_id in seen_clicks:
                    continue
                seen_clicks.add(click_id)
                if not user_id or camp_id not in campaigns:
                    continue
                ts = parse_timestamp(ts_b)
                if ts is None or not (click_window_start <= ts < window_end):
                    continue
                person = uf.canonical(user_id)
                person_clicks[person].append((ts, click_id, camp_id))
    del seen_clicks

    # 4. Sessionize, credit, assist, aggregate
    user_rows: list[tuple[str, datetime.date, int, int, int, str | None]] = []
    # (campaign_id, day_int) -> [sessions, purchasing_sessions, revenue_cents, assisted_sessions]
    camp_agg: dict[tuple[bytes, int], list[int]] = {}

    for person, evs in person_events.items():
        evs.sort(key=lambda x: x[0])
        clicks = person_clicks.get(person)
        if clicks:
            # Sort by ts ascending, click_id ascending (byte-wise)
            clicks.sort(key=lambda x: (x[0], x[1]))

        # Group into sessions
        sessions: list[tuple[int, int, int, list[tuple[str, int]]]] = []
        s_start = evs[0][0]
        prev_ts = s_start
        s_events = 0
        s_rev = 0
        s_purchases: list[tuple[str, int]] = []

        for ts, is_purch, cat, price in evs:
            if ts - prev_ts > THIRTY_MIN_MS:
                sessions.append((s_start, s_events, s_rev, s_purchases))
                s_start = ts
                s_events = 0
                s_rev = 0
                s_purchases = []
            prev_ts = ts
            s_events += 1
            if is_purch:
                s_rev += price
                s_purchases.append((cat, price))
        sessions.append((s_start, s_events, s_rev, s_purchases))

        # Aggregate user_daily per day
        # day_int -> [sessions, events, rev, cat_rev_dict]
        day_acc: dict[int, list] = {}

        # Two-pointer over clicks for attribution & assists
        c_left = 0
        c_right = 0
        n_clicks = len(clicks) if clicks else 0
        active_camp_counts: dict[bytes, int] = {}
        # Track latest ts click with smallest click_id at that ts
        latest_ts = -1
        latest_click_id = b""
        latest_camp = b""

        for s_start, s_events, s_rev, s_purchases in sessions:
            day_int = s_start // DAY_MS
            acc = day_acc.get(day_int)
            if acc is None:
                acc = [0, 0, 0, defaultdict(int)]
                day_acc[day_int] = acc
            acc[0] += 1
            acc[1] += s_events
            acc[2] += s_rev
            for cat, price in s_purchases:
                acc[3][cat] += price

            if clicks:
                while c_right < n_clicks and clicks[c_right][0] <= s_start:
                    cts, cid, camp = clicks[c_right]
                    active_camp_counts[camp] = active_camp_counts.get(camp, 0) + 1
                    # Since clicks sorted by (ts asc, click_id asc), first seen at a new ts has smallest click_id!
                    if cts > latest_ts:
                        latest_ts = cts
                        latest_click_id = cid
                        latest_camp = camp
                    c_right += 1
                min_ts = s_start - SEVEN_DAYS_MS
                while c_left < c_right and clicks[c_left][0] < min_ts:
                    _, _, camp = clicks[c_left]
                    cnt = active_camp_counts[camp] - 1
                    if cnt == 0:
                        del active_camp_counts[camp]
                    else:
                        active_camp_counts[camp] = cnt
                    c_left += 1

                if c_left < c_right and latest_ts >= min_ts:
                    credited = latest_camp
                else:
                    credited = b"organic"

                # Assists
                for camp in active_camp_counts:
                    key = (camp, day_int)
                    entry = camp_agg.get(key)
                    if entry is None:
                        camp_agg[key] = [0, 0, 0, 1]
                    else:
                        entry[3] += 1
            else:
                credited = b"organic"

            key = (credited, day_int)
            entry = camp_agg.get(key)
            if entry is None:
                camp_agg[key] = [1, 1 if s_purchases else 0, s_rev, 0]
            else:
                entry[0] += 1
                if s_purchases:
                    entry[1] += 1
                entry[2] += s_rev

        person_str = person.decode("utf-8")
        for day_int, (s_cnt, e_cnt, rev, cat_rev) in day_acc.items():
            if cat_rev:
                # Highest revenue, tie-break byte-wise smallest category
                top_cat = min(cat_rev.items(), key=lambda kv: (-kv[1], kv[0].encode("utf-8")))[0]
            else:
                top_cat = None
            day_date = datetime.date(1970, 1, 1) + datetime.timedelta(days=day_int)
            user_rows.append((person_str, day_date, s_cnt, e_cnt, rev, top_cat))

    # Write outputs
    user_dir = out_dir / "user_daily"
    camp_dir = out_dir / "campaign_daily"
    user_dir.mkdir(parents=True, exist_ok=True)
    camp_dir.mkdir(parents=True, exist_ok=True)

    user_schema = {
        "user_id": pl.String,
        "day": pl.Date,
        "sessions": pl.Int64,
        "events": pl.Int64,
        "revenue_cents": pl.Int64,
        "top_category": pl.String,
    }
    if user_rows:
        user_df = pl.DataFrame(user_rows, schema=user_schema, orient="row")
    else:
        user_df = pl.DataFrame(schema=user_schema)
    user_df.write_parquet(user_dir / "part-000.parquet")

    camp_rows = []
    for (camp_b, day_int), (sess, psess, rev, assists) in camp_agg.items():
        camp_str = camp_b.decode("utf-8")
        chan = "organic" if camp_b == b"organic" else campaigns[camp_b]
        day_date = datetime.date(1970, 1, 1) + datetime.timedelta(days=day_int)
        camp_rows.append((camp_str, chan, day_date, sess, psess, rev, assists))

    camp_schema = {
        "campaign_id": pl.String,
        "channel": pl.String,
        "day": pl.Date,
        "sessions": pl.Int64,
        "purchasing_sessions": pl.Int64,
        "revenue_cents": pl.Int64,
        "assisted_sessions": pl.Int64,
    }
    if camp_rows:
        camp_df = pl.DataFrame(camp_rows, schema=camp_schema, orient="row")
    else:
        camp_df = pl.DataFrame(schema=camp_schema)
    camp_df.write_parquet(camp_dir / "part-000.parquet")


def main() -> None:
    if len(sys.argv) >= 3:
        data_dir = Path(sys.argv[1])
        out_dir = Path(sys.argv[2])
    else:
        data_dir = Path(os.environ["DATA_DIR"])
        out_dir = Path(os.environ["OUTPUT_DIR"])
    run(data_dir, out_dir)


if __name__ == "__main__":
    main()
