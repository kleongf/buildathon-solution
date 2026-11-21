//! Timestamp and purchase-price parsing for the three accepted forms (F1, F2, F3).
//!
//! Uses a 620-byte L1-resident compile-time calendar table (zero divisions/modulos
//! for years 1970..=2100, with exact fallback outside that range), single-branch
//! bitwise ASCII digit validation, and an unrolled 13-digit F3 fast path.

pub const DAY_MS: i64 = 86_400_000;

const MIN_LUT_YEAR: u32 = 1970;
const MAX_LUT_YEAR: u32 = 2100;
const LUT_YEARS: usize = (MAX_LUT_YEAR - MIN_LUT_YEAR + 1) as usize;

#[inline(always)]
const fn is_leap(y: u32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

#[inline(always)]
pub const fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = ((m + 9) % 12) as i64;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

const DAYS_BEFORE_YEAR: [i32; LUT_YEARS] = {
    let mut t = [0i32; LUT_YEARS];
    let mut i = 0;
    while i < LUT_YEARS {
        t[i] = days_from_civil((MIN_LUT_YEAR as usize + i) as i64, 1, 1) as i32;
        i += 1;
    }
    t
};

const DAYS_BEFORE_MONTH: [[i32; 12]; 2] = [
    [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334],
    [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335],
];

const MONTH_DAYS: [[u8; 12]; 2] = [
    [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
    [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
];

/// Returns non-zero in high nibble (0xF0) iff `d >= 10` for any u8 `d`.
#[inline(always)]
fn non_digit_bits(d: u8) -> u8 {
    d | d.wrapping_add(6)
}

#[cold]
fn calendar_out_of_lut(y: u32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> Option<i64> {
    if y == 0 {
        return None;
    }
    let leap = is_leap(y) as usize;
    if d > MONTH_DAYS[leap][(mo - 1) as usize] as u32 {
        return None;
    }
    let days = days_from_civil(y as i64, mo, d);
    Some(days * DAY_MS + (h as i64 * 3600 + mi as i64 * 60 + s as i64) * 1000)
}

/// Parses `YYYY-MM-DDTHH:MM:SS` (first 19 bytes; caller ensures `b.len() >= 19`).
#[inline(always)]
fn calendar(b: &[u8]) -> Option<i64> {
    let p = b.as_ptr();
    unsafe {
        if ((*p.add(4) ^ b'-')
            | (*p.add(7) ^ b'-')
            | (*p.add(10) ^ b'T')
            | (*p.add(13) ^ b':')
            | (*p.add(16) ^ b':'))
            != 0
        {
            return None;
        }
        let y0 = (*p).wrapping_sub(b'0');
        let y1 = (*p.add(1)).wrapping_sub(b'0');
        let y2 = (*p.add(2)).wrapping_sub(b'0');
        let y3 = (*p.add(3)).wrapping_sub(b'0');
        let m0 = (*p.add(5)).wrapping_sub(b'0');
        let m1 = (*p.add(6)).wrapping_sub(b'0');
        let d0 = (*p.add(8)).wrapping_sub(b'0');
        let d1 = (*p.add(9)).wrapping_sub(b'0');
        let h0 = (*p.add(11)).wrapping_sub(b'0');
        let h1 = (*p.add(12)).wrapping_sub(b'0');
        let n0 = (*p.add(14)).wrapping_sub(b'0');
        let n1 = (*p.add(15)).wrapping_sub(b'0');
        let s0 = (*p.add(17)).wrapping_sub(b'0');
        let s1 = (*p.add(18)).wrapping_sub(b'0');

        let bad_digits = non_digit_bits(y0)
            | non_digit_bits(y1)
            | non_digit_bits(y2)
            | non_digit_bits(y3)
            | non_digit_bits(m0)
            | non_digit_bits(m1)
            | non_digit_bits(d0)
            | non_digit_bits(d1)
            | non_digit_bits(h0)
            | non_digit_bits(h1)
            | non_digit_bits(n0)
            | non_digit_bits(n1)
            | non_digit_bits(s0)
            | non_digit_bits(s1);
        if (bad_digits & 0xF0) != 0 {
            return None;
        }

        let y = y0 as u32 * 1000 + y1 as u32 * 100 + y2 as u32 * 10 + y3 as u32;
        let mo = m0 as u32 * 10 + m1 as u32;
        let d = d0 as u32 * 10 + d1 as u32;
        let h = h0 as u32 * 10 + h1 as u32;
        let mi = n0 as u32 * 10 + n1 as u32;
        let s = s0 as u32 * 10 + s1 as u32;

        if mo == 0 || mo > 12 || d == 0 || h > 23 || mi > 59 || s > 59 {
            return None;
        }
        let yi = y.wrapping_sub(MIN_LUT_YEAR);
        if yi < LUT_YEARS as u32 {
            let leap = ((y & 3 == 0) && y != 2100) as usize;
            let mi_idx = (mo - 1) as usize;
            if d > *MONTH_DAYS.get_unchecked(leap).get_unchecked(mi_idx) as u32 {
                return None;
            }
            let days = (*DAYS_BEFORE_YEAR.get_unchecked(yi as usize)
                + *DAYS_BEFORE_MONTH.get_unchecked(leap).get_unchecked(mi_idx)
                + (d - 1) as i32) as i64;
            Some(days * DAY_MS + (h as i64 * 3600 + mi as i64 * 60 + s as i64) * 1000)
        } else {
            calendar_out_of_lut(y, mo, d, h, mi, s)
        }
    }
}

/// Ultra-fast path for `ingest_ts`, which rules.md guarantees is always valid F1
/// (`YYYY-MM-DDTHH:MM:SSZ`). Falls back to strict `parse_ts` if out of LUT year range.
#[inline(always)]
pub fn parse_f1_fast(b: &[u8]) -> Option<i64> {
    if b.len() == 20 {
        let p = b.as_ptr();
        unsafe {
            let y = (*p).wrapping_sub(b'0') as u32 * 1000
                + (*p.add(1)).wrapping_sub(b'0') as u32 * 100
                + (*p.add(2)).wrapping_sub(b'0') as u32 * 10
                + (*p.add(3)).wrapping_sub(b'0') as u32;
            let yi = y.wrapping_sub(MIN_LUT_YEAR);
            let mo = (*p.add(5)).wrapping_sub(b'0') as u32 * 10 + (*p.add(6)).wrapping_sub(b'0') as u32;
            let d = (*p.add(8)).wrapping_sub(b'0') as u32 * 10 + (*p.add(9)).wrapping_sub(b'0') as u32;
            if yi < LUT_YEARS as u32 && mo >= 1 && mo <= 12 && d >= 1 && d <= 31 {
                let h = (*p.add(11)).wrapping_sub(b'0') as i64 * 10 + (*p.add(12)).wrapping_sub(b'0') as i64;
                let mi = (*p.add(14)).wrapping_sub(b'0') as i64 * 10 + (*p.add(15)).wrapping_sub(b'0') as i64;
                let s = (*p.add(17)).wrapping_sub(b'0') as i64 * 10 + (*p.add(18)).wrapping_sub(b'0') as i64;
                let leap = ((y & 3 == 0) && y != 2100) as usize;
                let days = (*DAYS_BEFORE_YEAR.get_unchecked(yi as usize)
                    + *DAYS_BEFORE_MONTH.get_unchecked(leap).get_unchecked((mo - 1) as usize)
                    + (d - 1) as i32) as i64;
                return Some(days * DAY_MS + (h * 3600 + mi * 60 + s) * 1000);
            }
        }
    }
    parse_ts(b)
}

/// Direct-mapped 32-slot L1 date-prefix cache indexed by day-of-month (`1..=31`).
/// For any <=27-day window (14d window + 7d click lookback + 6d late ingest), every
/// calendar day-of-month is unique, so 100% of in-window F1/F2 dates hit with a
/// single u64 prefix compare (`"YYYY-MM-"`) and skip all calendar/leap math.
#[derive(Clone, Copy)]
#[repr(C)]
struct DateSlot {
    prefix: u64, // little-endian 8 bytes "YYYY-MM-"; 0 = empty
    day_ms: i64,
}

pub struct DateCache {
    slots: [DateSlot; 32],
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (y + i64::from(m <= 2), m as u32, d as u32)
}

impl DateCache {
    pub fn new(ws: i64, we: i64) -> DateCache {
        let mut slots = [DateSlot { prefix: 0, day_ms: 0 }; 32];
        let d_start = (ws - 8 * DAY_MS).div_euclid(DAY_MS);
        let d_end = (we + 6 * DAY_MS).div_euclid(DAY_MS);
        let count = (d_end - d_start + 1).clamp(0, 28);
        for off in 0..count {
            let day = d_start + off;
            let (y, m, d) = civil_from_days(day);
            if y >= 1 && y <= 9999 && d >= 1 && d <= 31 {
                let s = format!("{y:04}-{m:02}-");
                let prefix = u64::from_le_bytes(s.as_bytes()[0..8].try_into().unwrap());
                slots[d as usize] = DateSlot { prefix, day_ms: day * DAY_MS };
            }
        }
        DateCache { slots }
    }

    #[inline(always)]
    fn calendar_cached(&self, b: &[u8]) -> Option<i64> {
        let p = b.as_ptr();
        unsafe {
            let d0 = (*p.add(8)).wrapping_sub(b'0');
            let d1 = (*p.add(9)).wrapping_sub(b'0');
            let d = (d0 as u32 * 10 + d1 as u32) as usize;
            if ((non_digit_bits(d0) | non_digit_bits(d1)) & 0xF0) == 0 && d < 32 {
                let slot = *self.slots.get_unchecked(d);
                let pref = (p as *const u64).read_unaligned();
                if pref == slot.prefix
                    && ((*p.add(10) ^ b'T') | (*p.add(13) ^ b':') | (*p.add(16) ^ b':')) == 0
                {
                    let h0 = (*p.add(11)).wrapping_sub(b'0');
                    let h1 = (*p.add(12)).wrapping_sub(b'0');
                    let n0 = (*p.add(14)).wrapping_sub(b'0');
                    let n1 = (*p.add(15)).wrapping_sub(b'0');
                    let s0 = (*p.add(17)).wrapping_sub(b'0');
                    let s1 = (*p.add(18)).wrapping_sub(b'0');
                    let bad = non_digit_bits(h0)
                        | non_digit_bits(h1)
                        | non_digit_bits(n0)
                        | non_digit_bits(n1)
                        | non_digit_bits(s0)
                        | non_digit_bits(s1);
                    if (bad & 0xF0) == 0 {
                        let h = h0 as u32 * 10 + h1 as u32;
                        let mi = n0 as u32 * 10 + n1 as u32;
                        let s = s0 as u32 * 10 + s1 as u32;
                        if h <= 23 && mi <= 59 && s <= 59 {
                            return Some(slot.day_ms + (h as i64 * 3600 + mi as i64 * 60 + s as i64) * 1000);
                        }
                    }
                    return None;
                }
            }
        }
        calendar(b)
    }

    /// Ultra-fast path for `ingest_ts` (guaranteed valid F1 per rules.md).
    #[inline(always)]
    pub fn parse_f1_ingest(&self, b: &[u8]) -> Option<i64> {
        if b.len() == 20 {
            let p = b.as_ptr();
            unsafe {
                let d = ((*p.add(8)).wrapping_sub(b'0') as usize) * 10
                    + (*p.add(9)).wrapping_sub(b'0') as usize;
                if d < 32 {
                    let slot = *self.slots.get_unchecked(d);
                    if (p as *const u64).read_unaligned() == slot.prefix {
                        let h = (*p.add(11)).wrapping_sub(b'0') as i64 * 10
                            + (*p.add(12)).wrapping_sub(b'0') as i64;
                        let mi = (*p.add(14)).wrapping_sub(b'0') as i64 * 10
                            + (*p.add(15)).wrapping_sub(b'0') as i64;
                        let s = (*p.add(17)).wrapping_sub(b'0') as i64 * 10
                            + (*p.add(18)).wrapping_sub(b'0') as i64;
                        return Some(slot.day_ms + (h * 3600 + mi * 60 + s) * 1000);
                    }
                }
            }
        }
        parse_f1_fast(b)
    }

    #[inline(always)]
    pub fn parse_ts(&self, b: &[u8]) -> Option<i64> {
        let n = b.len();
        if n == 20 && unsafe { *b.get_unchecked(19) } == b'Z' {
            return self.calendar_cached(b);
        }
        if n == 13 {
            return parse_f3_13(b);
        }
        if n == 25 {
            let p = b.as_ptr();
            unsafe {
                let sign = *p.add(19);
                if *p.add(22) == b':' && (sign == b'+' || sign == b'-') {
                    let base = self.calendar_cached(b)?;
                    let h0 = (*p.add(20)).wrapping_sub(b'0');
                    let h1 = (*p.add(21)).wrapping_sub(b'0');
                    let m0 = (*p.add(23)).wrapping_sub(b'0');
                    let m1 = (*p.add(24)).wrapping_sub(b'0');
                    if ((non_digit_bits(h0) | non_digit_bits(h1) | non_digit_bits(m0) | non_digit_bits(m1)) & 0xF0)
                        != 0
                    {
                        return None;
                    }
                    let oh = h0 as u32 * 10 + h1 as u32;
                    let om = m0 as u32 * 10 + m1 as u32;
                    if oh > 23 || om > 59 {
                        return None;
                    }
                    let off = (oh as i64 * 60 + om as i64) * 60_000;
                    return Some(if sign == b'+' { base - off } else { base + off });
                }
            }
        }
        parse_f3_general(b)
    }
}

#[inline(always)]
fn parse_f3_13(b: &[u8]) -> Option<i64> {
    let p = b.as_ptr();
    unsafe {
        let d0 = (*p).wrapping_sub(b'0');
        let d1 = (*p.add(1)).wrapping_sub(b'0');
        let d2 = (*p.add(2)).wrapping_sub(b'0');
        let d3 = (*p.add(3)).wrapping_sub(b'0');
        let d4 = (*p.add(4)).wrapping_sub(b'0');
        let d5 = (*p.add(5)).wrapping_sub(b'0');
        let d6 = (*p.add(6)).wrapping_sub(b'0');
        let d7 = (*p.add(7)).wrapping_sub(b'0');
        let d8 = (*p.add(8)).wrapping_sub(b'0');
        let d9 = (*p.add(9)).wrapping_sub(b'0');
        let d10 = (*p.add(10)).wrapping_sub(b'0');
        let d11 = (*p.add(11)).wrapping_sub(b'0');
        let d12 = (*p.add(12)).wrapping_sub(b'0');
        let bad = non_digit_bits(d0)
            | non_digit_bits(d1)
            | non_digit_bits(d2)
            | non_digit_bits(d3)
            | non_digit_bits(d4)
            | non_digit_bits(d5)
            | non_digit_bits(d6)
            | non_digit_bits(d7)
            | non_digit_bits(d8)
            | non_digit_bits(d9)
            | non_digit_bits(d10)
            | non_digit_bits(d11)
            | non_digit_bits(d12);
        if (bad & 0xF0) != 0 {
            return None;
        }
        let hi = (d0 as u64 * 100_000
            + d1 as u64 * 10_000
            + d2 as u64 * 1_000
            + d3 as u64 * 100
            + d4 as u64 * 10
            + d5 as u64)
            * 10_000_000;
        let lo = d6 as u64 * 1_000_000
            + d7 as u64 * 100_000
            + d8 as u64 * 10_000
            + d9 as u64 * 1_000
            + d10 as u64 * 100
            + d11 as u64 * 10
            + d12 as u64;
        Some((hi + lo) as i64)
    }
}

/// Returns epoch ms, or None for a bad timestamp. A digits-only value too large
/// for i64 is returned as i64::MAX (valid form, outside every window).
#[inline(always)]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 20 && unsafe { *b.get_unchecked(19) } == b'Z' {
        return calendar(b);
    }
    if n == 13 {
        return parse_f3_13(b);
    }
    if n == 25 {
        let p = b.as_ptr();
        unsafe {
            let sign = *p.add(19);
            if *p.add(22) == b':' && (sign == b'+' || sign == b'-') {
                let base = calendar(b)?;
                let h0 = (*p.add(20)).wrapping_sub(b'0');
                let h1 = (*p.add(21)).wrapping_sub(b'0');
                let m0 = (*p.add(23)).wrapping_sub(b'0');
                let m1 = (*p.add(24)).wrapping_sub(b'0');
                if ((non_digit_bits(h0) | non_digit_bits(h1) | non_digit_bits(m0) | non_digit_bits(m1)) & 0xF0) != 0 {
                    return None;
                }
                let oh = h0 as u32 * 10 + h1 as u32;
                let om = m0 as u32 * 10 + m1 as u32;
                if oh > 23 || om > 59 {
                    return None;
                }
                let off = (oh as i64 * 60 + om as i64) * 60_000;
                return Some(if sign == b'+' { base - off } else { base + off });
            }
        }
    }
    parse_f3_general(b)
}

#[cold]
fn parse_f3_general(b: &[u8]) -> Option<i64> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    let mut sig = 0usize;
    for &c in b {
        let x = c.wrapping_sub(b'0');
        if x >= 10 {
            return None;
        }
        if sig > 0 || x != 0 {
            sig += 1;
            if sig <= 18 {
                v = v * 10 + x as u64;
            }
        }
    }
    if sig > 18 {
        return Some(i64::MAX);
    }
    Some(v as i64)
}

/// Purchase price: digits only, value 1..=10_000_000. Returns 0 when invalid.
#[inline(always)]
pub fn parse_price(b: &[u8]) -> u32 {
    let n = b.len();
    if n == 0 {
        return 0;
    }
    if n <= 8 && b[0] != b'0' {
        let mut v: u32 = 0;
        for &c in b {
            let x = c.wrapping_sub(b'0');
            if x >= 10 {
                return 0;
            }
            v = v * 10 + x as u32;
        }
        return if v >= 1 && v <= 10_000_000 { v } else { 0 };
    }
    parse_price_slow(b)
}

#[cold]
fn parse_price_slow(b: &[u8]) -> u32 {
    let mut v: u64 = 0;
    let mut sig = 0usize;
    for &c in b {
        let x = c.wrapping_sub(b'0');
        if x >= 10 {
            return 0;
        }
        if sig > 0 || x != 0 {
            sig += 1;
            if sig > 9 {
                return 0;
            }
            v = v * 10 + x as u64;
        }
    }
    if (1..=10_000_000).contains(&v) {
        v as u32
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn forms_and_lut_edges() {
        // Exhaustively verify non_digit_bits for all 256 u8 values
        for x in 0u8..=255 {
            assert_eq!((non_digit_bits(x) & 0xF0) == 0, x < 10, "x={x}");
        }
        let f1 = parse_ts(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(f1, 1_791_071_890_000);
        assert_eq!(parse_f1_fast(b"2026-10-03T23:58:10Z"), Some(f1));
        assert_eq!(parse_ts(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse_ts(b"1791071890000"), Some(f1));
        assert_eq!(parse_ts(b"0001791071890000"), Some(f1));
        assert_eq!(parse_ts(b"2026-10-03T23:58:10-00:00"), Some(f1));
        assert_eq!(parse_ts(b"2026-10-03T23:58:10z"), None);
        assert_eq!(parse_ts(b"2026-10-03t23:58:10Z"), None);
        assert_eq!(parse_ts(b"2026-10-03 23:58:10Z"), None);
        assert_eq!(parse_ts(b"2026-02-29T00:00:00Z"), None);
        assert_eq!(parse_ts(b"2027-02-29T00:00:00Z"), None);
        assert!(parse_ts(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(parse_ts(b"2100-02-29T00:00:00Z"), None);
        assert!(parse_ts(b"2000-02-29T00:00:00Z").is_some());
        assert_eq!(parse_ts(b"0000-01-01T00:00:00Z"), None);
        assert!(parse_ts(b"0001-01-01T00:00:00Z").is_some());
        assert!(parse_ts(b"9999-12-31T23:59:59Z").is_some());
        assert_eq!(parse_ts(b"1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_ts(b"2026-10-03T24:00:00Z"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:60Z"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10+24:00"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10+05:60"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10+0200"), None);
        assert_eq!(parse_ts(b"1791071890000.0"), None);
        assert_eq!(parse_ts(b"-5"), None);
        assert_eq!(parse_ts(b""), None);
        assert_eq!(parse_ts(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_ts(b"9223372036854775808"), Some(i64::MAX));
        assert_eq!(parse_ts(b"99999999999999999999999"), Some(i64::MAX));
        assert_eq!(parse_ts(b"86400000"), Some(86_400_000));
        assert_eq!(parse_price(b"0250"), 250);
        assert_eq!(parse_price(b"10000000"), 10_000_000);
        assert_eq!(parse_price(b"10000001"), 0);
        assert_eq!(parse_price(b"0"), 0);
        assert_eq!(parse_price(b"000000000000000000001"), 1);
        assert_eq!(parse_price(b"1000\r"), 0);
    }
}
