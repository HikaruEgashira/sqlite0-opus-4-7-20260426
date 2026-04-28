//! Calendar arithmetic for sqlite3-compatible date/time functions.
//!
//! All sqlite3 date/time results live in the proleptic Gregorian calendar
//! over the year range -4713..=9999 (matching sqlite3 3.51.0; lower bound
//! corresponds to JDN 0). This module is pure math — no Value, no
//! allocator, no I/O. Format dispatch lives in `funcs_time.zig`; modifier
//! interpretation lives in `funcs_time_modifier.zig`.

const std = @import("std");

pub const DateTime = struct {
    year: i32, // -4713..=9999 (proleptic Gregorian, astronomical numbering)
    month: u8, // 1-12
    day: u8, // 1-31
    hour: u8 = 0,
    minute: u8 = 0,
    second: u8 = 0,
    millisecond: u16 = 0, // 0-999
};

pub const Ymd = struct { year: i32, month: u8, day: u8 };

pub const min_year: i32 = -4713;
pub const max_year: i32 = 9999;
/// JDN of `9999-12-31` (max representable date). `julianDayNumber(9999, 12, 31)`.
pub const max_jdn: i64 = 5373484;

/// Accept `YYYY-MM-DD`, `YYYY-MM-DD HH:MM[:SS[.fff]][Z|±HH:MM]`,
/// `YYYY-MM-DDTHH:MM[:SS[.fff]][Z|±HH:MM]`, or `HH:MM[:SS[.fff]][Z|±HH:MM]`
/// (time-only — date defaults to 2000-01-01 per sqlite3 time() docs).
/// Year must lie in 0..=9999, month in 1..=12, day in 1..=31.
/// Hour/minute/second are strict (≤23/59/59 — sqlite3 rejects `25:00:00`).
///
/// Sub-second precision: `.NNN…` reads as fractional seconds, stored as
/// integer milliseconds. First three digits are the integer ms; a fourth
/// digit ≥ '5' rounds up (with a clamp at 999 to keep the parser from
/// rolling into the next second — sqlite3 does the same). Trailing
/// digits past the fourth are ignored. `'2024-01-01 12:34:56.0009'`
/// thus stores 1ms; `'.9999'` stores 999ms (not 1000); `.50050'` → 501.
///
/// **Timezone**:
///   * `Z` (Zulu/UTC) — already-UTC, no shift.
///   * `±HH:MM` — input is at the given offset from UTC; we shift to UTC.
///     `+05:00` means input is 5h ahead of UTC, so we subtract 5h.
///     Range: hours 0..14, minutes 0..59 (sqlite3 rejects `+15:00`,
///     `+14:60`, etc.).
/// Anything else past the seconds (or fractional digits) → NULL.
///
/// **Partial time**: `HH:MM` (5 chars) is accepted, seconds default to 0.
/// `HH` alone is NOT a time — sqlite3 falls through to numeric/JD.
///
/// Day-overflow within a valid month (e.g. `2023-02-29`, `2023-04-31`)
/// is renormalised forward via Julian round-trip to match sqlite3's
/// "lenient day, strict everything else" rule (`date('2023-02-29')`
/// → `2023-03-01`). Renormalisation never crosses out of the 0..=9999
/// year window — the only literal that could trigger a year overflow
/// is day=32, which is rejected upstream.
pub fn parseDateTime(in: []const u8) ?DateTime {
    // sqlite3 strips trailing ASCII whitespace before parsing
    // (`date('2024-01-01  ')` → `'2024-01-01'`) but rejects any leading
    // whitespace (`date('  2024-01-01')` → NULL). Mirror exactly.
    var end = in.len;
    while (end > 0) {
        const c = in[end - 1];
        if (c != ' ' and c != '\t' and c != '\n' and c != '\r') break;
        end -= 1;
    }
    const s = in[0..end];
    // Time-only `HH:MM[:SS[.fff]][Z|±HH:MM]`: sqlite3 fills the date with 2000-01-01.
    if (s.len >= 5 and s[2] == ':') {
        const hour = parseUintFixed(u8, s[0..2]) orelse return null;
        const minute = parseUintFixed(u8, s[3..5]) orelse return null;
        // sqlite3 quirk: `24:MM:SS[.fff]` is accepted as a literal
        // representing midnight of the next day (`datetime('… 24:00:00')`
        // round-trips unchanged; `julianday` returns JDN+0.5). hour=25
        // is still rejected. minute/second remain strict ≤59.
        if (hour > 24 or minute > 59) return null;
        var second: u8 = 0;
        var ms_tail_start: usize = 5;
        var seconds_present = false;
        if (s.len >= 8 and s[5] == ':') {
            second = parseUintFixed(u8, s[6..8]) orelse return null;
            if (second > 59) return null;
            ms_tail_start = 8;
            seconds_present = true;
        }
        const tail = parseTzSubsecTail(s[ms_tail_start..], seconds_present) orelse return null;
        var dt: DateTime = .{ .year = 2000, .month = 1, .day = 1, .hour = hour, .minute = minute, .second = second, .millisecond = tail.ms };
        if (tail.offset_min != 0) dt = applyTzOffset(dt, tail.offset_min) orelse return null;
        return dt;
    }
    // Optional `-` sign on the year (BC dates: `-0001-12-31` etc.).
    // sqlite3 only allows a leading `-`, never `+` — `+0001-12-31` falls
    // through to the numeric-prefix path and gets rejected as a date.
    var off: usize = 0;
    var year_sign: i32 = 1;
    if (s.len >= 11 and s[0] == '-') {
        year_sign = -1;
        off = 1;
    }
    if (s.len < 10 + off) return null;
    if (s[4 + off] != '-' or s[7 + off] != '-') return null;
    const year_abs = parseUintFixed(u16, s[off .. 4 + off]) orelse return null;
    const month = parseUintFixed(u8, s[5 + off .. 7 + off]) orelse return null;
    const day = parseUintFixed(u8, s[8 + off .. 10 + off]) orelse return null;
    if (month < 1 or month > 12) return null;
    if (day < 1 or day > 31) return null;
    const year_signed: i32 = year_sign * @as(i32, year_abs);
    if (year_signed < min_year) return null;

    var dt = DateTime{ .year = year_signed, .month = month, .day = day };
    var offset_min: i16 = 0;
    if (s.len > 10 + off) {
        if (s[10 + off] != ' ' and s[10 + off] != 'T') return null;
        // sqlite3 quirk (date.c parseYyyyMmDd): a date-only string followed
        // by a bare `T` separator (no HH:MM after it) is accepted as the
        // date-only form. `datetime('2024-01-01T')` → `'2024-01-01 00:00:00'`.
        // Trailing space is already stripped earlier, so this only fires
        // for `'YYYY-MM-DDT'` exactly.
        if (s.len == 11 + off) return dt;
        // Datetime with time component: needs at least HH:MM (16 chars total + sign).
        if (s.len < 16 + off) return null;
        if (s[13 + off] != ':') return null;
        const hour = parseUintFixed(u8, s[11 + off .. 13 + off]) orelse return null;
        const minute = parseUintFixed(u8, s[14 + off .. 16 + off]) orelse return null;
        // sqlite3 quirk: `24:MM:SS[.fff]` is accepted as a literal
        // representing midnight of the next day (`datetime('… 24:00:00')`
        // round-trips unchanged; `julianday` returns JDN+0.5). hour=25
        // is still rejected. minute/second remain strict ≤59.
        if (hour > 24 or minute > 59) return null;
        var second: u8 = 0;
        var tail_start: usize = 16 + off;
        var seconds_present = false;
        if (s.len >= 19 + off and s[16 + off] == ':') {
            second = parseUintFixed(u8, s[17 + off .. 19 + off]) orelse return null;
            if (second > 59) return null;
            tail_start = 19 + off;
            seconds_present = true;
        }
        const tail = parseTzSubsecTail(s[tail_start..], seconds_present) orelse return null;
        dt.hour = hour;
        dt.minute = minute;
        dt.second = second;
        dt.millisecond = tail.ms;
        offset_min = tail.offset_min;
    }

    if (dt.day > daysInMonth(dt.year, dt.month)) {
        // Lenient day overflow → roll forward via Julian round-trip.
        // Time fields (including ms) ride through unchanged.
        const jd = dateTimeToJulianFloat(dt);
        dt = julianFloatToDateTime(jd) orelse return null;
    }
    if (offset_min != 0) dt = applyTzOffset(dt, offset_min) orelse return null;
    return dt;
}

const TzSubsecTail = struct { ms: u16, offset_min: i16 };

/// Subtract `offset_min` minutes from `dt` to convert input-local time
/// to UTC (`+05:00` input → subtract 5h). Round-trip via Julian to handle
/// day/month/year crossings. Out-of-range result (year < 0 or > 9999) → null.
fn applyTzOffset(dt: DateTime, offset_min: i16) ?DateTime {
    const jd = dateTimeToJulianFloat(dt);
    const adjusted_jd = jd - @as(f64, @floatFromInt(offset_min)) / 1440.0;
    return julianFloatToDateTime(adjusted_jd);
}

/// Parse the optional `.fff…[Z|±HH:MM]` tail after the seconds (or after
/// HH:MM when no seconds field is given). Empty / `Z` → 0ms, 0 offset. A
/// `.` must be followed by ≥1 digit. `±HH:MM` is accepted with hours
/// 0..14 and minutes 0..59; sqlite3 rejects `+15:00`, `+14:60`, missing
/// colon, single-digit hour, etc. Returns null if the tail contains
/// anything else after the recognized tokens.
fn parseTzSubsecTail(tail: []const u8, seconds_present: bool) ?TzSubsecTail {
    var idx: usize = 0;
    var ms: u16 = 0;
    // Fractional seconds are only valid AFTER HH:MM:SS — sqlite3 rejects
    // `'12:34.5'` because `.fff` requires the SS field. The TZ token (Z
    // or ±HH:MM) is still accepted in the no-seconds path.
    if (seconds_present and idx < tail.len and tail[idx] == '.') {
        idx += 1;
        // Require at least one digit after the dot — sqlite3 rejects `'…56.'`.
        if (idx >= tail.len or tail[idx] < '0' or tail[idx] > '9') return null;
        var digits: u8 = 0;
        while (idx < tail.len and tail[idx] >= '0' and tail[idx] <= '9' and digits < 3) : ({
            idx += 1;
            digits += 1;
        }) {
            ms = ms * 10 + (tail[idx] - '0');
        }
        // Pad to ms-precision: `.5` → 500, `.78` → 780, `.789` → 789.
        while (digits < 3) : (digits += 1) ms *= 10;
        // Round-half-up using the 4th digit, clamping at 999 — sqlite3
        // truncates rather than rolling into the next second
        // (`'2024-01-01 12:34:56.9995'` → `56.999`, NOT `57.000`).
        if (idx < tail.len and tail[idx] >= '0' and tail[idx] <= '9') {
            const fourth = tail[idx];
            idx += 1;
            if (fourth >= '5' and ms < 999) ms += 1;
            // Skip remaining digits — sqlite3 ignores anything past the 4th.
            while (idx < tail.len and tail[idx] >= '0' and tail[idx] <= '9') : (idx += 1) {}
        }
    }
    // sqlite3 `parseTimezone` (date.c) skips leading whitespace before the
    // TZ token — `'12:00:00 +09:00'` / `'12:00:00.5 +09:00'` / tab-separated
    // forms all accepted. The whitespace must be followed by a TZ token; a
    // bare trailing run of whitespace (no TZ) is handled at the outer
    // `parseDateTime` level via the trailing-space tolerance there.
    while (idx < tail.len and (tail[idx] == ' ' or tail[idx] == '\t' or tail[idx] == '\n' or tail[idx] == '\r')) idx += 1;
    var offset_min: i16 = 0;
    // sqlite3 accepts both `Z` and `z` for Zulu time.
    if (idx < tail.len and (tail[idx] == 'Z' or tail[idx] == 'z')) {
        idx += 1;
    } else if (idx < tail.len and (tail[idx] == '+' or tail[idx] == '-')) {
        const sign: i16 = if (tail[idx] == '+') 1 else -1;
        idx += 1;
        if (idx + 5 > tail.len or tail[idx + 2] != ':') return null;
        const tz_hour = parseUintFixed(u8, tail[idx .. idx + 2]) orelse return null;
        const tz_min = parseUintFixed(u8, tail[idx + 3 .. idx + 5]) orelse return null;
        if (tz_hour > 14 or tz_min > 59) return null;
        offset_min = sign * (@as(i16, tz_hour) * 60 + @as(i16, tz_min));
        idx += 5;
    }
    if (idx != tail.len) return null;
    return .{ .ms = ms, .offset_min = offset_min };
}

fn parseUintFixed(comptime T: type, s: []const u8) ?T {
    var n: T = 0;
    for (s) |c| {
        if (c < '0' or c > '9') return null;
        n = n * 10 + (c - '0');
    }
    return n;
}

/// Proleptic Gregorian leap-year rule extended to negative (BC) years
/// using astronomical year numbering. `@rem` keeps the `== 0` check
/// signs-agnostic — `@rem(-100, 400) = -100 ≠ 0` correctly classifies
/// 101 BC as non-leap; `@rem(-400, 400) = 0` keeps 401 BC leap. Verified
/// against sqlite3 3.51.0 (`julianday('-0001-02-29')` renormalises to
/// `-0001-03-01`; `julianday('-0008-02-29')` does not).
pub fn isLeapYear(y: i32) bool {
    return (@rem(y, 4) == 0 and @rem(y, 100) != 0) or @rem(y, 400) == 0;
}

pub fn daysInMonth(y: i32, m: u8) u8 {
    return switch (m) {
        1, 3, 5, 7, 8, 10, 12 => 31,
        4, 6, 9, 11 => 30,
        2 => if (isLeapYear(y)) 29 else 28,
        else => 0,
    };
}

pub fn dayOfYear(dt: DateTime) u16 {
    const cum = [_]u16{ 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 };
    var doy: u16 = cum[dt.month - 1] + dt.day;
    if (isLeapYear(dt.year) and dt.month > 2) doy += 1;
    return doy;
}

/// ISO 8601 weekday: 1=Monday, 2=Tuesday, ..., 7=Sunday. Wraps `dayOfWeek`
/// (which is `%w` semantics: Sun=0..Sat=6) so callers needing the
/// Mon-based form don't have to remap it themselves.
pub fn isoWeekday(dt: DateTime) u8 {
    const w = dayOfWeek(dt);
    return if (w == 0) 7 else w;
}

/// ISO 8601 week-of-year (1..=53) paired with the ISO week-numbering year.
/// Standard formula `(10 + doy - iso_wd) / 7` selects the week that
/// contains this date's Thursday; if that Thursday falls outside the
/// civil year, the week's iso-year shifts. Year 0/9999 boundary clamps
/// rather than overflows — those edge years matter only for %G near
/// Jan 1 / Dec 31, and clamping keeps the rendered output deterministic.
pub const IsoWeekYear = struct { week: u8, year: i32 };

pub fn isoWeekAndYear(dt: DateTime) IsoWeekYear {
    const doy: i32 = @intCast(dayOfYear(dt));
    const wd: i32 = @intCast(isoWeekday(dt));
    const week_raw = @divFloor(10 + doy - wd, 7);

    if (week_raw < 1) {
        if (dt.year == min_year) return .{ .week = 1, .year = min_year };
        return .{ .week = weeksInYear(dt.year - 1), .year = dt.year - 1 };
    }
    const wpy = weeksInYear(dt.year);
    if (week_raw > wpy) {
        if (dt.year == max_year) return .{ .week = wpy, .year = max_year };
        return .{ .week = 1, .year = dt.year + 1 };
    }
    return .{ .week = @intCast(week_raw), .year = dt.year };
}

/// Total ISO 8601 weeks in `y`. A year has 53 weeks iff Jan 1 is Thursday,
/// or Jan 1 is Wednesday in a leap year (the extra leap day pushes the
/// year's week 53 into existence). All other years have 52 weeks.
pub fn weeksInYear(y: i32) u8 {
    const jan1 = DateTime{ .year = y, .month = 1, .day = 1 };
    const wd = isoWeekday(jan1);
    if (wd == 4) return 53;
    if (wd == 3 and isLeapYear(y)) return 53;
    return 52;
}

/// Monday-based week-of-year (00..=53) — strftime `%W` semantics.
/// All days before the first Monday of the calendar year are week 00,
/// the first Monday starts week 01, and weeks roll over every Mon.
/// Formula `(doy - iso_wd + 7) / 7` lands week 01 on the first Mon
/// regardless of which weekday Jan 1 falls on.
pub fn weekOfYearMonday(dt: DateTime) u8 {
    const doy: i32 = @intCast(dayOfYear(dt));
    const wd: i32 = @intCast(isoWeekday(dt));
    return @intCast(@divFloor(doy - wd + 7, 7));
}

/// Sunday-based week-of-year (00..=53) — strftime `%U` semantics.
/// Mirror of `weekOfYearMonday` using `dayOfWeek` (Sun=0..Sat=6) directly,
/// so days before the first Sunday are week 00 and weeks roll over Sun.
pub fn weekOfYearSunday(dt: DateTime) u8 {
    const doy: i32 = @intCast(dayOfYear(dt));
    const dow: i32 = @intCast(dayOfWeek(dt));
    return @intCast(@divFloor(doy + 6 - dow, 7));
}

/// Sakamoto's algorithm. Returns 0=Sunday, 1=Monday, ..., 6=Saturday
/// (matches sqlite3's `%w`).
pub fn dayOfWeek(dt: DateTime) u8 {
    const t = [_]u8{ 0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4 };
    var y: i32 = dt.year;
    if (dt.month < 3) y -= 1;
    const m_idx: usize = @intCast(dt.month - 1);
    // `@divFloor` (not `@divTrunc`) keeps Sakamoto's algorithm correct for
    // negative years — e.g. `-1/4` must round toward -∞ to land on 0
    // (not -0 from trunc) for the day-of-week parity to hold across the
    // BC/AD boundary.
    const dow_signed = @rem(y + @divFloor(y, 4) - @divFloor(y, 100) + @divFloor(y, 400) + t[m_idx] + @as(i32, dt.day), 7);
    return @intCast(@mod(dow_signed, 7));
}

/// Standard Julian Day Number for the given proleptic Gregorian date at
/// noon UTC. Wikipedia's "JDN" formula — exact for any year ≥ -4800
/// (covering sqlite3's -4713..=9999 range) and linear in `day` (so
/// day-overflow input renormalises correctly when round-tripped through
/// `jdnToYmd`). Year is signed; the +4800 offset keeps the intermediate
/// `y` positive for all in-range inputs, so `@divTrunc` and `@divFloor`
/// agree.
pub fn julianDayNumber(year: i32, month: u8, day: u8) i64 {
    const a: i64 = @divTrunc(14 - @as(i64, month), 12);
    const y: i64 = @as(i64, year) + 4800 - a;
    const m: i64 = @as(i64, month) + 12 * a - 3;
    return @as(i64, day) + @divTrunc(153 * m + 2, 5) + 365 * y + @divTrunc(y, 4) - @divTrunc(y, 100) + @divTrunc(y, 400) - 32045;
}

/// Unix epoch seconds. Matches sqlite3's `%s`: midnight of `'1970-01-01'`
/// is 0; the time-of-day component is added as plain seconds (no
/// timezone conversion — both ends treat the input as UTC).
pub fn unixEpochSeconds(dt: DateTime) i64 {
    const jdn = julianDayNumber(dt.year, dt.month, dt.day);
    const tod_sec: i64 = @as(i64, dt.hour) * 3600 + @as(i64, dt.minute) * 60 + dt.second;
    return (jdn - 2440588) * 86400 + tod_sec;
}

/// Continuous Julian day (with fractional time-of-day) for `dt`. Mid-day UTC
/// of JDN N is N exactly; midnight is N - 0.5.
///
/// Sub-second precision detail: a naive `(jdn - 0.5) + tod_ms / 86_400_000`
/// computation loses precision near JD 0 because `0.5 + tod_ms_frac` then
/// `- 0.5` is a catastrophic cancellation (`julianday('-4713-11-24
/// 12:00:00.001')` should be `1.157e-08`, not `1.157407e-08` truncated).
/// We avoid this by re-anchoring time-of-day to noon (`tod_ms - 43_200_000`)
/// so JD = `jdn + (tod_ms - 43_200_000)/86_400_000` — no cancellation,
/// no `- 0.5` term.
pub fn dateTimeToJulianFloat(dt: DateTime) f64 {
    const jdn = julianDayNumber(dt.year, dt.month, dt.day);
    const tod_ms: i64 = (@as(i64, dt.hour) * 3600 + @as(i64, dt.minute) * 60 + dt.second) * 1000 + dt.millisecond;
    const tod_ms_from_noon: i64 = tod_ms - 43_200_000;
    return @as(f64, @floatFromInt(jdn)) + @as(f64, @floatFromInt(tod_ms_from_noon)) / 86_400_000.0;
}

/// Inverse of `dateTimeToJulianFloat`. Returns null if the resulting year
/// falls outside sqlite3's `min_year..=max_year` range, or if the input
/// JD is negative (sqlite3 rejects pre-JD-0 instants — anything earlier
/// than `-4713-11-24 12:00:00 UTC`).
pub fn julianFloatToDateTime(jd: f64) ?DateTime {
    if (jd < 0) return null;
    // Convert JD (noon-aligned) → JDN-aligned days starting at midnight.
    const adj = jd + 0.5;
    var jdn_floor: i64 = @intFromFloat(@floor(adj));
    const day_frac = adj - @as(f64, @floatFromInt(jdn_floor));
    // Round to whole milliseconds. If we land on exactly 86_400_000 (full
    // day), push the date forward and reset to 0 — keeps `'+24 hours'`
    // from producing `24:00:00.000`.
    var total_ms: i64 = @intFromFloat(@round(day_frac * 86_400_000.0));
    if (total_ms >= 86_400_000) {
        total_ms -= 86_400_000;
        jdn_floor += 1;
    } else if (total_ms < 0) {
        total_ms += 86_400_000;
        jdn_floor -= 1;
    }
    const ymd = jdnToYmd(jdn_floor) orelse return null;

    const total_sec: i64 = @divTrunc(total_ms, 1000);
    const millisecond: i64 = @mod(total_ms, 1000);
    const hour: i64 = @divTrunc(total_sec, 3600);
    const minute: i64 = @divTrunc(@mod(total_sec, 3600), 60);
    const second: i64 = @mod(total_sec, 60);

    return DateTime{
        .year = ymd.year,
        .month = ymd.month,
        .day = ymd.day,
        .hour = @intCast(hour),
        .minute = @intCast(minute),
        .second = @intCast(second),
        .millisecond = @intCast(millisecond),
    };
}

/// Inverse of `julianDayNumber`. Wikipedia "Calendar date from Julian Day
/// Number" — exact for any JDN ≥ 0 in the proleptic Gregorian calendar.
/// Returns null when the recovered year falls outside sqlite3's nominal
/// `min_year..=max_year` (-4713..=9999) range. JDN 0 is `-4713-11-24`,
/// JDN 5373484 is `9999-12-31`; below 0 or above `max_jdn` → null.
pub fn jdnToYmd(jdn: i64) ?Ymd {
    if (jdn < 0 or jdn > max_jdn) return null;
    const a: i64 = jdn + 32044;
    const b: i64 = @divTrunc(4 * a + 3, 146097);
    const c: i64 = a - @divTrunc(146097 * b, 4);
    const d: i64 = @divTrunc(4 * c + 3, 1461);
    const e: i64 = c - @divTrunc(1461 * d, 4);
    const m: i64 = @divTrunc(5 * e + 2, 153);

    const day: i64 = e - @divTrunc(153 * m + 2, 5) + 1;
    const month: i64 = m + 3 - 12 * @divTrunc(m, 10);
    const year: i64 = 100 * b + d - 4800 + @divTrunc(m, 10);

    if (year < min_year or year > max_year) return null;
    if (month < 1 or month > 12) return null;
    if (day < 1 or day > 31) return null;
    return Ymd{ .year = @intCast(year), .month = @intCast(month), .day = @intCast(day) };
}
