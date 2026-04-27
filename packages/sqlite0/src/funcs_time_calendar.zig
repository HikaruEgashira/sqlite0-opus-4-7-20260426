//! Calendar arithmetic for sqlite3-compatible date/time functions.
//!
//! All sqlite3 date/time results live in the proleptic Gregorian calendar
//! over the year range 0..=9999 (matching sqlite3 3.51.0). This module is
//! pure math — no Value, no allocator, no I/O. Format dispatch lives in
//! `funcs_time.zig`; modifier interpretation lives in
//! `funcs_time_modifier.zig`.

const std = @import("std");

pub const DateTime = struct {
    year: u16,
    month: u8, // 1-12
    day: u8, // 1-31
    hour: u8 = 0,
    minute: u8 = 0,
    second: u8 = 0,
    millisecond: u16 = 0, // 0-999
};

pub const Ymd = struct { year: u16, month: u8, day: u8 };

/// Accept `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS[.fff][Z]`,
/// `YYYY-MM-DDTHH:MM:SS[.fff][Z]`, or `HH:MM:SS[.fff][Z]` (time-only —
/// date defaults to 2000-01-01 per sqlite3 time() docs). Year must lie
/// in 0..=9999, month in 1..=12, day in 1..=31. Hour/minute/second are
/// strict (≤23/59/59 — sqlite3 rejects `25:00:00`).
///
/// Sub-second precision: `.NNN…` reads as fractional seconds, stored as
/// integer milliseconds. First three digits are the integer ms; a fourth
/// digit ≥ '5' rounds up (with a clamp at 999 to keep the parser from
/// rolling into the next second — sqlite3 does the same). Trailing
/// digits past the fourth are ignored. `'2024-01-01 12:34:56.0009'`
/// thus stores 1ms; `'.9999'` stores 999ms (not 1000); `.50050'` → 501.
///
/// `Z` (Zulu/UTC) is accepted at end of input — sqlite3 treats both
/// `'…56Z'` and `'…56.5Z'` as already-UTC and ignores the marker.
/// Anything else past the seconds (or fractional digits) → NULL.
///
/// Day-overflow within a valid month (e.g. `2023-02-29`, `2023-04-31`)
/// is renormalised forward via Julian round-trip to match sqlite3's
/// "lenient day, strict everything else" rule (`date('2023-02-29')`
/// → `2023-03-01`). Renormalisation never crosses out of the 0..=9999
/// year window — the only literal that could trigger a year overflow
/// is day=32, which is rejected upstream.
pub fn parseDateTime(s: []const u8) ?DateTime {
    // Time-only `HH:MM:SS[.fff][Z]`: sqlite3 fills the date with 2000-01-01.
    if (s.len >= 8 and s[2] == ':' and s[5] == ':') {
        const hour = parseUintFixed(u8, s[0..2]) orelse return null;
        const minute = parseUintFixed(u8, s[3..5]) orelse return null;
        const second = parseUintFixed(u8, s[6..8]) orelse return null;
        if (hour > 23 or minute > 59 or second > 59) return null;
        const ms = parseSubsecAndZ(s[8..]) orelse return null;
        return .{ .year = 2000, .month = 1, .day = 1, .hour = hour, .minute = minute, .second = second, .millisecond = ms };
    }
    if (s.len < 10) return null;
    if (s[4] != '-' or s[7] != '-') return null;
    const year = parseUintFixed(u16, s[0..4]) orelse return null;
    const month = parseUintFixed(u8, s[5..7]) orelse return null;
    const day = parseUintFixed(u8, s[8..10]) orelse return null;
    if (month < 1 or month > 12) return null;
    if (day < 1 or day > 31) return null;

    var dt = DateTime{ .year = year, .month = month, .day = day };
    if (s.len > 10) {
        if (s.len < 19) return null;
        if (s[10] != ' ' and s[10] != 'T') return null;
        if (s[13] != ':' or s[16] != ':') return null;
        const hour = parseUintFixed(u8, s[11..13]) orelse return null;
        const minute = parseUintFixed(u8, s[14..16]) orelse return null;
        const second = parseUintFixed(u8, s[17..19]) orelse return null;
        if (hour > 23 or minute > 59 or second > 59) return null;
        const ms = parseSubsecAndZ(s[19..]) orelse return null;
        dt.hour = hour;
        dt.minute = minute;
        dt.second = second;
        dt.millisecond = ms;
    }

    if (dt.day > daysInMonth(dt.year, dt.month)) {
        // Lenient day overflow → roll forward via Julian round-trip.
        // Time fields (including ms) ride through unchanged.
        const jd = dateTimeToJulianFloat(dt);
        return julianFloatToDateTime(jd);
    }
    return dt;
}

/// Parse the optional `.fff…[Z]` tail after `HH:MM:SS`. Empty or `Z`
/// alone → 0ms. A `.` must be followed by ≥1 digit. Returns null if the
/// tail contains anything else after the digits/optional Z (sqlite3 is
/// strict — `'…56xxxxx'` and `'…56.7Z!'` both fail to parse).
fn parseSubsecAndZ(tail: []const u8) ?u16 {
    var idx: usize = 0;
    var ms: u16 = 0;
    if (idx < tail.len and tail[idx] == '.') {
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
    if (idx < tail.len and tail[idx] == 'Z') idx += 1;
    if (idx != tail.len) return null;
    return ms;
}

fn parseUintFixed(comptime T: type, s: []const u8) ?T {
    var n: T = 0;
    for (s) |c| {
        if (c < '0' or c > '9') return null;
        n = n * 10 + (c - '0');
    }
    return n;
}

pub fn isLeapYear(y: u16) bool {
    return (y % 4 == 0 and y % 100 != 0) or (y % 400 == 0);
}

pub fn daysInMonth(y: u16, m: u8) u8 {
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
pub const IsoWeekYear = struct { week: u8, year: u16 };

pub fn isoWeekAndYear(dt: DateTime) IsoWeekYear {
    const doy: i32 = @intCast(dayOfYear(dt));
    const wd: i32 = @intCast(isoWeekday(dt));
    const week_raw = @divFloor(10 + doy - wd, 7);

    if (week_raw < 1) {
        if (dt.year == 0) return .{ .week = 1, .year = 0 };
        return .{ .week = weeksInYear(dt.year - 1), .year = dt.year - 1 };
    }
    const wpy = weeksInYear(dt.year);
    if (week_raw > wpy) {
        if (dt.year == 9999) return .{ .week = wpy, .year = 9999 };
        return .{ .week = 1, .year = dt.year + 1 };
    }
    return .{ .week = @intCast(week_raw), .year = dt.year };
}

/// Total ISO 8601 weeks in `y`. A year has 53 weeks iff Jan 1 is Thursday,
/// or Jan 1 is Wednesday in a leap year (the extra leap day pushes the
/// year's week 53 into existence). All other years have 52 weeks.
pub fn weeksInYear(y: u16) u8 {
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

/// Sakamoto's algorithm. Returns 0=Sunday, 1=Monday, ..., 6=Saturday
/// (matches sqlite3's `%w`).
pub fn dayOfWeek(dt: DateTime) u8 {
    const t = [_]u8{ 0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4 };
    var y: i32 = dt.year;
    if (dt.month < 3) y -= 1;
    const m_idx: usize = @intCast(dt.month - 1);
    const dow_signed = @rem(y + @divTrunc(y, 4) - @divTrunc(y, 100) + @divTrunc(y, 400) + t[m_idx] + @as(i32, dt.day), 7);
    return @intCast(@mod(dow_signed, 7));
}

/// Standard Julian Day Number for the given proleptic Gregorian date at
/// noon UTC. Wikipedia's "JDN" formula — exact for any year ≥ -4800
/// and linear in `day` (so day-overflow input renormalises correctly
/// when round-tripped through `jdnToYmd`).
pub fn julianDayNumber(year: u16, month: u8, day: u8) i64 {
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
/// of JDN N is N exactly; midnight is N - 0.5. This is sqlite3's
/// `julianday(...)` value, also used internally by every modifier path
/// to renormalise day/month/year overflow. Sub-second precision rides
/// through as `ms / 86_400_000` — f64 around year 2024 has ~9 fractional
/// decimal digits, leaving ~10× headroom over the 1.16e-8 per-ms unit.
pub fn dateTimeToJulianFloat(dt: DateTime) f64 {
    const jdn = julianDayNumber(dt.year, dt.month, dt.day);
    const tod_ms: i64 = (@as(i64, dt.hour) * 3600 + @as(i64, dt.minute) * 60 + dt.second) * 1000 + dt.millisecond;
    return @as(f64, @floatFromInt(jdn)) - 0.5 + @as(f64, @floatFromInt(tod_ms)) / 86_400_000.0;
}

/// Inverse of `dateTimeToJulianFloat`. Returns null if the resulting year
/// falls outside the 0..=9999 representable range.
pub fn julianFloatToDateTime(jd: f64) ?DateTime {
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
/// Returns null when the recovered year doesn't fit our u16 0..=9999 window.
pub fn jdnToYmd(jdn: i64) ?Ymd {
    const a: i64 = jdn + 32044;
    const b: i64 = @divTrunc(4 * a + 3, 146097);
    const c: i64 = a - @divTrunc(146097 * b, 4);
    const d: i64 = @divTrunc(4 * c + 3, 1461);
    const e: i64 = c - @divTrunc(1461 * d, 4);
    const m: i64 = @divTrunc(5 * e + 2, 153);

    const day: i64 = e - @divTrunc(153 * m + 2, 5) + 1;
    const month: i64 = m + 3 - 12 * @divTrunc(m, 10);
    const year: i64 = 100 * b + d - 4800 + @divTrunc(m, 10);

    if (year < 0 or year > 9999) return null;
    if (month < 1 or month > 12) return null;
    if (day < 1 or day > 31) return null;
    return Ymd{ .year = @intCast(year), .month = @intCast(month), .day = @intCast(day) };
}
