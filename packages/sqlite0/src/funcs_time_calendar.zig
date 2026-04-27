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
};

pub const Ymd = struct { year: u16, month: u8, day: u8 };

/// Accept `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, `YYYY-MM-DDTHH:MM:SS`,
/// or `HH:MM:SS` (time-only — date defaults to 2000-01-01 per sqlite3
/// time() docs). Year must lie in 0..=9999, month in 1..=12, day in
/// 1..=31. Hour/minute/second are strict (≤23/59/59 — sqlite3 rejects
/// `25:00:00`).
///
/// Day-overflow within a valid month (e.g. `2023-02-29`, `2023-04-31`)
/// is renormalised forward via Julian round-trip to match sqlite3's
/// "lenient day, strict everything else" rule (`date('2023-02-29')`
/// → `2023-03-01`). Renormalisation never crosses out of the 0..=9999
/// year window — the only literal that could trigger a year overflow
/// is day=32, which is rejected upstream.
pub fn parseDateTime(s: []const u8) ?DateTime {
    // Time-only `HH:MM:SS`: sqlite3 fills the date with 2000-01-01.
    if (s.len == 8 and s[2] == ':' and s[5] == ':') {
        const hour = parseUintFixed(u8, s[0..2]) orelse return null;
        const minute = parseUintFixed(u8, s[3..5]) orelse return null;
        const second = parseUintFixed(u8, s[6..8]) orelse return null;
        if (hour > 23 or minute > 59 or second > 59) return null;
        return .{ .year = 2000, .month = 1, .day = 1, .hour = hour, .minute = minute, .second = second };
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
        dt.hour = hour;
        dt.minute = minute;
        dt.second = second;
    }

    if (dt.day > daysInMonth(dt.year, dt.month)) {
        // Lenient day overflow → roll forward via Julian round-trip.
        // Time fields are preserved exactly because the round-trip
        // operates on the JD float without altering the second-of-day.
        const jd = dateTimeToJulianFloat(dt);
        return julianFloatToDateTime(jd);
    }
    return dt;
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
/// to renormalise day/month/year overflow.
pub fn dateTimeToJulianFloat(dt: DateTime) f64 {
    const jdn = julianDayNumber(dt.year, dt.month, dt.day);
    const tod_sec: i64 = @as(i64, dt.hour) * 3600 + @as(i64, dt.minute) * 60 + dt.second;
    return @as(f64, @floatFromInt(jdn)) - 0.5 + @as(f64, @floatFromInt(tod_sec)) / 86400.0;
}

/// Inverse of `dateTimeToJulianFloat`. Returns null if the resulting year
/// falls outside the 0..=9999 representable range.
pub fn julianFloatToDateTime(jd: f64) ?DateTime {
    // Convert JD (noon-aligned) → JDN-aligned days starting at midnight.
    const adj = jd + 0.5;
    var jdn_floor: i64 = @intFromFloat(@floor(adj));
    const day_frac = adj - @as(f64, @floatFromInt(jdn_floor));
    // Round seconds with a half-up rule. If we land exactly on 86400, push
    // the date forward and reset seconds to 0 — keeps `'+24 hours'` from
    // producing `24:00:00`.
    var total_sec: i64 = @intFromFloat(@round(day_frac * 86400.0));
    if (total_sec >= 86400) {
        total_sec -= 86400;
        jdn_floor += 1;
    } else if (total_sec < 0) {
        total_sec += 86400;
        jdn_floor -= 1;
    }
    const ymd = jdnToYmd(jdn_floor) orelse return null;

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
