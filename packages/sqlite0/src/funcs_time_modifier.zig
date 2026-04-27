//! sqlite3 date/time modifier interpreter.
//!
//! `applyModifier(dt, "<modifier>")` consumes one modifier string and
//! returns either a new `DateTime` or `null` (= sqlite3 NULL). The
//! supported subset:
//!
//!   `'±N <unit>'` where unit ∈ {seconds, minutes, hours, days, months, years}
//!     N may be fractional (`'+1.5 days'`); singular/plural unit forms accepted.
//!     day/hour/minute/second deltas update the Julian-day float directly so
//!     overflow propagates (e.g. `+90 minutes` from 12:00 → 13:30, `+25 hours`
//!     crosses midnight). month/year deltas adjust the year/month fields then
//!     round-trip through Julian day, so day overflow renormalises the way
//!     sqlite3 does (`'2024-01-31' + 1 month` → `'2024-03-02'`).
//!
//!   `'start of day'` / `'start of month'` / `'start of year'`
//!
//! sqlite3 is whitespace-strict — leading or trailing space, or any
//! malformed token, fails to parse and propagates as NULL.

const std = @import("std");
const util = @import("func_util.zig");
const calendar = @import("funcs_time_calendar.zig");

const DateTime = calendar.DateTime;

pub fn applyModifier(dt: DateTime, mod: []const u8) ?DateTime {
    if (mod.len == 0) return null;

    if (matchPrefixIgnoreCase(mod, "start of ")) |rest| {
        return applyStartOf(dt, rest);
    }

    if (matchPrefixIgnoreCase(mod, "weekday ")) |rest| {
        return applyWeekday(dt, rest);
    }

    // sqlite3 3.46+ accepts `floor` and `ceiling` as modifiers — they were
    // added for sub-second time rounding but in 3.51 they're effectively
    // no-ops on the parsed DateTime (verified empirically: `julianday(...,
    // 'floor')` and `julianday(...)` return the same value). Accept silently
    // so chains like `('2024-01-01', 'floor', '+1 day')` work.
    if (util.eqlIgnoreCase(mod, "floor") or util.eqlIgnoreCase(mod, "ceiling")) {
        return dt;
    }

    return applyDelta(dt, mod);
}

/// `'weekday N'` (sqlite3) — move the date forward (≥0 days) so the
/// resulting weekday matches `N`, where 0=Sun..6=Sat. If `N` already
/// matches the current weekday, the date is unchanged. The time of day
/// rides through untouched (`'2024-01-01 12:34:56' + weekday 3'` →
/// `'2024-01-03 12:34:56'`). `N` must be an integer literal in 0..=6;
/// `'weekday 7'` / `'weekday -1'` / `'weekday 2.5'` / `'weekday'` all
/// reject (sqlite3 → NULL).
fn applyWeekday(dt: DateTime, n_str: []const u8) ?DateTime {
    if (n_str.len != 1) return null;
    const c = n_str[0];
    if (c < '0' or c > '6') return null;
    const target: u8 = c - '0';
    const current = calendar.dayOfWeek(dt);
    const delta: u8 = (target + 7 - current) % 7;
    if (delta == 0) return dt;
    const jd = calendar.dateTimeToJulianFloat(dt) + @as(f64, @floatFromInt(delta));
    return calendar.julianFloatToDateTime(jd);
}

fn applyStartOf(dt: DateTime, scope: []const u8) ?DateTime {
    if (util.eqlIgnoreCase(scope, "day")) {
        return DateTime{ .year = dt.year, .month = dt.month, .day = dt.day };
    }
    if (util.eqlIgnoreCase(scope, "month")) {
        return DateTime{ .year = dt.year, .month = dt.month, .day = 1 };
    }
    if (util.eqlIgnoreCase(scope, "year")) {
        return DateTime{ .year = dt.year, .month = 1, .day = 1 };
    }
    return null;
}

fn applyDelta(dt: DateTime, mod: []const u8) ?DateTime {
    // Expect `<sign>?<number> <unit>`. Sign is optional (default positive,
    // matching sqlite3: `'1 day'` works the same as `'+1 day'`). Internal
    // multi-space between number and unit is tolerated, but leading or
    // trailing whitespace fails — sqlite3 rejects ` +1 day` and `+1 day `.
    var idx: usize = 0;
    var sign: f64 = 1.0;
    if (mod[0] == '+') {
        idx = 1;
    } else if (mod[0] == '-') {
        sign = -1.0;
        idx = 1;
    }
    const num_start = idx;
    while (idx < mod.len and mod[idx] != ' ') : (idx += 1) {
        const c = mod[idx];
        if (!((c >= '0' and c <= '9') or c == '.')) return null;
    }
    const num_str = mod[num_start..idx];
    if (num_str.len == 0) return null;
    if (idx >= mod.len) return null; // missing unit
    // Skip the separator space(s); reject any trailing space after the unit.
    while (idx < mod.len and mod[idx] == ' ') : (idx += 1) {}
    const unit = mod[idx..];
    if (unit.len == 0) return null;
    if (unit[unit.len - 1] == ' ') return null;
    const magnitude = std.fmt.parseFloat(f64, num_str) catch return null;
    const delta = sign * magnitude;

    // Days/hours/minutes/seconds modify the JD float directly so overflow
    // crosses date boundaries naturally. Months/years adjust year/month
    // fields then round-trip through JD to renormalise day overflow
    // (`'2024-01-31' + 1 month'` → `'2024-03-02'`, matching sqlite3).
    const day_offset_opt: ?f64 = switch (unitClass(unit)) {
        .second => delta / 86400.0,
        .minute => delta / 1440.0,
        .hour => delta / 24.0,
        .day => delta,
        .month, .year, .unknown => null,
    };
    if (day_offset_opt) |off| {
        const jd = calendar.dateTimeToJulianFloat(dt) + off;
        return calendar.julianFloatToDateTime(jd);
    }

    const u = unitClass(unit);
    if (u == .unknown) return null;

    // Month/year deltas split into an integer field bump (calendar-aware) and
    // a fractional days carry (sqlite3 quirk: `+0.5 month` = +15 days, not
    // "half a calendar month"; `+0.1 year` = +36.5 days). Trunc toward zero
    // keeps the negative case symmetric (`-1.5 month` = -1 month - 15 days).
    const trunc_part = @trunc(delta);
    const frac_part = delta - trunc_part;
    const int_delta: i32 = @intFromFloat(trunc_part);
    var year: i32 = dt.year;
    var month: i32 = dt.month;
    var frac_days: f64 = 0;
    switch (u) {
        .month => {
            month += int_delta;
            frac_days = frac_part * 30.0;
        },
        .year => {
            year += int_delta;
            frac_days = frac_part * 365.0;
        },
        else => unreachable,
    }
    // Normalise month into 1..=12 and carry into year.
    while (month > 12) : (month -= 12) year += 1;
    while (month < 1) : (month += 12) year -= 1;
    if (year < calendar.min_year or year > calendar.max_year) return null;
    // Round-trip through JD so day-overflow (Feb 30 → Mar 1/2) renormalises,
    // then layer the fractional-month/year days carry on top.
    const adjusted = DateTime{
        .year = year,
        .month = @intCast(month),
        .day = dt.day,
        .hour = dt.hour,
        .minute = dt.minute,
        .second = dt.second,
        .millisecond = dt.millisecond,
    };
    const jd = calendar.dateTimeToJulianFloat(adjusted) + frac_days;
    return calendar.julianFloatToDateTime(jd);
}

const UnitClass = enum { second, minute, hour, day, month, year, unknown };

fn unitClass(unit: []const u8) UnitClass {
    if (util.eqlIgnoreCase(unit, "second") or util.eqlIgnoreCase(unit, "seconds")) return .second;
    if (util.eqlIgnoreCase(unit, "minute") or util.eqlIgnoreCase(unit, "minutes")) return .minute;
    if (util.eqlIgnoreCase(unit, "hour") or util.eqlIgnoreCase(unit, "hours")) return .hour;
    if (util.eqlIgnoreCase(unit, "day") or util.eqlIgnoreCase(unit, "days")) return .day;
    if (util.eqlIgnoreCase(unit, "month") or util.eqlIgnoreCase(unit, "months")) return .month;
    if (util.eqlIgnoreCase(unit, "year") or util.eqlIgnoreCase(unit, "years")) return .year;
    return .unknown;
}

fn matchPrefixIgnoreCase(s: []const u8, prefix: []const u8) ?[]const u8 {
    if (s.len < prefix.len) return null;
    if (!util.eqlIgnoreCase(s[0..prefix.len], prefix)) return null;
    return s[prefix.len..];
}
