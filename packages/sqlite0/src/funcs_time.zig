//! `strftime(fmt, datestr, [modifier]*)` — sqlite3-compatible date/time
//! formatter (subset).
//!
//! Scope of this module: parse a date string, apply zero or more modifiers,
//! then format. The special time-string `'now'` needs access to the current
//! wall-clock time, which Zig 0.16.0 routes through `std.Io`. Threading
//! `std.Io` through the function dispatch ABI is a separate refactor; until
//! then `'now'` returns NULL (mirroring how sqlite3 returns NULL for
//! invalid time strings).
//!
//! Supported format specifiers: %Y %m %d %H %M %S %j %w %s %J %%.
//! Encountering an unsupported spec letter returns NULL — that's what
//! sqlite3 does (verified against 3.51.0: `strftime('%Z', '2024-01-01')`
//! → NULL). `%s` is Unix epoch seconds (integer); `%J` is the Julian
//! day, formatted via Zig's shortest-unique decimal (matches sqlite3's
//! `%.16g` output for the values we produce).
//!
//! Date string formats accepted:
//!   `YYYY-MM-DD`
//!   `YYYY-MM-DD HH:MM:SS`
//!   `YYYY-MM-DDTHH:MM:SS`
//!
//! Modifiers (per-arg, applied left-to-right):
//!   `'±N <unit>'` where unit ∈ {seconds, minutes, hours, days, months, years}
//!   `'start of day'` / `'start of month'` / `'start of year'`
//!   N may be fractional (`'+1.5 days'`); singular/plural unit forms accepted.
//!   day/hour/minute/second deltas update the Julian-day float directly so
//!   overflow propagates (e.g. `+90 minutes` from 12:00 → 13:30, `+25 hours`
//!   crosses midnight). month/year deltas adjust the year/month fields then
//!   round-trip through Julian day, so day overflow renormalises the way
//!   sqlite3 does (`'2024-01-31' + 1 month` → `'2024-03-02'`).
//!
//! Invalid dates, unknown/malformed modifiers, NULL input → NULL (sqlite3
//! parity, rendered as empty in the CLI).

const std = @import("std");
const util = @import("func_util.zig");
const ops = @import("ops.zig");

const Value = util.Value;
const Error = util.Error;

pub fn fnStrftime(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len < 2) return Error.WrongArgumentCount;
    const fmt = switch (args[0]) {
        .null => return Value.null,
        .text => |t| t,
        .blob => |b| b,
        else => return Value.null,
    };
    return formatDateTime(allocator, fmt, args[1..]);
}

/// `date(timestring, [modifier]*)` — sqlite3 shorthand for
/// `strftime('%Y-%m-%d', timestring, ...)`. Returns NULL on missing
/// arg / NULL / unparsable date / unknown modifier (mirrors strftime).
/// The 0-arg form (current date via `'now'`) is deferred until
/// std.Io plumbing lands.
pub fn fnDate(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len == 0) return Error.WrongArgumentCount;
    return formatDateTime(allocator, "%Y-%m-%d", args);
}

pub fn fnTime(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len == 0) return Error.WrongArgumentCount;
    return formatDateTime(allocator, "%H:%M:%S", args);
}

pub fn fnDatetime(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len == 0) return Error.WrongArgumentCount;
    return formatDateTime(allocator, "%Y-%m-%d %H:%M:%S", args);
}

/// Core formatter shared by strftime/date/time/datetime. `args` is the
/// post-format slice: [datestring, modifier1, modifier2, ...]. A NULL
/// or non-text/blob datestring or modifier collapses the whole result
/// to NULL (sqlite3 parity); an unrecognised %-specifier in `fmt`
/// likewise → NULL.
fn formatDateTime(allocator: std.mem.Allocator, fmt: []const u8, args: []const Value) Error!Value {
    const datestr = switch (args[0]) {
        .null => return Value.null,
        .text => |t| t,
        .blob => |b| b,
        else => return Value.null,
    };

    var dt = parseDateTime(datestr) orelse return Value.null;
    for (args[1..]) |mod_arg| {
        const mod_str = switch (mod_arg) {
            .null => return Value.null,
            .text => |t| t,
            .blob => |b| b,
            else => return Value.null,
        };
        dt = applyModifier(dt, mod_str) orelse return Value.null;
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);

    var i: usize = 0;
    while (i < fmt.len) {
        if (fmt[i] != '%') {
            try out.append(allocator, fmt[i]);
            i += 1;
            continue;
        }
        if (i + 1 >= fmt.len) {
            try out.append(allocator, '%');
            i += 1;
            continue;
        }
        const conv = fmt[i + 1];
        switch (conv) {
            '%' => try out.append(allocator, '%'),
            'Y' => try writeZeroPadded(allocator, &out, dt.year, 4),
            'm' => try writeZeroPadded(allocator, &out, dt.month, 2),
            'd' => try writeZeroPadded(allocator, &out, dt.day, 2),
            'H' => try writeZeroPadded(allocator, &out, dt.hour, 2),
            'M' => try writeZeroPadded(allocator, &out, dt.minute, 2),
            'S' => try writeZeroPadded(allocator, &out, dt.second, 2),
            'j' => try writeZeroPadded(allocator, &out, dayOfYear(dt), 3),
            'w' => try writeZeroPadded(allocator, &out, dayOfWeek(dt), 1),
            's' => try writeI64(allocator, &out, unixEpochSeconds(dt)),
            'J' => try writeJulianDay(allocator, &out, dt),
            else => {
                // Unsupported spec → NULL (sqlite3 behavior).
                out.deinit(allocator);
                return Value.null;
            },
        }
        i += 2;
    }

    return Value{ .text = try out.toOwnedSlice(allocator) };
}

const DateTime = struct {
    year: u16,
    month: u8, // 1-12
    day: u8, // 1-31
    hour: u8 = 0,
    minute: u8 = 0,
    second: u8 = 0,
};

/// Accept `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, `YYYY-MM-DDTHH:MM:SS`,
/// or `HH:MM:SS` (time-only — date defaults to 2000-01-01 per sqlite3
/// time() docs). Returns null on malformed input or out-of-range fields.
/// Stricter than sqlite3 in some edge cases (no fractional seconds, no
/// timezone offsets) — expand as needed when differential cases demand
/// it.
fn parseDateTime(s: []const u8) ?DateTime {
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
    if (day < 1 or day > daysInMonth(year, month)) return null;

    var dt = DateTime{ .year = year, .month = month, .day = day };
    if (s.len == 10) return dt;
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

fn isLeapYear(y: u16) bool {
    return (y % 4 == 0 and y % 100 != 0) or (y % 400 == 0);
}

fn daysInMonth(y: u16, m: u8) u8 {
    return switch (m) {
        1, 3, 5, 7, 8, 10, 12 => 31,
        4, 6, 9, 11 => 30,
        2 => if (isLeapYear(y)) 29 else 28,
        else => 0,
    };
}

fn dayOfYear(dt: DateTime) u16 {
    const cum = [_]u16{ 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 };
    var doy: u16 = cum[dt.month - 1] + dt.day;
    if (isLeapYear(dt.year) and dt.month > 2) doy += 1;
    return doy;
}

/// Sakamoto's algorithm. Returns 0=Sunday, 1=Monday, ..., 6=Saturday
/// (matches sqlite3's `%w`).
fn dayOfWeek(dt: DateTime) u8 {
    const t = [_]u8{ 0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4 };
    var y: i32 = dt.year;
    if (dt.month < 3) y -= 1;
    const m_idx: usize = @intCast(dt.month - 1);
    const dow_signed = @rem(y + @divTrunc(y, 4) - @divTrunc(y, 100) + @divTrunc(y, 400) + t[m_idx] + @as(i32, dt.day), 7);
    return @intCast(@mod(dow_signed, 7));
}

/// Standard Julian Day Number for the given proleptic Gregorian date at
/// noon UTC. Wikipedia's "JDN" formula — exact for any year ≥ -4800.
fn julianDayNumber(year: u16, month: u8, day: u8) i64 {
    const a: i64 = @divTrunc(14 - @as(i64, month), 12);
    const y: i64 = @as(i64, year) + 4800 - a;
    const m: i64 = @as(i64, month) + 12 * a - 3;
    return @as(i64, day) + @divTrunc(153 * m + 2, 5) + 365 * y + @divTrunc(y, 4) - @divTrunc(y, 100) + @divTrunc(y, 400) - 32045;
}

/// Unix epoch seconds. Matches sqlite3's `%s`: midnight of `'1970-01-01'`
/// is 0; the time-of-day component is added as plain seconds (no
/// timezone conversion — both ends treat the input as UTC).
fn unixEpochSeconds(dt: DateTime) i64 {
    const jdn = julianDayNumber(dt.year, dt.month, dt.day);
    const tod_sec: i64 = @as(i64, dt.hour) * 3600 + @as(i64, dt.minute) * 60 + dt.second;
    return (jdn - 2440588) * 86400 + tod_sec;
}

/// Julian Day formatted as a shortest-unique decimal — matches sqlite3's
/// `%J` (which uses `printf("%.16g", iJD/86400000.0)`). Midnight UTC of
/// JDN N → N - 0.5; noon UTC → N exactly.
fn writeJulianDay(allocator: std.mem.Allocator, out: *std.ArrayList(u8), dt: DateTime) !void {
    const jdn = julianDayNumber(dt.year, dt.month, dt.day);
    const tod_sec: i64 = @as(i64, dt.hour) * 3600 + @as(i64, dt.minute) * 60 + dt.second;
    const julian: f64 = @as(f64, @floatFromInt(jdn)) - 0.5 + @as(f64, @floatFromInt(tod_sec)) / 86400.0;
    // 64-byte buffer is far more than any f64's shortest decimal needs.
    var buf: [64]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, "{d}", .{julian}) catch unreachable;
    try out.appendSlice(allocator, s);
}

fn writeI64(allocator: std.mem.Allocator, out: *std.ArrayList(u8), value: i64) !void {
    // 32-byte buffer fits the longest i64 decimal (20 chars + sign).
    var buf: [32]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, "{d}", .{value}) catch unreachable;
    try out.appendSlice(allocator, s);
}

/// Apply one modifier to `dt`. Returns null on parse failure, unknown
/// modifier, or post-application out-of-range date — the caller propagates
/// that as NULL (sqlite3 parity). sqlite3 is whitespace-strict: leading or
/// trailing space on the modifier string fails to parse.
fn applyModifier(dt: DateTime, mod: []const u8) ?DateTime {
    if (mod.len == 0) return null;

    if (matchPrefixIgnoreCase(mod, "start of ")) |rest| {
        return applyStartOf(dt, rest);
    }

    return applyDelta(dt, mod);
}

fn applyStartOf(dt: DateTime, scope: []const u8) ?DateTime {
    if (eqlIgnoreCase(scope, "day")) {
        return DateTime{ .year = dt.year, .month = dt.month, .day = dt.day };
    }
    if (eqlIgnoreCase(scope, "month")) {
        return DateTime{ .year = dt.year, .month = dt.month, .day = 1 };
    }
    if (eqlIgnoreCase(scope, "year")) {
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
        const jd = dateTimeToJulianFloat(dt) + off;
        return julianFloatToDateTime(jd);
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
    if (year < 0 or year > 9999) return null;
    // Round-trip through JD so day-overflow (Feb 30 → Mar 1/2) renormalises,
    // then layer the fractional-month/year days carry on top.
    const adjusted = DateTime{
        .year = @intCast(year),
        .month = @intCast(month),
        .day = dt.day,
        .hour = dt.hour,
        .minute = dt.minute,
        .second = dt.second,
    };
    const jd = dateTimeToJulianFloat(adjusted) + frac_days;
    return julianFloatToDateTime(jd);
}

const UnitClass = enum { second, minute, hour, day, month, year, unknown };

fn unitClass(unit: []const u8) UnitClass {
    if (eqlIgnoreCase(unit, "second") or eqlIgnoreCase(unit, "seconds")) return .second;
    if (eqlIgnoreCase(unit, "minute") or eqlIgnoreCase(unit, "minutes")) return .minute;
    if (eqlIgnoreCase(unit, "hour") or eqlIgnoreCase(unit, "hours")) return .hour;
    if (eqlIgnoreCase(unit, "day") or eqlIgnoreCase(unit, "days")) return .day;
    if (eqlIgnoreCase(unit, "month") or eqlIgnoreCase(unit, "months")) return .month;
    if (eqlIgnoreCase(unit, "year") or eqlIgnoreCase(unit, "years")) return .year;
    return .unknown;
}

fn matchPrefixIgnoreCase(s: []const u8, prefix: []const u8) ?[]const u8 {
    if (s.len < prefix.len) return null;
    if (!eqlIgnoreCase(s[0..prefix.len], prefix)) return null;
    return s[prefix.len..];
}

fn eqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        if (std.ascii.toLower(ca) != std.ascii.toLower(cb)) return false;
    }
    return true;
}

/// Continuous Julian day (with fractional time-of-day) for `dt`. Mid-day UTC
/// of JDN N is N exactly; midnight is N - 0.5.
fn dateTimeToJulianFloat(dt: DateTime) f64 {
    const jdn = julianDayNumber(dt.year, dt.month, dt.day);
    const tod_sec: i64 = @as(i64, dt.hour) * 3600 + @as(i64, dt.minute) * 60 + dt.second;
    return @as(f64, @floatFromInt(jdn)) - 0.5 + @as(f64, @floatFromInt(tod_sec)) / 86400.0;
}

/// Inverse of `dateTimeToJulianFloat`. Returns null if the resulting year
/// falls outside the 0..=9999 representable range.
fn julianFloatToDateTime(jd: f64) ?DateTime {
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

const Ymd = struct { year: u16, month: u8, day: u8 };

/// Inverse of `julianDayNumber`. Wikipedia "Calendar date from Julian Day
/// Number" — exact for any JDN ≥ 0 in the proleptic Gregorian calendar.
/// Returns null when the recovered year doesn't fit our u16 0..=9999 window.
fn jdnToYmd(jdn: i64) ?Ymd {
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

fn writeZeroPadded(allocator: std.mem.Allocator, out: *std.ArrayList(u8), value: u16, width: u8) !void {
    var buf: [8]u8 = undefined;
    var i: usize = buf.len;
    var v = value;
    if (v == 0) {
        i -= 1;
        buf[i] = '0';
    } else {
        while (v > 0) : (v /= 10) {
            i -= 1;
            buf[i] = '0' + @as(u8, @intCast(v % 10));
        }
    }
    const digits = buf[i..];
    var pad: usize = 0;
    while (digits.len + pad < width) : (pad += 1) {
        try out.append(allocator, '0');
    }
    try out.appendSlice(allocator, digits);
}
