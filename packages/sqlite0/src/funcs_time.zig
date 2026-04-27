//! `strftime(fmt, datestr, ...)` — sqlite3-compatible date/time formatter
//! (subset).
//!
//! Scope of this module: parse a date string, validate, then format. The
//! special time-string `'now'` and modifiers like `'+N days'` need access to
//! the current wall-clock time, which Zig 0.16.0 routes through `std.Io`.
//! Threading `std.Io` through the function dispatch ABI is a separate
//! refactor; until then `'now'` returns NULL (mirroring how sqlite3 returns
//! NULL for invalid time strings).
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
//! Invalid dates (out-of-range month/day, malformed input) return NULL —
//! sqlite3 also returns NULL (rendered as empty in the CLI).

const std = @import("std");
const util = @import("func_util.zig");
const ops = @import("ops.zig");

const Value = util.Value;
const Error = util.Error;

pub fn fnStrftime(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len < 2) return Error.WrongArgumentCount;
    // Modifiers (args[2..]) are not implemented yet; treat presence of any
    // modifier as "unsupported" → NULL (sqlite3 returns NULL for many invalid
    // modifier combinations).
    if (args.len > 2) return Value.null;

    const fmt = switch (args[0]) {
        .null => return Value.null,
        .text => |t| t,
        .blob => |b| b,
        else => return Value.null,
    };
    const datestr = switch (args[1]) {
        .null => return Value.null,
        .text => |t| t,
        .blob => |b| b,
        else => return Value.null,
    };

    const dt = parseDateTime(datestr) orelse return Value.null;

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

/// Accept `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, and `YYYY-MM-DDTHH:MM:SS`.
/// Returns null on malformed input or out-of-range fields. Stricter than
/// sqlite3 in some edge cases (no fractional seconds, no timezone offsets) —
/// expand as needed when differential cases demand it.
fn parseDateTime(s: []const u8) ?DateTime {
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
