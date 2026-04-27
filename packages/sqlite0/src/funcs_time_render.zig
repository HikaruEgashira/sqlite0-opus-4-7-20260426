//! strftime-style format renderer for the date/time SQL functions.
//! Pure formatting — no parsing, no modifier interpretation. Calls into
//! `funcs_time_calendar.zig` for derived fields (day-of-week, ISO week,
//! unix epoch, julian day). Split out of `funcs_time.zig` to keep that
//! file under the 500-line discipline.

const std = @import("std");
const calendar = @import("funcs_time_calendar.zig");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");

const DateTime = calendar.DateTime;
const Value = value_mod.Value;
const Error = ops.Error;

/// Year-format flavor. sqlite3's `date()` / `datetime()` and `strftime`
/// disagree on how to render a negative `%Y`:
///   * `.strftime`: printf `%04d` of the signed year — width 4 *includes*
///     the sign, so year `-1` renders as `-001` (4 chars total) and year
///     `-1234` renders as `-1234` (5 chars). Used by `strftime()`.
///   * `.date_func`: sign + 4-digit zero-pad of `|year|` — year `-1`
///     renders as `-0001` (5 chars) and year `-1234` as `-1234` (still
///     5 chars). Used by `date()` / `time()` / `datetime()`.
///
/// Verified against sqlite3 3.51.0:
///   `strftime('%Y','-0001-01-01')` → `-001`
///   `date('-0001-01-01')`           → `-0001-01-01`
///   `strftime('%Y-%m-%d','-0001-01-01')` → `-001-01-01`
pub const YearFormat = enum { strftime, date_func };

/// Core formatter — renders an already-parsed `DateTime` against the
/// given strftime-style `fmt`. Unrecognised %-specifier → NULL (matches
/// sqlite3's PRINTF_DATEFUNC abort-on-bad-spec rule). Caller owns the
/// pre-parse step; this fn never re-reads the original args. `subsec`
/// flips `%s` into the `seconds.fff` REAL-as-text rendering — a sqlite3
/// quirk where the modifier overrides only `%s` (other format specs
/// are already user-controlled).
pub fn renderFormat(
    allocator: std.mem.Allocator,
    fmt: []const u8,
    dt: DateTime,
    subsec: bool,
    year_format: YearFormat,
) Error!Value {
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
            'Y' => try writeYear(allocator, &out, dt.year, year_format),
            'm' => try writeZeroPadded(allocator, &out, dt.month, 2),
            'd' => try writeZeroPadded(allocator, &out, dt.day, 2),
            'e' => try writeSpacePadded(allocator, &out, dt.day, 2),
            'H' => try writeZeroPadded(allocator, &out, dt.hour, 2),
            'I' => try writeZeroPadded(allocator, &out, twelveHour(dt.hour), 2),
            'M' => try writeZeroPadded(allocator, &out, dt.minute, 2),
            'S' => try writeZeroPadded(allocator, &out, dt.second, 2),
            'f' => {
                // sqlite3 `%f` is "SS.fff" (zero-padded second + 3-digit ms),
                // not just the fractional part — `strftime('%S.%f', …)`
                // surfaces this by emitting `56.56.789`.
                try writeZeroPadded(allocator, &out, dt.second, 2);
                try out.append(allocator, '.');
                try writeZeroPadded(allocator, &out, dt.millisecond, 3);
            },
            'j' => try writeZeroPadded(allocator, &out, calendar.dayOfYear(dt), 3),
            'w' => try writeZeroPadded(allocator, &out, calendar.dayOfWeek(dt), 1),
            'u' => try writeZeroPadded(allocator, &out, isoWeekday(calendar.dayOfWeek(dt)), 1),
            'W' => try writeZeroPadded(allocator, &out, calendar.weekOfYearMonday(dt), 2),
            'U' => try writeZeroPadded(allocator, &out, calendar.weekOfYearSunday(dt), 2),
            'V' => try writeZeroPadded(allocator, &out, calendar.isoWeekAndYear(dt).week, 2),
            'G' => try writeYear(allocator, &out, calendar.isoWeekAndYear(dt).year, year_format),
            'g' => try writeYearMod100(allocator, &out, @rem(calendar.isoWeekAndYear(dt).year, 100)),
            'p' => try out.appendSlice(allocator, if (dt.hour < 12) "AM" else "PM"),
            'P' => try out.appendSlice(allocator, if (dt.hour < 12) "am" else "pm"),
            'R' => {
                try writeZeroPadded(allocator, &out, dt.hour, 2);
                try out.append(allocator, ':');
                try writeZeroPadded(allocator, &out, dt.minute, 2);
            },
            'T' => {
                try writeZeroPadded(allocator, &out, dt.hour, 2);
                try out.append(allocator, ':');
                try writeZeroPadded(allocator, &out, dt.minute, 2);
                try out.append(allocator, ':');
                try writeZeroPadded(allocator, &out, dt.second, 2);
            },
            's' => {
                if (subsec) {
                    try writeI64(allocator, &out, calendar.unixEpochSeconds(dt));
                    try out.append(allocator, '.');
                    try writeZeroPadded(allocator, &out, dt.millisecond, 3);
                } else {
                    try writeI64(allocator, &out, calendar.unixEpochSeconds(dt));
                }
            },
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

/// Julian Day formatted as a shortest-unique decimal — matches sqlite3's
/// `%J` (which uses `printf("%.16g", iJD/86400000.0)`). Returned as TEXT
/// because `%J` is a strftime specifier; `julianday()` exists separately
/// for the REAL-typed shape sqlite3 uses there.
fn writeJulianDay(allocator: std.mem.Allocator, out: *std.ArrayList(u8), dt: DateTime) !void {
    const julian = calendar.dateTimeToJulianFloat(dt);
    var buf: [64]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, "{d}", .{julian}) catch unreachable;
    try out.appendSlice(allocator, s);
}

fn writeI64(allocator: std.mem.Allocator, out: *std.ArrayList(u8), value: i64) !void {
    var buf: [32]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, "{d}", .{value}) catch unreachable;
    try out.appendSlice(allocator, s);
}

/// 12-hour clock: 0 → 12, 1..11 → 1..11, 12 → 12, 13..23 → 1..11. Matches
/// sqlite3 strftime `%I` (`hh` mod 12, with the noon/midnight 0 → 12 quirk).
fn twelveHour(h: u16) u16 {
    const m = h % 12;
    return if (m == 0) 12 else m;
}

/// ISO 8601 weekday: Mon=1, Tue=2, ..., Sun=7. sqlite3 `%w` (0=Sun..6=Sat)
/// remaps to `%u` by sending Sunday from 0 to 7.
fn isoWeekday(w: u8) u16 {
    return if (w == 0) 7 else w;
}

/// Render a year per `flavor`. See `YearFormat` for the per-flavor rule.
fn writeYear(allocator: std.mem.Allocator, out: *std.ArrayList(u8), year: i32, flavor: YearFormat) !void {
    if (year >= 0) {
        try writeZeroPadded(allocator, out, @intCast(year), 4);
        return;
    }
    try out.append(allocator, '-');
    const abs: u64 = @intCast(-@as(i64, year));
    const min_digits: u8 = switch (flavor) {
        .strftime => 3,
        .date_func => 4,
    };
    try writeZeroPaddedU64(allocator, out, abs, min_digits);
}

/// `%g` (ISO year mod 100) renders printf-`%02d` style: width 2 includes
/// sign, so `-2` → `-2` (2 chars), `-23` → `-23` (3 chars, exceeds width
/// when needed), `9` → `09`.
fn writeYearMod100(allocator: std.mem.Allocator, out: *std.ArrayList(u8), val: i32) !void {
    if (val >= 0) {
        try writeZeroPaddedU64(allocator, out, @intCast(val), 2);
    } else {
        try out.append(allocator, '-');
        const abs_val: u64 = @intCast(-@as(i64, val));
        try writeZeroPaddedU64(allocator, out, abs_val, 1);
    }
}

fn writeZeroPaddedU64(allocator: std.mem.Allocator, out: *std.ArrayList(u8), value: u64, width: u8) !void {
    var buf: [24]u8 = undefined;
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

fn writeSpacePadded(allocator: std.mem.Allocator, out: *std.ArrayList(u8), value: u16, width: u8) !void {
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
        try out.append(allocator, ' ');
    }
    try out.appendSlice(allocator, digits);
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
