//! sqlite3-compatible date/time SQL functions.
//!
//! This module is the public ABI surface — `strftime`, `date`, `time`,
//! `datetime`, `julianday`. Calendar arithmetic lives in
//! `funcs_time_calendar.zig`; modifier interpretation lives in
//! `funcs_time_modifier.zig`. Each public function shares a single
//! "parse → apply modifiers → render" pipeline so divergence between
//! shorthand wrappers and the canonical strftime implementation is
//! impossible.
//!
//! Scope notes:
//!  - The `'now'` time-string needs `std.Io` (Zig 0.16.0 routed wall
//!    clock through it). Plumbing `std.Io` through the function dispatch
//!    ABI is a separate refactor; until then `'now'` returns NULL
//!    (mirroring how sqlite3 returns NULL for invalid time strings).
//!  - Supported strftime spec letters: %Y %m %d %H %M %S %j %w %s %J %%.
//!    Any other specifier returns NULL — that's what sqlite3 does
//!    (verified against 3.51.0: `strftime('%Z', '2024-01-01')` → NULL).
//!  - Date strings: `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`,
//!    `YYYY-MM-DDTHH:MM:SS`, or `HH:MM:SS` (sqlite3 fills date with
//!    2000-01-01 for time-only inputs). Day-overflow within a valid
//!    month renormalises forward (sqlite3: `date('2023-02-29')`
//!    → `2023-03-01`).
//!  - Invalid dates, unknown/malformed modifiers, NULL input → NULL
//!    (rendered as empty in the CLI).

const std = @import("std");
const util = @import("func_util.zig");
const calendar = @import("funcs_time_calendar.zig");
const modifier = @import("funcs_time_modifier.zig");

const Value = util.Value;
const Error = util.Error;
const DateTime = calendar.DateTime;

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

/// `julianday(timestring, [modifier]*)` — sqlite3 returns the
/// continuous Julian day as REAL (mid-day UTC of JDN N is exactly N;
/// midnight is N - 0.5). Distinct from `strftime('%J', ...)` which
/// returns the same value as TEXT — REAL avoids the `2460311 vs
/// 2460311.0` formatting divergence the CLI surfaces.
pub fn fnJulianday(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    _ = allocator;
    if (args.len == 0) return Error.WrongArgumentCount;
    const dt = parseAndApplyModifiers(args) orelse return Value.null;
    return Value{ .real = calendar.dateTimeToJulianFloat(dt) };
}

/// `unixepoch(timestring, [modifier]*)` — sqlite3 returns seconds since
/// 1970-01-01 00:00:00 UTC as INTEGER. The 0-arg `unixepoch()` form
/// requires resolving `'now'` which depends on `std.Io` plumbing — until
/// that lands, we return NULL (sqlite3 returns NULL for any input that
/// fails to resolve, so the surface contract is preserved).
pub fn fnUnixepoch(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    _ = allocator;
    if (args.len == 0) return Value.null;
    const dt = parseAndApplyModifiers(args) orelse return Value.null;
    return Value{ .integer = calendar.unixEpochSeconds(dt) };
}

/// Core formatter shared by strftime/date/time/datetime. `args` is the
/// post-format slice: [datestring, modifier1, modifier2, ...]. A NULL
/// or non-text/blob datestring or modifier collapses the whole result
/// to NULL (sqlite3 parity); an unrecognised %-specifier in `fmt`
/// likewise → NULL.
fn formatDateTime(allocator: std.mem.Allocator, fmt: []const u8, args: []const Value) Error!Value {
    const dt = parseAndApplyModifiers(args) orelse return Value.null;

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
            'e' => try writeSpacePadded(allocator, &out, dt.day, 2),
            'H' => try writeZeroPadded(allocator, &out, dt.hour, 2),
            'I' => try writeZeroPadded(allocator, &out, twelveHour(dt.hour), 2),
            'M' => try writeZeroPadded(allocator, &out, dt.minute, 2),
            'S' => try writeZeroPadded(allocator, &out, dt.second, 2),
            'f' => {
                // sqlite3 `%f` is "SS.fff" (zero-padded second + 3-digit ms),
                // not just the fractional part — `strftime('%S.%f', …)`
                // surfaces this by emitting `56.56.789`. Using `dt.second`
                // here (instead of @as(u16, dt.second)) keeps the inferred
                // u16 promotion implicit.
                try writeZeroPadded(allocator, &out, dt.second, 2);
                try out.append(allocator, '.');
                try writeZeroPadded(allocator, &out, dt.millisecond, 3);
            },
            'j' => try writeZeroPadded(allocator, &out, calendar.dayOfYear(dt), 3),
            'w' => try writeZeroPadded(allocator, &out, calendar.dayOfWeek(dt), 1),
            'u' => try writeZeroPadded(allocator, &out, isoWeekday(calendar.dayOfWeek(dt)), 1),
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
            's' => try writeI64(allocator, &out, calendar.unixEpochSeconds(dt)),
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

/// Parse `args[0]` as a date string and apply each `args[1..]` as a
/// modifier left-to-right. NULL / non-text / unparsable input or any
/// modifier failure collapses to null (sqlite3 → SQL NULL).
fn parseAndApplyModifiers(args: []const Value) ?DateTime {
    const datestr = switch (args[0]) {
        .null => return null,
        .text => |t| t,
        .blob => |b| b,
        else => return null,
    };

    var dt = calendar.parseDateTime(datestr) orelse return null;
    for (args[1..]) |mod_arg| {
        const mod_str = switch (mod_arg) {
            .null => return null,
            .text => |t| t,
            .blob => |b| b,
            else => return null,
        };
        dt = modifier.applyModifier(dt, mod_str) orelse return null;
    }
    return dt;
}

/// Julian Day formatted as a shortest-unique decimal — matches sqlite3's
/// `%J` (which uses `printf("%.16g", iJD/86400000.0)`). Returned as TEXT
/// because `%J` is a strftime specifier; `julianday()` exists separately
/// for the REAL-typed shape sqlite3 uses there.
fn writeJulianDay(allocator: std.mem.Allocator, out: *std.ArrayList(u8), dt: DateTime) !void {
    const julian = calendar.dateTimeToJulianFloat(dt);
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
