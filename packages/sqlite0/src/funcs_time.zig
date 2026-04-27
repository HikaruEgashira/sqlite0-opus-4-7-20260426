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
const render = @import("funcs_time_render.zig");

const Value = util.Value;
const Error = util.Error;
const DateTime = calendar.DateTime;
const YearFormat = render.YearFormat;
const renderFormat = render.renderFormat;

pub fn fnStrftime(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len < 2) return Error.WrongArgumentCount;
    const fmt = switch (args[0]) {
        .null => return Value.null,
        .text => |t| t,
        .blob => |b| b,
        else => return Value.null,
    };
    const r = parseAndApplyModifiers(args[1..]) orelse return Value.null;
    return renderFormat(allocator, fmt, r.dt, r.subsec, .strftime);
}

/// `date(timestring, [modifier]*)` — sqlite3 shorthand for
/// `strftime('%Y-%m-%d', timestring, ...)`. The `'subsec'` modifier has
/// no effect because `%Y-%m-%d` carries no time component.
pub fn fnDate(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len == 0) return Error.WrongArgumentCount;
    const r = parseAndApplyModifiers(args) orelse return Value.null;
    return renderFormat(allocator, "%Y-%m-%d", r.dt, r.subsec, .date_func);
}

pub fn fnTime(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len == 0) return Error.WrongArgumentCount;
    const r = parseAndApplyModifiers(args) orelse return Value.null;
    const fmt = if (r.subsec) "%H:%M:%f" else "%H:%M:%S";
    return renderFormat(allocator, fmt, r.dt, r.subsec, .date_func);
}

pub fn fnDatetime(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len == 0) return Error.WrongArgumentCount;
    const r = parseAndApplyModifiers(args) orelse return Value.null;
    const fmt = if (r.subsec) "%Y-%m-%d %H:%M:%f" else "%Y-%m-%d %H:%M:%S";
    return renderFormat(allocator, fmt, r.dt, r.subsec, .date_func);
}

/// `julianday(timestring, [modifier]*)` — sqlite3 returns the
/// continuous Julian day as REAL (mid-day UTC of JDN N is exactly N;
/// midnight is N - 0.5). Distinct from `strftime('%J', ...)` which
/// returns the same value as TEXT — REAL avoids the `2460311 vs
/// 2460311.0` formatting divergence the CLI surfaces. The `'subsec'`
/// modifier is accepted for parity with sqlite3 but is a no-op here:
/// julianday is already REAL with sub-sec precision baked in.
pub fn fnJulianday(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    _ = allocator;
    if (args.len == 0) return Error.WrongArgumentCount;
    const r = parseAndApplyModifiers(args) orelse return Value.null;
    return Value{ .real = calendar.dateTimeToJulianFloat(r.dt) };
}

/// `unixepoch(timestring, [modifier]*)` — sqlite3 returns seconds since
/// 1970-01-01 00:00:00 UTC. Default is INTEGER (truncated to whole
/// seconds); the `'subsec'` / `'subsecond'` modifier flips the return
/// to REAL with the millisecond fraction included.
pub fn fnUnixepoch(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    _ = allocator;
    if (args.len == 0) return Value.null;
    const r = parseAndApplyModifiers(args) orelse return Value.null;
    if (r.subsec) {
        const ms_total: f64 = @as(f64, @floatFromInt(calendar.unixEpochSeconds(r.dt))) + @as(f64, @floatFromInt(r.dt.millisecond)) / 1000.0;
        return Value{ .real = ms_total };
    }
    return Value{ .integer = calendar.unixEpochSeconds(r.dt) };
}

/// Parse `args[0]` as a date string and apply each `args[1..]` as a
/// modifier left-to-right. Returns `null` (= sqlite3 SQL NULL) for any
/// unparsable input or modifier failure.
///
/// Input-mode modifiers (must appear as `args[1]`):
///   * `'unixepoch'` — `args[0]` is seconds since 1970-01-01 UTC
///     (INTEGER / REAL / numeric-TEXT). Sub-second precision rides
///     through the f64 JD round-trip.
///   * `'julianday'` — `args[0]` is a Julian day float. Treated as
///     already a JD; bypasses date-string parsing.
///   * sqlite3 quirk: input-mode modifiers placed at position ≥ 2 fall
///     through to `applyModifier`, which doesn't recognise them → NULL.
///
/// Output-mode modifier (may appear anywhere in the chain):
///   * `'subsec'` / `'subsecond'` — flips default datetime/time format
///     to include `%f`, and `unixepoch()` to return REAL. No effect on
///     `julianday`/`date`/`strftime` (already format-controlled).
pub const ParseResult = struct {
    dt: DateTime,
    subsec: bool = false,
};

fn parseAndApplyModifiers(args: []const Value) ?ParseResult {
    var dt: DateTime = undefined;
    var subsec = false;
    var mod_start: usize = 1;

    if (args.len >= 2) {
        if (isLiteralModifier(args[1], "unixepoch")) {
            const sec = numericFromArg(args[0]) orelse return null;
            dt = unixepochToDateTime(sec) orelse return null;
            mod_start = 2;
        } else if (isLiteralModifier(args[1], "julianday")) {
            const jd = numericFromArg(args[0]) orelse return null;
            dt = calendar.julianFloatToDateTime(jd) orelse return null;
            mod_start = 2;
        } else if (isLiteralModifier(args[1], "auto")) {
            dt = autoInterpret(args[0]) orelse return null;
            mod_start = 2;
        } else {
            dt = parsePrimaryArg(args[0]) orelse return null;
        }
    } else {
        dt = parsePrimaryArg(args[0]) orelse return null;
    }

    for (args[mod_start..]) |mod_arg| {
        const mod_str = switch (mod_arg) {
            .null => return null,
            .text => |t| t,
            .blob => |b| b,
            else => return null,
        };
        if (util.eqlIgnoreCase(mod_str, "subsec") or util.eqlIgnoreCase(mod_str, "subsecond")) {
            subsec = true;
            continue;
        }
        dt = modifier.applyModifier(dt, mod_str) orelse return null;
    }
    // Final-instant validation: sqlite3 rejects any JD < 0 (i.e. pre
    // `-4713-11-24 12:00:00 UTC`). Modifier-applied DateTimes already
    // round-trip through `julianFloatToDateTime` which guards this; the
    // remaining hole is the no-modifier path where `parseDateTime`
    // accepts `-4713-11-24` (00:00, JD = -0.5) but the render path
    // would emit `-4713-11-24` instead of NULL.
    if (calendar.dateTimeToJulianFloat(dt) < 0) return null;
    return .{ .dt = dt, .subsec = subsec };
}

/// Resolve `args[0]` for the no-modifier / non-input-mode-modifier paths.
///
/// sqlite3 3.42+ rule (verified against 3.51.0):
///   * INTEGER / REAL → Julian day (must be ≥ 0).
///   * TEXT / BLOB:
///     - If the trimmed bytes parse end-to-end as a number AND value ≥ 0,
///       treat as Julian day (`'1234'` → JD 1234, `'+5'` → JD 5,
///       `'-0'` → JD 0). Negative numerics → NULL (sqlite3 quirk:
///       `date('-1')` is rejected).
///     - Otherwise fall through to the date-string parser
///       (`'2024-01-01'`, `'-0001-12-31'`, etc.).
///
/// NULL → NULL.
fn parsePrimaryArg(v: Value) ?DateTime {
    switch (v) {
        .null => return null,
        .integer => |i| {
            if (i < 0) return null;
            return calendar.julianFloatToDateTime(@floatFromInt(i));
        },
        .real => |r| {
            // -0.0 normalises to 0.0 — sqlite3 accepts `julianday(-0.0)`
            // as JD 0. `r >= 0` already covers this in IEEE.
            if (r < 0) return null;
            return calendar.julianFloatToDateTime(r);
        },
        .text => |t| return parsePrimaryText(t),
        .blob => |b| return parsePrimaryText(b),
    }
}

fn parsePrimaryText(s: []const u8) ?DateTime {
    if (util.parseFloatStrictOpt(s)) |jd| {
        if (jd < 0) return null;
        return calendar.julianFloatToDateTime(jd);
    }
    return calendar.parseDateTime(s);
}

fn isLiteralModifier(v: Value, name: []const u8) bool {
    const s = switch (v) {
        .text => |t| t,
        .blob => |b| b,
        else => return false,
    };
    return util.eqlIgnoreCase(s, name);
}

fn numericFromArg(v: Value) ?f64 {
    return switch (v) {
        .integer => |i| @floatFromInt(i),
        .real => |r| r,
        .text => |t| parseFloatTrimmed(t),
        .blob => |b| parseFloatTrimmed(b),
        .null => null,
    };
}

/// sqlite3 quirk: numeric inputs to `'unixepoch'` / `'julianday'` /
/// `'auto'` accept ASCII whitespace around the digits (`'  10  '` parses
/// as `10`), but reject any other tail (`'10xyz'` → NULL). `parseFloat`
/// alone fails on whitespace, so trim first.
fn parseFloatTrimmed(bytes: []const u8) ?f64 {
    const trimmed = std.mem.trim(u8, bytes, &std.ascii.whitespace);
    if (trimmed.len == 0) return null;
    return std.fmt.parseFloat(f64, trimmed) catch null;
}

/// `'auto'` modifier (sqlite3 3.46+): if the argument is numeric, choose
/// JD or unixepoch by magnitude — `0 ≤ v ≤ 5373484.5` lands inside the
/// JD range that maps to year 0..=9999, so use it as JD; everything else
/// (including negatives) is unixepoch seconds. Non-numeric TEXT falls
/// through to normal date-string parsing. Verified against sqlite3
/// 3.51.0 with `datetime(0,'auto')` → JD 0 (BC 4713-11-24) and
/// `datetime(5373485,'auto')` → unixepoch 5373485.
fn autoInterpret(arg: Value) ?DateTime {
    if (arg == .null) return null;
    const num: ?f64 = switch (arg) {
        .integer => |i| @as(f64, @floatFromInt(i)),
        .real => |r| r,
        .text => |t| parseFloatTrimmed(t),
        .blob => |b| parseFloatTrimmed(b),
        .null => null,
    };
    if (num) |v| {
        if (v >= 0 and v <= 5373484.5) {
            return calendar.julianFloatToDateTime(v);
        }
        return unixepochToDateTime(v);
    }
    const s = switch (arg) {
        .text => |t| t,
        .blob => |b| b,
        else => return null,
    };
    return calendar.parseDateTime(s);
}

/// Convert seconds-since-epoch (1970-01-01 00:00:00 UTC) to DateTime via
/// JD round-trip. Midnight 1970-01-01 = JD 2440587.5; a one-second tick
/// is 1/86400 of a JD. Sub-second fractions ride through as part of the
/// JD float — `julianFloatToDateTime` recovers ms via `round(day_frac *
/// 86_400_000)`. Returns null when the resulting year escapes 0..=9999.
fn unixepochToDateTime(sec: f64) ?DateTime {
    const jd = 2440587.5 + sec / 86400.0;
    return calendar.julianFloatToDateTime(jd);
}

