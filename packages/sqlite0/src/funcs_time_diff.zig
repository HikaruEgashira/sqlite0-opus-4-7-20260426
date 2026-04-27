//! sqlite3 `timediff(A, B)` — calendar-aware duration between two dates.
//!
//! Output is the fixed-width string `±YYYY-MM-DD HH:MM:SS.SSS` describing
//! "how much to add to B to reach A". Years and full months that don't
//! overshoot are peeled first; the remainder falls into days, hours,
//! minutes, seconds, milliseconds via JD-float subtraction.
//!
//! Why a separate module: sqlite3 accepts BC-era inputs (JD as low as 0,
//! which is November 24, 4714 BC). Our public `DateTime` clamps year to
//! 0..=9999 (u16) since user-facing parsing rejects negative years. For
//! timediff we decompose JD into a signed-year `ExtDate` so the algorithm
//! can subtract across the 0 BC/AD boundary without truncating.
//!
//! Range matches sqlite3 (datefunc.c `validJulianDay`): inputs are
//! accepted iff JD ∈ [0, 5373484.5]. JDN 5373484 is `9999-12-31`; one
//! more day pushes year past 9999 and sqlite3 returns NULL — same here.

const std = @import("std");
const util = @import("func_util.zig");
const calendar = @import("funcs_time_calendar.zig");

const Value = util.Value;
const Error = util.Error;

const ExtDate = struct {
    year: i32,
    month: u8,
    day: u8,
    hour: u8 = 0,
    minute: u8 = 0,
    second: u8 = 0,
    millisecond: u16 = 0,
};

pub fn fnTimediff(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 2) return Error.WrongArgumentCount;
    // sqlite3 docs: "amount of time that must be added to B in order to
    // reach A". A = args[0], B = args[1]. Calendar peel happens *on* B,
    // forward when A ≥ B and backward when A < B. The asymmetry matters
    // because months have different lengths — going forward 10 months
    // from Feb 25 lands on Dec 25 (a leap-year-dependent ~303 days), but
    // going backward 10 months from Jan 15 lands on Mar 15 (~306 days).
    const a = parseArg(args[0]) orelse return Value.null;
    var b = parseArg(args[1]) orelse return Value.null;

    const jd_a = extToJulianFloat(a);
    const jd_b = extToJulianFloat(b);

    // direction = +1 when peeling B forward toward A, -1 when backward.
    const dir: i32 = if (jd_a >= jd_b) 1 else -1;
    const sign: u8 = if (dir == 1) '+' else '-';

    // Year peel: largest |dY| such that B + dir*|dY| years has not passed
    // A. The anniversary check looks at whichever direction's m/d ordering
    // would cause overshoot. dir=+1 overshoots when (A.M, A.D) <
    // (B.M, B.D); dir=-1 overshoots when (B.M, B.D) < (A.M, A.D).
    const y_raw: i32 = if (dir == 1) a.year - b.year else b.year - a.year;
    var y_out: i32 = y_raw;
    var m_init: i32 = 0;
    const anniv_overshoot = if (dir == 1)
        (a.month < b.month or (a.month == b.month and a.day < b.day))
    else
        (b.month < a.month or (b.month == a.month and b.day < a.day));
    if (y_raw > 0 and anniv_overshoot) {
        y_out -= 1;
        m_init = 12;
    }
    b.year += dir * y_out;

    // Month peel — symmetric with the same direction-aware day check.
    var m_total: i32 = m_init + (if (dir == 1) @as(i32, a.month) - @as(i32, b.month) else @as(i32, b.month) - @as(i32, a.month));
    const day_overshoot = if (dir == 1) a.day < b.day else b.day < a.day;
    if (m_total > 0 and day_overshoot) {
        m_total -= 1;
    }

    // Apply months to B in the chosen direction.
    applyMonths(&b, dir * m_total);

    // Day-of-month overflow on the rolled date can still overshoot A
    // (e.g. Feb 25 + 10 months wraps onto a 28-day February if you started
    // in a 31-day Jan). Peel one more month if so.
    var jd_b_mod = extToJulianFloat(b);
    const overshot = if (dir == 1) jd_b_mod > jd_a else jd_b_mod < jd_a;
    if (m_total > 0 and overshot) {
        m_total -= 1;
        applyMonths(&b, -dir);
        jd_b_mod = extToJulianFloat(b);
    }

    // Residual sub-month diff via JD subtraction. Sign is folded into the
    // output prefix; the magnitude here is always non-negative.
    const ms_diff_f = (if (dir == 1) jd_a - jd_b_mod else jd_b_mod - jd_a) * 86_400_000.0;
    var total_ms: i64 = @intFromFloat(@round(ms_diff_f));
    if (total_ms < 0) total_ms = 0;

    const ms_part: i64 = @mod(total_ms, 1000);
    const total_s: i64 = @divTrunc(total_ms, 1000);
    const s_part: i64 = @mod(total_s, 60);
    const total_min: i64 = @divTrunc(total_s, 60);
    const min_part: i64 = @mod(total_min, 60);
    const total_h: i64 = @divTrunc(total_min, 60);
    const h_part: i64 = @mod(total_h, 24);
    const day_part: i64 = @divTrunc(total_h, 24);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.append(allocator, sign);
    try writeZeroPad(allocator, &out, y_out, 4);
    try out.append(allocator, '-');
    try writeZeroPad(allocator, &out, m_total, 2);
    try out.append(allocator, '-');
    try writeZeroPad(allocator, &out, day_part, 2);
    try out.append(allocator, ' ');
    try writeZeroPad(allocator, &out, h_part, 2);
    try out.append(allocator, ':');
    try writeZeroPad(allocator, &out, min_part, 2);
    try out.append(allocator, ':');
    try writeZeroPad(allocator, &out, s_part, 2);
    try out.append(allocator, '.');
    try writeZeroPad(allocator, &out, ms_part, 3);

    return Value{ .text = try out.toOwnedSlice(allocator) };
}

/// Add `delta` months to `dt` in-place, carrying year on overflow/underflow.
/// `delta` is bounded to `[-11, 11]` in the timediff path so single carry
/// is sufficient.
fn applyMonths(dt: *ExtDate, delta: i32) void {
    var m: i32 = @as(i32, dt.month) + delta;
    if (m > 12) {
        m -= 12;
        dt.year += 1;
    } else if (m < 1) {
        m += 12;
        dt.year -= 1;
    }
    dt.month = @intCast(m);
}

fn parseArg(v: Value) ?ExtDate {
    return switch (v) {
        .null => null,
        .integer => |i| jdFloatToExt(@as(f64, @floatFromInt(i))),
        .real => |r| jdFloatToExt(r),
        .text => |t| parseText(t),
        .blob => |b| parseText(b),
    };
}

/// Try strict date-string parse first, then fall back to numeric (= JD).
/// `'2024-01-15'` lands in the first arm; `'2460000.5'` (a JD float as
/// text) falls through to the second; `'2024-01-15.5'` matches neither.
fn parseText(s: []const u8) ?ExtDate {
    if (calendar.parseDateTime(s)) |dt| {
        return .{
            .year = @as(i32, dt.year),
            .month = dt.month,
            .day = dt.day,
            .hour = dt.hour,
            .minute = dt.minute,
            .second = dt.second,
            .millisecond = dt.millisecond,
        };
    }
    const trimmed = std.mem.trim(u8, s, &std.ascii.whitespace);
    if (trimmed.len == 0) return null;
    const f = std.fmt.parseFloat(f64, trimmed) catch return null;
    return jdFloatToExt(f);
}

fn jdFloatToExt(jd: f64) ?ExtDate {
    if (!(jd >= 0 and jd <= 5373484.5)) return null;

    const adj = jd + 0.5;
    var jdn: i64 = @intFromFloat(@floor(adj));
    const frac = adj - @as(f64, @floatFromInt(jdn));
    var total_ms: i64 = @intFromFloat(@round(frac * 86_400_000.0));
    if (total_ms >= 86_400_000) {
        total_ms -= 86_400_000;
        jdn += 1;
    }

    const ymd = jdnToYmdSigned(jdn);

    const total_s: i64 = @divTrunc(total_ms, 1000);
    const ms_field: i64 = @mod(total_ms, 1000);
    const hour: i64 = @divTrunc(total_s, 3600);
    const minute: i64 = @divTrunc(@mod(total_s, 3600), 60);
    const second: i64 = @mod(total_s, 60);

    return .{
        .year = ymd.year,
        .month = ymd.month,
        .day = ymd.day,
        .hour = @intCast(hour),
        .minute = @intCast(minute),
        .second = @intCast(second),
        .millisecond = @intCast(ms_field),
    };
}

const SignedYmd = struct { year: i32, month: u8, day: u8 };

fn jdnToYmdSigned(jdn: i64) SignedYmd {
    const a: i64 = jdn + 32044;
    const b: i64 = @divTrunc(4 * a + 3, 146097);
    const c: i64 = a - @divTrunc(146097 * b, 4);
    const d: i64 = @divTrunc(4 * c + 3, 1461);
    const e: i64 = c - @divTrunc(1461 * d, 4);
    const m: i64 = @divTrunc(5 * e + 2, 153);
    const day: i64 = e - @divTrunc(153 * m + 2, 5) + 1;
    const month: i64 = m + 3 - 12 * @divTrunc(m, 10);
    const year: i64 = 100 * b + d - 4800 + @divTrunc(m, 10);
    return .{ .year = @intCast(year), .month = @intCast(month), .day = @intCast(day) };
}

fn extToJulianFloat(dt: ExtDate) f64 {
    const a: i64 = @divTrunc(14 - @as(i64, dt.month), 12);
    const y: i64 = @as(i64, dt.year) + 4800 - a;
    const m: i64 = @as(i64, dt.month) + 12 * a - 3;
    const jdn: i64 = @as(i64, dt.day) + @divTrunc(153 * m + 2, 5) + 365 * y + @divTrunc(y, 4) - @divTrunc(y, 100) + @divTrunc(y, 400) - 32045;
    const tod_ms: i64 = (@as(i64, dt.hour) * 3600 + @as(i64, dt.minute) * 60 + dt.second) * 1000 + dt.millisecond;
    return @as(f64, @floatFromInt(jdn)) - 0.5 + @as(f64, @floatFromInt(tod_ms)) / 86_400_000.0;
}

fn writeZeroPad(allocator: std.mem.Allocator, out: *std.ArrayList(u8), value: i64, width: u8) !void {
    var buf: [16]u8 = undefined;
    var n_bytes: usize = 0;
    var v: i64 = if (value < 0) -value else value;
    if (v == 0) {
        buf[buf.len - 1] = '0';
        n_bytes = 1;
    } else {
        while (v > 0) : (v = @divTrunc(v, 10)) {
            const idx = buf.len - n_bytes - 1;
            buf[idx] = '0' + @as(u8, @intCast(@mod(v, 10)));
            n_bytes += 1;
        }
    }
    var pad: usize = 0;
    while (n_bytes + pad < width) : (pad += 1) {
        try out.append(allocator, '0');
    }
    const start = buf.len - n_bytes;
    try out.appendSlice(allocator, buf[start..]);
}
