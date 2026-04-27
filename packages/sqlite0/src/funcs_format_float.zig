//! `printf` float-spec implementations — `%f`, `%e`, `%E`, `%g`, `%G`.
//!
//! Lives apart from `funcs_format.zig` so the parent file stays under
//! the 500-line discipline. Integer/string/quoting specs (the 90% case
//! that doesn't touch f64 / log10 / dtoa) remain in the parent; this
//! file owns the float-specific buffering, exponent canonicalisation,
//! and `%g` form-selection logic.
//!
//! Float caveat (also documented in funcs_format.zig): Zig stdlib's
//! `{e:.N}` and `{d:.N}` use round-half-to-even while sqlite3's libc
//! printf uses round-half-away-from-zero. Tied values like `0.5` /
//! `2.5` at zero precision diverge byte-for-byte; non-tied values
//! match exactly.

const std = @import("std");
const funcs_format = @import("funcs_format.zig");
const util = @import("func_util.zig");

const Value = util.Value;
const Error = util.Error;
const Spec = funcs_format.Spec;

pub fn writeFloat(
    allocator: std.mem.Allocator,
    out: *std.ArrayList(u8),
    v: Value,
    spec: Spec,
) !void {
    const f = coerceToFloat(v);
    const precision: usize = spec.precision orelse 6;
    var buf: [80]u8 = undefined;
    const printed = std.fmt.bufPrint(&buf, "{d:.[1]}", .{ f, precision }) catch return Error.OutOfMemory;
    // sqlite3 `%#f` quirk: with precision 0 the alt-form keeps the trailing
    // decimal point (`printf('%#.0f', 1.0)` → `'1.'`). Zig's `{d:.0}`
    // produces `'1'`. Append the dot inline; width padding is applied by
    // `writeFloatBody`.
    if (spec.alt and precision == 0 and printed.len < buf.len) {
        buf[printed.len] = '.';
        try writeFloatBody(allocator, out, buf[0 .. printed.len + 1], spec);
        return;
    }
    try writeFloatBody(allocator, out, printed, spec);
}

/// `%e` / `%E`: scientific notation with a fixed mantissa precision.
/// Zig's `{e:.N}` already produces `M.MMMMMMeNN` form; we only post-process
/// the exponent into sqlite3/C-printf's `e+NN` shape (always-signed, ≥2
/// digits) and apply width/sign/zero-pad uniformly. Default precision is 6.
pub fn writeExp(
    allocator: std.mem.Allocator,
    out: *std.ArrayList(u8),
    v: Value,
    spec: Spec,
    upper: bool,
) !void {
    const f = coerceToFloat(v);
    const precision: usize = spec.precision orelse 6;
    var raw_buf: [96]u8 = undefined;
    const raw = std.fmt.bufPrint(&raw_buf, "{e:.[1]}", .{ f, precision }) catch return Error.OutOfMemory;
    var fmt_buf: [128]u8 = undefined;
    const normalized = normalizeExpForm(&fmt_buf, raw, upper);
    // sqlite3 `%#e` with precision 0 keeps the trailing dot before the
    // exponent (`printf('%#.0e', 1.0)` → `'1.e+00'`). Zig's `{e:.0}`
    // gives `1e+00`. Splice a `.` between the mantissa and `[eE]`.
    if (spec.alt and precision == 0) {
        var spliced: [144]u8 = undefined;
        const e_idx = std.mem.indexOfAny(u8, normalized, "eE") orelse normalized.len;
        @memcpy(spliced[0..e_idx], normalized[0..e_idx]);
        spliced[e_idx] = '.';
        @memcpy(spliced[e_idx + 1 .. normalized.len + 1], normalized[e_idx..]);
        try writeFloatBody(allocator, out, spliced[0 .. normalized.len + 1], spec);
        return;
    }
    try writeFloatBody(allocator, out, normalized, spec);
}

/// `%g` / `%G`: pick between `%f` and `%e` based on the rounded magnitude.
/// Default precision is 6 *significant digits*; `%.0g` is treated as
/// `%.1g`, matching C printf. We format `%e` first to capture the
/// post-rounding exponent — that catches the C-printf quirk where a
/// boundary value like `99999.9` rounds up to `100000` and gains a digit
/// (`%.5g 99999.9` → `1e+05`, not `100000`). Decision rule against the
/// *rounded* exponent: if `exp < -4` or `exp >= precision`, keep `%e`
/// form; otherwise reformat as `%f` with `precision-1-exp` decimals.
/// Without `#` flag, strip trailing zeros and a trailing `.`.
pub fn writeGeneral(
    allocator: std.mem.Allocator,
    out: *std.ArrayList(u8),
    v: Value,
    spec: Spec,
    upper: bool,
) !void {
    const f = coerceToFloat(v);
    var prec: usize = spec.precision orelse 6;
    if (prec == 0) prec = 1;

    // Format `%e` first so the exponent reflects post-rounding magnitude.
    var e_buf: [96]u8 = undefined;
    const e_raw = std.fmt.bufPrint(&e_buf, "{e:.[1]}", .{ f, prec - 1 }) catch return Error.OutOfMemory;
    const e_idx = std.mem.indexOfScalar(u8, e_raw, 'e') orelse e_raw.len;
    const exp_val: i32 = std.fmt.parseInt(i32, e_raw[e_idx + 1 ..], 10) catch 0;

    var body_buf: [128]u8 = undefined;
    const use_exp = exp_val < -4 or exp_val >= @as(i32, @intCast(prec));
    const body_full: []const u8 = if (use_exp) blk: {
        const stripped = stripGTrailing(&body_buf, e_raw, spec.alt, true);
        var final_buf: [128]u8 = undefined;
        const finalized = normalizeExpForm(&final_buf, stripped, upper);
        // sqlite3 `%#g` quirk: alt-form keeps a visible decimal point
        // even when the value has no fractional component
        // (`printf('%#.0g', 1.5)` → `'2.'`). Splice one in if missing.
        const out_slice = if (spec.alt) ensureMantissaDot(&body_buf, finalized) else body_buf[0..finalized.len];
        if (!spec.alt) @memcpy(body_buf[0..finalized.len], finalized);
        break :blk out_slice;
    } else blk: {
        const decimals_signed: i32 = @as(i32, @intCast(prec)) - 1 - exp_val;
        const decimals: usize = if (decimals_signed < 0) 0 else @intCast(decimals_signed);
        var raw_buf: [96]u8 = undefined;
        const raw = std.fmt.bufPrint(&raw_buf, "{d:.[1]}", .{ f, decimals }) catch return Error.OutOfMemory;
        const stripped = stripGTrailing(&body_buf, raw, spec.alt, false);
        if (spec.alt and std.mem.indexOfScalar(u8, stripped, '.') == null and stripped.len < body_buf.len) {
            body_buf[stripped.len] = '.';
            break :blk body_buf[0 .. stripped.len + 1];
        }
        break :blk stripped;
    };
    try writeFloatBody(allocator, out, body_full, spec);
}

/// Insert a `.` before the first `[eE]` (or at end) of `body` when no
/// `.` is present in the mantissa portion. Used for `%#g` / `%#G` alt
/// form when rounding collapsed the value to a single significant
/// digit. Caller owns `buf`; returns a slice into it.
fn ensureMantissaDot(buf: []u8, body: []const u8) []const u8 {
    const e_idx = std.mem.indexOfAny(u8, body, "eE") orelse body.len;
    const mantissa = body[0..e_idx];
    if (std.mem.indexOfScalar(u8, mantissa, '.') != null) {
        @memcpy(buf[0..body.len], body);
        return buf[0..body.len];
    }
    @memcpy(buf[0..e_idx], mantissa);
    buf[e_idx] = '.';
    @memcpy(buf[e_idx + 1 .. body.len + 1], body[e_idx..]);
    return buf[0 .. body.len + 1];
}

/// Convert Zig `{e}` exponent (`e0`, `e7`, `e-4`, `e100`) to sqlite3/C
/// printf form (`e+00`, `e+07`, `e-04`, `e+100`): sign always present,
/// digits zero-padded to ≥ 2. `upper` swaps the prefix to `E`. Caller
/// owns `buf`; returns a slice into it.
fn normalizeExpForm(buf: []u8, body: []const u8, upper: bool) []const u8 {
    const e_idx = std.mem.indexOfScalar(u8, body, 'e') orelse return body;
    @memcpy(buf[0..e_idx], body[0..e_idx]);
    var pos = e_idx;
    buf[pos] = if (upper) 'E' else 'e';
    pos += 1;
    var exp_str = body[e_idx + 1 ..];
    var exp_sign: u8 = '+';
    if (exp_str.len > 0 and exp_str[0] == '-') {
        exp_sign = '-';
        exp_str = exp_str[1..];
    } else if (exp_str.len > 0 and exp_str[0] == '+') {
        exp_str = exp_str[1..];
    }
    buf[pos] = exp_sign;
    pos += 1;
    if (exp_str.len < 2) {
        buf[pos] = '0';
        pos += 1;
    }
    @memcpy(buf[pos .. pos + exp_str.len], exp_str);
    pos += exp_str.len;
    return buf[0..pos];
}

/// `%g` post-processing: strip trailing zeros from the fractional part,
/// then strip a trailing `.`. For `%e`-form input (`is_exp = true`), only
/// the mantissa portion (before `e`) is processed; the exponent is
/// preserved verbatim. With the `#` flag (alt-form) the input is returned
/// unchanged — both trailing zeros and the decimal point survive, so
/// `printf('%#g', 1.5)` → `1.50000` (full 6-sig-digit width).
/// Caller owns `buf`; returns a slice into it.
fn stripGTrailing(buf: []u8, body: []const u8, alt: bool, is_exp: bool) []const u8 {
    if (alt) {
        @memcpy(buf[0..body.len], body);
        return buf[0..body.len];
    }
    var mantissa_end: usize = body.len;
    var exp_tail: []const u8 = &.{};
    if (is_exp) {
        if (std.mem.indexOfScalar(u8, body, 'e')) |ei| {
            mantissa_end = ei;
            exp_tail = body[ei..];
        }
    }
    var mantissa = body[0..mantissa_end];
    if (std.mem.indexOfScalar(u8, mantissa, '.')) |_| {
        var end = mantissa.len;
        while (end > 0 and mantissa[end - 1] == '0') : (end -= 1) {}
        if (end > 0 and mantissa[end - 1] == '.') end -= 1;
        mantissa = mantissa[0..end];
    }
    @memcpy(buf[0..mantissa.len], mantissa);
    @memcpy(buf[mantissa.len .. mantissa.len + exp_tail.len], exp_tail);
    return buf[0 .. mantissa.len + exp_tail.len];
}

/// Shared sign + width + zero-pad path for `%f`, `%e`, `%g`. The body
/// arrives already-formatted with a leading `-` (if negative); a positive
/// value gets `+` or ` ` from flags, otherwise no sign byte.
fn writeFloatBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayList(u8),
    body_full: []const u8,
    spec: Spec,
) !void {
    var sign: ?u8 = null;
    var body: []const u8 = body_full;
    if (body_full.len > 0 and body_full[0] == '-') {
        sign = '-';
        body = body_full[1..];
    } else if (spec.plus) {
        sign = '+';
    } else if (spec.space) {
        sign = ' ';
    }
    const sign_len: usize = if (sign != null) 1 else 0;
    const content_len = body.len + sign_len;
    const pad: usize = if (content_len < spec.width) spec.width - content_len else 0;
    const use_zero_pad = spec.zero_pad and !spec.left_align;
    if (!spec.left_align and !use_zero_pad) try funcs_format.appendN(allocator, out, ' ', pad);
    if (sign) |s| try out.append(allocator, s);
    if (use_zero_pad) try funcs_format.appendN(allocator, out, '0', pad);
    try out.appendSlice(allocator, body);
    if (spec.left_align) try funcs_format.appendN(allocator, out, ' ', pad);
}

fn coerceToFloat(v: Value) f64 {
    // sqlite3 quirk: TEXT/BLOB→REAL coercion uses `sqlite3AtoF`-style
    // *prefix* parse (same atof logic CAST AS REAL uses). `'1.5abc'`
    // parses as 1.5 instead of erroring or returning 0; `parseFloatLoose`
    // mirrors the rule. The .real path forwards as-is so signed zero
    // propagates here — the caller (writeFloat / writeExp / writeGeneral)
    // is the right place to normalise -0.0 → 0.0 to match sqlite3's
    // `realvalue<0.0` test (-0.0 compares equal to 0.0, so sqlite3 never
    // sets a `-` sign for negative zero).
    var f: f64 = switch (v) {
        .null => 0,
        .integer => |i| @floatFromInt(i),
        .real => |r| r,
        .text => |t| util.parseFloatLoose(t),
        .blob => |b| util.parseFloatLoose(b),
    };
    if (f == 0) f = 0; // collapse -0.0 → +0.0 (sqlite3 etFLOAT parity)
    return f;
}
