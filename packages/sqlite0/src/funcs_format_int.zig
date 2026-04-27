//! Integer-spec rendering for `printf` (`%d`, `%i`, `%u`, `%x`, `%X`, `%o`).
//!
//! Lives apart from `funcs_format.zig` so the parent file stays under
//! the 500-line discipline (CLAUDE.md "Module Splitting Rules"). Pure
//! string-building helpers; the only IO is `ArrayList.append` on the
//! caller-owned accumulator.

const std = @import("std");
const util = @import("func_util.zig");
const funcs_format = @import("funcs_format.zig");

const Value = util.Value;
const Spec = funcs_format.Spec;

/// Coerce any Value to i64 with sqlite3 printf semantics:
///   * NULL → 0
///   * REAL → saturating cast (lossyCast clamps Inf/NaN/over-range to
///     i64.max / i64.min / 0 the way sqlite3 does)
///   * TEXT/BLOB → atoi64 prefix parse, falling back to parseFloat →
///     saturating-i64 for pure-exponential strings like '9.99e99'.
pub fn coerceToInt(v: Value) i64 {
    return switch (v) {
        .null => 0,
        .integer => |i| i,
        // lossyCast clamps non-finite/out-of-range f64 to match sqlite3:
        // +Inf and values > i64.max → LLONG_MAX, -Inf and values < i64.min
        // → LLONG_MIN, NaN → 0. Plain `@intFromFloat` panics in safety
        // builds for any of these — `printf('%d', 9223372036854775808)`
        // hit the panic until this saturating path landed.
        .real => |r| std.math.lossyCast(i64, r),
        .text => |t| coerceTextToInt(t),
        .blob => |b| coerceTextToInt(b),
    };
}

/// printf TEXT→i64: try the atoi64 prefix parse first (sqlite3's
/// printf accepts `'10abc'` → 10, `'  10  '` → 10, `'0x10'` → 0). If
/// that finds nothing, fall back to parseFloat → saturating-i64 so
/// pure-exponential strings like `'9.99e99'` still produce a saturated
/// integer the way sqlite3 does. atoi64 itself lives in `func_util` so
/// `eval_cast` can share it.
fn coerceTextToInt(s: []const u8) i64 {
    if (util.atoi64Prefix(s)) |i| return i;
    if (std.fmt.parseFloat(f64, s)) |f| return std.math.lossyCast(i64, f) else |_| {}
    return 0;
}

pub fn writeSignedInt(
    allocator: std.mem.Allocator,
    out: *std.ArrayList(u8),
    v: Value,
    spec: Spec,
    base: u8,
    upper: bool,
) !void {
    _ = upper;
    const n = coerceToInt(v);
    var digits_buf: [32]u8 = undefined;
    const abs_val: u64 = if (n < 0) @as(u64, @intCast(-(n + 1))) + 1 else @as(u64, @intCast(n));
    const digits = formatDigits(&digits_buf, abs_val, base, false);
    var sign: ?u8 = null;
    if (n < 0) sign = '-' else if (spec.plus) sign = '+' else if (spec.space) sign = ' ';
    try writeIntPaddedPrefixed(allocator, out, digits, sign, "", spec);
}

pub fn writeUnsignedInt(
    allocator: std.mem.Allocator,
    out: *std.ArrayList(u8),
    v: Value,
    spec: Spec,
    base: u8,
    upper: bool,
) !void {
    const n = coerceToInt(v);
    const u: u64 = @bitCast(n);
    var digits_buf: [32]u8 = undefined;
    const digits = formatDigits(&digits_buf, u, base, upper);
    // sqlite3 alt-form (`#` flag): %#o prepends `0` to non-zero octal,
    // %#x/%#X prepends `0x`/`0X` to non-zero hex. Value 0 gets no prefix
    // in any base — sqlite3 quirk distinct from C printf, which would
    // emit `0x0` for `%#x` of 0.
    const prefix: []const u8 = if (spec.alt and u != 0)
        switch (base) {
            8 => "0",
            16 => if (upper) "0X" else "0x",
            else => "",
        }
    else
        "";
    try writeIntPaddedPrefixed(allocator, out, digits, null, prefix, spec);
}

fn formatDigits(buf: []u8, value: u64, base: u8, upper: bool) []const u8 {
    if (value == 0) {
        buf[0] = '0';
        return buf[0..1];
    }
    var v = value;
    var i: usize = buf.len;
    while (v > 0) : (v /= base) {
        i -= 1;
        const d: u8 = @intCast(v % base);
        buf[i] = if (d < 10) '0' + d else (if (upper) 'A' + (d - 10) else 'a' + (d - 10));
    }
    return buf[i..];
}

fn writeIntPaddedPrefixed(
    allocator: std.mem.Allocator,
    out: *std.ArrayList(u8),
    digits: []const u8,
    sign: ?u8,
    prefix: []const u8,
    spec: Spec,
) !void {
    // sqlite3 quirk (printf.c::etRADIX): the `0` flag forces the effective
    // precision up to `width - sign_len`, so the zero-padding becomes part of
    // the digit run rather than a separate fill. Two consequences fall out of
    // this single rule:
    //   * `0` wins over `-`: when content fills width via boosted precision,
    //     there is no slack left for left-justify spaces. `printf('%-05d', 42)`
    //     → "00042", not "42   ".
    //   * `0` wins over explicit precision when the precision is too small:
    //     `printf('%05.0d', 0)` → "00000", not "    0".
    // Note `prefix.len` (alt-form `0x`/`0X`/`0`) is intentionally NOT subtracted
    // — sqlite3 adds the alt-form prefix AFTER zero-padding, which can push
    // total length past `width`. `printf('%-#05x', 255)` → "0x000ff" (7 chars).
    const sign_len: usize = if (sign != null) 1 else 0;
    const explicit_prec = spec.precision orelse 0;
    const min_digits: usize = if (spec.zero_pad and spec.width > sign_len)
        @max(explicit_prec, spec.width - sign_len)
    else
        explicit_prec;
    const pad_zeros: usize = if (digits.len < min_digits) min_digits - digits.len else 0;
    const content_len = digits.len + pad_zeros + sign_len + prefix.len;
    const total_pad: usize = if (content_len < spec.width) spec.width - content_len else 0;
    if (!spec.left_align) try funcs_format.appendN(allocator, out, ' ', total_pad);
    if (sign) |s| try out.append(allocator, s);
    try out.appendSlice(allocator, prefix);
    try funcs_format.appendN(allocator, out, '0', pad_zeros);
    try out.appendSlice(allocator, digits);
    if (spec.left_align) try funcs_format.appendN(allocator, out, ' ', total_pad);
}
