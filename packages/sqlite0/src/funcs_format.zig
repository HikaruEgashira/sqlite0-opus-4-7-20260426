//! `printf(fmt, ...)` / `format(fmt, ...)` — sqlite3-compatible format
//! function.
//!
//! Supports the common conversion specifiers: `%d`, `%i`, `%u`, `%x`, `%X`,
//! `%o`, `%s`, `%c`, `%f`, `%%`. Each spec accepts the standard flags
//! (`-`, `+`, ` `, `0`, `#`), optional width, and optional precision (for
//! integers: minimum digits; for `%f`: decimal places; for `%s`: max chars).
//!
//! Argument coercion follows sqlite3:
//!   * non-numeric TEXT → `0` for integer specs, `0.0` for float specs
//!   * NULL (any spec)  → empty string for `%s`, `0` / `0.0` for numeric
//!   * missing arg      → same defaults as NULL
//!
//! Out-of-spec specifiers (`%e`, `%g`, ...) are left unimplemented and the
//! formatter writes the literal `%X` for X (matches sqlite3's "unknown spec"
//! passthrough behavior closely enough for the differential cases we care
//! about).
//!
//! SQL-quoting specs (added Iter29.Z):
//!   * `%q` — escape single-quotes by doubling. NULL → literal `(NULL)`.
//!   * `%Q` — like `%q` and wrap in single quotes. NULL → literal `NULL`
//!     (no quotes; this is the only NULL-distinct quoting form).
//!   * `%w` — escape double-quotes by doubling (SQL identifier quoting).
//!     NULL → literal `(NULL)`.
//! Width/precision apply: precision truncates the *raw* input first, doubling
//! and (for `%Q`) wrapping then run, and width pads the final body. sqlite3's
//! C-string convention truncates these at the first NUL byte; we mirror.

const std = @import("std");
const util = @import("func_util.zig");
const ops = @import("ops.zig");

const Value = util.Value;
const Error = util.Error;

pub fn fnPrintf(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len == 0) return Error.WrongArgumentCount;
    const fmt = switch (args[0]) {
        .null => return Value.null, // sqlite3: printf(NULL, ...) → NULL
        .text => |t| t,
        .blob => |b| b,
        else => {
            // INTEGER/REAL: render to text and use as the format string.
            const t = ops.valueToOwnedText(allocator, args[0]) catch |err| switch (err) {
                error.OutOfMemory => return Error.OutOfMemory,
                error.NotConvertible => return Error.UnsupportedFeature,
            };
            defer allocator.free(t);
            return formatToValue(allocator, t, args[1..]);
        },
    };
    return formatToValue(allocator, fmt, args[1..]);
}

fn formatToValue(allocator: std.mem.Allocator, fmt: []const u8, args: []const Value) Error!Value {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);

    var i: usize = 0;
    var arg_idx: usize = 0;
    while (i < fmt.len) {
        const c = fmt[i];
        if (c != '%') {
            try out.append(allocator, c);
            i += 1;
            continue;
        }
        // Parse spec starting at fmt[i+1]
        const spec = parseSpec(fmt, i + 1);
        i = spec.end;

        switch (spec.conv) {
            '%' => try out.append(allocator, '%'),
            'd', 'i' => {
                const v = if (arg_idx < args.len) args[arg_idx] else Value.null;
                arg_idx += 1;
                try writeSignedInt(allocator, &out, v, spec, 10, false);
            },
            'u' => {
                const v = if (arg_idx < args.len) args[arg_idx] else Value.null;
                arg_idx += 1;
                try writeUnsignedInt(allocator, &out, v, spec, 10, false);
            },
            'x' => {
                const v = if (arg_idx < args.len) args[arg_idx] else Value.null;
                arg_idx += 1;
                try writeUnsignedInt(allocator, &out, v, spec, 16, false);
            },
            'X' => {
                const v = if (arg_idx < args.len) args[arg_idx] else Value.null;
                arg_idx += 1;
                try writeUnsignedInt(allocator, &out, v, spec, 16, true);
            },
            'o' => {
                const v = if (arg_idx < args.len) args[arg_idx] else Value.null;
                arg_idx += 1;
                try writeUnsignedInt(allocator, &out, v, spec, 8, false);
            },
            's' => {
                const v = if (arg_idx < args.len) args[arg_idx] else Value.null;
                arg_idx += 1;
                try writeString(allocator, &out, v, spec);
            },
            'c' => {
                const v = if (arg_idx < args.len) args[arg_idx] else Value.null;
                arg_idx += 1;
                try writeChar(allocator, &out, v);
            },
            'f' => {
                const v = if (arg_idx < args.len) args[arg_idx] else Value.null;
                arg_idx += 1;
                try writeFloat(allocator, &out, v, spec);
            },
            'q' => {
                const v = if (arg_idx < args.len) args[arg_idx] else Value.null;
                arg_idx += 1;
                try writeQuoted(allocator, &out, v, spec, '\'', false);
            },
            'Q' => {
                const v = if (arg_idx < args.len) args[arg_idx] else Value.null;
                arg_idx += 1;
                try writeQuoted(allocator, &out, v, spec, '\'', true);
            },
            'w' => {
                const v = if (arg_idx < args.len) args[arg_idx] else Value.null;
                arg_idx += 1;
                try writeQuoted(allocator, &out, v, spec, '"', false);
            },
            else => {
                // Unknown spec: write the original `%X` literally.
                try out.appendSlice(allocator, fmt[spec.start - 1 .. spec.end]);
            },
        }
    }
    return Value{ .text = try out.toOwnedSlice(allocator) };
}

const Spec = struct {
    start: usize, // index of first byte after `%`
    end: usize, // one-past the conversion char
    conv: u8,
    left_align: bool = false,
    plus: bool = false,
    space: bool = false,
    alt: bool = false,
    zero_pad: bool = false,
    width: usize = 0,
    precision: ?usize = null,
};

fn parseSpec(fmt: []const u8, start: usize) Spec {
    var s = Spec{ .start = start, .end = start, .conv = '%' };
    var p = start;
    // Flags
    while (p < fmt.len) : (p += 1) {
        switch (fmt[p]) {
            '-' => s.left_align = true,
            '+' => s.plus = true,
            ' ' => s.space = true,
            '#' => s.alt = true,
            '0' => s.zero_pad = true,
            else => break,
        }
    }
    // Width
    while (p < fmt.len and fmt[p] >= '0' and fmt[p] <= '9') : (p += 1) {
        s.width = s.width * 10 + (fmt[p] - '0');
    }
    // Precision
    if (p < fmt.len and fmt[p] == '.') {
        p += 1;
        var prec: usize = 0;
        while (p < fmt.len and fmt[p] >= '0' and fmt[p] <= '9') : (p += 1) {
            prec = prec * 10 + (fmt[p] - '0');
        }
        s.precision = prec;
    }
    if (p < fmt.len) {
        s.conv = fmt[p];
        s.end = p + 1;
    } else {
        s.end = p;
    }
    return s;
}

fn coerceToInt(v: Value) i64 {
    return switch (v) {
        .null => 0,
        .integer => |i| i,
        .real => |r| @intFromFloat(r),
        .text => |t| std.fmt.parseInt(i64, t, 10) catch 0,
        .blob => |b| std.fmt.parseInt(i64, b, 10) catch 0,
    };
}

fn coerceToFloat(v: Value) f64 {
    return switch (v) {
        .null => 0,
        .integer => |i| @floatFromInt(i),
        .real => |r| r,
        .text => |t| std.fmt.parseFloat(f64, t) catch 0,
        .blob => |b| std.fmt.parseFloat(f64, b) catch 0,
    };
}

fn writeSignedInt(
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
    try writeIntPadded(allocator, out, digits, sign, spec);
}

fn writeUnsignedInt(
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
    try writeIntPadded(allocator, out, digits, null, spec);
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

fn writeIntPadded(
    allocator: std.mem.Allocator,
    out: *std.ArrayList(u8),
    digits: []const u8,
    sign: ?u8,
    spec: Spec,
) !void {
    // precision = minimum number of digits
    const min_digits = spec.precision orelse 0;
    const pad_zeros: usize = if (digits.len < min_digits) min_digits - digits.len else 0;
    const sign_len: usize = if (sign != null) 1 else 0;
    const content_len = digits.len + pad_zeros + sign_len;
    const total_pad: usize = if (content_len < spec.width) spec.width - content_len else 0;
    // sqlite3: explicit precision suppresses zero-pad flag
    const use_zero_pad = spec.zero_pad and spec.precision == null and !spec.left_align;
    if (!spec.left_align and !use_zero_pad) try appendN(allocator, out, ' ', total_pad);
    if (sign) |s| try out.append(allocator, s);
    if (use_zero_pad) try appendN(allocator, out, '0', total_pad);
    try appendN(allocator, out, '0', pad_zeros);
    try out.appendSlice(allocator, digits);
    if (spec.left_align) try appendN(allocator, out, ' ', total_pad);
}

fn writeString(
    allocator: std.mem.Allocator,
    out: *std.ArrayList(u8),
    v: Value,
    spec: Spec,
) !void {
    var owned: ?[]u8 = null;
    defer if (owned) |b| allocator.free(b);
    const s: []const u8 = switch (v) {
        .null => "",
        .text => |t| t,
        .blob => |b| b,
        else => blk: {
            owned = ops.valueToOwnedText(allocator, v) catch |err| switch (err) {
                error.OutOfMemory => return Error.OutOfMemory,
                error.NotConvertible => return Error.UnsupportedFeature,
            };
            break :blk owned.?;
        },
    };
    const slice = if (spec.precision) |p| s[0..@min(p, s.len)] else s;
    const pad: usize = if (slice.len < spec.width) spec.width - slice.len else 0;
    if (!spec.left_align) try appendN(allocator, out, ' ', pad);
    try out.appendSlice(allocator, slice);
    if (spec.left_align) try appendN(allocator, out, ' ', pad);
}

fn writeChar(allocator: std.mem.Allocator, out: *std.ArrayList(u8), v: Value) !void {
    // sqlite3 quirk: `%c` writes the FIRST CHARACTER of the substituted
    // string (not a codepoint from an integer). `printf('%c', 65)` → "6"
    // (first char of "65"), not "A". Match that behavior.
    var owned: ?[]u8 = null;
    defer if (owned) |b| allocator.free(b);
    const s: []const u8 = switch (v) {
        .null => "",
        .text => |t| t,
        .blob => |b| b,
        else => blk: {
            owned = ops.valueToOwnedText(allocator, v) catch |err| switch (err) {
                error.OutOfMemory => return Error.OutOfMemory,
                error.NotConvertible => return Error.UnsupportedFeature,
            };
            break :blk owned.?;
        },
    };
    if (s.len > 0) try out.append(allocator, s[0]);
}

/// Render a value as an SQL-quoted body and append with width padding.
/// `qc` is the quote byte to double (`'` for `%q`/`%Q`, `"` for `%w`).
/// `wrap` (true only for `%Q`) wraps the doubled body in `qc` and uses the
/// literal `NULL` (no quotes) as the NULL rendering. `%q`/`%w` emit `(NULL)`
/// for NULL — sqlite3 distinguishes these to keep `%Q` round-trippable as
/// SQL while keeping `%q`/`%w` debug-readable.
fn writeQuoted(
    allocator: std.mem.Allocator,
    out: *std.ArrayList(u8),
    v: Value,
    spec: Spec,
    qc: u8,
    wrap: bool,
) !void {
    var body: std.ArrayList(u8) = .empty;
    defer body.deinit(allocator);

    if (v == .null) {
        const s: []const u8 = if (wrap) "NULL" else "(NULL)";
        const slice = if (spec.precision) |p| s[0..@min(p, s.len)] else s;
        try body.appendSlice(allocator, slice);
    } else {
        var owned: ?[]u8 = null;
        defer if (owned) |b| allocator.free(b);
        const raw_full: []const u8 = switch (v) {
            .text => |t| t,
            .blob => |b| b,
            else => blk: {
                owned = ops.valueToOwnedText(allocator, v) catch |err| switch (err) {
                    error.OutOfMemory => return Error.OutOfMemory,
                    error.NotConvertible => return Error.UnsupportedFeature,
                };
                break :blk owned.?;
            },
        };
        // sqlite3's C-string convention stops scanning at the first NUL byte —
        // mirror that so embedded-NUL inputs produce byte-equal output.
        const raw_to_nul: []const u8 = if (std.mem.indexOfScalar(u8, raw_full, 0)) |n|
            raw_full[0..n]
        else
            raw_full;
        const slice = if (spec.precision) |p| raw_to_nul[0..@min(p, raw_to_nul.len)] else raw_to_nul;
        if (wrap) try body.append(allocator, qc);
        for (slice) |c| {
            try body.append(allocator, c);
            if (c == qc) try body.append(allocator, qc);
        }
        if (wrap) try body.append(allocator, qc);
    }

    const pad: usize = if (body.items.len < spec.width) spec.width - body.items.len else 0;
    if (!spec.left_align) try appendN(allocator, out, ' ', pad);
    try out.appendSlice(allocator, body.items);
    if (spec.left_align) try appendN(allocator, out, ' ', pad);
}

fn writeFloat(
    allocator: std.mem.Allocator,
    out: *std.ArrayList(u8),
    v: Value,
    spec: Spec,
) !void {
    const f = coerceToFloat(v);
    const precision: usize = spec.precision orelse 6;
    var buf: [64]u8 = undefined;
    const printed = std.fmt.bufPrint(&buf, "{d:.[1]}", .{ f, precision }) catch return Error.OutOfMemory;
    var sign: ?u8 = null;
    var body: []const u8 = printed;
    if (printed.len > 0 and printed[0] == '-') {
        sign = '-';
        body = printed[1..];
    } else if (spec.plus) {
        sign = '+';
    } else if (spec.space) {
        sign = ' ';
    }
    const sign_len: usize = if (sign != null) 1 else 0;
    const content_len = body.len + sign_len;
    const pad: usize = if (content_len < spec.width) spec.width - content_len else 0;
    const use_zero_pad = spec.zero_pad and !spec.left_align;
    if (!spec.left_align and !use_zero_pad) try appendN(allocator, out, ' ', pad);
    if (sign) |s| try out.append(allocator, s);
    if (use_zero_pad) try appendN(allocator, out, '0', pad);
    try out.appendSlice(allocator, body);
    if (spec.left_align) try appendN(allocator, out, ' ', pad);
}

fn appendN(allocator: std.mem.Allocator, out: *std.ArrayList(u8), c: u8, n: usize) !void {
    var i: usize = 0;
    while (i < n) : (i += 1) try out.append(allocator, c);
}

test "fnPrintf: %q doubles single quotes" {
    const a = std.testing.allocator;
    var args = [_]Value{ .{ .text = "%q" }, .{ .text = "it's" } };
    const r = try fnPrintf(a, &args);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("it''s", r.text);
}

test "fnPrintf: %Q wraps and quotes; NULL → bare NULL" {
    const a = std.testing.allocator;
    var args1 = [_]Value{ .{ .text = "%Q" }, .{ .text = "x" } };
    const r1 = try fnPrintf(a, &args1);
    defer a.free(r1.text);
    try std.testing.expectEqualStrings("'x'", r1.text);

    var args2 = [_]Value{ .{ .text = "%Q" }, .null };
    const r2 = try fnPrintf(a, &args2);
    defer a.free(r2.text);
    try std.testing.expectEqualStrings("NULL", r2.text);
}

test "fnPrintf: %w doubles double quotes" {
    const a = std.testing.allocator;
    var args = [_]Value{ .{ .text = "%w" }, .{ .text = "a\"b" } };
    const r = try fnPrintf(a, &args);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("a\"\"b", r.text);
}

test "fnPrintf: %.3q truncates BEFORE doubling" {
    // sqlite3: precision counts input chars before quote-doubling expands.
    const a = std.testing.allocator;
    var args = [_]Value{ .{ .text = "%.3q" }, .{ .text = "ab'cdef" } };
    const r = try fnPrintf(a, &args);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("ab''", r.text);
}

test "fnPrintf: %q truncates at embedded NUL byte (C-string convention)" {
    const a = std.testing.allocator;
    var args = [_]Value{ .{ .text = "%q" }, .{ .text = "ab\x00cd" } };
    const r = try fnPrintf(a, &args);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("ab", r.text);
}

test "fnPrintf: %q NULL → (NULL); %Q NULL → NULL; precision applies" {
    const a = std.testing.allocator;
    var p1 = [_]Value{ .{ .text = "%q" }, .null };
    const r1 = try fnPrintf(a, &p1);
    defer a.free(r1.text);
    try std.testing.expectEqualStrings("(NULL)", r1.text);

    var p2 = [_]Value{ .{ .text = "%.4q" }, .null };
    const r2 = try fnPrintf(a, &p2);
    defer a.free(r2.text);
    try std.testing.expectEqualStrings("(NUL", r2.text);
}
