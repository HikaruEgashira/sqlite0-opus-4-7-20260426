//! `printf(fmt, ...)` / `format(fmt, ...)` — sqlite3-compatible format
//! function.
//!
//! Supports the common conversion specifiers: `%d`, `%i`, `%u`, `%x`, `%X`,
//! `%o`, `%s`, `%c`, `%f`, `%e`, `%E`, `%g`, `%G`, `%%`. Each spec accepts
//! the standard flags (`-`, `+`, ` `, `0`, `#`), optional width, and
//! optional precision (for integers: minimum digits; for `%f`/`%e`: decimal
//! places; for `%g`: significant digits; for `%s`: max chars).
//!
//! Argument coercion follows sqlite3:
//!   * non-numeric TEXT → `0` for integer specs, `0.0` for float specs
//!   * NULL (any spec)  → empty string for `%s`, `0` / `0.0` for numeric
//!   * missing arg      → same defaults as NULL
//!
//! Unknown spec letters follow sqlite3's PRINTF_SQLFUNC abort-on-bad-spec
//! rule: the format scan stops at the bad spec, bytes already produced by
//! *preceding* specs are returned as TEXT (width / precision applied
//! normally), and an empty accumulator becomes SQL NULL — sqlite3 calls
//! `sqlite3_result_text` with a NULL pointer, which the API converts to a
//! NULL value.
//!
//! Float caveat: Zig stdlib's `{e:.N}` and `{d:.N}` use round-half-to-even
//! while C printf uses round-half-away-from-zero. Tied values like `0.5` /
//! `2.5` at zero-precision will diverge byte-for-byte; non-tied values
//! (the overwhelming majority) match exactly.
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
const float_fmt = @import("funcs_format_float.zig");

const Value = util.Value;
const Error = util.Error;

pub fn fnPrintf(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    // sqlite3 quirk: `printf()` with no arguments returns SQL NULL rather
    // than raising — same surface contract as `printf('')` and any other
    // empty-accumulator path. WrongArgumentCount would diverge from
    // `typeof(printf()) → 'null'`.
    if (args.len == 0) return Value.null;
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
                try float_fmt.writeFloat(allocator, &out, v, spec);
            },
            'e' => {
                const v = if (arg_idx < args.len) args[arg_idx] else Value.null;
                arg_idx += 1;
                try float_fmt.writeExp(allocator, &out, v, spec, false);
            },
            'E' => {
                const v = if (arg_idx < args.len) args[arg_idx] else Value.null;
                arg_idx += 1;
                try float_fmt.writeExp(allocator, &out, v, spec, true);
            },
            'g' => {
                const v = if (arg_idx < args.len) args[arg_idx] else Value.null;
                arg_idx += 1;
                try float_fmt.writeGeneral(allocator, &out, v, spec, false);
            },
            'G' => {
                const v = if (arg_idx < args.len) args[arg_idx] else Value.null;
                arg_idx += 1;
                try float_fmt.writeGeneral(allocator, &out, v, spec, true);
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
                // sqlite3 PRINTF_SQLFUNC aborts the format scan at any
                // unimplemented / unknown spec. Bytes already accumulated by
                // preceding specs are returned as TEXT (width/precision were
                // applied per-spec); an empty accumulator becomes SQL NULL
                // because sqlite3 calls sqlite3_result_text with a NULL
                // pointer, which the API converts to a NULL value.
                if (out.items.len == 0) {
                    out.deinit(allocator);
                    return Value.null;
                }
                return Value{ .text = try out.toOwnedSlice(allocator) };
            },
        }
    }
    // sqlite3 PRINTF_SQLFUNC: `sqlite3_str_finish` returns NULL when zero
    // bytes have been written; `sqlite3_result_text(..., NULL, ...)` converts
    // that to a SQL NULL. Mirror it so `printf('')`, `printf('%s', NULL)`,
    // `printf('%s', '')`, and any other empty-accumulator result becomes
    // NULL instead of empty TEXT.
    if (out.items.len == 0) {
        out.deinit(allocator);
        return Value.null;
    }
    return Value{ .text = try out.toOwnedSlice(allocator) };
}

pub const Spec = struct {
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

fn writeIntPadded(
    allocator: std.mem.Allocator,
    out: *std.ArrayList(u8),
    digits: []const u8,
    sign: ?u8,
    spec: Spec,
) !void {
    try writeIntPaddedPrefixed(allocator, out, digits, sign, "", spec);
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
    if (!spec.left_align) try appendN(allocator, out, ' ', total_pad);
    if (sign) |s| try out.append(allocator, s);
    try out.appendSlice(allocator, prefix);
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
    const raw: []const u8 = switch (v) {
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
    // sqlite3 treats the %s argument as a C string and stops at the first
    // NUL byte. NUL-truncate BEFORE precision so `%.10s` of `"ab\0cd"`
    // returns `"ab"`, not `"ab\0cd"` (precision 10 doesn't pull post-NUL
    // bytes back). Width pads against the truncated length.
    const s = nulTruncate(raw);
    const slice = if (spec.precision) |p| s[0..@min(p, s.len)] else s;
    const pad: usize = if (slice.len < spec.width) spec.width - slice.len else 0;
    if (!spec.left_align) try appendN(allocator, out, ' ', pad);
    try out.appendSlice(allocator, slice);
    if (spec.left_align) try appendN(allocator, out, ' ', pad);
}

fn writeChar(allocator: std.mem.Allocator, out: *std.ArrayList(u8), v: Value) !void {
    // sqlite3 quirk: `%c` reads the C-string's first byte and ALWAYS writes
    // exactly 1 byte to the accumulator. Empty / NULL / leading-NUL inputs
    // all produce a NUL byte (because `bufpt[0]` is the NUL terminator).
    // INTEGER 65 is text-rendered to `"65"` first, so `printf('%c', 65)`
    // emits `'6'` (first char of `"65"`), not `'A'`.
    var owned: ?[]u8 = null;
    defer if (owned) |b| allocator.free(b);
    const raw: []const u8 = switch (v) {
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
    const c: u8 = if (raw.len > 0) raw[0] else 0;
    try out.append(allocator, c);
}

fn nulTruncate(s: []const u8) []const u8 {
    return if (std.mem.indexOfScalar(u8, s, 0)) |n| s[0..n] else s;
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
        const raw_to_nul = nulTruncate(raw_full);
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

pub fn appendN(allocator: std.mem.Allocator, out: *std.ArrayList(u8), c: u8, n: usize) !void {
    var i: usize = 0;
    while (i < n) : (i += 1) try out.append(allocator, c);
}

// Float-spec impls (`%f`, `%e`, `%E`, `%g`, `%G`) live in
// funcs_format_float.zig (500-line discipline). Tests live in
// funcs_format_test.zig.
