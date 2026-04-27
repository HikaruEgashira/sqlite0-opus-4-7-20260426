//! Helpers shared by `funcs.zig` and `funcs_text.zig`.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");

pub const Value = value_mod.Value;
pub const Error = ops.Error;

pub fn eqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| {
        if (std.ascii.toLower(x) != std.ascii.toLower(y)) return false;
    }
    return true;
}

pub fn dupeValue(allocator: std.mem.Allocator, v: Value) Error!Value {
    return switch (v) {
        .text => |t| Value{ .text = try allocator.dupe(u8, t) },
        .blob => |b| Value{ .blob = try allocator.dupe(u8, b) },
        else => v,
    };
}

/// Allocate text bytes for a non-NULL Value. TEXT/BLOB are duped, INTEGER/REAL
/// are formatted using SQLite's %g-style renderer. NULL is treated as a usage
/// error (callers must short-circuit before calling this).
pub fn ensureText(allocator: std.mem.Allocator, v: Value) Error![]u8 {
    return switch (v) {
        .text => |t| try allocator.dupe(u8, t),
        .blob => |b| try allocator.dupe(u8, b),
        .integer, .real => ops.valueToOwnedText(allocator, v) catch |err| switch (err) {
            error.OutOfMemory => return Error.OutOfMemory,
            error.NotConvertible => return Error.UnsupportedFeature,
        },
        .null => Error.UnsupportedFeature,
    };
}

/// Currently identical to `ensureText` because we don't have a distinct BLOB
/// path yet — kept as a separate name so the call sites read intentfully and
/// we have one place to revisit when blob-only semantics diverge.
pub fn ensureBytes(allocator: std.mem.Allocator, v: Value) Error![]u8 {
    return ensureText(allocator, v);
}

/// Count UTF-8 *characters* in `bytes` using sqlite3's lead-byte rule (every
/// non-continuation byte starts a new character). Used both by `length()`
/// (UTF-8 char count) and by `like()` ESCAPE-arg validation (sqlite3 requires
/// the escape to be exactly one UTF-8 character, not byte). Invalid input
/// (orphan leaders) counts each non-continuation byte as one — matches the
/// `SQLITE_SKIP_UTF8` loop.
pub fn utf8CharCount(bytes: []const u8) usize {
    var n: usize = 0;
    for (bytes) |b| {
        if ((b & 0xC0) != 0x80) n += 1;
    }
    return n;
}

/// sqlite3 `atoi64`-style lenient prefix parse for TEXT→i64 coercion.
/// Skip leading ASCII whitespace, accept optional `+`/`-`, take the
/// longest run of decimal digits, stop at the first non-digit (no hex,
/// no exponent — `'0x10'` parses the leading `0` then stops, `'1e6'`
/// stops at `e`). Returns null when no digits appear at all (caller
/// can chain a float fallback). Saturates to LLONG_MIN/MAX on overflow,
/// matching sqlite3's `printf` and `CAST AS INTEGER` paths.
pub fn atoi64Prefix(bytes: []const u8) ?i64 {
    var i: usize = 0;
    while (i < bytes.len and (bytes[i] == ' ' or bytes[i] == '\t' or bytes[i] == '\n' or bytes[i] == '\r')) i += 1;
    var neg = false;
    if (i < bytes.len and (bytes[i] == '+' or bytes[i] == '-')) {
        neg = bytes[i] == '-';
        i += 1;
    }
    if (i >= bytes.len or bytes[i] < '0' or bytes[i] > '9') return null;
    var n: i64 = 0;
    while (i < bytes.len and bytes[i] >= '0' and bytes[i] <= '9') : (i += 1) {
        const d: i64 = bytes[i] - '0';
        if (n > @divTrunc(std.math.maxInt(i64) - d, 10)) {
            return if (neg) std.math.minInt(i64) else std.math.maxInt(i64);
        }
        n = n * 10 + d;
    }
    return if (neg) -n else n;
}

pub fn toIntCoerce(v: Value) Error!i64 {
    return switch (v) {
        .integer => |i| i,
        // Saturating cast — clamps out-of-range / non-finite floats to
        // i64.min/max (sqlite3 parity, see `ops.coerceToI64`). `@intFromFloat`
        // would panic on `printf('%d', 9.99e99)` and similar inputs.
        .real => |f| std.math.lossyCast(i64, f),
        .text, .blob => |bytes| std.math.lossyCast(i64, parseFloatLoose(bytes)),
        .null => Error.UnsupportedFeature,
    };
}

pub fn numericAsReal(v: Value) f64 {
    return switch (v) {
        .integer => |i| @floatFromInt(i),
        .real => |f| f,
        .text, .blob => |bytes| parseFloatLoose(bytes),
        .null => 0,
    };
}

/// sqlite3 `sqlite3AtoF`-style lenient numeric prefix parser. Returns 0 for
/// inputs that don't begin with valid numeric syntax — sqlite3 quirk: this
/// function REJECTS literal `NaN`/`Inf` (Zig's `std.fmt.parseFloat` accepts
/// them). Whitespace prefix and trailing garbage are tolerated; the longest
/// valid numeric prefix is returned. The prefix must contain at least one
/// digit somewhere — bare signs (`+`/`-`/`.`) yield 0.
///
/// Used by `CAST(... AS REAL)`, type-affinity coercion in arithmetic, and
/// builtins like `abs(text)`. sqlite3's behaviour comes from `sqlite3AtoF`
/// in util.c which only accepts digit-led mantissas.
pub fn parseFloatLoose(bytes: []const u8) f64 {
    var i: usize = 0;
    while (i < bytes.len and (bytes[i] == ' ' or bytes[i] == '\t' or bytes[i] == '\n' or bytes[i] == '\r')) i += 1;
    const start = i;
    if (i < bytes.len and (bytes[i] == '+' or bytes[i] == '-')) i += 1;
    var saw_digit = false;
    while (i < bytes.len and bytes[i] >= '0' and bytes[i] <= '9') : (i += 1) saw_digit = true;
    if (i < bytes.len and bytes[i] == '.') {
        i += 1;
        while (i < bytes.len and bytes[i] >= '0' and bytes[i] <= '9') : (i += 1) saw_digit = true;
    }
    if (!saw_digit) return 0;
    var end = i;
    if (i < bytes.len and (bytes[i] == 'e' or bytes[i] == 'E')) {
        var j = i + 1;
        if (j < bytes.len and (bytes[j] == '+' or bytes[j] == '-')) j += 1;
        const exp_start = j;
        while (j < bytes.len and bytes[j] >= '0' and bytes[j] <= '9') j += 1;
        if (j > exp_start) end = j;
    }
    return std.fmt.parseFloat(f64, bytes[start..end]) catch 0;
}

pub fn nibble(n: u8) u8 {
    return if (n < 10) '0' + n else 'A' + (n - 10);
}
