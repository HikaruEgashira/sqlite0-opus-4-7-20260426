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

pub fn toIntCoerce(v: Value) Error!i64 {
    return switch (v) {
        .integer => |i| i,
        .real => |f| @intFromFloat(f),
        .text, .blob => |bytes| @intFromFloat(parseFloatLoose(bytes)),
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

/// Parse the longest valid numeric prefix of `bytes`. Anything that doesn't
/// parse cleanly returns 0 (matching SQLite's `CAST(...AS REAL)` semantics
/// for non-numeric text).
pub fn parseFloatLoose(bytes: []const u8) f64 {
    if (std.fmt.parseFloat(f64, bytes)) |f| return f else |_| {}
    var end = bytes.len;
    while (end > 0) : (end -= 1) {
        if (std.fmt.parseFloat(f64, bytes[0..end])) |f| return f else |_| {}
    }
    return 0;
}

pub fn nibble(n: u8) u8 {
    return if (n < 10) '0' + n else 'A' + (n - 10);
}
