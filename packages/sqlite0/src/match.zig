//! Pattern matching for SQL `LIKE` and (future) `GLOB`. Extracted from
//! ops.zig at Iter13.B prep when ops.zig hit 493/500. Keeps ops.zig focused
//! on arithmetic/comparison/3VL/affinity — LIKE/GLOB are pure byte-pattern
//! matchers that don't share machinery with those.
//!
//! Both operators coerce numeric values to text and treat BLOB bytes as raw
//! bytes (matching sqlite3 behavior). Either operand being NULL yields NULL.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");

const Value = value_mod.Value;
const Error = ops.Error;

/// SQL `LIKE`: case-insensitive ASCII match. `%` matches zero-or-more bytes,
/// `_` matches exactly one byte. NULL on either side propagates as NULL.
/// Numeric values are coerced to text first; BLOBs are matched as raw bytes
/// — sqlite3 treats both classes identically for LIKE.
///
/// `escape`, when set, marks one byte that suppresses the special meaning of
/// the next pattern byte (Iter13.C; pass null for plain LIKE).
pub fn applyLike(
    allocator: std.mem.Allocator,
    value: Value,
    pattern: Value,
    escape: ?u8,
) Error!Value {
    if (value == .null or pattern == .null) return Value.null;
    const v_bytes = try valueToMatchBytes(allocator, value);
    defer freeMatchBytes(allocator, value, v_bytes);
    const p_bytes = try valueToMatchBytes(allocator, pattern);
    defer freeMatchBytes(allocator, pattern, p_bytes);
    return ops.boolValue(matchLike(v_bytes, p_bytes, escape));
}

/// Borrow text/blob bytes directly; format integer/real into a fresh slice.
/// The caller pairs this with `freeMatchBytes` carrying the original Value
/// so only the integer/real branch frees.
pub fn valueToMatchBytes(allocator: std.mem.Allocator, v: Value) Error![]const u8 {
    return switch (v) {
        .text => |t| t,
        .blob => |b| b,
        .integer, .real => ops.valueToOwnedText(allocator, v) catch |err| switch (err) {
            error.OutOfMemory => Error.OutOfMemory,
            error.NotConvertible => Error.UnsupportedFeature,
        },
        .null => unreachable,
    };
}

pub fn freeMatchBytes(allocator: std.mem.Allocator, original: Value, bytes: []const u8) void {
    switch (original) {
        .integer, .real => allocator.free(bytes),
        else => {},
    }
}

/// Recursive-descent matcher. `%` greedily matches via right-anchored
/// recursion; `_` consumes one byte. ASCII case-fold via `std.ascii.toLower`.
/// `escape` suppresses the special meaning of the next byte when present.
pub fn matchLike(text: []const u8, pattern: []const u8, escape: ?u8) bool {
    var ti: usize = 0;
    var pi: usize = 0;
    while (pi < pattern.len) {
        const pc = pattern[pi];
        if (escape) |esc| if (pc == esc and pi + 1 < pattern.len) {
            const lit = pattern[pi + 1];
            if (ti >= text.len or !asciiEqIgnoreCase(text[ti], lit)) return false;
            ti += 1;
            pi += 2;
            continue;
        };
        switch (pc) {
            '%' => {
                while (pi + 1 < pattern.len and pattern[pi + 1] == '%') pi += 1;
                if (pi + 1 == pattern.len) return true;
                pi += 1;
                while (ti <= text.len) : (ti += 1) {
                    if (matchLike(text[ti..], pattern[pi..], escape)) return true;
                }
                return false;
            },
            '_' => {
                if (ti >= text.len) return false;
                ti += 1;
                pi += 1;
            },
            else => {
                if (ti >= text.len or !asciiEqIgnoreCase(text[ti], pc)) return false;
                ti += 1;
                pi += 1;
            },
        }
    }
    return ti == text.len;
}

fn asciiEqIgnoreCase(a: u8, b: u8) bool {
    return std.ascii.toLower(a) == std.ascii.toLower(b);
}

test "match: matchLike basic %" {
    try std.testing.expect(matchLike("abc", "a%", null));
    try std.testing.expect(matchLike("abc", "%c", null));
    try std.testing.expect(matchLike("abc", "%b%", null));
    try std.testing.expect(!matchLike("abc", "x%", null));
}

test "match: matchLike _ wildcard" {
    try std.testing.expect(matchLike("abc", "a_c", null));
    try std.testing.expect(!matchLike("abc", "a_", null));
    try std.testing.expect(!matchLike("ab", "a__", null));
}

test "match: matchLike empty patterns" {
    try std.testing.expect(matchLike("", "", null));
    try std.testing.expect(matchLike("", "%", null));
    try std.testing.expect(matchLike("", "%%", null));
    try std.testing.expect(!matchLike("a", "", null));
    try std.testing.expect(!matchLike("", "_", null));
}

test "match: matchLike case insensitive" {
    try std.testing.expect(matchLike("aBc", "ABC", null));
    try std.testing.expect(matchLike("ABC", "abc", null));
}

test "match: applyLike NULL propagation" {
    try std.testing.expectEqual(Value.null, try applyLike(std.testing.allocator, .null, .{ .text = "a" }, null));
    try std.testing.expectEqual(Value.null, try applyLike(std.testing.allocator, .{ .text = "a" }, .null, null));
}

test "match: applyLike numeric coercion" {
    const r = try applyLike(std.testing.allocator, .{ .integer = 123 }, .{ .text = "1%" }, null);
    try std.testing.expectEqual(@as(i64, 1), r.integer);
}
