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
const util = @import("func_util.zig");

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
        if (escape) |esc| if (pc == esc) {
            // Trailing escape is malformed → no match (sqlite3
            // `patternCompare` returns SQLITE_NOMATCH when the byte after
            // ESC is end-of-pattern; otherwise the next byte loses its
            // wildcard meaning even if it's the escape char itself).
            if (pi + 1 >= pattern.len) return false;
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

/// SQL `GLOB`: case-sensitive wildcard match. `*` matches zero-or-more,
/// `?` matches exactly one byte, `[abc]` / `[a-z]` matches a single byte
/// against a character class, with leading `!` or `^` negating the class.
/// NULL on either side propagates as NULL. Numeric values are coerced to
/// text first.
pub fn applyGlob(allocator: std.mem.Allocator, value: Value, pattern: Value) Error!Value {
    if (value == .null or pattern == .null) return Value.null;
    const v_bytes = try valueToMatchBytes(allocator, value);
    defer freeMatchBytes(allocator, value, v_bytes);
    const p_bytes = try valueToMatchBytes(allocator, pattern);
    defer freeMatchBytes(allocator, pattern, p_bytes);
    return ops.boolValue(matchGlob(v_bytes, p_bytes));
}

/// `*` greedily matches via right-anchored recursion; `?` consumes one byte.
/// Character classes `[...]` are inlined into one matchClass call; a class
/// without a closing `]` is treated as a malformed pattern and matches
/// nothing — sqlite3 returns 0 in that case (`'[' GLOB '['` → 0).
pub fn matchGlob(text: []const u8, pattern: []const u8) bool {
    var ti: usize = 0;
    var pi: usize = 0;
    while (pi < pattern.len) {
        const pc = pattern[pi];
        switch (pc) {
            '*' => {
                while (pi + 1 < pattern.len and pattern[pi + 1] == '*') pi += 1;
                if (pi + 1 == pattern.len) return true;
                pi += 1;
                while (ti <= text.len) : (ti += 1) {
                    if (matchGlob(text[ti..], pattern[pi..])) return true;
                }
                return false;
            },
            '?' => {
                if (ti >= text.len) return false;
                ti += 1;
                pi += 1;
            },
            '[' => {
                if (ti >= text.len) return false;
                const close = findClassEnd(pattern, pi + 1) orelse return false;
                if (!matchClass(pattern[pi + 1 .. close], text[ti])) return false;
                ti += 1;
                pi = close + 1;
            },
            else => {
                if (ti >= text.len or text[ti] != pc) return false;
                ti += 1;
                pi += 1;
            },
        }
    }
    return ti == text.len;
}

/// Locate the closing `]` for a `[...]` class starting at index `start`
/// (the byte after `[`). Per sqlite3 only `^` is a leading negation marker;
/// `!` is a literal class member. An immediately-leading `]` is also a
/// literal member (e.g. `[]a]` matches `]` or `a`), so we skip past one
/// optional `^` then optionally one `]` before searching for the closing
/// bracket. Returns null if no closing bracket exists.
fn findClassEnd(pattern: []const u8, start: usize) ?usize {
    if (start >= pattern.len) return null;
    var i: usize = start;
    if (pattern[i] == '^') i += 1;
    if (i < pattern.len and pattern[i] == ']') i += 1;
    while (i < pattern.len) : (i += 1) {
        if (pattern[i] == ']') return i;
    }
    return null;
}

fn matchClass(class: []const u8, c: u8) bool {
    if (class.len == 0) return false;
    var i: usize = 0;
    var negated = false;
    if (class[0] == '^') {
        negated = true;
        i = 1;
    }
    var found = false;
    while (i < class.len) {
        if (i + 2 < class.len and class[i + 1] == '-') {
            const lo = class[i];
            const hi = class[i + 2];
            if (c >= lo and c <= hi) found = true;
            i += 3;
        } else {
            if (class[i] == c) found = true;
            i += 1;
        }
    }
    return if (negated) !found else found;
}

/// `like(pattern, text [, escape])` scalar function. sqlite3 exposes the LIKE
/// operator as a callable function with the *pattern first*, opposite of the
/// operator form (`text LIKE pattern`). Any NULL argument — including the
/// optional escape — short-circuits to NULL. The 3-arg form requires escape
/// to be exactly one UTF-8 character (sqlite3 checks char count, not byte
/// length); we accept the same surface but the matcher only consults the
/// first byte, so a multi-byte single-char escape only escapes when the
/// pattern's lead byte matches — see Deferred note.
pub fn fnLike(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 2 and args.len != 3) return Error.WrongArgumentCount;
    if (args[0] == .null or args[1] == .null) return Value.null;
    const escape: ?u8 = if (args.len == 3) blk: {
        if (args[2] == .null) return Value.null;
        const esc_bytes = try util.ensureText(allocator, args[2]);
        defer allocator.free(esc_bytes);
        if (util.utf8CharCount(esc_bytes) != 1) return Error.SyntaxError;
        break :blk esc_bytes[0];
    } else null;
    return applyLike(allocator, args[1], args[0], escape);
}

/// `glob(pattern, text)` scalar function — strict 2-arg, pattern-first
/// (mirrors `fnLike`'s arg-order swap). Either NULL → NULL.
pub fn fnGlob(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 2) return Error.WrongArgumentCount;
    if (args[0] == .null or args[1] == .null) return Value.null;
    return applyGlob(allocator, args[1], args[0]);
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

test "match: matchGlob basic" {
    try std.testing.expect(matchGlob("abc", "a*"));
    try std.testing.expect(matchGlob("abc", "*c"));
    try std.testing.expect(matchGlob("abc", "a?c"));
    try std.testing.expect(!matchGlob("abc", "A*"));
    try std.testing.expect(!matchGlob("ab", "a??"));
}

test "match: matchGlob character classes" {
    try std.testing.expect(matchGlob("a", "[abc]"));
    try std.testing.expect(matchGlob("c", "[a-c]"));
    try std.testing.expect(!matchGlob("d", "[a-c]"));
    try std.testing.expect(matchGlob("d", "[^abc]"));
    try std.testing.expect(!matchGlob("a", "[^abc]"));
    // Only `^` negates; `!` is a literal class member (matches sqlite3).
    try std.testing.expect(matchGlob("!", "[!abc]"));
    try std.testing.expect(matchGlob("a", "[!abc]"));
    try std.testing.expect(!matchGlob("d", "[!abc]"));
}

test "match: matchGlob unmatched bracket" {
    try std.testing.expect(!matchGlob("[", "["));
    try std.testing.expect(!matchGlob("a", "[ab"));
}

test "match: applyGlob NULL propagation" {
    try std.testing.expectEqual(Value.null, try applyGlob(std.testing.allocator, .null, .{ .text = "*" }));
    try std.testing.expectEqual(Value.null, try applyGlob(std.testing.allocator, .{ .text = "a" }, .null));
}

test "match: fnLike pattern-first arg order" {
    const a = std.testing.allocator;
    var args = [_]Value{ .{ .text = "a%" }, .{ .text = "abc" } };
    const r = try fnLike(a, &args);
    try std.testing.expectEqual(@as(i64, 1), r.integer);
}

test "match: fnLike NULL escape returns NULL" {
    const a = std.testing.allocator;
    var args = [_]Value{ .{ .text = "a" }, .{ .text = "a" }, .null };
    const r = try fnLike(a, &args);
    try std.testing.expectEqual(Value.null, r);
}

test "match: fnLike multi-byte 1-char escape accepted" {
    const a = std.testing.allocator;
    // 'あ' is 1 UTF-8 char (3 bytes). sqlite3 accepts; we should too.
    var args = [_]Value{ .{ .text = "a" }, .{ .text = "b" }, .{ .text = "あ" } };
    const r = try fnLike(a, &args);
    try std.testing.expectEqual(@as(i64, 0), r.integer);
}

test "match: fnLike rejects multi-char escape" {
    const a = std.testing.allocator;
    var args = [_]Value{ .{ .text = "a" }, .{ .text = "a" }, .{ .text = "XY" } };
    try std.testing.expectError(Error.SyntaxError, fnLike(a, &args));
}

test "match: fnGlob pattern-first arg order" {
    const a = std.testing.allocator;
    var args = [_]Value{ .{ .text = "a*" }, .{ .text = "abc" } };
    const r = try fnGlob(a, &args);
    try std.testing.expectEqual(@as(i64, 1), r.integer);
}

test "match: fnGlob arg-count" {
    const a = std.testing.allocator;
    var two = [_]Value{ .{ .text = "*" }, .{ .text = "a" } };
    _ = try fnGlob(a, &two);
    var one = [_]Value{.{ .text = "*" }};
    try std.testing.expectError(Error.WrongArgumentCount, fnGlob(a, &one));
    var three = [_]Value{ .{ .text = "*" }, .{ .text = "a" }, .{ .text = "b" } };
    try std.testing.expectError(Error.WrongArgumentCount, fnGlob(a, &three));
}
