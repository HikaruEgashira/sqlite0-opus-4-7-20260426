//! String/byte-manipulation builtins: replace, hex, quote, trim/ltrim/rtrim,
//! instr, char, unicode. Imported by `funcs.zig`'s dispatcher.

const std = @import("std");
const ops = @import("ops.zig");
const util = @import("func_util.zig");

const Value = util.Value;
const Error = util.Error;

pub fn fnReplace(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 3) return Error.WrongArgumentCount;
    for (args) |a| if (a == .null) return Value.null;

    const s = try util.ensureText(allocator, args[0]);
    defer allocator.free(s);
    const find = try util.ensureText(allocator, args[1]);
    defer allocator.free(find);
    const repl = try util.ensureText(allocator, args[2]);
    defer allocator.free(repl);

    if (find.len == 0) return Value{ .text = try allocator.dupe(u8, s) };

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);

    var i: usize = 0;
    while (i < s.len) {
        if (i + find.len <= s.len and std.mem.eql(u8, s[i .. i + find.len], find)) {
            try out.appendSlice(allocator, repl);
            i += find.len;
        } else {
            try out.append(allocator, s[i]);
            i += 1;
        }
    }
    return Value{ .text = try out.toOwnedSlice(allocator) };
}

pub fn fnHex(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    if (args[0] == .null) return Value.null;
    const bytes = try util.ensureBytes(allocator, args[0]);
    defer allocator.free(bytes);

    const out = try allocator.alloc(u8, bytes.len * 2);
    for (bytes, 0..) |b, i| {
        out[i * 2] = util.nibble(b >> 4);
        out[i * 2 + 1] = util.nibble(b & 0xF);
    }
    return Value{ .text = out };
}

/// SQLite `quote(X)`:
///   - NULL  → 'NULL'  (the literal four-byte text)
///   - INTEGER/REAL → render as text
///   - TEXT  → wrap in single quotes, double internal apostrophes
///   - BLOB  → X'<hex>'
pub fn fnQuote(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    switch (args[0]) {
        .null => return Value{ .text = try allocator.dupe(u8, "NULL") },
        .integer, .real => return Value{
            .text = ops.valueToOwnedText(allocator, args[0]) catch |err| switch (err) {
                error.OutOfMemory => return Error.OutOfMemory,
                error.NotConvertible => return Error.UnsupportedFeature,
            },
        },
        .text => |t| {
            var out: std.ArrayList(u8) = .empty;
            errdefer out.deinit(allocator);
            try out.append(allocator, '\'');
            for (t) |c| {
                if (c == '\'') try out.append(allocator, '\'');
                try out.append(allocator, c);
            }
            try out.append(allocator, '\'');
            return Value{ .text = try out.toOwnedSlice(allocator) };
        },
        .blob => |b| {
            var out: std.ArrayList(u8) = .empty;
            errdefer out.deinit(allocator);
            try out.append(allocator, 'X');
            try out.append(allocator, '\'');
            for (b) |c| {
                try out.append(allocator, util.nibble(c >> 4));
                try out.append(allocator, util.nibble(c & 0xF));
            }
            try out.append(allocator, '\'');
            return Value{ .text = try out.toOwnedSlice(allocator) };
        },
    }
}

pub const TrimSide = enum { left, right, both };

pub fn fnTrim(allocator: std.mem.Allocator, args: []const Value, side: TrimSide) Error!Value {
    if (args.len < 1 or args.len > 2) return Error.WrongArgumentCount;
    if (args[0] == .null) return Value.null;
    if (args.len == 2 and args[1] == .null) return Value.null;

    const s = try util.ensureText(allocator, args[0]);
    defer allocator.free(s);

    const default_ws: []const u8 = " ";
    var owned_chars: ?[]u8 = null;
    defer if (owned_chars) |bytes| allocator.free(bytes);
    const chars: []const u8 = if (args.len == 2) blk: {
        const c = try util.ensureText(allocator, args[1]);
        owned_chars = c;
        break :blk c;
    } else default_ws;

    var lo: usize = 0;
    var hi: usize = s.len;
    if (side == .left or side == .both) {
        while (lo < hi and std.mem.indexOfScalar(u8, chars, s[lo]) != null) lo += 1;
    }
    if (side == .right or side == .both) {
        while (hi > lo and std.mem.indexOfScalar(u8, chars, s[hi - 1]) != null) hi -= 1;
    }
    return Value{ .text = try allocator.dupe(u8, s[lo..hi]) };
}

/// SQLite `instr(haystack, needle)` — 1-based byte offset of first match,
/// 0 if not found, NULL if either argument is NULL.
pub fn fnInstr(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 2) return Error.WrongArgumentCount;
    if (args[0] == .null or args[1] == .null) return Value.null;
    const hay = try util.ensureText(allocator, args[0]);
    defer allocator.free(hay);
    const needle = try util.ensureText(allocator, args[1]);
    defer allocator.free(needle);

    if (needle.len == 0) return Value{ .integer = 1 };
    const idx = std.mem.indexOf(u8, hay, needle) orelse return Value{ .integer = 0 };
    return Value{ .integer = @intCast(idx + 1) };
}

/// SQLite `char(N1, N2, ...)` — concatenate the UTF-8 encodings of the given
/// unicode code points. Out-of-range or invalid code points are silently
/// skipped (matches the sqlite3 CLI behavior on bad inputs).
pub fn fnChar(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    for (args) |a| {
        const cp = try util.toIntCoerce(a);
        if (cp < 0 or cp > 0x10FFFF) continue;
        const code: u21 = @intCast(cp);
        var buf: [4]u8 = undefined;
        const len = std.unicode.utf8Encode(code, &buf) catch continue;
        try out.appendSlice(allocator, buf[0..len]);
    }
    return Value{ .text = try out.toOwnedSlice(allocator) };
}

pub fn fnUnicode(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    _ = allocator;
    if (args.len != 1) return Error.WrongArgumentCount;
    if (args[0] == .null) return Value.null;
    const bytes = switch (args[0]) {
        .text => |t| t,
        .blob => |b| b,
        else => return Error.UnsupportedFeature,
    };
    if (bytes.len == 0) return Value.null;
    const cp_len = std.unicode.utf8ByteSequenceLength(bytes[0]) catch return Value{ .integer = bytes[0] };
    if (cp_len > bytes.len) return Value{ .integer = bytes[0] };
    const cp = std.unicode.utf8Decode(bytes[0..cp_len]) catch return Value{ .integer = bytes[0] };
    return Value{ .integer = cp };
}

test "fnReplace: simple replacement" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "hello world" }, .{ .text = "world" }, .{ .text = "zig" } };
    const r = try fnReplace(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("hello zig", r.text);
}

test "fnReplace: empty find returns subject unchanged" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "abc" }, .{ .text = "" }, .{ .text = "X" } };
    const r = try fnReplace(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("abc", r.text);
}

test "fnHex: ASCII bytes" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "abc" }};
    const r = try fnHex(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("616263", r.text);
}

test "fnQuote: text wraps and doubles apostrophes" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "it's" }};
    const r = try fnQuote(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("'it''s'", r.text);
}

test "fnQuote: NULL is the literal text 'NULL'" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.null};
    const r = try fnQuote(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("NULL", r.text);
}

test "fnTrim: defaults to whitespace, both sides" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "  hi  " }};
    const r = try fnTrim(allocator, &args, .both);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("hi", r.text);
}

test "fnInstr: 1-based offset, 0 when missing" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "hello world" }, .{ .text = "world" } };
    const r1 = try fnInstr(allocator, &args);
    try std.testing.expectEqual(@as(i64, 7), r1.integer);

    var miss = [_]Value{ .{ .text = "hello" }, .{ .text = "world" } };
    const r2 = try fnInstr(allocator, &miss);
    try std.testing.expectEqual(@as(i64, 0), r2.integer);
}

test "fnChar: ASCII code points" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .integer = 72 }, .{ .integer = 105 } };
    const r = try fnChar(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("Hi", r.text);
}
