//! String/byte-manipulation builtins: replace, hex, quote, trim/ltrim/rtrim,
//! instr, char, unicode, length, octet_length, unhex. Imported by
//! `funcs.zig`'s dispatcher.

const std = @import("std");
const ops = @import("ops.zig");
const util = @import("func_util.zig");

const Value = util.Value;
const Error = util.Error;

pub fn fnLength(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const v = args[0];
    return switch (v) {
        .null => Value.null,
        .text => |t| Value{ .integer = @intCast(util.utf8CharCount(t)) },
        .blob => |b| Value{ .integer = @intCast(b.len) },
        .integer, .real => blk: {
            const t = ops.valueToOwnedText(allocator, v) catch |err| switch (err) {
                error.OutOfMemory => return Error.OutOfMemory,
                error.NotConvertible => return Error.UnsupportedFeature,
            };
            defer allocator.free(t);
            break :blk Value{ .integer = @intCast(t.len) };
        },
    };
}

/// `octet_length(X)` — sqlite3 byte-count companion to `length(X)`. The
/// only divergence from `length` is the TEXT branch: this returns the raw
/// UTF-8 byte length, while `length` returns the character count. BLOB
/// passes the raw byte count through (same as `length`), and NULL stays
/// NULL. INTEGER/REAL render through `valueToOwnedText` and report the
/// byte length of the rendered string — matching sqlite3's behaviour of
/// running the value through `sqlite3_snprintf` first.
pub fn fnOctetLength(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const v = args[0];
    return switch (v) {
        .null => Value.null,
        .text => |t| Value{ .integer = @intCast(t.len) },
        .blob => |b| Value{ .integer = @intCast(b.len) },
        .integer, .real => blk: {
            const t = ops.valueToOwnedText(allocator, v) catch |err| switch (err) {
                error.OutOfMemory => return Error.OutOfMemory,
                error.NotConvertible => return Error.UnsupportedFeature,
            };
            defer allocator.free(t);
            break :blk Value{ .integer = @intCast(t.len) };
        },
    };
}

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

/// `concat(a, b, ...)` — sqlite3's NULL-skipping TEXT concatenator.
/// Each non-NULL argument is rendered as text (INTEGER/REAL via the
/// %g-style renderer, BLOB as raw bytes, TEXT verbatim) and joined
/// with no separator. NULL arguments are silently dropped — even an
/// all-NULL call returns a non-NULL empty TEXT (sqlite3 quirk).
/// Requires `args.len >= 1`.
pub fn fnConcat(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len < 1) return Error.WrongArgumentCount;
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    for (args) |a| {
        if (a == .null) continue;
        try appendValueAsText(allocator, &out, a);
    }
    return Value{ .text = try out.toOwnedSlice(allocator) };
}

/// `concat_ws(sep, a, b, ...)` — sqlite3's NULL-aware separator-join.
/// Requires `args.len >= 2`. A NULL separator collapses the whole
/// expression to NULL (the only case where NULL propagates). NULL
/// values among the joinees are silently skipped, and the separator
/// is only inserted between successive non-NULL contributors — so
/// `concat_ws('-', 'a', NULL, 'b')` is `'a-b'`, not `'a--b'`. An
/// all-NULL value list still returns an empty TEXT.
pub fn fnConcatWs(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len < 2) return Error.WrongArgumentCount;
    if (args[0] == .null) return Value.null;

    const sep = try util.ensureText(allocator, args[0]);
    defer allocator.free(sep);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var first = true;
    for (args[1..]) |a| {
        if (a == .null) continue;
        if (!first) try out.appendSlice(allocator, sep);
        try appendValueAsText(allocator, &out, a);
        first = false;
    }
    return Value{ .text = try out.toOwnedSlice(allocator) };
}

fn appendValueAsText(
    allocator: std.mem.Allocator,
    out: *std.ArrayList(u8),
    v: Value,
) !void {
    switch (v) {
        .text, .blob => |bytes| try out.appendSlice(allocator, bytes),
        .integer, .real => {
            const t = ops.valueToOwnedText(allocator, v) catch |err| switch (err) {
                error.OutOfMemory => return Error.OutOfMemory,
                error.NotConvertible => return Error.UnsupportedFeature,
            };
            defer allocator.free(t);
            try out.appendSlice(allocator, t);
        },
        .null => unreachable,
    }
}

/// `unhex(text [, ignore])` — decode a hex string into a BLOB. The optional
/// 2nd arg is a *byte set*: any input byte that occurs in `ignore` is
/// dropped before the hex check (case-sensitive — `unhex('41x42','x')`
/// works, `unhex('41X42','x')` does not). Whatever remains must be all
/// `[0-9A-Fa-f]` and an even number of bytes; otherwise the result is
/// NULL. NULL on either argument propagates to NULL. On success the
/// result is always BLOB, even when empty (`unhex('','') = x''`).
pub fn fnUnhex(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len < 1 or args.len > 2) return Error.WrongArgumentCount;
    for (args) |a| if (a == .null) return Value.null;

    const subject = try util.ensureText(allocator, args[0]);
    defer allocator.free(subject);

    var ignore_owned: ?[]u8 = null;
    defer if (ignore_owned) |b| allocator.free(b);
    const ignore: []const u8 = if (args.len == 2) blk: {
        const i = try util.ensureText(allocator, args[1]);
        ignore_owned = i;
        break :blk i;
    } else "";

    var hex_only: std.ArrayList(u8) = .empty;
    defer hex_only.deinit(allocator);
    try hex_only.ensureTotalCapacity(allocator, subject.len);

    for (subject) |b| {
        if (std.mem.indexOfScalar(u8, ignore, b) != null) continue;
        if (!isHexDigitByte(b)) return Value.null;
        hex_only.appendAssumeCapacity(b);
    }

    if (hex_only.items.len % 2 != 0) return Value.null;

    const out = try allocator.alloc(u8, hex_only.items.len / 2);
    var i: usize = 0;
    while (i < out.len) : (i += 1) {
        const hi: u8 = hexDigitValue(hex_only.items[i * 2]);
        const lo: u8 = hexDigitValue(hex_only.items[i * 2 + 1]);
        out[i] = (hi << 4) | lo;
    }
    return Value{ .blob = out };
}

fn isHexDigitByte(c: u8) bool {
    return (c >= '0' and c <= '9') or
        (c >= 'a' and c <= 'f') or
        (c >= 'A' and c <= 'F');
}

fn hexDigitValue(c: u8) u8 {
    return switch (c) {
        '0'...'9' => c - '0',
        'a'...'f' => c - 'a' + 10,
        'A'...'F' => c - 'A' + 10,
        else => unreachable,
    };
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

test "fnLength: UTF-8 char count not byte count" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "aあ" }};
    const r = try fnLength(allocator, &args);
    try std.testing.expectEqual(@as(i64, 2), r.integer);
}

test "fnOctetLength: TEXT byte count" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "aあ" }};
    const r = try fnOctetLength(allocator, &args);
    try std.testing.expectEqual(@as(i64, 4), r.integer);
}

test "fnUnhex: ignore-set strips spaces, decodes to BLOB" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "41 42" }, .{ .text = " " } };
    const r = try fnUnhex(allocator, &args);
    defer allocator.free(r.blob);
    try std.testing.expectEqualSlices(u8, &.{ 0x41, 0x42 }, r.blob);
}

test "fnUnhex: odd hex length yields NULL" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "414" }};
    const r = try fnUnhex(allocator, &args);
    try std.testing.expectEqual(Value.null, r);
}

test "fnConcat: NULL skipped, all-NULL returns empty TEXT not NULL" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .null, .null };
    const r = try fnConcat(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("", r.text);
}

test "fnConcatWs: NULL sep collapses, NULL value skipped" {
    const allocator = std.testing.allocator;
    var null_sep = [_]Value{ .null, .{ .text = "a" }, .{ .text = "b" } };
    const r1 = try fnConcatWs(allocator, &null_sep);
    try std.testing.expectEqual(Value.null, r1);

    var null_value = [_]Value{ .{ .text = "-" }, .{ .text = "a" }, .null, .{ .text = "b" } };
    const r2 = try fnConcatWs(allocator, &null_value);
    defer allocator.free(r2.text);
    try std.testing.expectEqualStrings("a-b", r2.text);
}
