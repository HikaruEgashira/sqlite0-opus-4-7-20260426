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
        .text => |t| blk: {
            // sqlite3 treats TEXT as a C string in `length()`: scanning
            // stops at the first NUL byte. `octet_length` is unaffected
            // (returns the raw byte count even with embedded NULs).
            const scan = if (std.mem.indexOfScalar(u8, t, 0)) |n| t[0..n] else t;
            break :blk Value{ .integer = @intCast(util.utf8CharCount(scan)) };
        },
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
/// unicode code points.
///
/// sqlite3 quirks reproduced here:
///   * Out-of-range codepoints (`< 0` or `> 0x10FFFF`) emit U+FFFD
///     (EF BF BD), the Unicode replacement character — NOT skipped.
///   * Surrogate codepoints (0xD800-0xDFFF) emit a literal 3-byte
///     WTF-8 sequence (ED Ax/Bx xx). sqlite3's UTF-8 encoder doesn't
///     reject surrogates the way std.unicode.utf8Encode does — match
///     by writing the bytes directly.
///   * `char(0)` emits a single NUL byte (no replacement).
pub fn fnChar(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    for (args) |a| {
        const cp = try util.toIntCoerce(a);
        if (cp < 0 or cp > 0x10FFFF) {
            try out.appendSlice(allocator, "\xEF\xBF\xBD");
            continue;
        }
        const code: u32 = @intCast(cp);
        if (code >= 0xD800 and code <= 0xDFFF) {
            try out.append(allocator, 0xE0 | @as(u8, @intCast((code >> 12) & 0x0F)));
            try out.append(allocator, 0x80 | @as(u8, @intCast((code >> 6) & 0x3F)));
            try out.append(allocator, 0x80 | @as(u8, @intCast(code & 0x3F)));
            continue;
        }
        var buf: [4]u8 = undefined;
        const len = std.unicode.utf8Encode(@intCast(code), &buf) catch unreachable;
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

// Tests live in funcs_text_test.zig (split out for the 500-line discipline).
