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
    // sqlite3 quirk: hex(NULL) returns empty TEXT, not SQL NULL — distinct
    // from most scalar fns which propagate NULL. The empty-string return
    // mirrors hex() of an empty BLOB.
    if (args[0] == .null) return Value{ .text = try allocator.dupe(u8, "") };
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
    // sqlite3 quirk: when haystack is TEXT (regardless of needle type),
    // the search advances char-by-char and matches the needle bytes only
    // at character boundaries. Two consequences fall out:
    //   * `instr('é', x'a9')` → 0 (needle is the *second* byte of 'é';
    //     matching at byte 1 isn't a char boundary, so the search misses).
    //   * `instr('日本語', '本')` → 2 (returns *char* position, not byte).
    // BLOB haystack always uses byte-by-byte search and byte position.
    const hay_is_text = args[0] != .blob;

    const hay = try util.ensureText(allocator, args[0]);
    defer allocator.free(hay);
    const needle = try util.ensureText(allocator, args[1]);
    defer allocator.free(needle);

    if (needle.len == 0) return Value{ .integer = 1 };
    if (hay_is_text) return Value{ .integer = textInstrCharPos(hay, needle) };
    const idx = std.mem.indexOf(u8, hay, needle) orelse return Value{ .integer = 0 };
    return Value{ .integer = @intCast(idx + 1) };
}

/// Char-by-char advance through a TEXT haystack, raw-byte-comparing the
/// needle at each character boundary. Returns 1-indexed char position
/// (sqlite3 parity) or 0 if no match. Lenient about malformed UTF-8 —
/// non-leading bytes (`>= 0x80`, `< 0xC0`) advance one byte at a time
/// like sqlite3's `SQLITE_SKIP_UTF8` macro.
fn textInstrCharPos(hay: []const u8, needle: []const u8) i64 {
    var byte_pos: usize = 0;
    var char_pos: usize = 0;
    while (byte_pos + needle.len <= hay.len) {
        if (std.mem.eql(u8, hay[byte_pos .. byte_pos + needle.len], needle)) {
            return @intCast(char_pos + 1);
        }
        const lead = hay[byte_pos];
        const step: usize = if (lead >= 0xF0) 4 else if (lead >= 0xE0) 3 else if (lead >= 0xC0) 2 else 1;
        const advance = @min(step, hay.len - byte_pos);
        byte_pos += advance;
        char_pos += 1;
    }
    return 0;
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
        // sqlite3 quirk: NULL args coerce to codepoint 0 (NUL byte) rather
        // than collapsing the whole call to NULL. `char(65, NULL, 66)`
        // emits `'A\0B'`, three bytes wide, even though the terminal /
        // sqlite3 CLI stops display at the embedded NUL.
        const cp: i64 = if (a == .null) 0 else try util.toIntCoerce(a);
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

// sqlite3 utf.c::sqlite3Utf8Trans1: lookup table for the initial accumulator
// value when reading a multi-byte UTF-8 sequence. Indexed by lead-byte − 0xC0.
// Entries 0..31 mask 5 LSBs (2-byte lead 0xC0..0xDF), 32..47 mask 4 LSBs
// (3-byte lead 0xE0..0xEF), 48..55 mask 3 LSBs (4-byte lead 0xF0..0xF7),
// 56..59 mask 2 LSBs (5-byte invalid 0xF8..0xFB), 60..61 mask 1 LSB (6-byte
// invalid 0xFC..0xFD), 62..63 are 0 (0xFE/0xFF — never UTF-8 lead).
const utf8_trans1 = [_]u8{
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
    0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F,
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x00, 0x01, 0x02, 0x03, 0x00, 0x01, 0x00, 0x00,
};

pub fn fnUnicode(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    if (args[0] == .null) return Value.null;
    // sqlite3's `unicodeFunc` reads `sqlite3_value_text(argv[0])` so all input
    // types funnel through the TEXT casting path. INTEGER/REAL render to their
    // canonical text form (`unicode(1)` → 49 = ASCII '1'), BLOB bytes are
    // treated as TEXT bytes verbatim. Then `if (z && z[0])` gates the result —
    // empty TEXT and TEXT whose first byte is NUL both yield SQL NULL.
    var owned: ?[]u8 = null;
    defer if (owned) |b| allocator.free(b);
    const raw: []const u8 = switch (args[0]) {
        .text => |t| t,
        .blob => |b| b,
        else => blk: {
            owned = ops.valueToOwnedText(allocator, args[0]) catch |err| switch (err) {
                error.OutOfMemory => return Error.OutOfMemory,
                error.NotConvertible => return Error.UnsupportedFeature,
            };
            break :blk owned.?;
        },
    };
    // C-string convention: stop at first NUL. Empty input → NULL (sqlite3:
    // `if (z[0]) sqlite3_result_int(...)` — false branch leaves the result
    // unset, which the API converts to SQL NULL).
    const bytes = if (std.mem.indexOfScalar(u8, raw, 0)) |n| raw[0..n] else raw;
    if (bytes.len == 0) return Value.null;
    return Value{ .integer = @intCast(decodeUtf8FirstCodepoint(bytes)) };
}

// sqlite3 utf.c::sqlite3Utf8Read — lenient UTF-8 codepoint reader. Differs
// from a strict decoder in three ways:
//   * Bytes < 0xC0 are returned raw (so 0x80..0xBF "orphan continuation"
//     bytes pass through unchanged — `unicode(x'80')` → 128).
//   * Lead bytes >= 0xC0 always consume the maximum run of continuation
//     bytes available; the lead byte's nominal length (e.g. 0xE0 = 3 bytes)
//     is NOT enforced. `unicode(x'f0a080')` decodes as a 3-byte form despite
//     0xF0 being a 4-byte lead (result: 0x0800).
//   * After accumulation, the result is replaced with U+FFFD if it would be
//     overlong (< 0x80), a UTF-16 surrogate (D800..DFFF), or a non-character
//     ending in FFFE/FFFF.
fn decodeUtf8FirstCodepoint(bytes: []const u8) u32 {
    const lead = bytes[0];
    if (lead < 0xC0) return lead;
    var c: u32 = utf8_trans1[lead - 0xC0];
    var i: usize = 1;
    while (i < bytes.len and (bytes[i] & 0xC0) == 0x80) : (i += 1) {
        c = (c << 6) + (bytes[i] & 0x3F);
    }
    if (c < 0x80) return 0xFFFD;
    if ((c & 0xFFFFF800) == 0xD800) return 0xFFFD;
    if ((c & 0xFFFFFFFE) == 0xFFFE) return 0xFFFD;
    return c;
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

/// `unhex(text [, ignore])` — decode a hex string into a BLOB.
///
/// Algorithm (matches sqlite3 3.51.0):
///   * Hex digits (`[0-9A-Fa-f]`) are always processed as digits — even
///     if they also appear in the `ignore` byte-set. (`unhex('414243',
///     '4')` → `'ABC'`, the `'4'` in ignore is moot because `'4'` is
///     hex first.)
///   * Non-hex bytes that occur in `ignore` are skipped, BUT only when
///     we're between pairs (i.e. just emitted a complete byte). Skipping
///     a non-hex ignore byte mid-pair (after the high nibble, before the
///     low nibble) is a sqlite3 error → NULL. (`unhex('4 1 4 2 4 3',
///     ' ')` → NULL because the space lands after `'4'`'s high nibble.)
///   * Any other non-hex byte → NULL.
///   * Final state must be between-pair (no dangling high nibble).
///
/// NULL on either argument propagates to NULL. On success the result is
/// always BLOB, even when empty (`unhex('','') = x''`).
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

    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);
    try out.ensureTotalCapacity(allocator, subject.len / 2 + 1);
    var pending_hi: ?u8 = null;
    for (subject) |b| {
        if (isHexDigitByte(b)) {
            const v = hexDigitValue(b);
            if (pending_hi) |hi| {
                try out.append(allocator, (hi << 4) | v);
                pending_hi = null;
            } else {
                pending_hi = v;
            }
            continue;
        }
        if (std.mem.indexOfScalar(u8, ignore, b) != null) {
            // Mid-pair ignore byte → sqlite3 rejects.
            if (pending_hi != null) return Value.null;
            continue;
        }
        return Value.null;
    }
    if (pending_hi != null) return Value.null;
    return Value{ .blob = try out.toOwnedSlice(allocator) };
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
