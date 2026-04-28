//! `substr(X, P, N)` / `substr(X, P)` implementation. Split out of
//! `funcs.zig` for the 500-line discipline.
//!
//! TEXT semantics: P and N count UTF-8 *characters* (not bytes), and the
//! scan honors the C-string convention (stops at the first NUL byte).
//! BLOB semantics: P and N are byte indices, and the result type is
//! BLOB. P=0 is a special case that pretends the string starts at index 1
//! but trims the count by one. Negative N (TEXT or BLOB) takes |N| chars/
//! bytes ending before P. INTEGER/REAL inputs render via
//! `valueToOwnedText` and use TEXT semantics — char-count matches
//! byte-count for the canonical numeric forms.

const std = @import("std");
const ops = @import("ops.zig");
const util = @import("func_util.zig");

const Value = util.Value;
const Error = util.Error;

pub fn fnSubstr(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 2 and args.len != 3) return Error.WrongArgumentCount;
    if (args[0] == .null or args[1] == .null) return Value.null;
    if (args.len == 3 and args[2] == .null) return Value.null;

    const has_n = args.len == 3;
    const p_raw = try util.toIntCoerce(args[1]);
    const n_raw: i64 = if (has_n) try util.toIntCoerce(args[2]) else 0;

    if (args[0] == .blob) {
        const slice = byteSubstrSlice(args[0].blob, p_raw, n_raw, has_n);
        return Value{ .blob = try allocator.dupe(u8, slice) };
    }

    var owned: ?[]u8 = null;
    defer if (owned) |b| allocator.free(b);
    const subject_text: []const u8 = switch (args[0]) {
        .text => |t| t,
        else => blk: {
            owned = ops.valueToOwnedText(allocator, args[0]) catch |err| switch (err) {
                error.OutOfMemory => return Error.OutOfMemory,
                error.NotConvertible => return Error.UnsupportedFeature,
            };
            break :blk owned.?;
        },
    };
    const slice = utf8CharSubstrSlice(subject_text, p_raw, n_raw, has_n);
    return Value{ .text = try allocator.dupe(u8, slice) };
}

fn byteSubstrSlice(z: []const u8, p_raw: i64, n_raw: i64, has_n: bool) []const u8 {
    const len_i: i64 = @intCast(z.len);
    var p1: i64 = p_raw;
    // sqlite3 substrFunc uses SQLITE_LIMIT_LENGTH (default 1e9) as the
    // sentinel for "no count given". The downstream `p2 -= 1` (P=0
    // start-before-1 rule) and `p2 += p1` (negative P clamp) stay
    // safely positive that way; if we used `len_i` instead we'd lose
    // bytes (e.g. substr(x'010203', 0) drops to 2 bytes when 1B sentinel
    // would yield 3). Final `p1+p2 > len_i` clamp bounds the result.
    var p2: i64 = if (has_n) n_raw else 1_000_000_000;
    var negP2: bool = false;
    if (has_n and p2 < 0) {
        p2 = -p2;
        negP2 = true;
    }
    if (p1 < 0) {
        p1 += len_i;
        if (p1 < 0) {
            p2 += p1;
            if (p2 < 0) p2 = 0;
            p1 = 0;
        }
    } else if (p1 > 0) {
        p1 -= 1;
    } else if (p2 > 0) {
        p2 -= 1;
    }
    if (negP2) {
        p1 -= p2;
        if (p1 < 0) {
            p2 += p1;
            p1 = 0;
        }
    }
    if (p1 + p2 > len_i) p2 = len_i - p1;
    if (p2 < 0) p2 = 0;
    if (p1 > len_i) return z[0..0];
    const start: usize = @intCast(p1);
    const end: usize = @intCast(p1 + p2);
    return z[start..end];
}

fn utf8CharSubstrSlice(z: []const u8, p_raw: i64, n_raw: i64, has_n: bool) []const u8 {
    var p1: i64 = p_raw;
    // Without an explicit count, sqlite3 substitutes SQLITE_LIMIT_LENGTH
    // (~1 billion) — a sentinel for "rest of string". We use i64 max for
    // the same effect; the NUL-stop loop bounds the actual reach.
    var p2: i64 = if (has_n) n_raw else std.math.maxInt(i64);
    var negP2: bool = false;
    if (has_n and p2 < 0) {
        p2 = -p2;
        negP2 = true;
    }
    if (p1 < 0) {
        // sqlite3: char count up to first NUL (C-string convention).
        var z_idx: usize = 0;
        var len: i64 = 0;
        while (z_idx < z.len and z[z_idx] != 0) : (len += 1) {
            z_idx = util.skipUtf8Char(z, z_idx);
        }
        p1 += len;
        if (p1 < 0) {
            if (has_n) p2 += p1;
            if (p2 < 0) p2 = 0;
            p1 = 0;
        }
    } else if (p1 > 0) {
        p1 -= 1;
    } else if (p2 > 0) {
        p2 -= 1;
    }
    if (negP2) {
        p1 -= p2;
        if (p1 < 0) {
            p2 += p1;
            p1 = 0;
        }
    }
    var i: usize = 0;
    while (i < z.len and z[i] != 0 and p1 > 0) : (p1 -= 1) {
        i = util.skipUtf8Char(z, i);
    }
    const start = i;
    while (i < z.len and z[i] != 0 and p2 > 0) : (p2 -= 1) {
        i = util.skipUtf8Char(z, i);
    }
    return z[start..i];
}

