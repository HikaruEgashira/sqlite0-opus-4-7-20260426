//! Numeric- and BLOB-literal value extraction for `parser.zig`. Split
//! out to keep `parser.zig` under the 500-line discipline (CLAUDE.md
//! "Module Splitting Rules") after Iter29.R added hex unary sign-fold.
//!
//! All helpers here run after the lexer has already validated token
//! shape — they do post-lex value reconstruction (decimal/hex/real
//! parse, digit-separator handling, unary sign-fold). No I/O, no AST
//! construction; the caller wraps results into `Value`s + AST nodes.

const std = @import("std");
const value_mod = @import("value.zig");

const Value = value_mod.Value;

/// Decode one ASCII hex digit. Caller must ensure the byte was already
/// validated by the lexer (so we don't bother returning ?u8).
pub fn hexDigitValue(c: u8) u8 {
    if (c >= '0' and c <= '9') return c - '0';
    if (c >= 'a' and c <= 'f') return c - 'a' + 10;
    return c - 'A' + 10;
}

/// Parse a `.integer` token's text into a `Value`. Hex literals
/// (`0x`/`0X` prefix) are read as u64 and bit-cast to i64 — sqlite3
/// wraps the same way (`0xFFFFFFFFFFFFFFFF` → -1, `0x8000000000000000`
/// → LLONG_MIN). Decimal literals fit as i64 when in range; otherwise
/// they fall back to REAL (sqlite3 quirk: `9223372036854775808` →
/// `9.22337203685478e+18`). `std.fmt.parseInt` accepts embedded `_` as
/// a digit separator natively; the REAL fallback delegates to
/// `parseRealLiteral` which strips them before calling parseFloat.
pub fn parseIntegerLiteralAsValue(allocator: std.mem.Allocator, text: []const u8) !Value {
    if (text.len > 2 and text[0] == '0' and (text[1] == 'x' or text[1] == 'X')) {
        const u = try std.fmt.parseInt(u64, text[2..], 16);
        return Value{ .integer = @bitCast(u) };
    }
    if (std.fmt.parseInt(i64, text, 10)) |i| {
        return Value{ .integer = i };
    } else |err| switch (err) {
        error.Overflow => return Value{ .real = try parseRealLiteral(allocator, text) },
        else => return err,
    }
}

/// Returns `-<text>` parsed as i64 if the operand fits as a signed
/// negative i64 (i.e., the u64 magnitude ≤ 2^63). The boundary case
/// `2^63` maps to LLONG_MIN, the only i64 value whose positive form
/// overflows. Caller must not pass hex-prefixed text — sqlite3's
/// `-0x...` form has different semantics (see `parser.parseUnary`).
pub fn parseNegatedDecimalI64(text: []const u8) !i64 {
    const u = try std.fmt.parseInt(u64, text, 10);
    const llong_min_mag: u64 = @as(u64, 1) << 63;
    if (u > llong_min_mag) return error.Overflow;
    if (u == llong_min_mag) return std.math.minInt(i64);
    return -@as(i64, @intCast(u));
}

/// Parse a `.real` token's text into f64. `std.fmt.parseFloat` does not
/// strip digit-separator underscores, so we copy out a clean span when
/// `text` contains any. Allocation only fires for the rare separator
/// case; the hot path stays zero-alloc.
pub fn parseRealLiteral(allocator: std.mem.Allocator, text: []const u8) !f64 {
    if (std.mem.indexOfScalar(u8, text, '_') == null) {
        return std.fmt.parseFloat(f64, text);
    }
    const buf = try allocator.alloc(u8, text.len);
    defer allocator.free(buf);
    var w: usize = 0;
    for (text) |c| {
        if (c == '_') continue;
        buf[w] = c;
        w += 1;
    }
    return std.fmt.parseFloat(f64, buf[0..w]);
}
