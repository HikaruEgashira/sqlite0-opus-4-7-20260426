//! Unit tests for `lex.zig`. Split out so `lex.zig` itself stays under
//! the 500-line discipline — all assertions go through the public
//! `Lexer` / `TokenKind` API so this file has no special access.

const std = @import("std");
const lex = @import("lex.zig");

const Lexer = lex.Lexer;
const TokenKind = lex.TokenKind;

test "Lexer: SELECT integer" {
    var lx = Lexer.init("SELECT 42;");
    try std.testing.expectEqual(TokenKind.keyword_select, lx.next().kind);
    const num = lx.next();
    try std.testing.expectEqual(TokenKind.integer, num.kind);
    try std.testing.expectEqualStrings("42", num.slice("SELECT 42;"));
    try std.testing.expectEqual(TokenKind.semicolon, lx.next().kind);
    try std.testing.expectEqual(TokenKind.eof, lx.next().kind);
}

test "Lexer: arithmetic" {
    var lx = Lexer.init("1 + 2 * 3");
    try std.testing.expectEqual(TokenKind.integer, lx.next().kind);
    try std.testing.expectEqual(TokenKind.plus, lx.next().kind);
    try std.testing.expectEqual(TokenKind.integer, lx.next().kind);
    try std.testing.expectEqual(TokenKind.star, lx.next().kind);
    try std.testing.expectEqual(TokenKind.integer, lx.next().kind);
    try std.testing.expectEqual(TokenKind.eof, lx.next().kind);
}

test "Lexer: real number" {
    var lx = Lexer.init("3.14 1e10 2.5e-3");
    try std.testing.expectEqual(TokenKind.real, lx.next().kind);
    try std.testing.expectEqual(TokenKind.real, lx.next().kind);
    try std.testing.expectEqual(TokenKind.real, lx.next().kind);
}

test "Lexer: string literal with escaped quote" {
    var lx = Lexer.init("'it''s' 'plain'");
    const a = lx.next();
    try std.testing.expectEqual(TokenKind.string, a.kind);
    try std.testing.expectEqualStrings("'it''s'", a.slice("'it''s' 'plain'"));
    const b = lx.next();
    try std.testing.expectEqual(TokenKind.string, b.kind);
}

test "Lexer: comparison operators" {
    var lx = Lexer.init("= != <> <= >= < >");
    try std.testing.expectEqual(TokenKind.eq, lx.next().kind);
    try std.testing.expectEqual(TokenKind.ne, lx.next().kind);
    try std.testing.expectEqual(TokenKind.ne, lx.next().kind);
    try std.testing.expectEqual(TokenKind.le, lx.next().kind);
    try std.testing.expectEqual(TokenKind.ge, lx.next().kind);
    try std.testing.expectEqual(TokenKind.lt, lx.next().kind);
    try std.testing.expectEqual(TokenKind.gt, lx.next().kind);
}

test "Lexer: concat operator ||" {
    var lx = Lexer.init("'a' || 'b'");
    try std.testing.expectEqual(TokenKind.string, lx.next().kind);
    try std.testing.expectEqual(TokenKind.concat, lx.next().kind);
    try std.testing.expectEqual(TokenKind.string, lx.next().kind);
}

test "Lexer: line comment" {
    var lx = Lexer.init("SELECT -- comment here\n 1");
    try std.testing.expectEqual(TokenKind.keyword_select, lx.next().kind);
    try std.testing.expectEqual(TokenKind.integer, lx.next().kind);
}

test "Lexer: blob literal x'<hex>'" {
    var lx = Lexer.init("x'AABB' X'cafe' x''");
    const a = lx.next();
    try std.testing.expectEqual(TokenKind.blob_lit, a.kind);
    try std.testing.expectEqualStrings("x'AABB'", a.slice("x'AABB' X'cafe' x''"));
    const b = lx.next();
    try std.testing.expectEqual(TokenKind.blob_lit, b.kind);
    try std.testing.expectEqualStrings("X'cafe'", b.slice("x'AABB' X'cafe' x''"));
    const c = lx.next();
    try std.testing.expectEqual(TokenKind.blob_lit, c.kind);
    try std.testing.expectEqualStrings("x''", c.slice("x'AABB' X'cafe' x''"));
}

test "Lexer: bare x is identifier" {
    // The blob-literal trigger is `x` *immediately* followed by `'`. A
    // single-letter `x` column reference must keep parsing as an
    // identifier so existing schemas that name a column `x` still work.
    var lx = Lexer.init("x x_y X");
    const a = lx.next();
    try std.testing.expectEqual(TokenKind.identifier, a.kind);
    try std.testing.expectEqualStrings("x", a.slice("x x_y X"));
    const b = lx.next();
    try std.testing.expectEqual(TokenKind.identifier, b.kind);
    try std.testing.expectEqualStrings("x_y", b.slice("x x_y X"));
    const c = lx.next();
    try std.testing.expectEqual(TokenKind.identifier, c.kind);
}

test "Lexer: blob literal rejects odd-length / non-hex / unterminated" {
    // Odd-length hex span — sqlite3 rejects at prepare time.
    var lx_odd = Lexer.init("x'A'");
    try std.testing.expectEqual(TokenKind.invalid, lx_odd.next().kind);
    // Non-hex character.
    var lx_nonhex = Lexer.init("x'GG'");
    try std.testing.expectEqual(TokenKind.invalid, lx_nonhex.next().kind);
    // Unterminated.
    var lx_unterm = Lexer.init("x'AB");
    try std.testing.expectEqual(TokenKind.invalid, lx_unterm.next().kind);
}

test "Lexer: hex integer literal" {
    var lx = Lexer.init("0x10 0xa 0XFF 0x7FFFFFFFFFFFFFFF");
    const a = lx.next();
    try std.testing.expectEqual(TokenKind.integer, a.kind);
    try std.testing.expectEqualStrings("0x10", a.slice("0x10 0xa 0XFF 0x7FFFFFFFFFFFFFFF"));
    const b = lx.next();
    try std.testing.expectEqual(TokenKind.integer, b.kind);
    try std.testing.expectEqualStrings("0xa", b.slice("0x10 0xa 0XFF 0x7FFFFFFFFFFFFFFF"));
    const c = lx.next();
    try std.testing.expectEqual(TokenKind.integer, c.kind);
    try std.testing.expectEqualStrings("0XFF", c.slice("0x10 0xa 0XFF 0x7FFFFFFFFFFFFFFF"));
    const d = lx.next();
    try std.testing.expectEqual(TokenKind.integer, d.kind);
}

test "Lexer: hex literal rejects empty digits / trailing tail" {
    // `0x` with nothing after — sqlite3: "unrecognized token: 0x".
    var lx_empty = Lexer.init("0x");
    try std.testing.expectEqual(TokenKind.invalid, lx_empty.next().kind);
    // `0xg` — non-hex char immediately after the prefix.
    var lx_g = Lexer.init("0xg");
    try std.testing.expectEqual(TokenKind.invalid, lx_g.next().kind);
    // `0x10g` — valid hex span followed by a non-hex identifier char.
    // sqlite3 treats this as one bad token, not `0x10` + `g`.
    var lx_tail = Lexer.init("0x10g");
    const t = lx_tail.next();
    try std.testing.expectEqual(TokenKind.invalid, t.kind);
    try std.testing.expectEqualStrings("0x10g", t.slice("0x10g"));
}

test "Lexer: digit separator `_` accepted between digits" {
    // Decimal / hex / real all accept embedded `_` between two digits.
    var lx = Lexer.init("1_000_000 0xff_ff 1.5_5 1e1_0");
    const a = lx.next();
    try std.testing.expectEqual(TokenKind.integer, a.kind);
    try std.testing.expectEqualStrings("1_000_000", a.slice("1_000_000 0xff_ff 1.5_5 1e1_0"));
    const b = lx.next();
    try std.testing.expectEqual(TokenKind.integer, b.kind);
    try std.testing.expectEqualStrings("0xff_ff", b.slice("1_000_000 0xff_ff 1.5_5 1e1_0"));
    const c = lx.next();
    try std.testing.expectEqual(TokenKind.real, c.kind);
    try std.testing.expectEqualStrings("1.5_5", c.slice("1_000_000 0xff_ff 1.5_5 1e1_0"));
    const d = lx.next();
    try std.testing.expectEqual(TokenKind.real, d.kind);
    try std.testing.expectEqualStrings("1e1_0", d.slice("1_000_000 0xff_ff 1.5_5 1e1_0"));
}

test "Lexer: digit separator rejects edge cases" {
    // Trailing `_` makes the whole token bad (was the silent-accept bug).
    var lx_trail = Lexer.init("1_");
    const t1 = lx_trail.next();
    try std.testing.expectEqual(TokenKind.invalid, t1.kind);
    try std.testing.expectEqualStrings("1_", t1.slice("1_"));
    // Double `__` rejected.
    var lx_dub = Lexer.init("1__0");
    try std.testing.expectEqual(TokenKind.invalid, lx_dub.next().kind);
    // `_` adjacent to `.` rejected on either side.
    var lx_dot = Lexer.init("1_.5");
    try std.testing.expectEqual(TokenKind.invalid, lx_dot.next().kind);
    // `_` adjacent to `e` rejected.
    var lx_exp = Lexer.init("1e_10");
    try std.testing.expectEqual(TokenKind.invalid, lx_exp.next().kind);
    // `0x_ff` — `_` immediately after `0x` is rejected.
    var lx_hex = Lexer.init("0x_ff");
    try std.testing.expectEqual(TokenKind.invalid, lx_hex.next().kind);
    // `1e` with no exponent digits is bad.
    var lx_e = Lexer.init("1e");
    try std.testing.expectEqual(TokenKind.invalid, lx_e.next().kind);
    // Real with trailing identifier char (`1.5g` / `1e10g`) is one bad token.
    var lx_rg = Lexer.init("1.5g");
    try std.testing.expectEqual(TokenKind.invalid, lx_rg.next().kind);
}
