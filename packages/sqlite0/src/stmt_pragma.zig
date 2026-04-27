//! PRAGMA statement parsing.
//!
//! Iter27.C wired only `PRAGMA wal_checkpoint [(MODE)]`. The grammar
//! `PRAGMA <name> [(arg)]` is wider — sqlite3 supports dozens of
//! pragmas — but adding the rest belongs to its own iteration. We
//! parse the generic shape so the surface is reusable; the engine
//! enforces that only known pragmas dispatch successfully.

const std = @import("std");
const ops = @import("ops.zig");
const parser_mod = @import("parser.zig");

const Parser = parser_mod.Parser;
const Error = ops.Error;

pub const ParsedPragma = struct {
    /// Borrowed slice into `Parser.src`. Compare via `func_util.eqlIgnoreCase`.
    name: []const u8,
    /// Optional `(IDENT)` argument (e.g. `PASSIVE` / `TRUNCATE`).
    /// Borrowed slice into `Parser.src`; null if no parens.
    arg: ?[]const u8,
};

pub fn parsePragmaStatement(p: *Parser) Error!ParsedPragma {
    try p.expect(.keyword_pragma);
    if (p.cur.kind != .identifier) return Error.SyntaxError;
    const name = p.cur.slice(p.src);
    p.advance();
    var arg: ?[]const u8 = null;
    if (p.cur.kind == .lparen) {
        p.advance();
        if (p.cur.kind != .identifier) return Error.SyntaxError;
        arg = p.cur.slice(p.src);
        p.advance();
        try p.expect(.rparen);
    }
    return .{ .name = name, .arg = arg };
}
