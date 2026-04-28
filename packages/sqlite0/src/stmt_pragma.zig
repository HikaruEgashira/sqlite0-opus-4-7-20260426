//! PRAGMA statement parsing.
//!
//! Iter27.C wired only `PRAGMA wal_checkpoint [(MODE)]`. Iter31.W added
//! several read-only PRAGMAs returning sqlite3 defaults. Iter31.Y
//! extends the grammar to `PRAGMA <name> [(arg) | = <int>]` so writers
//! like `PRAGMA user_version = 5` parse as the same `ParsedPragma`
//! shape and dispatch to the engine. String / ident values
//! (`= 'UTF-8'` / `= ON`) are not yet supported — sqlite3 silently
//! coerces those to 0 for integer-typed PRAGMAs but capturing that
//! quirk requires `func_util.parseIntStrict` plumbing into the parser.

const std = @import("std");
const ops = @import("ops.zig");
const parser_mod = @import("parser.zig");

const Parser = parser_mod.Parser;
const Error = ops.Error;

pub const ParsedPragma = struct {
    /// Borrowed slice into `Parser.src`. Compare via `func_util.eqlIgnoreCase`.
    name: []const u8,
    /// Optional `(IDENT)` argument (e.g. `PASSIVE` / `TRUNCATE`).
    /// Borrowed slice into `Parser.src`; null if no parens-style arg.
    arg: ?[]const u8 = null,
    /// Iter31.Y — optional integer set value. Captured from
    /// `PRAGMA name = N` and `PRAGMA name(N)` (negative literals
    /// supported). When present, `arg` is left null.
    int_value: ?i64 = null,
};

pub fn parsePragmaStatement(p: *Parser) Error!ParsedPragma {
    try p.expect(.keyword_pragma);
    if (p.cur.kind != .identifier) return Error.SyntaxError;
    const name = p.cur.slice(p.src);
    p.advance();
    if (p.cur.kind == .lparen) {
        p.advance();
        // Iter31.Y: `(N)` (with optional sign) sets `int_value`;
        // `(IDENT)` keeps the legacy `arg` field for wal_checkpoint.
        if (p.cur.kind == .integer or p.cur.kind == .minus) {
            const v = try parseSignedInt(p);
            try p.expect(.rparen);
            return .{ .name = name, .int_value = v };
        }
        if (p.cur.kind != .identifier) return Error.SyntaxError;
        const arg = p.cur.slice(p.src);
        p.advance();
        try p.expect(.rparen);
        return .{ .name = name, .arg = arg };
    }
    if (p.cur.kind == .eq) {
        p.advance();
        if (p.cur.kind != .integer and p.cur.kind != .minus) return Error.SyntaxError;
        const v = try parseSignedInt(p);
        return .{ .name = name, .int_value = v };
    }
    return .{ .name = name };
}

fn parseSignedInt(p: *Parser) Error!i64 {
    var negate = false;
    if (p.cur.kind == .minus) {
        negate = true;
        p.advance();
    }
    if (p.cur.kind != .integer) return Error.SyntaxError;
    const slice = p.cur.slice(p.src);
    p.advance();
    const positive = std.fmt.parseInt(i64, slice, 10) catch return Error.SyntaxError;
    return if (negate) -positive else positive;
}

