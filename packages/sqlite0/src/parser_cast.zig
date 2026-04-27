//! `CAST(<expr> AS <type-name>)` parser helper (Iter23).
//!
//! Split out of `parser.zig` so that file can stay under the 500-line
//! discipline (CLAUDE.md "Module Splitting Rules"). The split point is
//! "after the CAST keyword is recognized": the Parser dispatches into
//! this module, which consumes the `(`, expression, `AS`, type-name,
//! and `)`, and emits an `ast.Expr.cast` node.
//!
//! Type-name affinity follows sqlite3's substring rules (CREATE TABLE
//! column-type / CAST target use the same algorithm):
//!   - contains "INT"                       → integer
//!   - contains "CHAR" / "CLOB" / "TEXT"    → text
//!   - contains "BLOB"                      → blob
//!   - contains "REAL" / "FLOA" / "DOUB"    → real
//!   - otherwise                            → numeric
//!
//! When the type-name is a multi-word phrase (`DOUBLE PRECISION`), each
//! identifier is classified independently and the first non-numeric class
//! wins. Trailing `(N)` / `(N, M)` parens are silently consumed — sqlite3
//! ignores precision/scale on CAST as well.

const std = @import("std");
const ast = @import("ast.zig");
const ops = @import("ops.zig");
const lex = @import("lex.zig");

const Error = ops.Error;

/// Parse the body of a CAST expression. The Parser must have already
/// advanced past the `CAST` keyword. On return, `cur` is positioned just
/// after the closing `)`.
pub fn parseCastBody(p: anytype) Error!*ast.Expr {
    try p.expect(.lparen);
    const inner = try p.parseExpr();
    errdefer inner.deinit(p.allocator);
    try p.expect(.keyword_as);
    const affinity = try parseAffinityName(p);
    try p.expect(.rparen);
    return ast.makeCast(p.allocator, inner, affinity);
}

/// Consume one or more identifier tokens forming a type name and infer
/// sqlite3 affinity from the joined text. Optional `(N)` / `(N, M)` after
/// the name is silently consumed.
fn parseAffinityName(p: anytype) Error!ast.Expr.Affinity {
    if (p.cur.kind != .identifier) return Error.SyntaxError;
    const first = p.cur.slice(p.src);
    var cls = classifyAffinity(first);
    p.advance();
    // sqlite3 spells `DOUBLE PRECISION` as two identifiers. Eagerly consume
    // any further identifier and re-classify with the joined text —
    // `PRECISION` / `VARYING` etc. don't change affinity but need to be
    // eaten so they don't bleed into the next token.
    while (p.cur.kind == .identifier) {
        const extra = p.cur.slice(p.src);
        p.advance();
        cls = combineAffinity(cls, classifyAffinity(extra));
    }
    if (p.cur.kind == .lparen) {
        p.advance();
        // Eat anything until the matching ')'. We don't validate contents
        // — sqlite3 accepts pretty much anything inside (`VARCHAR(N OF M)`
        // parses but the parens are ignored).
        var depth: usize = 1;
        while (depth > 0) {
            switch (p.cur.kind) {
                .lparen => depth += 1,
                .rparen => depth -= 1,
                .eof => return Error.SyntaxError,
                else => {},
            }
            p.advance();
        }
    }
    return cls;
}

/// Classify a single identifier into an Affinity using sqlite3's substring
/// rules. Match is case-insensitive on the substring.
fn classifyAffinity(name: []const u8) ast.Expr.Affinity {
    if (containsIgnoreCase(name, "INT")) return .integer;
    if (containsIgnoreCase(name, "CHAR") or
        containsIgnoreCase(name, "CLOB") or
        containsIgnoreCase(name, "TEXT")) return .text;
    if (containsIgnoreCase(name, "BLOB")) return .blob;
    if (containsIgnoreCase(name, "REAL") or
        containsIgnoreCase(name, "FLOA") or
        containsIgnoreCase(name, "DOUB")) return .real;
    return .numeric;
}

fn combineAffinity(a: ast.Expr.Affinity, b: ast.Expr.Affinity) ast.Expr.Affinity {
    // sqlite3 walks the joined type-name in order and returns the first
    // matching rule (INT > CHAR/CLOB/TEXT > BLOB > REAL/FLOA/DOUB > else).
    // Earlier-classified `a` already encodes the leading rule; `b` only
    // overrides when `a` was the catch-all (numeric) and `b` is concrete.
    return if (a == .numeric) b else a;
}

fn containsIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    if (needle.len > haystack.len) return false;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        var match = true;
        for (needle, 0..) |nc, j| {
            if (std.ascii.toLower(haystack[i + j]) != std.ascii.toLower(nc)) {
                match = false;
                break;
            }
        }
        if (match) return true;
    }
    return false;
}
