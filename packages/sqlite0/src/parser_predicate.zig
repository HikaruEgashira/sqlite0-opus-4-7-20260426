//! Postfix predicate parsers extracted from `parser.zig`.
//!
//! Split out so parser.zig stays under the 500-line discipline (CLAUDE.md
//! "Module Splitting Rules") as Iter22.C grows the predicate set with
//! `IN (SELECT ...)` and `EXISTS (SELECT ...)`. The cluster is cohesive:
//! every function here parses a postfix membership/match operator that
//! takes a left-hand expression already parsed by the precedence chain.
//!
//! Each function takes `*parser.Parser` so it can drive the lexer and
//! recurse back into the parser's expression rules. Ownership rules
//! mirror the originals: on any error the input `value` is freed via
//! `errdefer` and partial buffers are reclaimed.

const std = @import("std");
const ast = @import("ast.zig");
const ops = @import("ops.zig");
const parser = @import("parser.zig");

const Error = ops.Error;

/// `value BETWEEN lo AND hi` — takes ownership of `value` and frees it
/// on any error before returning.
pub fn parseBetween(p: *parser.Parser, value: *ast.Expr, negated: bool) Error!*ast.Expr {
    errdefer value.deinit(p.allocator);
    const lo = try p.parseComparison();
    errdefer lo.deinit(p.allocator);
    try p.expect(.keyword_and);
    const hi = try p.parseComparison();
    errdefer hi.deinit(p.allocator);
    return ast.makeBetween(p.allocator, value, lo, hi, negated);
}

/// `value LIKE pattern [ESCAPE <expr>]` / `value GLOB pattern`. ESCAPE
/// is only valid after LIKE; following GLOB it is reported as a syntax
/// error to match sqlite3 ("near \"ESCAPE\": syntax error"). Takes
/// ownership of `value` and frees it on any error before returning.
pub fn parseLike(p: *parser.Parser, value: *ast.Expr, op: ast.LikeOp, negated: bool) Error!*ast.Expr {
    errdefer value.deinit(p.allocator);
    const pattern = try p.parseAddSub();
    errdefer pattern.deinit(p.allocator);
    var escape: ?*ast.Expr = null;
    errdefer if (escape) |e| e.deinit(p.allocator);
    if (p.cur.kind == .keyword_escape) {
        if (op != .like) return Error.SyntaxError;
        p.advance();
        escape = try p.parseAddSub();
    }
    return ast.makeLike(p.allocator, op, value, pattern, escape, negated);
}

/// `value IN (...)` — dispatches between the value-list form and the
/// `IN (SELECT ...)` subquery form by peeking the first token after `(`.
/// Takes ownership of `value` and frees it on any error before returning.
pub fn parseInList(p: *parser.Parser, value: *ast.Expr, negated: bool) Error!*ast.Expr {
    errdefer value.deinit(p.allocator);
    try p.expect(.lparen);
    if (p.cur.kind == .keyword_select) {
        const stmt_mod = @import("stmt.zig");
        const ps = try stmt_mod.parseSelectStatement(p);
        errdefer stmt_mod.freeParsedSelectFields(p.allocator, ps);
        try p.expect(.rparen);
        return ast.makeInSubquery(p.allocator, value, ps, negated);
    }
    var items: std.ArrayList(*ast.Expr) = .empty;
    errdefer {
        for (items.items) |it| it.deinit(p.allocator);
        items.deinit(p.allocator);
    }
    if (p.cur.kind != .rparen) {
        try items.ensureUnusedCapacity(p.allocator, 1);
        items.appendAssumeCapacity(try p.parseExpr());
        while (p.cur.kind == .comma) {
            p.advance();
            try items.ensureUnusedCapacity(p.allocator, 1);
            items.appendAssumeCapacity(try p.parseExpr());
        }
    }
    try p.expect(.rparen);
    const items_slice = try items.toOwnedSlice(p.allocator);
    return ast.makeInList(p.allocator, value, items_slice, negated) catch |err| {
        for (items_slice) |it| it.deinit(p.allocator);
        p.allocator.free(items_slice);
        return err;
    };
}

/// `EXISTS (SELECT ...)` — Iter22.C primary-position predicate. The
/// `EXISTS` keyword has already been consumed by `parsePrimary`. Unlike
/// scalar/IN subqueries, EXISTS does not constrain column count. The
/// `NOT EXISTS` form falls out naturally: the leading `NOT` is consumed
/// by `parseNot` which wraps the result in `logical_not`.
pub fn parseExists(p: *parser.Parser) Error!*ast.Expr {
    try p.expect(.lparen);
    if (p.cur.kind != .keyword_select) return Error.SyntaxError;
    const stmt_mod = @import("stmt.zig");
    const ps = try stmt_mod.parseSelectStatement(p);
    errdefer stmt_mod.freeParsedSelectFields(p.allocator, ps);
    try p.expect(.rparen);
    return ast.makeExists(p.allocator, ps);
}
