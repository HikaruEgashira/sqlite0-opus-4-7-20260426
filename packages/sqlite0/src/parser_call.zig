//! Function-call and CASE-expression parsers extracted from `parser.zig`.
//!
//! Both rules consume bracketed sub-expressions that recurse into the
//! full precedence chain (`p.parseExpr()`), so they share the same
//! "drives the lexer through the host Parser" shape as
//! `parser_predicate.zig`. Splitting them out keeps `parser.zig`
//! under the 500-line discipline (CLAUDE.md "Module Splitting Rules")
//! ahead of the next parser-touching iteration that adds bitwise
//! operators.
//!
//! Ownership: each function returns a fully-built AST node owned by
//! the caller. Partial-build error paths free their interim
//! allocations through the `errdefer` chain so the parser never
//! leaks on a SyntaxError mid-rule.

const std = @import("std");
const ast = @import("ast.zig");
const ops = @import("ops.zig");
const parser = @import("parser.zig");

const Error = ops.Error;

/// Function-call body parser, called after the name and `(` have been
/// consumed by `parsePrimary`.
///
/// `name(*)` is the aggregate-`*` form (`count(*)`). It's modeled as a
/// 0-arg call because `count()`/`count(*)`/`count(1)` all behave
/// identically as "count rows" once aggregate dispatch sees a 0-arg or
/// always-truthy arg. The `*` is consumed silently — distinguishing it
/// from a true 0-arg call would require an AST shape we don't need.
pub fn parseFunctionCallTail(p: *parser.Parser, name: []const u8) Error!*ast.Expr {
    p.advance(); // consume lparen
    var args: std.ArrayList(*ast.Expr) = .empty;
    errdefer {
        for (args.items) |a| a.deinit(p.allocator);
        args.deinit(p.allocator);
    }
    // Optional DISTINCT modifier (sqlite3 allows this on any function call;
    // aggregate dispatch enforces the "exactly one argument" rule). `ALL`
    // is accepted as the explicit non-distinct counterpart to keep parity
    // with sqlite3 (`count(ALL x)`).
    var distinct = false;
    if (p.cur.kind == .keyword_distinct) {
        distinct = true;
        p.advance();
    } else if (p.cur.kind == .keyword_all) {
        p.advance();
    }
    if (p.cur.kind == .star) {
        p.advance();
    } else if (p.cur.kind != .rparen) {
        try args.ensureUnusedCapacity(p.allocator, 1);
        args.appendAssumeCapacity(try p.parseExpr());
        while (p.cur.kind == .comma) {
            p.advance();
            try args.ensureUnusedCapacity(p.allocator, 1);
            args.appendAssumeCapacity(try p.parseExpr());
        }
    }
    try p.expect(.rparen);
    const args_slice = try args.toOwnedSlice(p.allocator);
    return ast.makeFuncCall(p.allocator, name, args_slice, distinct) catch |err| {
        for (args_slice) |a| a.deinit(p.allocator);
        p.allocator.free(args_slice);
        return err;
    };
}

/// SQL CASE expression. Two forms:
///   simple:   CASE expr WHEN v1 THEN r1 ... [ELSE rd] END
///             — first WHEN whose value `=` the subject wins (truthy
///             check; NULL = NULL is NULL → false, so NULL never matches)
///   searched: CASE WHEN c1 THEN r1 ... [ELSE rd] END
///             — first WHEN whose condition is truthy wins
/// Returns NULL when no WHEN matches and no ELSE is provided.
pub fn parseCase(p: *parser.Parser) Error!*ast.Expr {
    p.advance(); // consume CASE

    var scrutinee: ?*ast.Expr = null;
    errdefer if (scrutinee) |s| s.deinit(p.allocator);

    if (p.cur.kind != .keyword_when) {
        scrutinee = try p.parseExpr();
    }

    var branches: std.ArrayList(ast.Expr.CaseBranch) = .empty;
    errdefer {
        for (branches.items) |b| {
            b.when.deinit(p.allocator);
            b.then.deinit(p.allocator);
        }
        branches.deinit(p.allocator);
    }

    if (p.cur.kind != .keyword_when) return Error.SyntaxError;

    while (p.cur.kind == .keyword_when) {
        p.advance();
        const when = try p.parseExpr();
        errdefer when.deinit(p.allocator);
        try p.expect(.keyword_then);
        const then_expr = try p.parseExpr();
        errdefer then_expr.deinit(p.allocator);
        try branches.ensureUnusedCapacity(p.allocator, 1);
        branches.appendAssumeCapacity(.{ .when = when, .then = then_expr });
    }

    var else_branch: ?*ast.Expr = null;
    errdefer if (else_branch) |eb| eb.deinit(p.allocator);
    if (p.cur.kind == .keyword_else) {
        p.advance();
        else_branch = try p.parseExpr();
    }
    try p.expect(.keyword_end);

    const branches_slice = try branches.toOwnedSlice(p.allocator);
    return ast.makeCaseExpr(p.allocator, scrutinee, branches_slice, else_branch) catch |err| {
        for (branches_slice) |b| {
            b.when.deinit(p.allocator);
            b.then.deinit(p.allocator);
        }
        p.allocator.free(branches_slice);
        return err;
    };
}
