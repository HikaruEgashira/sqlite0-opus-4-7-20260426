//! `VALUES (e1, ...) [, (...)]` statement / body parsing. Split out of
//! `stmt.zig` to keep that file under the 500-line discipline (CLAUDE.md
//! "Module Splitting Rules") before Iter31.Z added CTE parsing. The
//! split point is "VALUES is a row-producer that doesn't share helpers
//! with SELECT" — the only call sites outside this module are
//! `engine.dispatchOne` (top-level VALUES), `stmt.parseInsertStatement`
//! (`INSERT INTO t VALUES ...`), and `stmt_from.parseFromClause`
//! (`FROM (VALUES ...)`).

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const ast = @import("ast.zig");
const parser_mod = @import("parser.zig");
const eval = @import("eval.zig");

const Value = value_mod.Value;
const Error = ops.Error;
const Parser = parser_mod.Parser;

/// `VALUES (e1, ...) [, (...)]` at the statement top level. Like
/// `parseSelectStatement`, leaves the cursor on `.semicolon` or `.eof`.
pub fn parseValuesStatement(p: *Parser) ![][]Value {
    try p.expect(.keyword_values);
    return parseValuesBody(p);
}

/// VALUES tuple list (after the `VALUES` keyword has been consumed). Used
/// both at the top level and inside a FROM subquery / INSERT body. Eagerly
/// evaluates each tuple with empty row context — VALUES cannot correlate
/// with outer columns in standard SQL, so this is correct.
pub fn parseValuesBody(p: *Parser) ![][]Value {
    var rows: std.ArrayList([]Value) = .empty;
    errdefer {
        for (rows.items) |row| {
            for (row) |v| ops.freeValue(p.allocator, v);
            p.allocator.free(row);
        }
        rows.deinit(p.allocator);
    }

    const first = try parseValuesTuple(p);
    try rows.append(p.allocator, first);
    const arity = first.len;

    while (p.cur.kind == .comma) {
        p.advance();
        const tuple = try parseValuesTuple(p);
        if (tuple.len != arity) {
            for (tuple) |v| ops.freeValue(p.allocator, v);
            p.allocator.free(tuple);
            return Error.SyntaxError;
        }
        try rows.append(p.allocator, tuple);
    }

    return rows.toOwnedSlice(p.allocator);
}

fn parseValuesTuple(p: *Parser) ![]Value {
    try p.expect(.lparen);
    const asts = try parseExpressionAsts(p);
    defer freeAsts(p.allocator, asts);
    try p.expect(.rparen);
    return evaluateRow(p.allocator, asts, &.{}, &.{}, p.db);
}

fn parseExpressionAsts(p: *Parser) ![]*ast.Expr {
    var asts: std.ArrayList(*ast.Expr) = .empty;
    errdefer {
        for (asts.items) |e| e.deinit(p.allocator);
        asts.deinit(p.allocator);
    }
    try asts.ensureUnusedCapacity(p.allocator, 1);
    asts.appendAssumeCapacity(try p.parseExpr());
    while (p.cur.kind == .comma) {
        p.advance();
        try asts.ensureUnusedCapacity(p.allocator, 1);
        asts.appendAssumeCapacity(try p.parseExpr());
    }
    return asts.toOwnedSlice(p.allocator);
}

fn evaluateRow(
    allocator: std.mem.Allocator,
    asts: []const *ast.Expr,
    current_row: []const Value,
    columns: []const []const u8,
    db: ?*@import("database.zig").Database,
) Error![]Value {
    const row = try allocator.alloc(Value, asts.len);
    var produced: usize = 0;
    errdefer {
        for (row[0..produced]) |v| ops.freeValue(allocator, v);
        allocator.free(row);
    }
    const ctx = eval.EvalContext{
        .allocator = allocator,
        .current_row = current_row,
        .columns = columns,
        .db = db,
    };
    for (asts) |expr| {
        row[produced] = try eval.evalExpr(ctx, expr);
        produced += 1;
    }
    return row;
}

fn freeAsts(allocator: std.mem.Allocator, asts: []const *ast.Expr) void {
    for (asts) |e| e.deinit(allocator);
    allocator.free(asts);
}
