//! Top-level statement dispatch (`SELECT`, `VALUES`). Calls into the Parser
//! defined in `parser.zig` to build the expression AST, then `eval.evalExpr`
//! lowers each AST to a row of `Value`. Per ADR-0002 Iter8.C, this is the
//! consumer-side boundary that turns `*ast.Expr` back into the `[][]Value`
//! interface `exec.zig` expects.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const parser_mod = @import("parser.zig");
const eval = @import("eval.zig");

const Value = value_mod.Value;
const Error = ops.Error;
const Parser = parser_mod.Parser;

/// Parse a top-level statement and return its rows.
///
/// Supported forms:
///   - `SELECT <expr-list>`        — one row of expression values
///   - `VALUES (e, ...) [, (...)]` — N rows; all tuples must match arity
///
/// Caller owns the returned outer slice and each inner slice; free each
/// `Value` with `ops.freeValue` then free the slices.
pub fn parseStatement(allocator: std.mem.Allocator, sql: []const u8) ![][]Value {
    var p = Parser.init(allocator, sql);
    switch (p.cur.kind) {
        .keyword_select => return parseSelectStatement(&p),
        .keyword_values => return parseValuesStatement(&p),
        else => return Error.SyntaxError,
    }
}

/// Convenience wrapper retained for callers that only handle scalar SELECT.
/// Returns the first row, asserting that exactly one row was produced.
pub fn parseSelect(allocator: std.mem.Allocator, sql: []const u8) ![]Value {
    const rows = try parseStatement(allocator, sql);
    errdefer freeRows(allocator, rows);
    if (rows.len != 1) {
        freeRows(allocator, rows);
        return Error.SyntaxError;
    }
    const single = rows[0];
    allocator.free(rows);
    return single;
}

fn freeRows(allocator: std.mem.Allocator, rows: [][]Value) void {
    for (rows) |row| {
        for (row) |v| ops.freeValue(allocator, v);
        allocator.free(row);
    }
    allocator.free(rows);
}

fn parseSelectStatement(p: *Parser) ![][]Value {
    try p.expect(.keyword_select);
    const row = try parseExpressionList(p);
    errdefer {
        for (row) |v| ops.freeValue(p.allocator, v);
        p.allocator.free(row);
    }
    if (p.cur.kind == .semicolon) p.advance();
    if (p.cur.kind != .eof) return Error.SyntaxError;

    var rows = try p.allocator.alloc([]Value, 1);
    rows[0] = row;
    return rows;
}

/// `VALUES (e1, e2, ...) [, (e1, e2, ...)] ...` — every tuple must have the
/// same arity (sqlite raises "all VALUES must have the same number of terms"
/// at parse time; we mirror that with `Error.SyntaxError`).
fn parseValuesStatement(p: *Parser) ![][]Value {
    try p.expect(.keyword_values);

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

    if (p.cur.kind == .semicolon) p.advance();
    if (p.cur.kind != .eof) return Error.SyntaxError;

    return rows.toOwnedSlice(p.allocator);
}

fn parseValuesTuple(p: *Parser) ![]Value {
    try p.expect(.lparen);
    const row = try parseExpressionList(p);
    errdefer {
        for (row) |v| ops.freeValue(p.allocator, v);
        p.allocator.free(row);
    }
    try p.expect(.rparen);
    return row;
}

fn parseExpressionList(p: *Parser) ![]Value {
    var values: std.ArrayList(Value) = .empty;
    defer values.deinit(p.allocator);
    errdefer for (values.items) |v| ops.freeValue(p.allocator, v);

    try appendOneEvaluated(p, &values);
    while (p.cur.kind == .comma) {
        p.advance();
        try appendOneEvaluated(p, &values);
    }
    return values.toOwnedSlice(p.allocator);
}

/// Parse one expression, evaluate it against an empty row context, and
/// push the resulting `Value` onto `values`. The AST is freed before
/// return; allocation failure on `append` releases the just-evaluated
/// `Value` instead of leaking it.
fn appendOneEvaluated(p: *Parser, values: *std.ArrayList(Value)) Error!void {
    const expr = try p.parseExpr();
    defer expr.deinit(p.allocator);
    const v = try eval.evalExpr(.{ .allocator = p.allocator }, expr);
    values.append(p.allocator, v) catch |err| {
        ops.freeValue(p.allocator, v);
        return err;
    };
}

test "stmt: SELECT 1 returns one row" {
    const allocator = std.testing.allocator;
    const rows = try parseStatement(allocator, "SELECT 1");
    defer {
        for (rows) |row| {
            for (row) |v| ops.freeValue(allocator, v);
            allocator.free(row);
        }
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(i64, 1), rows[0][0].integer);
}

test "stmt: VALUES (1), (2) returns two rows" {
    const allocator = std.testing.allocator;
    const rows = try parseStatement(allocator, "VALUES (1), (2)");
    defer {
        for (rows) |row| {
            for (row) |v| ops.freeValue(allocator, v);
            allocator.free(row);
        }
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqual(@as(i64, 1), rows[0][0].integer);
    try std.testing.expectEqual(@as(i64, 2), rows[1][0].integer);
}

test "stmt: VALUES with mismatched arity is SyntaxError" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(Error.SyntaxError, parseStatement(allocator, "VALUES (1, 2), (3)"));
}

test "stmt: trailing token after SELECT is SyntaxError" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(Error.SyntaxError, parseStatement(allocator, "SELECT 1 garbage"));
}
