//! Top-level statement dispatch (`SELECT`, `VALUES`). Calls into the Parser
//! defined in `parser.zig` to build the expression AST, then `eval.evalExpr`
//! lowers each AST to a row of `Value`. Per ADR-0002 Iter8.C, this is the
//! consumer-side boundary that turns `*ast.Expr` back into the `[][]Value`
//! interface `exec.zig` expects. Iter8.D adds a FROM clause:
//! `SELECT <list> FROM (VALUES (...)) [AS] <alias>(c1, c2, ...)` evaluates
//! the SELECT list once per source row with `EvalContext.current_row` /
//! `.columns` populated.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const ast = @import("ast.zig");
const parser_mod = @import("parser.zig");
const eval = @import("eval.zig");

const Value = value_mod.Value;
const Error = ops.Error;
const Parser = parser_mod.Parser;

/// Parse a top-level statement and return its rows.
///
/// Supported forms:
///   - `SELECT <expr-list> [FROM (VALUES ...) AS alias(c1,...)]` — N rows
///     (1 when no FROM, otherwise one per source row)
///   - `VALUES (e, ...) [, (...)]`                              — N rows
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

    const select_asts = try parseExpressionAsts(p);
    defer freeAsts(p.allocator, select_asts);

    var source_owned = false;
    var source: FromSource = .{ .rows = &.{}, .columns = &.{} };
    defer if (source_owned) source.deinit(p.allocator);
    if (p.cur.kind == .keyword_from) {
        source = try parseFromClause(p);
        source_owned = true;
    }

    var where_ast: ?*ast.Expr = null;
    defer if (where_ast) |w| w.deinit(p.allocator);
    if (p.cur.kind == .keyword_where) {
        p.advance();
        where_ast = try p.parseExpr();
    }

    if (p.cur.kind == .semicolon) p.advance();
    if (p.cur.kind != .eof) return Error.SyntaxError;

    if (!source_owned) {
        return executeWithoutFrom(p.allocator, select_asts, where_ast);
    }
    return executeWithFrom(p.allocator, select_asts, &source, where_ast);
}

/// FROM-less SELECT, optionally filtered by WHERE against a synthetic empty
/// row. Matches sqlite3 (`SELECT 1 WHERE 0` returns no rows; `SELECT 1` and
/// `SELECT 1 WHERE 1` both return one row).
fn executeWithoutFrom(
    allocator: std.mem.Allocator,
    select_asts: []const *ast.Expr,
    where_ast: ?*ast.Expr,
) ![][]Value {
    if (!try evalWhereTruthy(allocator, where_ast, &.{}, &.{})) {
        return allocator.alloc([]Value, 0);
    }
    const row = try evaluateRow(allocator, select_asts, &.{}, &.{});
    errdefer {
        for (row) |v| ops.freeValue(allocator, v);
        allocator.free(row);
    }
    var rows = try allocator.alloc([]Value, 1);
    rows[0] = row;
    return rows;
}

/// SELECT with FROM clause. For each source row, optionally filter by
/// WHERE, then evaluate the SELECT list with that row bound.
fn executeWithFrom(
    allocator: std.mem.Allocator,
    select_asts: []const *ast.Expr,
    source: *const FromSource,
    where_ast: ?*ast.Expr,
) ![][]Value {
    var rows: std.ArrayList([]Value) = .empty;
    errdefer {
        for (rows.items) |row| {
            for (row) |v| ops.freeValue(allocator, v);
            allocator.free(row);
        }
        rows.deinit(allocator);
    }
    for (source.rows) |source_row| {
        if (!try evalWhereTruthy(allocator, where_ast, source_row, source.columns)) continue;
        const out_row = try evaluateRow(allocator, select_asts, source_row, source.columns);
        rows.append(allocator, out_row) catch |err| {
            for (out_row) |v| ops.freeValue(allocator, v);
            allocator.free(out_row);
            return err;
        };
    }
    return rows.toOwnedSlice(allocator);
}

/// Evaluate an optional WHERE predicate against a row. NULL is treated as
/// false (SQL three-valued logic: WHERE NULL filters the row out, same as
/// WHERE 0). Returns `true` when no predicate is present so callers can
/// branch uniformly.
fn evalWhereTruthy(
    allocator: std.mem.Allocator,
    where_ast: ?*ast.Expr,
    current_row: []const Value,
    columns: []const []const u8,
) !bool {
    const w = where_ast orelse return true;
    const ctx = eval.EvalContext{
        .allocator = allocator,
        .current_row = current_row,
        .columns = columns,
    };
    const cond = try eval.evalExpr(ctx, w);
    defer ops.freeValue(allocator, cond);
    return ops.truthy(cond) orelse false;
}

const FromSource = struct {
    rows: [][]Value,
    /// Synthesized `column1`, `column2`, ... — owned by this struct.
    columns: [][]const u8,

    fn deinit(self: *FromSource, allocator: std.mem.Allocator) void {
        for (self.rows) |row| {
            for (row) |v| ops.freeValue(allocator, v);
            allocator.free(row);
        }
        allocator.free(self.rows);
        for (self.columns) |name| allocator.free(name);
        allocator.free(self.columns);
    }
};

/// `FROM ( VALUES (...) [, (...) ...] ) [ [AS] <alias> ]`
///
/// The inner VALUES is evaluated eagerly with an empty row context (no
/// outer correlation in standard SQL). The optional alias is consumed but
/// not stored — Iter8.D only supports unqualified column references and
/// SQLite itself doesn't accept the `AS alias(col_list)` form here, so
/// columns are auto-named `column1`, `column2`, ... per tuple arity.
fn parseFromClause(p: *Parser) !FromSource {
    try p.expect(.keyword_from);
    try p.expect(.lparen);
    try p.expect(.keyword_values);

    const rows = try parseValuesBody(p);
    errdefer {
        for (rows) |row| {
            for (row) |v| ops.freeValue(p.allocator, v);
            p.allocator.free(row);
        }
        p.allocator.free(rows);
    }

    try p.expect(.rparen);

    if (p.cur.kind == .keyword_as) p.advance();
    if (p.cur.kind == .identifier) p.advance(); // alias is presentation-only

    const arity: usize = if (rows.len > 0) rows[0].len else 0;
    const columns = try synthesizeColumnNames(p.allocator, arity);
    return FromSource{ .rows = rows, .columns = columns };
}

/// Allocate `column1`, `column2`, ... `columnN` matching SQLite's
/// auto-naming for `(VALUES ...)` subqueries. Each name is a separate
/// allocation owned by `FromSource`; `FromSource.deinit` frees them.
fn synthesizeColumnNames(allocator: std.mem.Allocator, n: usize) ![][]const u8 {
    var names = try allocator.alloc([]const u8, n);
    var produced: usize = 0;
    errdefer {
        for (names[0..produced]) |name| allocator.free(name);
        allocator.free(names);
    }
    while (produced < n) : (produced += 1) {
        names[produced] = try std.fmt.allocPrint(allocator, "column{d}", .{produced + 1});
    }
    return names;
}

/// `VALUES (e1, e2, ...) [, (e1, e2, ...)] ...` — every tuple must have the
/// same arity (sqlite raises "all VALUES must have the same number of terms"
/// at parse time; we mirror that with `Error.SyntaxError`).
fn parseValuesStatement(p: *Parser) ![][]Value {
    try p.expect(.keyword_values);
    const rows = try parseValuesBody(p);
    errdefer {
        for (rows) |row| {
            for (row) |v| ops.freeValue(p.allocator, v);
            p.allocator.free(row);
        }
        p.allocator.free(rows);
    }

    if (p.cur.kind == .semicolon) p.advance();
    if (p.cur.kind != .eof) return Error.SyntaxError;

    return rows;
}

/// VALUES tuple list (after the `VALUES` keyword has been consumed).
/// Used both at the top level (`parseValuesStatement`) and inside a FROM
/// subquery (`parseFromClause`). Eagerly evaluates each tuple with empty
/// row context — VALUES inside FROM cannot correlate with outer columns
/// in standard SQL, so this is correct.
fn parseValuesBody(p: *Parser) ![][]Value {
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
    return evaluateRow(p.allocator, asts, &.{}, &.{});
}

/// Parse a comma-separated expression list as ASTs (no evaluation).
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

/// Evaluate `asts` with the given `current_row` and `columns` to produce a
/// single result row. On failure all already-evaluated Values are freed.
fn evaluateRow(
    allocator: std.mem.Allocator,
    asts: []const *ast.Expr,
    current_row: []const Value,
    columns: []const []const u8,
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

test "stmt: SELECT 1 returns one row" {
    const allocator = std.testing.allocator;
    const rows = try parseStatement(allocator, "SELECT 1");
    defer freeRows(allocator, rows);
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(i64, 1), rows[0][0].integer);
}

test "stmt: VALUES (1), (2) returns two rows" {
    const allocator = std.testing.allocator;
    const rows = try parseStatement(allocator, "VALUES (1), (2)");
    defer freeRows(allocator, rows);
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

test "stmt: SELECT column1 FROM (VALUES (1), (2), (3))" {
    const allocator = std.testing.allocator;
    const rows = try parseStatement(allocator, "SELECT column1 FROM (VALUES (1), (2), (3))");
    defer freeRows(allocator, rows);
    try std.testing.expectEqual(@as(usize, 3), rows.len);
    try std.testing.expectEqual(@as(i64, 1), rows[0][0].integer);
    try std.testing.expectEqual(@as(i64, 2), rows[1][0].integer);
    try std.testing.expectEqual(@as(i64, 3), rows[2][0].integer);
}

test "stmt: SELECT column1+1 FROM (VALUES (10), (20))" {
    const allocator = std.testing.allocator;
    const rows = try parseStatement(allocator, "SELECT column1+1 FROM (VALUES (10), (20))");
    defer freeRows(allocator, rows);
    try std.testing.expectEqual(@as(i64, 11), rows[0][0].integer);
    try std.testing.expectEqual(@as(i64, 21), rows[1][0].integer);
}

test "stmt: column ref is case-insensitive" {
    const allocator = std.testing.allocator;
    const rows = try parseStatement(allocator, "SELECT COLUMN1 FROM (VALUES (7))");
    defer freeRows(allocator, rows);
    try std.testing.expectEqual(@as(i64, 7), rows[0][0].integer);
}

test "stmt: optional AS alias is consumed" {
    const allocator = std.testing.allocator;
    const rows = try parseStatement(allocator, "SELECT column1 FROM (VALUES (1)) AS v");
    defer freeRows(allocator, rows);
    try std.testing.expectEqual(@as(i64, 1), rows[0][0].integer);
}

test "stmt: alias without AS keyword" {
    const allocator = std.testing.allocator;
    const rows = try parseStatement(allocator, "SELECT column1 FROM (VALUES (1)) v");
    defer freeRows(allocator, rows);
    try std.testing.expectEqual(@as(i64, 1), rows[0][0].integer);
}

test "stmt: unknown column is SyntaxError" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(Error.SyntaxError, parseStatement(allocator, "SELECT y FROM (VALUES (1))"));
}

test "stmt: multiple columns SELECT column1, column2 FROM (VALUES (1, 'a'), (2, 'b'))" {
    const allocator = std.testing.allocator;
    const rows = try parseStatement(allocator, "SELECT column2, column1 FROM (VALUES (1, 'a'), (2, 'b'))");
    defer freeRows(allocator, rows);
    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqualStrings("a", rows[0][0].text);
    try std.testing.expectEqual(@as(i64, 1), rows[0][1].integer);
    try std.testing.expectEqualStrings("b", rows[1][0].text);
}

test "stmt: WHERE filters rows" {
    const allocator = std.testing.allocator;
    const rows = try parseStatement(allocator, "SELECT column1 FROM (VALUES (1), (2), (3)) WHERE column1 > 1");
    defer freeRows(allocator, rows);
    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqual(@as(i64, 2), rows[0][0].integer);
    try std.testing.expectEqual(@as(i64, 3), rows[1][0].integer);
}

test "stmt: WHERE NULL filters out (three-valued logic)" {
    const allocator = std.testing.allocator;
    const rows = try parseStatement(allocator, "SELECT column1 FROM (VALUES (1), (NULL), (3)) WHERE column1 > 1");
    defer freeRows(allocator, rows);
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(i64, 3), rows[0][0].integer);
}

test "stmt: SELECT 1 WHERE 0 returns 0 rows" {
    const allocator = std.testing.allocator;
    const rows = try parseStatement(allocator, "SELECT 1 WHERE 0");
    defer freeRows(allocator, rows);
    try std.testing.expectEqual(@as(usize, 0), rows.len);
}

test "stmt: SELECT 1 WHERE 1 returns 1 row" {
    const allocator = std.testing.allocator;
    const rows = try parseStatement(allocator, "SELECT 1 WHERE 1");
    defer freeRows(allocator, rows);
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(i64, 1), rows[0][0].integer);
}
