//! Statement parsing. Each `parse*Statement` function consumes one statement
//! starting from the parser's current cursor and returns a `Parsed*` struct
//! describing what was found — execution is the caller's responsibility
//! (`database.zig` Database.dispatchOne). On exit the cursor sits on
//! `.semicolon` or `.eof`.
//!
//! VALUES inside FROM is eagerly evaluated at parse time because it cannot
//! correlate with outer scope; the resulting rows are arena-allocated and
//! freed with the per-statement arena.
//!
//! Integration tests for execution-level behavior live in `database.zig` and
//! `tests/differential/cases.txt`; this file deliberately contains no tests
//! that exercise full execution paths.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const ast = @import("ast.zig");
const parser_mod = @import("parser.zig");
const eval = @import("eval.zig");
const select = @import("select.zig");

const Value = value_mod.Value;
const Error = ops.Error;
const Parser = parser_mod.Parser;

// ── SELECT ──────────────────────────────────────────────────────────────────

pub const ParsedSelect = struct {
    items: []select.SelectItem,
    /// FROM terms, in clause order. Empty slice = no FROM. Each term is a
    /// source plus the optional ON predicate from the JOIN keyword that
    /// introduced it. Multiple terms produce the Cartesian product at
    /// execute time, with ON predicates AND-folded into the row filter.
    from: []FromTerm = &.{},
    where: ?*ast.Expr,
    distinct: bool = false,
    /// `GROUP BY e1 [, e2 ...]` — one ASTs per group key. Empty slice when
    /// the clause is absent. The aggregate execution path treats empty
    /// group_by as "implicit single group" only when at least one aggregate
    /// call is present in items/having/order_by.
    group_by: []*ast.Expr = &.{},
    /// `HAVING <expr>` filter applied after grouping. May reference
    /// aggregates not in the SELECT list (sqlite3 allows this). Null when
    /// absent.
    having: ?*ast.Expr = null,
    order_by: []OrderTerm = &.{},
    limit: ?*ast.Expr = null,
    offset: ?*ast.Expr = null,
};

pub const OrderDirection = enum { asc, desc };

pub const OrderTerm = struct {
    /// When `position` is non-null, the ORDER BY term is a 1-based column
    /// position into the SELECT list (sqlite3 quirk: `ORDER BY 2` sorts by
    /// the second projected column). When position is null, evaluate `expr`
    /// against the source row.
    expr: *ast.Expr,
    position: ?usize = null,
    dir: OrderDirection,
};

// FROM clause — types and parsing live in stmt_from.zig (split out to
// keep this file under the 500-line discipline). Re-exported here so
// existing call sites keep their `stmt.ParsedFromSource` reference working.
const stmt_from = @import("stmt_from.zig");
pub const ParsedFromSource = stmt_from.ParsedFromSource;
pub const FromTerm = stmt_from.FromTerm;
pub const JoinKind = stmt_from.JoinKind;
pub const freeParsedFrom = stmt_from.freeParsedFrom;
pub const freeFromList = stmt_from.freeFromList;

pub fn parseSelectStatement(p: *Parser) !ParsedSelect {
    try p.expect(.keyword_select);

    var distinct = false;
    if (p.cur.kind == .keyword_distinct) {
        distinct = true;
        p.advance();
    }

    const items = try select.parseSelectList(p);
    errdefer select.freeSelectList(p.allocator, items);

    var from: []FromTerm = &.{};
    errdefer freeFromList(p.allocator, from);
    if (p.cur.kind == .keyword_from) {
        from = try stmt_from.parseFromClause(p);
    }

    var where_ast: ?*ast.Expr = null;
    errdefer if (where_ast) |w| w.deinit(p.allocator);
    if (p.cur.kind == .keyword_where) {
        p.advance();
        where_ast = try p.parseExpr();
    }

    var group_by: []*ast.Expr = &.{};
    errdefer freeExprList(p.allocator, group_by);
    if (p.cur.kind == .keyword_group) {
        p.advance();
        try p.expect(.keyword_by);
        group_by = try parseExprList(p);
    }

    var having_ast: ?*ast.Expr = null;
    errdefer if (having_ast) |h| h.deinit(p.allocator);
    if (p.cur.kind == .keyword_having) {
        p.advance();
        having_ast = try p.parseExpr();
    }

    var order_by: []OrderTerm = &.{};
    errdefer freeOrderTerms(p.allocator, order_by);
    if (p.cur.kind == .keyword_order) {
        order_by = try parseOrderBy(p);
    }

    var limit_ast: ?*ast.Expr = null;
    errdefer if (limit_ast) |e| e.deinit(p.allocator);
    var offset_ast: ?*ast.Expr = null;
    errdefer if (offset_ast) |e| e.deinit(p.allocator);
    if (p.cur.kind == .keyword_limit) {
        p.advance();
        limit_ast = try p.parseExpr();
        // sqlite3 quirk: `LIMIT a, b` means "skip a, take b" — the comma
        // form flips arg order. The lhs (already parsed into limit_ast) is
        // actually the offset; the rhs is the row count.
        if (p.cur.kind == .comma) {
            p.advance();
            offset_ast = limit_ast;
            limit_ast = try p.parseExpr();
        } else if (p.cur.kind == .keyword_offset) {
            p.advance();
            offset_ast = try p.parseExpr();
        }
    }

    return .{
        .items = items,
        .from = from,
        .where = where_ast,
        .distinct = distinct,
        .group_by = group_by,
        .having = having_ast,
        .order_by = order_by,
        .limit = limit_ast,
        .offset = offset_ast,
    };
}

/// Parse `<expr> [, <expr>]*`. Used by GROUP BY (and trivially extensible
/// to other comma-list contexts that don't need extra per-term metadata).
fn parseExprList(p: *Parser) ![]*ast.Expr {
    var list: std.ArrayList(*ast.Expr) = .empty;
    errdefer {
        for (list.items) |e| e.deinit(p.allocator);
        list.deinit(p.allocator);
    }
    try list.append(p.allocator, try p.parseExpr());
    while (p.cur.kind == .comma) {
        p.advance();
        try list.append(p.allocator, try p.parseExpr());
    }
    return list.toOwnedSlice(p.allocator);
}

pub fn freeExprList(allocator: std.mem.Allocator, list: []*ast.Expr) void {
    for (list) |e| e.deinit(allocator);
    allocator.free(list);
}

fn parseOrderBy(p: *Parser) ![]OrderTerm {
    try p.expect(.keyword_order);
    try p.expect(.keyword_by);
    var terms: std.ArrayList(OrderTerm) = .empty;
    errdefer freeOrderTerms(p.allocator, terms.items);
    errdefer terms.deinit(p.allocator);
    try parseOrderTerm(p, &terms);
    while (p.cur.kind == .comma) {
        p.advance();
        try parseOrderTerm(p, &terms);
    }
    return terms.toOwnedSlice(p.allocator);
}

fn parseOrderTerm(p: *Parser, terms: *std.ArrayList(OrderTerm)) !void {
    const expr = try p.parseExpr();
    // sqlite3 quirk: a bare integer literal in ORDER BY refers to the
    // SELECT-list column position (1-based), not its evaluated value. Detect
    // that case here so the engine can look up `projected_row[N-1]` at sort
    // time. Any non-literal expression (including `1+0`) keeps expression
    // semantics — sqlite3 also distinguishes "literal vs. expression".
    var position: ?usize = null;
    if (expr.* == .literal) {
        switch (expr.*.literal) {
            .integer => |n| if (n > 0) {
                position = @intCast(n);
            },
            else => {},
        }
    }
    var dir: OrderDirection = .asc;
    if (p.cur.kind == .keyword_asc) {
        p.advance();
    } else if (p.cur.kind == .keyword_desc) {
        dir = .desc;
        p.advance();
    }
    try terms.append(p.allocator, .{ .expr = expr, .position = position, .dir = dir });
}

pub fn freeOrderTerms(allocator: std.mem.Allocator, terms: []OrderTerm) void {
    for (terms) |t| t.expr.deinit(allocator);
    allocator.free(terms);
}

// ── VALUES ──────────────────────────────────────────────────────────────────

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

// ── CREATE TABLE ────────────────────────────────────────────────────────────
// Implementation lives in stmt_ddl.zig; re-exported here so existing call
// sites keep their `stmt.parseCreateTableStatement` reference working.

const stmt_ddl = @import("stmt_ddl.zig");
pub const ParsedCreateTable = stmt_ddl.ParsedCreateTable;
pub const parseCreateTableStatement = stmt_ddl.parseCreateTableStatement;

// ── INSERT ──────────────────────────────────────────────────────────────────

/// `INSERT INTO <name> [(c1, c2, ...)] (VALUES (...) [, (...)] | SELECT ...)`
///
/// `columns == null` means "all table columns in declaration order" (the
/// default). When non-null, each name borrows from `Parser.src` and the
/// outer slice lives in the per-statement arena.
///
/// `source` holds either eagerly-evaluated VALUES rows or an unevaluated
/// `ParsedSelect`; the engine resolves the SELECT path against `Database`
/// state at execute time.
pub const ParsedInsert = struct {
    table: []const u8,
    columns: ?[][]const u8,
    source: Source,

    pub const Source = union(enum) {
        values: [][]Value,
        select: ParsedSelect,
    };
};

pub fn parseInsertStatement(p: *Parser) !ParsedInsert {
    try p.expect(.keyword_insert);
    try p.expect(.keyword_into);
    if (p.cur.kind != .identifier) return Error.SyntaxError;
    const table = p.cur.slice(p.src);
    p.advance();

    var columns: ?[][]const u8 = null;
    errdefer if (columns) |cs| p.allocator.free(cs);
    if (p.cur.kind == .lparen) {
        p.advance();
        columns = try parseInsertColumnList(p);
        try p.expect(.rparen);
    }

    switch (p.cur.kind) {
        .keyword_values => {
            p.advance();
            const rows = try parseValuesBody(p);
            return .{ .table = table, .columns = columns, .source = .{ .values = rows } };
        },
        .keyword_select => {
            const ps = try parseSelectStatement(p);
            return .{ .table = table, .columns = columns, .source = .{ .select = ps } };
        },
        else => return Error.SyntaxError,
    }
}

fn parseInsertColumnList(p: *Parser) ![][]const u8 {
    var names: std.ArrayList([]const u8) = .empty;
    errdefer names.deinit(p.allocator);
    if (p.cur.kind != .identifier) return Error.SyntaxError;
    try names.append(p.allocator, p.cur.slice(p.src));
    p.advance();
    while (p.cur.kind == .comma) {
        p.advance();
        if (p.cur.kind != .identifier) return Error.SyntaxError;
        try names.append(p.allocator, p.cur.slice(p.src));
        p.advance();
    }
    return names.toOwnedSlice(p.allocator);
}
