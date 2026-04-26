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
    from: ?ParsedFrom,
    where: ?*ast.Expr,
    distinct: bool = false,
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

pub const ParsedFrom = union(enum) {
    /// `FROM (VALUES ...)` — rows already evaluated at parse time, columns
    /// auto-named `column1, column2, ...`. All allocations are in
    /// `Parser.allocator` (per-statement arena).
    inline_values: struct {
        rows: [][]Value,
        columns: [][]const u8,
    },
    /// `FROM <identifier>` — name borrows from `Parser.src`. Caller resolves
    /// against `Database.tables` at execute time. Optional alias is consumed
    /// but not stored (Iter14.D will track it).
    table_ref: []const u8,
};

pub fn parseSelectStatement(p: *Parser) !ParsedSelect {
    try p.expect(.keyword_select);

    var distinct = false;
    if (p.cur.kind == .keyword_distinct) {
        distinct = true;
        p.advance();
    }

    const items = try select.parseSelectList(p);
    errdefer select.freeSelectList(p.allocator, items);

    var from: ?ParsedFrom = null;
    errdefer if (from) |f| freeParsedFrom(p.allocator, f);
    if (p.cur.kind == .keyword_from) {
        from = try parseFromClause(p);
    }

    var where_ast: ?*ast.Expr = null;
    errdefer if (where_ast) |w| w.deinit(p.allocator);
    if (p.cur.kind == .keyword_where) {
        p.advance();
        where_ast = try p.parseExpr();
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
        .order_by = order_by,
        .limit = limit_ast,
        .offset = offset_ast,
    };
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

/// Free arena-owned content of a `ParsedFrom`. Safe to call on either variant
/// — `table_ref` borrows from `Parser.src` and owns nothing.
pub fn freeParsedFrom(allocator: std.mem.Allocator, from: ParsedFrom) void {
    switch (from) {
        .inline_values => |iv| {
            for (iv.rows) |row| {
                for (row) |v| ops.freeValue(allocator, v);
                allocator.free(row);
            }
            allocator.free(iv.rows);
            for (iv.columns) |c| allocator.free(c);
            allocator.free(iv.columns);
        },
        .table_ref => {},
    }
}

fn parseFromClause(p: *Parser) !ParsedFrom {
    try p.expect(.keyword_from);
    if (p.cur.kind == .lparen) {
        p.advance();
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
        return .{ .inline_values = .{ .rows = rows, .columns = columns } };
    }
    if (p.cur.kind == .identifier) {
        const name = p.cur.slice(p.src);
        p.advance();
        if (p.cur.kind == .keyword_as) p.advance();
        if (p.cur.kind == .identifier) p.advance(); // alias (consumed)
        return .{ .table_ref = name };
    }
    return Error.SyntaxError;
}

/// Allocate `column1`, `column2`, ... `columnN` matching SQLite's auto-naming
/// for `(VALUES ...)` subqueries.
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

/// All slices borrow from `Parser.src`, which the caller (`Database.execute`)
/// holds for the entire `execute` call; the outer slice is in
/// `Parser.allocator` (per-statement arena). The caller dupes `name` and
/// `columns[*]` to long-lived memory before storing them.
pub const ParsedCreateTable = struct {
    name: []const u8,
    columns: [][]const u8,
};

/// `CREATE TABLE <name> ( <col-def> [, <col-def> ...] )`
///
/// Each col-def is a column name optionally followed by a type-name and any
/// column-constraints; the type and constraints are consumed and discarded
/// (Iter14.B). SQLite3 dynamic typing means the absence of constraint
/// enforcement is observationally equivalent for Phase 2.
pub fn parseCreateTableStatement(p: *Parser) !ParsedCreateTable {
    try p.expect(.keyword_create);
    try p.expect(.keyword_table);

    if (p.cur.kind != .identifier) return Error.SyntaxError;
    const name = p.cur.slice(p.src);
    p.advance();

    try p.expect(.lparen);

    var columns: std.ArrayList([]const u8) = .empty;
    errdefer columns.deinit(p.allocator);

    while (true) {
        const col_name = try parseColumnDef(p);
        try columns.append(p.allocator, col_name);
        if (p.cur.kind == .comma) {
            p.advance();
            continue;
        }
        break;
    }
    try p.expect(.rparen);

    return .{ .name = name, .columns = try columns.toOwnedSlice(p.allocator) };
}

/// Column-def: `<name> [<type-name> ...] [<column-constraint> ...]`. We only
/// care about the column name; everything until the next `,` or `)` at
/// paren-depth 0 is consumed and ignored. This permits `x INTEGER NOT NULL`,
/// `x INT DEFAULT (1+1)`, `x VARCHAR(255)`, etc.
fn parseColumnDef(p: *Parser) ![]const u8 {
    if (p.cur.kind != .identifier) return Error.SyntaxError;
    const name = p.cur.slice(p.src);
    p.advance();
    var depth: u32 = 0;
    while (true) {
        switch (p.cur.kind) {
            .comma, .rparen => if (depth == 0) return name,
            else => {},
        }
        switch (p.cur.kind) {
            .lparen => depth += 1,
            .rparen => depth -= 1,
            .eof => return Error.SyntaxError,
            else => {},
        }
        p.advance();
    }
}

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
