//! SELECT-list parsing and per-row execution.
//!
//! `stmt.zig` orchestrates SELECT statements (FROM clause, WHERE clause,
//! row iteration) and calls into this file for the SELECT-list-specific
//! parts: parsing items (`*` vs expression), expanding `*` against the
//! FROM source, and evaluating the WHERE predicate per row.
//!
//! Kept separate from `stmt.zig` to keep both files under the 500-line
//! discipline (CLAUDE.md "Module Splitting Rules") after Iter11 grew
//! `stmt.zig` past the limit.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const ast = @import("ast.zig");
const parser_mod = @import("parser.zig");
const eval = @import("eval.zig");
const post = @import("select_post.zig");

const Value = value_mod.Value;
const Error = ops.Error;
const Parser = parser_mod.Parser;

pub const OrderTerm = post.OrderTerm;
pub const PostProcess = post.PostProcess;

/// One element in a SELECT list: either `*` (expand to all FROM columns)
/// or an expression with an optional output alias. `*` is invalid without a
/// FROM clause and rejected at execute time to mirror sqlite3 (`SELECT *` →
/// "no tables specified").
///
/// `alias` is purely metadata: it's parsed for syntactic completeness (so
/// `SELECT 1 + 2 AS sum` accepts) but the current CLI doesn't print headers,
/// so it has no observable effect on output. Future iterations (header mode,
/// ORDER BY referencing aliases) will read it.
pub const SelectItem = union(enum) {
    star,
    expr: ExprItem,

    pub const ExprItem = struct {
        expr: *ast.Expr,
        alias: ?[]const u8 = null,
    };
};

pub fn parseSelectList(p: *Parser) ![]SelectItem {
    var items: std.ArrayList(SelectItem) = .empty;
    errdefer freeSelectListItems(p.allocator, &items);
    try parseSelectItem(p, &items);
    while (p.cur.kind == .comma) {
        p.advance();
        try parseSelectItem(p, &items);
    }
    return items.toOwnedSlice(p.allocator);
}

fn parseSelectItem(p: *Parser, items: *std.ArrayList(SelectItem)) !void {
    try items.ensureUnusedCapacity(p.allocator, 1);
    if (p.cur.kind == .star) {
        p.advance();
        items.appendAssumeCapacity(.star);
        return;
    }
    const expr = try p.parseExpr();
    const alias = parseOptionalAlias(p);
    items.appendAssumeCapacity(.{ .expr = .{ .expr = expr, .alias = alias } });
}

/// Recognize `AS <ident>` (mandatory ident after AS) or a bare `<ident>`
/// alias. A bare keyword (FROM, WHERE, ...) is left for the surrounding
/// statement parser. The alias slice borrows from `Parser.src` and lives as
/// long as the input string.
///
/// CAVEAT: this greedily consumes any post-expression identifier as an
/// alias. When adding new infix operators (IS, IN, BETWEEN, ...), register
/// them as keywords in `lex.zig` BEFORE wiring them into `parseExpr` —
/// otherwise this function silently steals the operator name as a bare
/// alias and the resulting parse error misleadingly points at the rhs.
fn parseOptionalAlias(p: *Parser) ?[]const u8 {
    if (p.cur.kind == .keyword_as) {
        p.advance();
        if (p.cur.kind != .identifier) return null;
        const name = p.cur.slice(p.src);
        p.advance();
        return name;
    }
    if (p.cur.kind == .identifier) {
        const name = p.cur.slice(p.src);
        p.advance();
        return name;
    }
    return null;
}

fn freeSelectListItems(allocator: std.mem.Allocator, items: *std.ArrayList(SelectItem)) void {
    for (items.items) |item| switch (item) {
        .star => {},
        .expr => |e| e.expr.deinit(allocator),
    };
    items.deinit(allocator);
}

pub fn freeSelectList(allocator: std.mem.Allocator, items: []SelectItem) void {
    for (items) |item| switch (item) {
        .star => {},
        .expr => |e| e.expr.deinit(allocator),
    };
    allocator.free(items);
}

pub fn containsStar(items: []const SelectItem) bool {
    for (items) |item| if (item == .star) return true;
    return false;
}

/// Execute a SELECT against a synthetic empty row (no FROM). Optionally
/// filtered by WHERE — `SELECT 1 WHERE 0` returns no rows. `*` is rejected
/// upstream by `containsStar`, so every item here is `.expr`.
pub fn executeWithoutFrom(
    allocator: std.mem.Allocator,
    items: []const SelectItem,
    where_ast: ?*ast.Expr,
    pp: PostProcess,
) ![][]Value {
    if (!try evalWhereTruthy(allocator, where_ast, &.{}, &.{})) {
        return allocator.alloc([]Value, 0);
    }
    const row = try evaluateSelectRow(allocator, items, &.{}, &.{});
    errdefer {
        for (row) |v| ops.freeValue(allocator, v);
        allocator.free(row);
    }
    var rows = try allocator.alloc([]Value, 1);
    rows[0] = row;
    // ORDER BY is a no-op on a single row, but LIMIT/OFFSET still applies.
    return post.applyLimitOffset(allocator, rows, pp);
}

/// Execute a SELECT against a FROM source. For each source row, optionally
/// filter by WHERE, then evaluate the SELECT list (`*` expanding to all
/// source columns) bound to that row. `pp.order_by` is evaluated against
/// the SOURCE row (sqlite3 resolves ORDER BY in the FROM scope first).
pub fn executeWithFrom(
    allocator: std.mem.Allocator,
    items: []const SelectItem,
    source_rows: []const []const Value,
    source_columns: []const []const u8,
    where_ast: ?*ast.Expr,
    pp: PostProcess,
) ![][]Value {
    var rows: std.ArrayList([]Value) = .empty;
    errdefer {
        for (rows.items) |row| {
            for (row) |v| ops.freeValue(allocator, v);
            allocator.free(row);
        }
        rows.deinit(allocator);
    }
    var sort_keys: std.ArrayList([]Value) = .empty;
    errdefer {
        for (sort_keys.items) |key| {
            for (key) |v| ops.freeValue(allocator, v);
            allocator.free(key);
        }
        sort_keys.deinit(allocator);
    }

    for (source_rows) |source_row| {
        if (!try evalWhereTruthy(allocator, where_ast, source_row, source_columns)) continue;
        const out_row = try evaluateSelectRow(allocator, items, source_row, source_columns);
        rows.append(allocator, out_row) catch |err| {
            for (out_row) |v| ops.freeValue(allocator, v);
            allocator.free(out_row);
            return err;
        };
        if (pp.order_by.len > 0) {
            const key = try evaluateOrderKey(allocator, pp.order_by, source_row, source_columns, out_row);
            sort_keys.append(allocator, key) catch |err| {
                for (key) |v| ops.freeValue(allocator, v);
                allocator.free(key);
                return err;
            };
        }
    }

    if (pp.order_by.len > 0) {
        try post.sortRowsByKeys(allocator, rows.items, sort_keys.items, pp.order_by);
        for (sort_keys.items) |key| {
            for (key) |v| ops.freeValue(allocator, v);
            allocator.free(key);
        }
        sort_keys.deinit(allocator);
        sort_keys = .empty;
    }

    var all_rows = try rows.toOwnedSlice(allocator);
    if (pp.distinct) all_rows = post.dedupeRows(allocator, all_rows);
    return post.applyLimitOffset(allocator, all_rows, pp);
}

fn evaluateOrderKey(
    allocator: std.mem.Allocator,
    terms: []const OrderTerm,
    current_row: []const Value,
    columns: []const []const u8,
    projected_row: []const Value,
) ![]Value {
    const key = try allocator.alloc(Value, terms.len);
    var produced: usize = 0;
    errdefer {
        for (key[0..produced]) |v| ops.freeValue(allocator, v);
        allocator.free(key);
    }
    const ctx = eval.EvalContext{
        .allocator = allocator,
        .current_row = current_row,
        .columns = columns,
    };
    while (produced < terms.len) : (produced += 1) {
        const term = terms[produced];
        if (term.position) |pos| {
            // sqlite3 silently clamps: positions out of range produce NULL.
            // Out-of-range here means 0 (handled at parse time) or > arity.
            const idx = pos - 1;
            if (idx < projected_row.len) {
                key[produced] = try dupeRowValue(allocator, projected_row[idx]);
            } else {
                key[produced] = Value.null;
            }
        } else {
            key[produced] = try eval.evalExpr(ctx, term.expr);
        }
    }
    return key;
}

/// Evaluate a SELECT list against one source row. Star items expand to
/// `current_row` in order (each Value is duped so the result outlives the
/// source row); expression items are evaluated normally.
fn evaluateSelectRow(
    allocator: std.mem.Allocator,
    items: []const SelectItem,
    current_row: []const Value,
    columns: []const []const u8,
) Error![]Value {
    var total: usize = 0;
    for (items) |item| total += switch (item) {
        .star => current_row.len,
        .expr => 1,
    };
    const row = try allocator.alloc(Value, total);
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
    for (items) |item| switch (item) {
        .star => {
            for (current_row) |src_v| {
                row[produced] = try dupeRowValue(allocator, src_v);
                produced += 1;
            }
        },
        .expr => |e| {
            row[produced] = try eval.evalExpr(ctx, e.expr);
            produced += 1;
        },
    };
    return row;
}

/// Dupe TEXT/BLOB bytes so a source-row value survives FromSource
/// teardown. INTEGER/REAL/NULL copy implicitly. Mirrors `eval.dupeLiteral`
/// — kept here because that helper is private to `eval.zig`.
fn dupeRowValue(allocator: std.mem.Allocator, v: Value) !Value {
    return switch (v) {
        .text => |t| Value{ .text = try allocator.dupe(u8, t) },
        .blob => |b| Value{ .blob = try allocator.dupe(u8, b) },
        else => v,
    };
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
