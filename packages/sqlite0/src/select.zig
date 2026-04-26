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

const Value = value_mod.Value;
const Error = ops.Error;
const Parser = parser_mod.Parser;

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

/// Optional `ORDER BY ... LIMIT N OFFSET M` post-processing. Caller passes
/// the parsed ORDER terms and limit/offset expressions which are evaluated
/// once before applying.
///
/// When `position` is non-null, the term refers to the 1-based SELECT-list
/// column (sqlite3 quirk — `ORDER BY 2` sorts by the second projected
/// column). Otherwise `expr` is evaluated against the source row.
pub const OrderTerm = struct {
    expr: *ast.Expr,
    position: ?usize = null,
    descending: bool,
};

pub const PostProcess = struct {
    order_by: []const OrderTerm = &.{},
    limit: ?*ast.Expr = null,
    offset: ?*ast.Expr = null,
};

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
    return applyLimitOffset(allocator, rows, pp);
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
        try sortRowsByKeys(allocator, rows.items, sort_keys.items, pp.order_by);
        // Sort keys are no longer needed after sorting completes; free them.
        for (sort_keys.items) |key| {
            for (key) |v| ops.freeValue(allocator, v);
            allocator.free(key);
        }
        sort_keys.deinit(allocator);
        sort_keys = .empty;
    }

    const all_rows = try rows.toOwnedSlice(allocator);
    return applyLimitOffset(allocator, all_rows, pp);
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

/// In-place stable sort of `rows` by parallel `keys`, applying per-term
/// direction. Uses indirect indices so we don't have to swap the (larger)
/// projected rows during comparisons.
fn sortRowsByKeys(
    allocator: std.mem.Allocator,
    rows: [][]Value,
    keys: [][]Value,
    terms: []const OrderTerm,
) !void {
    std.debug.assert(rows.len == keys.len);
    const indices = try allocator.alloc(usize, rows.len);
    defer allocator.free(indices);
    for (indices, 0..) |*slot, i| slot.* = i;

    const Ctx = struct {
        keys: [][]Value,
        terms: []const OrderTerm,
        fn lessThan(self: @This(), a: usize, b: usize) bool {
            for (self.terms, 0..) |term, ti| {
                const cmp = compareValues(self.keys[a][ti], self.keys[b][ti]);
                if (cmp == 0) continue;
                return if (term.descending) cmp > 0 else cmp < 0;
            }
            return false;
        }
    };
    std.sort.pdq(usize, indices, Ctx{ .keys = keys, .terms = terms }, Ctx.lessThan);

    // Permute rows according to indices. Use a scratch buffer to apply.
    const scratch = try allocator.alloc([]Value, rows.len);
    defer allocator.free(scratch);
    for (indices, 0..) |src_idx, dst_idx| scratch[dst_idx] = rows[src_idx];
    @memcpy(rows, scratch);
}

/// Compare two Values for ORDER BY using sqlite3's storage-class rules:
/// NULL first, then numeric (INTEGER/REAL coerced to f64 for cross-class
/// compare), then TEXT (byte-wise), then BLOB. Returns -1/0/1.
fn compareValues(a: Value, b: Value) i32 {
    const ord_a = classOrder(a);
    const ord_b = classOrder(b);
    if (ord_a != ord_b) return if (ord_a < ord_b) -1 else 1;
    switch (ord_a) {
        0 => return 0, // both NULL
        1 => {
            const af: f64 = switch (a) {
                .integer => |i| @floatFromInt(i),
                .real => |r| r,
                else => unreachable,
            };
            const bf: f64 = switch (b) {
                .integer => |i| @floatFromInt(i),
                .real => |r| r,
                else => unreachable,
            };
            if (af < bf) return -1;
            if (af > bf) return 1;
            return 0;
        },
        2 => {
            const at = a.text;
            const bt = b.text;
            return switch (std.mem.order(u8, at, bt)) {
                .lt => -1,
                .eq => 0,
                .gt => 1,
            };
        },
        3 => {
            const ab = a.blob;
            const bb = b.blob;
            return switch (std.mem.order(u8, ab, bb)) {
                .lt => -1,
                .eq => 0,
                .gt => 1,
            };
        },
        else => unreachable,
    }
}

fn classOrder(v: Value) u8 {
    return switch (v) {
        .null => 0,
        .integer => 1,
        .real => 1, // sqlite3 treats INTEGER / REAL as one numeric class for sort
        .text => 2,
        .blob => 3,
    };
}

fn applyLimitOffset(
    allocator: std.mem.Allocator,
    rows: [][]Value,
    pp: PostProcess,
) ![][]Value {
    if (pp.limit == null and pp.offset == null) return rows;
    // Eval limit/offset first. If either errors, free everything we own and
    // propagate. Once we start freeing rows we disarm this guard.
    var owned = true;
    errdefer if (owned) {
        for (rows) |row| {
            for (row) |v| ops.freeValue(allocator, v);
            allocator.free(row);
        }
        allocator.free(rows);
    };
    const ctx = eval.EvalContext{
        .allocator = allocator,
        .current_row = &.{},
        .columns = &.{},
    };
    var skip: usize = 0;
    if (pp.offset) |e| {
        const v = try eval.evalExpr(ctx, e);
        defer ops.freeValue(allocator, v);
        skip = clampNonNegative(coerceToInt(v));
    }
    var keep: usize = std.math.maxInt(usize);
    if (pp.limit) |e| {
        const v = try eval.evalExpr(ctx, e);
        defer ops.freeValue(allocator, v);
        const n = coerceToInt(v);
        if (n >= 0) keep = @intCast(n);
        // sqlite3: negative LIMIT means "no limit" — keep stays at maxInt.
    }
    const start = @min(skip, rows.len);
    const end = @min(start + keep, rows.len);
    if (start == 0 and end == rows.len) return rows;
    // Free dropped rows. From here on, `rows[0..start]` and `rows[end..]`
    // are no-longer-owned; the errdefer above would double-free, so disarm
    // before any memory work.
    owned = false;
    for (rows[0..start]) |row| {
        for (row) |v| ops.freeValue(allocator, v);
        allocator.free(row);
    }
    for (rows[end..]) |row| {
        for (row) |v| ops.freeValue(allocator, v);
        allocator.free(row);
    }
    const kept = end - start;
    if (kept > 0 and start != 0) {
        var i: usize = 0;
        while (i < kept) : (i += 1) rows[i] = rows[start + i];
    }
    if (allocator.resize(rows, kept)) return rows[0..kept];
    const shrunk = allocator.alloc([]Value, kept) catch |err| {
        // OOM during resize fallback: free the kept rows we still own.
        for (rows[0..kept]) |row| {
            for (row) |v| ops.freeValue(allocator, v);
            allocator.free(row);
        }
        allocator.free(rows);
        return err;
    };
    @memcpy(shrunk, rows[0..kept]);
    allocator.free(rows);
    return shrunk;
}

fn coerceToInt(v: Value) i64 {
    return switch (v) {
        .null => 0,
        .integer => |i| i,
        .real => |r| @intFromFloat(r),
        .text => |t| std.fmt.parseInt(i64, t, 10) catch 0,
        .blob => |b| std.fmt.parseInt(i64, b, 10) catch 0,
    };
}

fn clampNonNegative(n: i64) usize {
    return if (n < 0) 0 else @intCast(n);
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
