//! AST walk helpers for aggregate detection.
//!
//! Two responsibilities:
//!   - `selectHasAggregates`: yes/no scan to choose between the per-row and
//!     grouped execution paths.
//!   - `collectAggregateCalls`: produce the ordered list of aggregate
//!     `*const Expr` pointers that the grouped driver needs to allocate
//!     accumulator state for and substitute in finalised values.
//!
//! Split out of `aggregate.zig` to keep that file under the 500-line
//! discipline (CLAUDE.md "Module Splitting Rules"). The walking logic is
//! self-contained — it touches AST shapes only — and has no dependency on
//! the execution driver, so the boundary is clean.

const std = @import("std");
const ast = @import("ast.zig");
const select = @import("select.zig");
const select_post = @import("select_post.zig");
const func_util = @import("func_util.zig");

/// Aggregate function names recognised in this module. `min` and `max` are
/// also scalar functions; `isAggregateCall` distinguishes by arity.
pub fn isAggregateName(name: []const u8) bool {
    return func_util.eqlIgnoreCase(name, "count") or
        func_util.eqlIgnoreCase(name, "sum") or
        func_util.eqlIgnoreCase(name, "avg") or
        func_util.eqlIgnoreCase(name, "total") or
        func_util.eqlIgnoreCase(name, "min") or
        func_util.eqlIgnoreCase(name, "max") or
        func_util.eqlIgnoreCase(name, "group_concat") or
        func_util.eqlIgnoreCase(name, "string_agg");
}

/// Aggregate vs. scalar disambiguation. `count` is always aggregate (0 or 1
/// arg). `sum`/`avg`/`total` are always aggregate (1 arg). `min`/`max` are
/// aggregate only at arity 1; arity ≥ 2 is the scalar `min(a, b, ...)` /
/// `max(a, b, ...)` form handled in `funcs.zig`. `group_concat` accepts 1 or
/// 2 args (the second being a per-row dynamic separator). `string_agg` is
/// the same accumulator as `group_concat` but strict 2-arg (sqlite3 errors
/// on the 1-arg form, no implicit "," default).
pub fn isAggregateCall(fc: ast.Expr.FuncCall) bool {
    if (func_util.eqlIgnoreCase(fc.name, "count")) return fc.args.len <= 1;
    if (func_util.eqlIgnoreCase(fc.name, "sum") or
        func_util.eqlIgnoreCase(fc.name, "avg") or
        func_util.eqlIgnoreCase(fc.name, "total")) return fc.args.len == 1;
    if (func_util.eqlIgnoreCase(fc.name, "min") or
        func_util.eqlIgnoreCase(fc.name, "max")) return fc.args.len == 1;
    if (func_util.eqlIgnoreCase(fc.name, "group_concat")) return fc.args.len == 1 or fc.args.len == 2;
    if (func_util.eqlIgnoreCase(fc.name, "string_agg")) return fc.args.len == 2;
    return false;
}

/// Walk the full SELECT (items + having + order_by) to detect any aggregate
/// call. Returns `true` as soon as one is found.
pub fn selectHasAggregates(
    items: []const select.SelectItem,
    having: ?*const ast.Expr,
    order_by: []const select_post.OrderTerm,
) bool {
    for (items) |item| switch (item) {
        .star => {},
        .expr => |e| if (exprHasAggregate(e.expr)) return true,
    };
    if (having) |h| if (exprHasAggregate(h)) return true;
    for (order_by) |t| if (exprHasAggregate(t.expr)) return true;
    return false;
}

fn exprHasAggregate(expr: *const ast.Expr) bool {
    return switch (expr.*) {
        .literal, .column_ref => false,
        .binary_arith => |b| exprHasAggregate(b.left) or exprHasAggregate(b.right),
        .binary_concat => |b| exprHasAggregate(b.left) or exprHasAggregate(b.right),
        .unary_negate => |inner| exprHasAggregate(inner),
        .unary_bit_not => |inner| exprHasAggregate(inner),
        .compare => |c| exprHasAggregate(c.left) or exprHasAggregate(c.right),
        .eq_check => |e| exprHasAggregate(e.left) or exprHasAggregate(e.right),
        .is_check => |e| exprHasAggregate(e.left) or exprHasAggregate(e.right),
        .is_truthy => |e| exprHasAggregate(e.value),
        .between => |b| exprHasAggregate(b.value) or exprHasAggregate(b.lo) or exprHasAggregate(b.hi),
        .in_list => |il| blk: {
            if (exprHasAggregate(il.value)) break :blk true;
            for (il.items) |it| if (exprHasAggregate(it)) break :blk true;
            break :blk false;
        },
        .logical_and, .logical_or => |b| exprHasAggregate(b.left) or exprHasAggregate(b.right),
        .logical_not => |inner| exprHasAggregate(inner),
        .case_expr => |ce| blk: {
            if (ce.scrutinee) |s| if (exprHasAggregate(s)) break :blk true;
            for (ce.branches) |br| {
                if (exprHasAggregate(br.when)) break :blk true;
                if (exprHasAggregate(br.then)) break :blk true;
            }
            if (ce.else_branch) |eb| if (exprHasAggregate(eb)) break :blk true;
            break :blk false;
        },
        .func_call => |fc| blk: {
            if (isAggregateCall(fc)) break :blk true;
            for (fc.args) |a| if (exprHasAggregate(a)) break :blk true;
            break :blk false;
        },
        .like => |l| exprHasAggregate(l.value) or
            exprHasAggregate(l.pattern) or
            (l.escape != null and exprHasAggregate(l.escape.?)),
        // Subquery forms (Iter22.B/C): aggregates inside the inner SELECT
        // are scoped to that SELECT — they don't promote the outer SELECT
        // into the aggregate path. Same logic for `collectInExpr` below.
        .subquery, .exists => false,
        .in_subquery => |is| exprHasAggregate(is.value),
        .cast => |c| exprHasAggregate(c.value),
        .collate => |c| exprHasAggregate(c.value),
    };
}

/// Walk the SELECT (items + having + order_by) and append every aggregate
/// `*const Expr` to `out`. The discovery order is preserved: the grouped
/// driver indexes into `out` and into a parallel `Aggregator` slice using
/// the same positions.
pub fn collectAggregateCalls(
    allocator: std.mem.Allocator,
    items: []const select.SelectItem,
    having: ?*const ast.Expr,
    order_by: []const select_post.OrderTerm,
    out: *std.ArrayList(*const ast.Expr),
) !void {
    for (items) |item| switch (item) {
        .star => {},
        .expr => |e| try collectInExpr(allocator, e.expr, out),
    };
    if (having) |h| try collectInExpr(allocator, h, out);
    for (order_by) |t| try collectInExpr(allocator, t.expr, out);
}

fn collectInExpr(
    allocator: std.mem.Allocator,
    expr: *const ast.Expr,
    out: *std.ArrayList(*const ast.Expr),
) !void {
    switch (expr.*) {
        .literal, .column_ref => {},
        .binary_arith => |b| {
            try collectInExpr(allocator, b.left, out);
            try collectInExpr(allocator, b.right, out);
        },
        .binary_concat => |b| {
            try collectInExpr(allocator, b.left, out);
            try collectInExpr(allocator, b.right, out);
        },
        .unary_negate => |inner| try collectInExpr(allocator, inner, out),
        .unary_bit_not => |inner| try collectInExpr(allocator, inner, out),
        .compare => |c| {
            try collectInExpr(allocator, c.left, out);
            try collectInExpr(allocator, c.right, out);
        },
        .eq_check => |e| {
            try collectInExpr(allocator, e.left, out);
            try collectInExpr(allocator, e.right, out);
        },
        .is_check => |e| {
            try collectInExpr(allocator, e.left, out);
            try collectInExpr(allocator, e.right, out);
        },
        .is_truthy => |e| try collectInExpr(allocator, e.value, out),
        .between => |b| {
            try collectInExpr(allocator, b.value, out);
            try collectInExpr(allocator, b.lo, out);
            try collectInExpr(allocator, b.hi, out);
        },
        .in_list => |il| {
            try collectInExpr(allocator, il.value, out);
            for (il.items) |it| try collectInExpr(allocator, it, out);
        },
        .logical_and, .logical_or => |b| {
            try collectInExpr(allocator, b.left, out);
            try collectInExpr(allocator, b.right, out);
        },
        .logical_not => |inner| try collectInExpr(allocator, inner, out),
        .case_expr => |ce| {
            if (ce.scrutinee) |s| try collectInExpr(allocator, s, out);
            for (ce.branches) |br| {
                try collectInExpr(allocator, br.when, out);
                try collectInExpr(allocator, br.then, out);
            }
            if (ce.else_branch) |eb| try collectInExpr(allocator, eb, out);
        },
        .func_call => |fc| {
            if (isAggregateCall(fc)) {
                try out.append(allocator, expr);
                // Per sqlite3, nested aggregates (sum(count(x))) are
                // disallowed; we don't recurse into args of aggregate
                // calls so a hypothetical inner aggregate isn't double-
                // counted. The argument sub-expression is still evaluated
                // per source row inside `feed` so non-aggregate work
                // (like sum(x*2)) is supported.
                return;
            }
            for (fc.args) |a| try collectInExpr(allocator, a, out);
        },
        .like => |l| {
            try collectInExpr(allocator, l.value, out);
            try collectInExpr(allocator, l.pattern, out);
            if (l.escape) |e| try collectInExpr(allocator, e, out);
        },
        // Subquery forms: aggregates inside the inner SELECT are scoped
        // to that SELECT (and resolved by its own aggregate.executeAggregated
        // call) — they don't get hoisted into the outer SELECT's aggregate
        // list. `in_subquery.value` is on the outer side, so its aggregates
        // (if any) still need collection.
        .subquery, .exists => {},
        .in_subquery => |is| try collectInExpr(allocator, is.value, out),
        .cast => |c| try collectInExpr(allocator, c.value, out),
        .collate => |c| try collectInExpr(allocator, c.value, out),
    }
}
