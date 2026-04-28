//! Aggregate function execution: count/sum/avg/min/max/total + GROUP BY +
//! HAVING. Sits between `engine.executeSelect` and the per-row evaluator —
//! when a SELECT contains aggregate calls or GROUP BY clauses, this module
//! takes over the row-loop, accumulates per-group state, and produces the
//! final output rows.
//!
//! Two main concepts:
//!
//! 1. **Aggregate detection**. We walk all expressions in the SELECT list,
//!    HAVING, and ORDER BY to find every `func_call` AST node whose name +
//!    arity matches an aggregate. The set of these node pointers (plus the
//!    GROUP BY presence) decides whether to enter this module or fall back
//!    to the per-row scalar path.
//!
//! 2. **Aggregate substitution**. During the group scan, each aggregate
//!    accumulates into an `Aggregator` state. After the scan we finalise
//!    each call to a Value and put `*const Expr → Value` mappings into an
//!    `eval.AggregateValues` map. The downstream `eval.evalExpr` looks up
//!    func_call nodes in this map and returns the precomputed value
//!    instead of dispatching to scalar `funcs.call`. That keeps SELECT
//!    list / HAVING / ORDER BY evaluation uniform with the non-aggregate
//!    path; the only difference is the substitution map.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const ast = @import("ast.zig");
const eval = @import("eval.zig");
const select = @import("select.zig");
const select_post = @import("select_post.zig");
const walk = @import("aggregate_walk.zig");
const state = @import("aggregate_state.zig");
const database = @import("database.zig");
const func_util = @import("func_util.zig");
const collation = @import("collation.zig");
const stmt_mod = @import("stmt.zig");

const Value = value_mod.Value;
const Error = ops.Error;
const Aggregator = state.Aggregator;
const dupeArena = state.dupeArena;
const aggregatorFromCall = state.aggregatorFromCall;
const Database = database.Database;

// Re-export walking helpers so callers (engine.zig) can keep importing the
// aggregate driver as their single entry point.
pub const isAggregateName = walk.isAggregateName;
pub const isAggregateCall = walk.isAggregateCall;
pub const selectHasAggregates = walk.selectHasAggregates;

const Group = struct {
    /// Group key Values (parallel to `group_by` exprs). Owned by `key_alloc`.
    key: []Value,
    /// Per-aggregate-call state, parallel to `agg_calls`.
    aggs: []Aggregator,
    /// First source row that fell into this group. Holds borrowed pointers
    /// into the source table so bare-column refs in the SELECT list can
    /// resolve. We don't dupe — the source table outlives this Group object
    /// (we tear groups down before the per-statement arena).
    representative: []const Value,
};

/// Public entry point. Takes ownership of nothing; allocates output rows in
/// `alloc` (per-statement arena). All intermediate state — group keys,
/// MIN/MAX best-value keepers, accumulator slices — also lives in `alloc`,
/// so callers tear everything down by deinit'ing the arena.
pub fn executeAggregated(
    alloc: std.mem.Allocator,
    db: ?*Database,
    items: []const select.SelectItem,
    source_rows: []const []const Value,
    source_columns: []const []const u8,
    source_qualifiers: []const []const u8,
    source_collations: []const ast.CollationKind,
    where_ast: ?*ast.Expr,
    group_by: []const stmt_mod.GroupByTerm,
    having: ?*ast.Expr,
    pp: select_post.PostProcess,
    outer_frames: []const eval.OuterFrame,
) ![][]Value {
    if (select.containsStar(items)) return Error.SyntaxError;

    var agg_calls: std.ArrayList(*const ast.Expr) = .empty;
    defer agg_calls.deinit(alloc);
    try walk.collectAggregateCalls(alloc, items, having, pp.order_by, &agg_calls);

    // sqlite3 reports "DISTINCT aggregates must have exactly one argument" for
    // `count(DISTINCT)` (zero args) and `count(DISTINCT a, b)` (two+). Catch
    // both shapes here before the row scan starts.
    for (agg_calls.items) |call_expr| {
        const fc = call_expr.*.func_call;
        if (fc.distinct and fc.args.len != 1) return Error.SyntaxError;
    }

    var groups: std.ArrayList(Group) = .empty;
    // No defer cleanup needed — `alloc` is the per-statement arena and
    // callers reclaim everything by deinit'ing it.

    // Scan: for each source row, compute group key, find/create group,
    // feed each aggregate.
    for (source_rows) |row| {
        if (where_ast) |w| {
            const ctx = eval.EvalContext{
                .allocator = alloc,
                .current_row = row,
                .columns = source_columns,
                .column_qualifiers = source_qualifiers,
                .column_collations = source_collations,
                .db = db,
                .outer_frames = outer_frames,
            };
            const cond = try eval.evalExpr(ctx, w);
            defer ops.freeValue(alloc, cond);
            if (!(ops.truthy(cond) orelse false)) continue;
        }

        const key = try evaluateGroupKey(alloc, db, group_by, items, row, source_columns, source_qualifiers, source_collations, outer_frames);
        const group_idx = if (findGroup(groups.items, key, group_by, source_columns, source_qualifiers, source_collations)) |idx| blk: {
            // Existing group — discard the redundant key (arena reclaims).
            for (key) |v| ops.freeValue(alloc, v);
            alloc.free(key);
            break :blk idx;
        } else blk: {
            const idx = groups.items.len;
            try groups.append(alloc, .{
                .key = key,
                .aggs = try makeAggregators(alloc, agg_calls.items, source_columns, source_qualifiers, source_collations),
                .representative = row,
            });
            break :blk idx;
        };
        try feedRow(alloc, db, &groups.items[group_idx], agg_calls.items, row, source_columns, source_qualifiers, source_collations, outer_frames);
    }

    // Implicit-group rule: if there are aggregates but no GROUP BY and no
    // rows survived WHERE, sqlite3 still emits a single row of finalised
    // aggregates (count → 0, others → NULL). We satisfy that by ensuring
    // exactly one group exists when group_by is empty.
    if (groups.items.len == 0 and group_by.len == 0 and agg_calls.items.len > 0) {
        try groups.append(alloc, .{
            .key = &.{},
            .aggs = try makeAggregators(alloc, agg_calls.items, source_columns, source_qualifiers, source_collations),
            .representative = &.{},
        });
    }

    return finaliseGroups(alloc, db, items, having, pp, agg_calls.items, groups.items, source_columns, source_qualifiers, source_collations, outer_frames);
}

fn evaluateGroupKey(
    alloc: std.mem.Allocator,
    db: ?*Database,
    group_by: []const stmt_mod.GroupByTerm,
    items: []const select.SelectItem,
    row: []const Value,
    columns: []const []const u8,
    column_qualifiers: []const []const u8,
    column_collations: []const ast.CollationKind,
    outer_frames: []const eval.OuterFrame,
) ![]Value {
    const key = try alloc.alloc(Value, group_by.len);
    var produced: usize = 0;
    errdefer {
        for (key[0..produced]) |v| ops.freeValue(alloc, v);
        alloc.free(key);
    }
    const ctx = eval.EvalContext{
        .allocator = alloc,
        .current_row = row,
        .columns = columns,
        .column_qualifiers = column_qualifiers,
        .column_collations = column_collations,
        .db = db,
        .outer_frames = outer_frames,
    };
    while (produced < group_by.len) : (produced += 1) {
        // sqlite3 quirk: bare positive integer literal in GROUP BY refers to
        // the SELECT-list column at that 1-based index. `*` items are
        // rejected (sqlite3 errors "GROUP BY term out of range") because
        // they expand to multiple columns; we collapse to SyntaxError.
        // Also: an unqualified column-ref that matches a SELECT-list alias
        // resolves to that aliased expression (sqlite3 ORDER BY / GROUP BY
        // alias resolution; a bare name that's not a source column nor an
        // alias falls through to evalExpr where it raises "no such column").
        const target = if (group_by[produced].position) |pos| blk: {
            if (pos == 0 or pos > items.len) return Error.SyntaxError;
            const item = items[pos - 1];
            switch (item) {
                .star => return Error.SyntaxError,
                .expr => |e| break :blk e.expr,
            }
        } else (resolveGroupByAlias(group_by[produced].expr, items) orelse group_by[produced].expr);
        key[produced] = try eval.evalExpr(ctx, target);
    }
    return key;
}

fn resolveGroupByAlias(expr: *ast.Expr, items: []const select.SelectItem) ?*ast.Expr {
    // sqlite3 unwraps COLLATE for alias lookup and re-applies it for compare;
    // mirror by peeling here. The kind already lives on `GroupByTerm.collation`
    // (extracted at parse time) so equality picks it up regardless.
    const inner = collation.peel(expr).inner;
    if (inner.* != .column_ref) return null;
    const cref = inner.*.column_ref;
    if (cref.qualifier != null) return null;
    for (items) |item| switch (item) {
        .star => {},
        .expr => |e| if (e.alias) |alias| {
            if (std.ascii.eqlIgnoreCase(alias, cref.name)) return e.expr;
        },
    };
    return null;
}

fn findGroup(
    groups: []const Group,
    key: []const Value,
    group_by: []const stmt_mod.GroupByTerm,
    source_columns: []const []const u8,
    source_qualifiers: []const []const u8,
    source_collations: []const ast.CollationKind,
) ?usize {
    for (groups, 0..) |g, i| {
        if (groupKeysEqual(g.key, key, group_by, source_columns, source_qualifiers, source_collations)) return i;
    }
    return null;
}

/// sqlite3 GROUP BY collation: NULL == NULL within group key. TEXT pairs
/// honor the per-term COLLATE wrapper (Iter31.P) and fall back to the
/// referenced column's schema collation when the term is a bare column-ref
/// without an explicit wrapper (Iter31.R) — so `GROUP BY x` on a NOCASE
/// column folds 'A'/'a' into one group.
fn groupKeysEqual(
    a: []const Value,
    b: []const Value,
    group_by: []const stmt_mod.GroupByTerm,
    source_columns: []const []const u8,
    source_qualifiers: []const []const u8,
    source_collations: []const ast.CollationKind,
) bool {
    if (a.len != b.len) return false;
    for (a, b, 0..) |va, vb, i| {
        if (va == .null and vb == .null) continue;
        if (va == .null or vb == .null) return false;
        const kind = if (i < group_by.len)
            (group_by[i].collation orelse
                collation.columnDefault(group_by[i].expr, source_columns, source_qualifiers, source_collations) orelse
                .binary)
        else
            .binary;
        if (collation.compareValuesCollated(va, vb, kind) != .eq) return false;
    }
    return true;
}

fn makeAggregators(
    alloc: std.mem.Allocator,
    calls: []const *const ast.Expr,
    source_columns: []const []const u8,
    source_qualifiers: []const []const u8,
    source_collations: []const ast.CollationKind,
) ![]Aggregator {
    const aggs = try alloc.alloc(Aggregator, calls.len);
    for (calls, aggs) |call_expr, *slot| {
        slot.* = aggregatorFromCall(call_expr.*.func_call, source_columns, source_qualifiers, source_collations);
    }
    return aggs;
}

fn feedRow(
    alloc: std.mem.Allocator,
    db: ?*Database,
    group: *Group,
    calls: []const *const ast.Expr,
    row: []const Value,
    columns: []const []const u8,
    column_qualifiers: []const []const u8,
    column_collations: []const ast.CollationKind,
    outer_frames: []const eval.OuterFrame,
) !void {
    const ctx = eval.EvalContext{
        .allocator = alloc,
        .current_row = row,
        .columns = columns,
        .column_qualifiers = column_qualifiers,
        .column_collations = column_collations,
        .db = db,
        .outer_frames = outer_frames,
    };
    for (calls, group.aggs) |call_expr, *agg| {
        const fc = call_expr.*.func_call;
        if (agg.kind == .count_star or fc.args.len == 0) {
            try agg.feed(alloc, Value{ .integer = 1 });
            continue;
        }
        // group_concat(x, sep): the separator is dynamic — re-evaluated per
        // row. We stash it on the aggregator so `feed` can read it before
        // appending the current contributor's text.
        if (agg.kind == .group_concat and fc.args.len == 2) {
            const sep_v = try eval.evalExpr(ctx, fc.args[1]);
            defer ops.freeValue(alloc, sep_v);
            agg.sep_explicit = true;
            if (sep_v == .null) {
                agg.sep_override = null;
            } else {
                // ensureText returns a fresh arena alloc; lifetime only
                // needs to span this feed call (text_buf appendSlice
                // copies). The arena reclaims at statement teardown.
                const sep_text = try func_util.ensureText(alloc, sep_v);
                agg.sep_override = sep_text;
            }
            const v = try eval.evalExpr(ctx, fc.args[0]);
            defer ops.freeValue(alloc, v);
            try agg.feed(alloc, v);
            continue;
        }
        const v = try eval.evalExpr(ctx, fc.args[0]);
        defer ops.freeValue(alloc, v);
        try agg.feed(alloc, v);
    }
}

fn finaliseGroups(
    alloc: std.mem.Allocator,
    db: ?*Database,
    items: []const select.SelectItem,
    having: ?*ast.Expr,
    pp: select_post.PostProcess,
    calls: []const *const ast.Expr,
    groups: []Group,
    source_columns: []const []const u8,
    source_qualifiers: []const []const u8,
    source_collations: []const ast.CollationKind,
    outer_frames: []const eval.OuterFrame,
) ![][]Value {
    var rows: std.ArrayList([]Value) = .empty;
    errdefer {
        for (rows.items) |row| {
            for (row) |v| ops.freeValue(alloc, v);
            alloc.free(row);
        }
        rows.deinit(alloc);
    }
    var sort_keys: std.ArrayList([]Value) = .empty;
    errdefer {
        for (sort_keys.items) |k| {
            for (k) |v| ops.freeValue(alloc, v);
            alloc.free(k);
        }
        sort_keys.deinit(alloc);
    }

    for (groups) |*g| {
        // Build the substitution map for this group.
        var agg_map: eval.AggregateValues = .empty;
        defer {
            var it = agg_map.valueIterator();
            while (it.next()) |v| ops.freeValue(alloc, v.*);
            agg_map.deinit(alloc);
        }
        try agg_map.ensureTotalCapacity(alloc, @intCast(calls.len));
        for (calls, g.aggs) |call_expr, *agg| {
            const fv = try agg.finalize(alloc);
            agg_map.putAssumeCapacity(call_expr, fv);
        }

        const ctx = eval.EvalContext{
            .allocator = alloc,
            .current_row = g.representative,
            .columns = source_columns,
            .column_qualifiers = source_qualifiers,
            .column_collations = source_collations,
            .agg_values = &agg_map,
            .db = db,
            .outer_frames = outer_frames,
        };

        if (having) |h| {
            const cond = try eval.evalExpr(ctx, h);
            defer ops.freeValue(alloc, cond);
            if (!(ops.truthy(cond) orelse false)) continue;
        }

        const out_row = try alloc.alloc(Value, items.len);
        var produced: usize = 0;
        errdefer {
            for (out_row[0..produced]) |v| ops.freeValue(alloc, v);
            alloc.free(out_row);
        }
        for (items) |item| switch (item) {
            .star => unreachable,
            .expr => |e| {
                out_row[produced] = try eval.evalExpr(ctx, e.expr);
                produced += 1;
            },
        };
        try rows.append(alloc, out_row);

        if (pp.order_by.len > 0) {
            const key = try evaluateAggOrderKey(alloc, db, pp.order_by, g.representative, source_columns, source_qualifiers, source_collations, out_row, &agg_map, outer_frames);
            sort_keys.append(alloc, key) catch |err| {
                for (key) |v| ops.freeValue(alloc, v);
                alloc.free(key);
                return err;
            };
        }
    }

    if (pp.order_by.len > 0) {
        try select_post.sortRowsByKeys(alloc, rows.items, sort_keys.items, pp.order_by, source_columns, source_qualifiers, source_collations);
        for (sort_keys.items) |k| {
            for (k) |v| ops.freeValue(alloc, v);
            alloc.free(k);
        }
        sort_keys.deinit(alloc);
        sort_keys = .empty;
    }

    var all_rows = try rows.toOwnedSlice(alloc);
    if (pp.distinct) {
        const arity = if (all_rows.len > 0) all_rows[0].len else 0;
        const kinds = try select_post.extractDistinctCollations(alloc, items, arity, source_columns, source_qualifiers, source_collations);
        defer alloc.free(kinds);
        all_rows = select_post.dedupeRows(alloc, all_rows, kinds);
    }
    return select_post.applyLimitOffset(alloc, db, all_rows, pp, outer_frames);
}

fn evaluateAggOrderKey(
    alloc: std.mem.Allocator,
    db: ?*Database,
    terms: []const select_post.OrderTerm,
    current_row: []const Value,
    columns: []const []const u8,
    column_qualifiers: []const []const u8,
    column_collations: []const ast.CollationKind,
    projected_row: []const Value,
    agg_map: *const eval.AggregateValues,
    outer_frames: []const eval.OuterFrame,
) ![]Value {
    const key = try alloc.alloc(Value, terms.len);
    var produced: usize = 0;
    errdefer {
        for (key[0..produced]) |v| ops.freeValue(alloc, v);
        alloc.free(key);
    }
    const ctx = eval.EvalContext{
        .allocator = alloc,
        .current_row = current_row,
        .columns = columns,
        .column_qualifiers = column_qualifiers,
        .column_collations = column_collations,
        .agg_values = agg_map,
        .db = db,
        .outer_frames = outer_frames,
    };
    while (produced < terms.len) : (produced += 1) {
        const term = terms[produced];
        if (term.position) |pos| {
            const idx = pos - 1;
            key[produced] = if (idx < projected_row.len)
                try dupeArena(alloc, projected_row[idx])
            else
                Value.null;
        } else {
            key[produced] = try eval.evalExpr(ctx, term.expr);
        }
    }
    return key;
}
