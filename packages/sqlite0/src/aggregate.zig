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
const func_util = @import("func_util.zig");
const walk = @import("aggregate_walk.zig");

const Value = value_mod.Value;
const Error = ops.Error;

// Re-export walking helpers so callers (engine.zig) can keep importing the
// aggregate driver as their single entry point.
pub const isAggregateName = walk.isAggregateName;
pub const isAggregateCall = walk.isAggregateCall;
pub const selectHasAggregates = walk.selectHasAggregates;

/// Per-aggregate accumulator. Keeps the running state for one func_call
/// across the rows of one group. Created once per (group × aggregate-call)
/// pair when a group is first seen.
const Aggregator = struct {
    kind: Kind,
    /// Shared 64-bit row counter. For COUNT it's the result; for
    /// SUM/AVG/TOTAL it's the non-NULL contributor count.
    count: u64 = 0,
    /// Integer running sum (i128 to defer overflow until finalize). Active
    /// while every contributor parses as an integer.
    sum_int: i128 = 0,
    /// Real running sum. Active once any contributor is non-integer (REAL,
    /// non-integer text/blob coercion).
    sum_real: f64 = 0,
    /// Promotion latch: once true we accumulate into `sum_real` only.
    is_real: bool = false,
    /// Best non-NULL value seen so far for MIN/MAX. The bytes live in the
    /// per-statement arena passed to `feed` — arena lifetime spans the
    /// whole grouped SELECT, so this pointer stays valid until finalise.
    best: ?Value = null,

    const Kind = enum { count_star, count, sum, avg, min, max, total };

    fn init(kind: Kind) Aggregator {
        return .{ .kind = kind };
    }

    /// Try to parse `bytes` as an i64. Returns null if the text isn't a
    /// pure integer (matches sqlite3's "sum keeps integer mode while every
    /// contributor is an integer-shaped string" behaviour).
    fn parseIntStrict(bytes: []const u8) ?i64 {
        const trimmed = std.mem.trim(u8, bytes, " \t\r\n");
        if (trimmed.len == 0) return null;
        return std.fmt.parseInt(i64, trimmed, 10) catch null;
    }

    fn promoteToReal(self: *Aggregator) void {
        if (self.is_real) return;
        self.sum_real = @floatFromInt(self.sum_int);
        self.is_real = true;
    }

    /// Feed one row's contribution. `v` is owned by the caller. For MIN/MAX
    /// the kept best-value is duped into `arena` so the pointer remains
    /// valid after the caller frees `v`.
    fn feed(self: *Aggregator, arena: std.mem.Allocator, v: Value) Error!void {
        switch (self.kind) {
            .count_star => self.count += 1,
            .count => if (v != .null) {
                self.count += 1;
            },
            .sum, .avg, .total => {
                if (v == .null) return;
                self.count += 1;
                switch (v) {
                    .integer => |i| {
                        if (self.is_real) {
                            self.sum_real += @floatFromInt(i);
                        } else {
                            self.sum_int += i;
                        }
                    },
                    .real => |r| {
                        self.promoteToReal();
                        self.sum_real += r;
                    },
                    .text, .blob => |bytes| {
                        if (!self.is_real) {
                            if (parseIntStrict(bytes)) |as_int| {
                                self.sum_int += as_int;
                                return;
                            }
                            self.promoteToReal();
                        }
                        self.sum_real += func_util.parseFloatLoose(bytes);
                    },
                    .null => unreachable,
                }
            },
            .min, .max => {
                if (v == .null) return;
                if (self.best) |current| {
                    const order = ops.compareValues(current, v);
                    const replace = switch (self.kind) {
                        .min => order == .gt,
                        .max => order == .lt,
                        else => unreachable,
                    };
                    if (replace) {
                        ops.freeValue(arena, current);
                        self.best = try dupeArena(arena, v);
                    }
                } else {
                    self.best = try dupeArena(arena, v);
                }
            },
        }
    }

    /// Produce the finalised Value for this aggregator. TEXT/BLOB bytes are
    /// duped into `out_alloc` so the result outlives the accumulator's own
    /// `best` storage (callers free both independently).
    fn finalize(self: *Aggregator, out_alloc: std.mem.Allocator) Error!Value {
        return switch (self.kind) {
            .count_star, .count => Value{ .integer = @intCast(self.count) },
            .sum => blk: {
                if (self.count == 0) break :blk Value.null;
                if (self.is_real) break :blk Value{ .real = self.sum_real };
                if (self.sum_int < std.math.minInt(i64) or self.sum_int > std.math.maxInt(i64)) {
                    return Error.IntegerOverflow;
                }
                break :blk Value{ .integer = @intCast(self.sum_int) };
            },
            .total => blk: {
                if (self.count == 0) break :blk Value{ .real = 0 };
                if (self.is_real) break :blk Value{ .real = self.sum_real };
                break :blk Value{ .real = @floatFromInt(self.sum_int) };
            },
            .avg => blk: {
                if (self.count == 0) break :blk Value.null;
                const numer: f64 = if (self.is_real) self.sum_real else @floatFromInt(self.sum_int);
                break :blk Value{ .real = numer / @as(f64, @floatFromInt(self.count)) };
            },
            .min, .max => blk: {
                if (self.best) |current| break :blk dupeArena(out_alloc, current) catch |err| return err;
                break :blk Value.null;
            },
        };
    }
};

fn dupeArena(allocator: std.mem.Allocator, v: Value) !Value {
    return switch (v) {
        .text => |t| Value{ .text = try allocator.dupe(u8, t) },
        .blob => |b| Value{ .blob = try allocator.dupe(u8, b) },
        else => v,
    };
}

fn aggregatorFromCall(fc: ast.Expr.FuncCall) Aggregator {
    const kind: Aggregator.Kind = if (func_util.eqlIgnoreCase(fc.name, "count"))
        if (fc.args.len == 0) .count_star else .count
    else if (func_util.eqlIgnoreCase(fc.name, "sum")) .sum
    else if (func_util.eqlIgnoreCase(fc.name, "avg")) .avg
    else if (func_util.eqlIgnoreCase(fc.name, "total")) .total
    else if (func_util.eqlIgnoreCase(fc.name, "min")) .min
    else if (func_util.eqlIgnoreCase(fc.name, "max")) .max
    else unreachable;
    return Aggregator.init(kind);
}

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
    items: []const select.SelectItem,
    source_rows: []const []const Value,
    source_columns: []const []const u8,
    where_ast: ?*ast.Expr,
    group_by: []const *ast.Expr,
    having: ?*ast.Expr,
    pp: select_post.PostProcess,
) ![][]Value {
    if (select.containsStar(items)) return Error.SyntaxError;

    var agg_calls: std.ArrayList(*const ast.Expr) = .empty;
    defer agg_calls.deinit(alloc);
    try walk.collectAggregateCalls(alloc, items, having, pp.order_by, &agg_calls);

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
            };
            const cond = try eval.evalExpr(ctx, w);
            defer ops.freeValue(alloc, cond);
            if (!(ops.truthy(cond) orelse false)) continue;
        }

        const key = try evaluateGroupKey(alloc, group_by, row, source_columns);
        const group_idx = if (findGroup(groups.items, key)) |idx| blk: {
            // Existing group — discard the redundant key (arena reclaims).
            for (key) |v| ops.freeValue(alloc, v);
            alloc.free(key);
            break :blk idx;
        } else blk: {
            const idx = groups.items.len;
            try groups.append(alloc, .{
                .key = key,
                .aggs = try makeAggregators(alloc, agg_calls.items),
                .representative = row,
            });
            break :blk idx;
        };
        try feedRow(alloc, &groups.items[group_idx], agg_calls.items, row, source_columns);
    }

    // Implicit-group rule: if there are aggregates but no GROUP BY and no
    // rows survived WHERE, sqlite3 still emits a single row of finalised
    // aggregates (count → 0, others → NULL). We satisfy that by ensuring
    // exactly one group exists when group_by is empty.
    if (groups.items.len == 0 and group_by.len == 0 and agg_calls.items.len > 0) {
        try groups.append(alloc, .{
            .key = &.{},
            .aggs = try makeAggregators(alloc, agg_calls.items),
            .representative = &.{},
        });
    }

    return finaliseGroups(alloc, items, having, pp, agg_calls.items, groups.items, source_columns);
}

fn evaluateGroupKey(
    alloc: std.mem.Allocator,
    group_by: []const *ast.Expr,
    row: []const Value,
    columns: []const []const u8,
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
    };
    while (produced < group_by.len) : (produced += 1) {
        key[produced] = try eval.evalExpr(ctx, group_by[produced]);
    }
    return key;
}

fn findGroup(groups: []const Group, key: []const Value) ?usize {
    for (groups, 0..) |g, i| {
        if (groupKeysEqual(g.key, key)) return i;
    }
    return null;
}

/// sqlite3 GROUP BY collation: NULL == NULL within group key (one of the few
/// places NULL is treated as equal). Numeric values cross-compare via the
/// REAL coercion already in `compareValues`.
fn groupKeysEqual(a: []const Value, b: []const Value) bool {
    if (a.len != b.len) return false;
    for (a, b) |va, vb| {
        if (va == .null and vb == .null) continue;
        if (va == .null or vb == .null) return false;
        if (ops.compareValues(va, vb) != .eq) return false;
    }
    return true;
}

fn makeAggregators(alloc: std.mem.Allocator, calls: []const *const ast.Expr) ![]Aggregator {
    const aggs = try alloc.alloc(Aggregator, calls.len);
    for (calls, aggs) |call_expr, *slot| {
        slot.* = aggregatorFromCall(call_expr.*.func_call);
    }
    return aggs;
}

fn feedRow(
    alloc: std.mem.Allocator,
    group: *Group,
    calls: []const *const ast.Expr,
    row: []const Value,
    columns: []const []const u8,
) !void {
    const ctx = eval.EvalContext{
        .allocator = alloc,
        .current_row = row,
        .columns = columns,
    };
    for (calls, group.aggs) |call_expr, *agg| {
        const fc = call_expr.*.func_call;
        if (agg.kind == .count_star or fc.args.len == 0) {
            try agg.feed(alloc, Value{ .integer = 1 });
            continue;
        }
        const v = try eval.evalExpr(ctx, fc.args[0]);
        defer ops.freeValue(alloc, v);
        try agg.feed(alloc, v);
    }
}

fn finaliseGroups(
    alloc: std.mem.Allocator,
    items: []const select.SelectItem,
    having: ?*ast.Expr,
    pp: select_post.PostProcess,
    calls: []const *const ast.Expr,
    groups: []Group,
    source_columns: []const []const u8,
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
            .agg_values = &agg_map,
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
            const key = try evaluateAggOrderKey(alloc, pp.order_by, g.representative, source_columns, out_row, &agg_map);
            sort_keys.append(alloc, key) catch |err| {
                for (key) |v| ops.freeValue(alloc, v);
                alloc.free(key);
                return err;
            };
        }
    }

    if (pp.order_by.len > 0) {
        try select_post.sortRowsByKeys(alloc, rows.items, sort_keys.items, pp.order_by);
        for (sort_keys.items) |k| {
            for (k) |v| ops.freeValue(alloc, v);
            alloc.free(k);
        }
        sort_keys.deinit(alloc);
        sort_keys = .empty;
    }

    var all_rows = try rows.toOwnedSlice(alloc);
    if (pp.distinct) all_rows = select_post.dedupeRows(alloc, all_rows);
    return select_post.applyLimitOffset(alloc, all_rows, pp);
}

fn evaluateAggOrderKey(
    alloc: std.mem.Allocator,
    terms: []const select_post.OrderTerm,
    current_row: []const Value,
    columns: []const []const u8,
    projected_row: []const Value,
    agg_map: *const eval.AggregateValues,
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
        .agg_values = agg_map,
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
