//! ORDER BY sort + LIMIT/OFFSET post-processing for the SELECT pipeline.
//!
//! Extracted from `select.zig` so that file can stay under the 500-line
//! discipline (CLAUDE.md "Module Splitting Rules"). The split point is
//! "after rows are projected": `select.zig` produces the rows + per-row
//! sort keys, then this module sorts and slices them.
//!
//! The PostProcess struct is the public boundary: callers in
//! `engine.zig` build one and pass it through. `select.zig` consumes it
//! during its main row-loop and hands the result here.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const ast = @import("ast.zig");
const eval = @import("eval.zig");

const Value = value_mod.Value;

pub const OrderTerm = struct {
    expr: *ast.Expr,
    /// 1-based SELECT-list column reference (sqlite3 quirk: `ORDER BY 2`
    /// sorts by the second projected column rather than the constant `2`).
    /// `null` means "evaluate `expr` against the source row".
    position: ?usize = null,
    descending: bool,
};

pub const PostProcess = struct {
    distinct: bool = false,
    order_by: []const OrderTerm = &.{},
    limit: ?*ast.Expr = null,
    offset: ?*ast.Expr = null,
};

/// In-place deduplication of `rows` by projected-row equality, keeping the
/// first occurrence (so behavior is stable when called after `sortRowsByKeys`
/// or unsorted). Frees dropped rows. Returns a slice into `rows[0..kept]`.
///
/// Equality is sqlite3 DISTINCT semantics: NULL == NULL, all other classes
/// require same class + bytewise/numeric equality.
pub fn dedupeRows(allocator: std.mem.Allocator, rows: [][]Value) [][]Value {
    if (rows.len <= 1) return rows;
    var kept: usize = 1;
    var i: usize = 1;
    while (i < rows.len) : (i += 1) {
        const candidate = rows[i];
        if (rowsContains(rows[0..kept], candidate)) {
            for (candidate) |v| ops.freeValue(allocator, v);
            allocator.free(candidate);
        } else {
            rows[kept] = candidate;
            kept += 1;
        }
    }
    return rows[0..kept];
}

fn rowsContains(rows: []const []Value, candidate: []const Value) bool {
    for (rows) |row| {
        if (rowsEqual(row, candidate)) return true;
    }
    return false;
}

fn rowsEqual(a: []const Value, b: []const Value) bool {
    if (a.len != b.len) return false;
    for (a, b) |va, vb| {
        if (!valuesEqualForDistinct(va, vb)) return false;
    }
    return true;
}

fn valuesEqualForDistinct(a: Value, b: Value) bool {
    return switch (a) {
        .null => b == .null,
        .integer => |ai| switch (b) {
            .integer => |bi| ai == bi,
            .real => |br| @as(f64, @floatFromInt(ai)) == br,
            else => false,
        },
        .real => |ar| switch (b) {
            .integer => |bi| ar == @as(f64, @floatFromInt(bi)),
            .real => |br| ar == br,
            else => false,
        },
        .text => |at| switch (b) {
            .text => |bt| std.mem.eql(u8, at, bt),
            else => false,
        },
        .blob => |ab| switch (b) {
            .blob => |bb| std.mem.eql(u8, ab, bb),
            else => false,
        },
    };
}

/// Stable sort of `rows` by parallel `keys`, applying per-term direction.
/// Uses an indirection array so the (larger) projected rows aren't swapped
/// during comparisons.
pub fn sortRowsByKeys(
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

    const scratch = try allocator.alloc([]Value, rows.len);
    defer allocator.free(scratch);
    for (indices, 0..) |src_idx, dst_idx| scratch[dst_idx] = rows[src_idx];
    @memcpy(rows, scratch);
}

/// Apply `LIMIT N OFFSET M` to `rows`. Takes ownership of `rows` — on
/// success returns either the same slice (when no clipping happens) or a
/// new shorter slice with the dropped rows freed in between.
pub fn applyLimitOffset(
    allocator: std.mem.Allocator,
    rows: [][]Value,
    pp: PostProcess,
) ![][]Value {
    if (pp.limit == null and pp.offset == null) return rows;
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

/// Compare two Values for ORDER BY using sqlite3 storage-class ordering:
/// NULL < numeric (INTEGER/REAL coerced to f64) < TEXT < BLOB. Returns
/// -1/0/1 (3-way).
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
        2 => return switch (std.mem.order(u8, a.text, b.text)) {
            .lt => -1,
            .eq => 0,
            .gt => 1,
        },
        3 => return switch (std.mem.order(u8, a.blob, b.blob)) {
            .lt => -1,
            .eq => 0,
            .gt => 1,
        },
        else => unreachable,
    }
}

fn classOrder(v: Value) u8 {
    return switch (v) {
        .null => 0,
        .integer => 1,
        .real => 1, // INTEGER and REAL share a numeric class for sort
        .text => 2,
        .blob => 3,
    };
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
