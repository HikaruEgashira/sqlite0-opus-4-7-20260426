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
const database = @import("database.zig");
const util = @import("func_util.zig");
const collation = @import("collation.zig");

const Value = value_mod.Value;
const Database = database.Database;

pub const OrderTerm = struct {
    expr: *ast.Expr,
    /// 1-based SELECT-list column reference (sqlite3 quirk: `ORDER BY 2`
    /// sorts by the second projected column rather than the constant `2`).
    /// `null` means "evaluate `expr` against the source row".
    position: ?usize = null,
    descending: bool,
    /// Resolved NULLS placement (translation layer applies the direction
    /// default — sqlite3: ASC→true, DESC→false — when the parser saw no
    /// explicit `NULLS FIRST` / `NULLS LAST`).
    nulls_first: bool = true,
    /// Iter31.O — collating sequence for TEXT-vs-TEXT key comparison.
    /// `null` = no explicit COLLATE wrapper; sort site falls back to a
    /// bare column-ref's schema collation (Iter31.R) before defaulting
    /// to `.binary`. `.binary` (some) means the user wrote
    /// `COLLATE BINARY` and that explicit choice locks BINARY in even
    /// when the column is declared NOCASE.
    collation: ?ast.CollationKind = null,
};

pub const PostProcess = struct {
    distinct: bool = false,
    order_by: []const OrderTerm = &.{},
    limit: ?*ast.Expr = null,
    offset: ?*ast.Expr = null,
};

/// In-place deduplication of `rows` by projected-row equality, keeping the
/// first occurrence. Frees dropped rows. Returns a slice into `rows[0..kept]`.
/// `kinds` (Iter31.Q) is parallel to row columns; empty slice = all BINARY.
/// Equality is sqlite3 DISTINCT semantics: NULL == NULL, INTEGER and REAL
/// numerically equal, TEXT under per-column collation, BLOB bytewise.
pub fn dedupeRows(allocator: std.mem.Allocator, rows: [][]Value, kinds: []const ast.CollationKind) [][]Value {
    if (rows.len <= 1) return rows;
    var kept: usize = 1;
    var i: usize = 1;
    while (i < rows.len) : (i += 1) {
        const candidate = rows[i];
        if (rowsContains(rows[0..kept], candidate, kinds)) {
            for (candidate) |v| ops.freeValue(allocator, v);
            allocator.free(candidate);
        } else {
            rows[kept] = candidate;
            kept += 1;
        }
    }
    return rows[0..kept];
}

/// Variant of `dedupeRows` for set operators (UNION/INTERSECT/EXCEPT). When
/// a duplicate is encountered, the earlier slot's row content is freed and
/// replaced by the new occurrence's row. Position = FIRST occurrence; content
/// = LAST. Matches sqlite3 UNION dedup (3.51.0: `SELECT 1 UNION SELECT 1.0`
/// returns `1.0`, `SELECT 1.0 UNION SELECT 1` returns `1` — last seen wins).
/// `kinds` (Iter31.Q) is parallel to row columns — set-op dedup picks the
/// "left-collated wins" kind across branches (computed by the engine before
/// each combine() call); empty slice falls back to BINARY.
pub fn dedupeRowsKeepLast(allocator: std.mem.Allocator, rows: [][]Value, kinds: []const ast.CollationKind) [][]Value {
    if (rows.len == 0) return rows;
    var kept: usize = 0;
    var i: usize = 0;
    while (i < rows.len) : (i += 1) {
        const candidate = rows[i];
        var matched_at: ?usize = null;
        for (rows[0..kept], 0..) |existing, idx| {
            if (rowsEqual(existing, candidate, kinds)) {
                matched_at = idx;
                break;
            }
        }
        if (matched_at) |idx| {
            for (rows[idx]) |v| ops.freeValue(allocator, v);
            allocator.free(rows[idx]);
            rows[idx] = candidate;
        } else {
            rows[kept] = candidate;
            kept += 1;
        }
    }
    return rows[0..kept];
}

fn rowsContains(rows: []const []Value, candidate: []const Value, kinds: []const ast.CollationKind) bool {
    for (rows) |row| {
        if (rowsEqual(row, candidate, kinds)) return true;
    }
    return false;
}

/// True when `a` and `b` would compare equal under sqlite3 DISTINCT / UNION
/// semantics: NULL matches NULL, INTEGER/REAL numerically (1 == 1.0), TEXT
/// under `kinds[i]` (or BINARY when `kinds` is empty / shorter), BLOB
/// bytewise, mixed types never match.
pub fn rowsEqual(a: []const Value, b: []const Value, kinds: []const ast.CollationKind) bool {
    if (a.len != b.len) return false;
    for (a, b, 0..) |va, vb, i| {
        const kind = if (i < kinds.len) kinds[i] else .binary;
        if (!valuesEqualForDistinct(va, vb, kind)) return false;
    }
    return true;
}

fn valuesEqualForDistinct(a: Value, b: Value, kind: ast.CollationKind) bool {
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
            .text => |bt| collation.compareTextCollated(at, bt, kind) == .eq,
            else => false,
        },
        .blob => |ab| switch (b) {
            .blob => |bb| std.mem.eql(u8, ab, bb),
            else => false,
        },
    };
}

/// Build per-column kinds from SELECT items, with Iter31.R schema-default
/// fall-back: explicit COLLATE wrapper > bare-column-ref's column-level
/// COLLATE > BINARY. Returns empty (= BINARY fallback) when items contains
/// `*` (star expansion arity is dynamic) or when items length doesn't match
/// row arity. Caller owns the returned slice.
pub fn extractDistinctCollations(
    allocator: std.mem.Allocator,
    items: []const @import("select.zig").SelectItem,
    row_arity: usize,
    source_columns: []const []const u8,
    source_qualifiers: []const []const u8,
    source_collations: []const ast.CollationKind,
) ![]ast.CollationKind {
    for (items) |item| if (item == .star) return &.{};
    if (items.len != row_arity) return &.{};
    const out = try allocator.alloc(ast.CollationKind, items.len);
    for (items, out) |item, *o| {
        o.* = collation.peekKind(item.expr.expr) orelse
            collation.columnDefault(item.expr.expr, source_columns, source_qualifiers, source_collations) orelse
            .binary;
    }
    return out;
}

/// Stable sort of `rows` by parallel `keys`, applying per-term direction
/// and collation. Uses an indirection array so the (larger) projected
/// rows aren't swapped during comparisons. Resolves each term's effective
/// collation up front: explicit wrapper > bare-column-ref schema default
/// > BINARY (Iter31.R).
///
/// Stability matters under COLLATE: sqlite3 preserves input order on
/// equal keys (verified `('A','a','B','b') ORDER BY column1 COLLATE
/// NOCASE` returns the input verbatim). `std.sort.pdq` is unstable;
/// `std.sort.block` is stable and the closest drop-in replacement.
pub fn sortRowsByKeys(
    allocator: std.mem.Allocator,
    rows: [][]Value,
    keys: [][]Value,
    terms: []const OrderTerm,
    source_columns: []const []const u8,
    source_qualifiers: []const []const u8,
    source_collations: []const ast.CollationKind,
) !void {
    std.debug.assert(rows.len == keys.len);
    const resolved = try allocator.alloc(ast.CollationKind, terms.len);
    defer allocator.free(resolved);
    for (terms, resolved) |t, *r| {
        r.* = t.collation orelse
            collation.columnDefault(t.expr, source_columns, source_qualifiers, source_collations) orelse
            .binary;
    }
    const indices = try allocator.alloc(usize, rows.len);
    defer allocator.free(indices);
    for (indices, 0..) |*slot, i| slot.* = i;

    const Ctx = struct {
        keys: [][]Value,
        terms: []const OrderTerm,
        resolved: []const ast.CollationKind,
        fn lessThan(self: @This(), a: usize, b: usize) bool {
            for (self.terms, 0..) |term, ti| {
                const va = self.keys[a][ti];
                const vb = self.keys[b][ti];
                const a_null = va == .null;
                const b_null = vb == .null;
                if (a_null and b_null) continue;
                if (a_null or b_null) {
                    return if (a_null) term.nulls_first else !term.nulls_first;
                }
                const cmp = compareValuesCollated(va, vb, self.resolved[ti]);
                if (cmp == 0) continue;
                return if (term.descending) cmp > 0 else cmp < 0;
            }
            return false;
        }
    };
    std.sort.block(usize, indices, Ctx{ .keys = keys, .terms = terms, .resolved = resolved }, Ctx.lessThan);

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
    db: ?*Database,
    rows: [][]Value,
    pp: PostProcess,
    outer_frames: []const eval.OuterFrame,
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
        .db = db,
        .outer_frames = outer_frames,
    };
    var skip: usize = 0;
    if (pp.offset) |e| {
        const v = try eval.evalExpr(ctx, e);
        defer ops.freeValue(allocator, v);
        skip = clampNonNegative(try coerceLimitStrict(v));
    }
    var keep: usize = std.math.maxInt(usize);
    if (pp.limit) |e| {
        const v = try eval.evalExpr(ctx, e);
        defer ops.freeValue(allocator, v);
        const n = try coerceLimitStrict(v);
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
/// -1/0/1 (3-way). `kind` selects the TEXT-vs-TEXT comparator —
/// `.binary` is the default, `.nocase` / `.rtrim` activate when the
/// per-term ORDER BY collation override is in effect (Iter31.O).
fn compareValuesCollated(a: Value, b: Value, kind: ast.CollationKind) i32 {
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
        2 => return switch (collation.compareTextCollated(a.text, b.text, kind)) {
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

/// Strict LIMIT/OFFSET integer coercion — mirrors sqlite3 OP_MustBeInt.
/// sqlite3 evaluates the LIMIT/OFFSET expr, applies NUMERIC affinity,
/// then requires the result to be representable as an i64 with no
/// fractional / non-numeric loss. Anything else raises SQLITE_MISMATCH
/// (20) "datatype mismatch" at step time.
///
/// Rules (vdbe.c OP_MustBeInt + applyNumericAffinity):
///   - INTEGER → as-is
///   - REAL → only if `r == @floor(r)` AND in [INT64_MIN, INT64_MAX] range.
///     Note INT64_MAX (2^63-1) cannot be represented exactly as f64; the
///     nearest f64 is 2^63 itself, which we reject. NaN/Inf → reject.
///   - TEXT → trim ASCII whitespace, then parse as a strict numeric
///     (no trailing garbage); resulting REAL must be whole + in range.
///     Empty / no-digit / `0x...` prefix → reject (sqlite3AtoF strict).
///   - BLOB / NULL → reject (sqlite3 OP_MustBeInt errors on non-numeric).
fn coerceLimitStrict(v: Value) ops.Error!i64 {
    return switch (v) {
        .null, .blob => ops.Error.DatatypeMismatch,
        .integer => |i| i,
        .real => |r| try realToI64Strict(r),
        .text => |t| blk: {
            const s = std.mem.trim(u8, t, " \t\n\r");
            if (s.len == 0) break :blk ops.Error.DatatypeMismatch;
            const r = util.parseFloatStrictOpt(s) orelse break :blk ops.Error.DatatypeMismatch;
            break :blk try realToI64Strict(r);
        },
    };
}

fn realToI64Strict(r: f64) ops.Error!i64 {
    if (std.math.isNan(r) or std.math.isInf(r)) return ops.Error.DatatypeMismatch;
    // i64 max = 2^63-1, but f64 mantissa is 52 bits — the nearest
    // f64 to 2^63-1 IS 2^63 (rounds up). So accept r in
    // [-2^63, 2^63) only; equality with 2^63 is out-of-range.
    if (r < -9223372036854775808.0 or r >= 9223372036854775808.0) return ops.Error.DatatypeMismatch;
    const i: i64 = @intFromFloat(r);
    if (@as(f64, @floatFromInt(i)) != r) return ops.Error.DatatypeMismatch;
    return i;
}

fn clampNonNegative(n: i64) usize {
    return if (n < 0) 0 else @intCast(n);
}
