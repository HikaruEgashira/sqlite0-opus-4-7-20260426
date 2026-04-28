//! Set-operator combining (Iter20). After each branch SELECT executes,
//! `combine` folds its rows into the accumulated left side using the
//! requested operator. `applySetopPostProcess` then sorts and slices the
//! combined result with the chain-level ORDER BY / LIMIT / OFFSET.
//!
//! All allocations live in the per-statement arena; freeing dropped rows is
//! a hygienic gesture (the arena reclaims everything at statement teardown).

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const ast = @import("ast.zig");
const stmt_mod = @import("stmt.zig");
const select_mod = @import("select.zig");
const select_post = @import("select_post.zig");
const collation = @import("collation.zig");
const database = @import("database.zig");
const eval = @import("eval.zig");
const func_util = @import("func_util.zig");

const Value = value_mod.Value;
const SetopKind = stmt_mod.SetopKind;
const Error = ops.Error;
const Database = database.Database;

/// Per-column dedup collation tracker for a set-op chain (Iter31.Q).
/// Implements sqlite3's "leftmost branch with ANY explicit COLLATE wins"
/// precedence: explicit BINARY (some(.binary)) locks the column out of
/// later-branch upgrades, while no wrapper (null) yields to the next
/// branch's choice. `resolve` snapshots the current state into a
/// `[]CollationKind` (defaulting null → .binary) for `combine`.
pub const SetopKinds = struct {
    opt: []?ast.CollationKind = &.{},

    pub fn seed(self: *SetopKinds, alloc: std.mem.Allocator, items: []const select_mod.SelectItem, arity: usize) !void {
        if (self.opt.len > 0) return;
        self.opt = try extractItemKindsOpt(alloc, items, arity);
    }

    pub fn merge(self: *SetopKinds, alloc: std.mem.Allocator, items: []const select_mod.SelectItem, arity: usize) !void {
        if (self.opt.len == 0) {
            self.opt = try extractItemKindsOpt(alloc, items, arity);
            return;
        }
        const right = try extractItemKindsOpt(alloc, items, arity);
        for (self.opt, 0..) |k, i| {
            if (k == null and i < right.len) self.opt[i] = right[i];
        }
    }

    pub fn resolve(self: *const SetopKinds, alloc: std.mem.Allocator) ![]ast.CollationKind {
        const out = try alloc.alloc(ast.CollationKind, self.opt.len);
        for (self.opt, out) |k, *r| r.* = k orelse .binary;
        return out;
    }
};

fn extractItemKindsOpt(
    alloc: std.mem.Allocator,
    items: []const select_mod.SelectItem,
    row_arity: usize,
) ![]?ast.CollationKind {
    for (items) |item| if (item == .star) return &.{};
    if (items.len != row_arity) return &.{};
    const out = try alloc.alloc(?ast.CollationKind, items.len);
    for (items, out) |item, *o| {
        o.* = collation.peekKind(item.expr.expr);
    }
    return out;
}

pub fn combine(
    arena: std.mem.Allocator,
    op: SetopKind,
    left: [][]Value,
    right: [][]Value,
    kinds: []const ast.CollationKind,
) Error![][]Value {
    return switch (op) {
        .union_all => concat(arena, left, right),
        .union_distinct => select_post.dedupeRowsKeepLast(arena, try concat(arena, left, right), kinds),
        .intersect => filterByMembership(arena, left, right, true, kinds),
        .except => filterByMembership(arena, left, right, false, kinds),
    };
}

fn concat(arena: std.mem.Allocator, left: [][]Value, right: [][]Value) Error![][]Value {
    const out = try arena.alloc([]Value, left.len + right.len);
    @memcpy(out[0..left.len], left);
    @memcpy(out[left.len..], right);
    return out;
}

fn filterByMembership(
    arena: std.mem.Allocator,
    left: [][]Value,
    right: [][]Value,
    keep_when_member: bool,
    kinds: []const ast.CollationKind,
) Error![][]Value {
    // Both sides are dedup-replace-last first so identical-value rows on
    // either side don't multiply the result. (Verified against sqlite3:
    // `(VALUES (1), (1)) INTERSECT (VALUES (1))` yields a single row.)
    const left_dedup = select_post.dedupeRowsKeepLast(arena, left, kinds);
    const right_dedup = select_post.dedupeRowsKeepLast(arena, right, kinds);

    var kept: usize = 0;
    for (left_dedup) |row| {
        const member = rowExistsIn(right_dedup, row, kinds);
        if (member == keep_when_member) {
            left_dedup[kept] = row;
            kept += 1;
        } else {
            for (row) |v| ops.freeValue(arena, v);
            arena.free(row);
        }
    }
    for (right_dedup) |row| {
        for (row) |v| ops.freeValue(arena, v);
        arena.free(row);
    }
    return left_dedup[0..kept];
}

fn rowExistsIn(set: []const []Value, row: []const Value, kinds: []const ast.CollationKind) bool {
    for (set) |existing| {
        if (select_post.rowsEqual(existing, row, kinds)) return true;
    }
    return false;
}

/// Apply chain-level ORDER BY / LIMIT / OFFSET to combined setop rows.
///
/// ORDER BY in a setop chain is restricted to two forms (matching sqlite3):
///   - `ORDER BY <position>` — `ORDER BY 1` sorts by the first projected column.
///   - `ORDER BY <name>` — bare column-ref whose name appears in the leftmost
///     branch's projection. Resolution is case-insensitive (`ORDER BY X`
///     matches column `x`); ties resolve to the first matching column.
///
/// Other expressions (`ORDER BY abs(1)`, `ORDER BY a + b`) are rejected.
/// sqlite3 reports "1st ORDER BY term does not match any column in the
/// result set" for these — we surface the same case as `SyntaxError`.
pub fn applySetopPostProcess(
    arena: std.mem.Allocator,
    db: ?*Database,
    rows: [][]Value,
    order_by: []const stmt_mod.OrderTerm,
    limit: ?*ast.Expr,
    offset: ?*ast.Expr,
    leftmost_columns: []const []const u8,
    leftmost_qualifiers: []const []const u8,
    leftmost_collations: []const ast.CollationKind,
    outer_frames: []const eval.OuterFrame,
) Error![][]Value {
    const current = rows;
    if (order_by.len > 0 and current.len > 1) {
        const keys = try arena.alloc([]Value, current.len);
        const so_terms = try arena.alloc(select_post.OrderTerm, order_by.len);
        for (order_by, so_terms) |s, *o| {
            const resolved = try resolveSetopOrderPosition(s, leftmost_columns);
            const desc = s.dir == .desc;
            o.* = .{ .expr = s.expr, .position = resolved, .descending = desc, .nulls_first = s.nulls_first orelse !desc, .collation = s.collation };
        }
        for (current, keys) |row, *slot| {
            const key = try arena.alloc(Value, order_by.len);
            for (so_terms, key) |term, *kv| {
                const pos = term.position.?;
                kv.* = if (pos > 0 and pos <= row.len) row[pos - 1] else Value.null;
            }
            slot.* = key;
        }
        // Setop chain ORDER BY collation: explicit wrapper wins, then
        // leftmost branch's per-projection-item column-default collation
        // (sqlite3 carries column-level NOCASE/RTRIM through UNION ALL —
        // verified `SELECT x FROM t COLLATE NOCASE UNION ... ORDER BY x`).
        try select_post.sortRowsByKeys(arena, current, keys, so_terms, leftmost_columns, leftmost_qualifiers, leftmost_collations);
    }
    const pp = select_post.PostProcess{
        .distinct = false,
        .order_by = &.{},
        .limit = limit,
        .offset = offset,
    };
    return select_post.applyLimitOffset(arena, db, current, pp, outer_frames);
}

/// Reduce a setop ORDER BY term to a 1-based column position. Position-form
/// passes through; bare column-refs resolve against `leftmost_columns`
/// (case-insensitive, first match wins). Anything else is a syntax error
/// — sqlite3's "does not match any column in the result set" surfaces here.
fn resolveSetopOrderPosition(
    term: stmt_mod.OrderTerm,
    leftmost_columns: []const []const u8,
) Error!usize {
    if (term.position) |p| return p;
    switch (term.expr.*) {
        .column_ref => |c| {
            // Qualified refs (`t.x`) never match — once we've left every
            // branch's source scope, table aliases no longer exist.
            if (c.qualifier != null) return Error.SyntaxError;
            for (leftmost_columns, 1..) |col, idx| {
                if (func_util.eqlIgnoreCase(c.name, col)) return idx;
            }
            return Error.SyntaxError;
        },
        else => return Error.SyntaxError,
    }
}
