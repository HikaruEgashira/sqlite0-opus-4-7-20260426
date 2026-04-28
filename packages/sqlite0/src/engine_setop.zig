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
const select_post = @import("select_post.zig");
const database = @import("database.zig");
const eval = @import("eval.zig");
const func_util = @import("func_util.zig");

const Value = value_mod.Value;
const SetopKind = stmt_mod.SetopKind;
const Error = ops.Error;
const Database = database.Database;

pub fn combine(
    arena: std.mem.Allocator,
    op: SetopKind,
    left: [][]Value,
    right: [][]Value,
) Error![][]Value {
    return switch (op) {
        .union_all => concat(arena, left, right),
        .union_distinct => select_post.dedupeRowsKeepLast(arena, try concat(arena, left, right)),
        .intersect => filterByMembership(arena, left, right, true),
        .except => filterByMembership(arena, left, right, false),
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
) Error![][]Value {
    // Both sides are dedup-replace-last first so identical-value rows on
    // either side don't multiply the result. (Verified against sqlite3:
    // `(VALUES (1), (1)) INTERSECT (VALUES (1))` yields a single row.)
    const left_dedup = select_post.dedupeRowsKeepLast(arena, left);
    const right_dedup = select_post.dedupeRowsKeepLast(arena, right);

    var kept: usize = 0;
    for (left_dedup) |row| {
        const member = rowExistsIn(right_dedup, row);
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

fn rowExistsIn(set: []const []Value, row: []const Value) bool {
    for (set) |existing| {
        if (select_post.rowsEqual(existing, row)) return true;
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
    outer_frames: []const eval.OuterFrame,
) Error![][]Value {
    const current = rows;
    if (order_by.len > 0 and current.len > 1) {
        const keys = try arena.alloc([]Value, current.len);
        const so_terms = try arena.alloc(select_post.OrderTerm, order_by.len);
        for (order_by, so_terms) |s, *o| {
            const resolved = try resolveSetopOrderPosition(s, leftmost_columns);
            const desc = s.dir == .desc;
            o.* = .{ .expr = s.expr, .position = resolved, .descending = desc, .nulls_first = s.nulls_first orelse !desc };
        }
        for (current, keys) |row, *slot| {
            const key = try arena.alloc(Value, order_by.len);
            for (so_terms, key) |term, *kv| {
                const pos = term.position.?;
                kv.* = if (pos > 0 and pos <= row.len) row[pos - 1] else Value.null;
            }
            slot.* = key;
        }
        try select_post.sortRowsByKeys(arena, current, keys, so_terms);
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
