//! Iter31.Z + .AA — non-recursive CTE materialisation. Split out of
//! `engine.zig` (Iter31.AB precondition: engine.zig had crossed the
//! 500-line discipline threshold). `engine.dispatchOne` calls
//! `materialize` before running the main SELECT; the produced
//! `TransientCte` slots live on the per-statement arena and are
//! published incrementally so a later CTE can reference an earlier
//! one through `engine_from.resolveSource`.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const stmt_mod = @import("stmt.zig");
const stmt_setop_mod = @import("stmt_setop.zig");
const database = @import("database.zig");
const engine = @import("engine.zig");
const stmt_from = @import("stmt_from.zig");
const select_post = @import("select_post.zig");
const func_util = @import("func_util.zig");

const Value = value_mod.Value;
const Database = database.Database;
const Error = ops.Error;

/// Hard cap on rows produced by a single recursive CTE. sqlite3 has a
/// `recursive_triggers` knob and an iteration counter; sqlite0 keeps a
/// fixed bound so a runaway query (forgotten WHERE) errors out rather
/// than OOMs. 10k rows handles realistic workloads (1..10000 series,
/// trees up to that depth) without unbounded growth.
const MAX_RECURSIVE_ROWS: usize = 10_000;

/// Execute each CTE body left-to-right and stash the result on
/// `db.transient_ctes`. A later CTE that references an earlier one
/// resolves through `engine_from.resolveSource` because we extend the
/// visible slice after each materialisation. All allocations live in
/// the per-statement arena (`alloc`); the caller's defer restores
/// `db.transient_ctes` to its prior value, after which the arena
/// teardown reclaims the rows.
pub fn materialize(
    db: *Database,
    alloc: std.mem.Allocator,
    ctes: []const stmt_mod.ParsedCte,
) Error!void {
    const slots = try alloc.alloc(database.TransientCte, ctes.len);
    var i: usize = 0;
    while (i < ctes.len) : (i += 1) {
        const cte = ctes[i];
        // Iter31.AD — recursive CTE detection. SELECT body that
        // self-references its own name through any FROM term goes
        // through the fixed-point loop in `materializeRecursive`;
        // everything else uses the simple eager path.
        const is_recursive = switch (cte.body) {
            .select => |ps| selectReferencesName(ps, cte.name),
            .values => false,
        };
        const rows_const, const columns_default = if (is_recursive)
            try materializeRecursive(db, alloc, slots, i, cte)
        else
            try materializeOneBody(db, alloc, cte.body);
        // Iter31.AA — `WITH t(c1, c2) AS (...)` overrides the body's
        // projected names. Width must match (sqlite3 errors with
        // "table t has N values for M columns"); we surface the same
        // condition as a SyntaxError to stay within the existing error
        // taxonomy.
        const columns: []const []const u8 = if (cte.column_names) |overrides| blk: {
            if (overrides.len != columns_default.len) return Error.SyntaxError;
            break :blk overrides;
        } else columns_default;
        slots[i] = .{
            .name = cte.name,
            .columns = columns,
            .rows = rows_const,
        };
        // Publish through `db.transient_ctes` so the next CTE in the
        // list can resolve a `.table_ref` against earlier names.
        db.transient_ctes = slots[0 .. i + 1];
    }
}

/// Execute one CTE body and return `(rows, default_columns)`. SELECT
/// path delegates to `engine.executeSelectWithColumns`. VALUES path
/// returns the eagerly-evaluated rows verbatim and synthesises
/// `column1`/`column2`/... via `stmt_from.synthesizeColumnNames`,
/// matching sqlite3's auto-naming. Both paths allocate in the
/// per-statement arena (`alloc`).
fn materializeOneBody(
    db: *Database,
    alloc: std.mem.Allocator,
    body: stmt_mod.CteBody,
) Error!struct { []const []const Value, []const []const u8 } {
    return switch (body) {
        .select => |ps| blk: {
            const result = try engine.executeSelectWithColumns(db, alloc, ps);
            const rows_const = try alloc.alloc([]const Value, result.rows.len);
            for (result.rows, rows_const) |row, *slot| slot.* = row;
            break :blk .{ rows_const, result.columns };
        },
        .values => |rows| blk: {
            const arity: usize = if (rows.len > 0) rows[0].len else 0;
            const cols = try stmt_from.synthesizeColumnNames(alloc, arity);
            const rows_const = try alloc.alloc([]const Value, rows.len);
            for (rows, rows_const) |row, *slot| slot.* = row;
            break :blk .{ rows_const, cols };
        },
    };
}

/// Iter31.AD — recursive CTE materialisation by fixed-point iteration.
/// Required body shape (mirrors sqlite3's recursive grammar at the
/// scope we ship): a SELECT with exactly one setop branch
/// (UNION / UNION ALL), where the leftmost (base) does NOT reference
/// the CTE name and the rightmost (recursive step) does. Other shapes
/// (multiple branches, INTERSECT/EXCEPT, recursion in base, no
/// recursion in step) collapse to `Error.SyntaxError`.
///
/// Algorithm:
///   1. Run base alone → initial rows R0 (using `branches=&.{}`).
///   2. result = R0; working_set = R0 (deduped if UNION).
///   3. Publish a TransientCte slot whose rows == working_set.
///   4. Loop:
///        - Run the recursive branch's SELECT (it sees `name` resolving
///          to the current working_set).
///        - For UNION ALL: append all new rows to result; working_set =
///          new rows.
///        - For UNION: filter new rows that aren't already in result;
///          append unique to result; working_set = unique.
///        - If working_set empty, stop.
///        - Update the slot's rows to working_set so the next iteration
///          sees only the just-produced rows (sqlite3 quirk: `name`
///          inside the recursive step holds *only* the rows added by the
///          previous step, not the cumulative result).
///   5. Return cumulative result + the SELECT's projected columns.
fn materializeRecursive(
    db: *Database,
    alloc: std.mem.Allocator,
    slots: []database.TransientCte,
    idx: usize,
    cte: stmt_mod.ParsedCte,
) Error!struct { []const []const Value, []const []const u8 } {
    const ps = switch (cte.body) {
        .select => |s| s,
        .values => return Error.SyntaxError,
    };
    if (ps.branches.len != 1) return Error.SyntaxError;
    const branch = ps.branches[0];
    const setop_kind = branch.kind;
    if (setop_kind != .union_all and setop_kind != .union_distinct) return Error.SyntaxError;
    if (selectReferencesName(branch.select, cte.name)) {
        // recursive step: OK
    } else return Error.SyntaxError;
    // Build a "base only" view of ps by stripping the setop branches
    // and any chain-level ORDER BY/LIMIT/OFFSET (those bind to the
    // whole recursive expression, not the base alone). Other fields
    // are borrowed; ps still owns the AST.
    const base_only: stmt_mod.ParsedSelect = .{
        .items = ps.items,
        .from = ps.from,
        .where = ps.where,
        .distinct = ps.distinct,
        .group_by = ps.group_by,
        .having = ps.having,
        .branches = &.{},
        .order_by = &.{},
        .limit = null,
        .offset = null,
    };
    const base_result = try engine.executeSelectWithColumns(db, alloc, base_only);
    const arity = base_result.columns.len;
    const kinds = try alloc.alloc(@import("ast.zig").CollationKind, arity);
    for (kinds) |*k| k.* = .binary;

    // Iter31.AD — column_names override is applied to the slot WHILE
    // we iterate, not just at the end, so the recursive step's column
    // refs (`SELECT n+1 FROM r WHERE n<5`) resolve into the renamed
    // schema. Width mismatch surfaces as SyntaxError to match sqlite3.
    const visible_columns: []const []const u8 = if (cte.column_names) |overrides| blk: {
        if (overrides.len != arity) return Error.SyntaxError;
        break :blk overrides;
    } else base_result.columns;

    var result_list: std.ArrayList([]const Value) = .empty;
    var working_set: [][]Value = base_result.rows;
    if (setop_kind == .union_distinct) {
        working_set = select_post.dedupeRowsKeepLast(alloc, working_set, kinds);
    }
    for (working_set) |row| try result_list.append(alloc, row);

    // Publish initial working_set so the recursive step's references
    // to `name` resolve via engine_from.resolveSource with the
    // user-visible (possibly renamed) column names.
    slots[idx] = .{ .name = cte.name, .columns = visible_columns, .rows = try toConstRows(alloc, working_set) };
    db.transient_ctes = slots[0 .. idx + 1];

    var iterations: usize = 0;
    while (working_set.len > 0) : (iterations += 1) {
        if (result_list.items.len > MAX_RECURSIVE_ROWS) return Error.SyntaxError;
        if (iterations > MAX_RECURSIVE_ROWS) return Error.SyntaxError;
        const step = try engine.executeSelect(db, alloc, branch.select);
        if (setop_kind == .union_all) {
            for (step) |row| try result_list.append(alloc, row);
            working_set = step;
        } else {
            // UNION: keep only rows not already in result.
            var kept: usize = 0;
            for (step) |row| {
                if (rowExistsInList(result_list.items, row, kinds)) continue;
                step[kept] = row;
                kept += 1;
            }
            const unique = step[0..kept];
            for (unique) |row| try result_list.append(alloc, row);
            working_set = unique;
        }
        slots[idx].rows = try toConstRows(alloc, working_set);
    }

    const rows_const = try result_list.toOwnedSlice(alloc);
    return .{ rows_const, visible_columns };
}

/// Allocate `[]const []const Value` parallel to a mutable rows slice
/// — Zig's type system distinguishes the const layers, so we copy the
/// outer slice (each entry is a borrowed inner slice; the bytes don't
/// move).
fn toConstRows(alloc: std.mem.Allocator, rows: [][]Value) ![]const []const Value {
    const out = try alloc.alloc([]const Value, rows.len);
    for (rows, out) |src, *slot| slot.* = src;
    return out;
}

fn rowExistsInList(rows: []const []const Value, row: []const Value, kinds: []const @import("ast.zig").CollationKind) bool {
    for (rows) |existing| {
        if (select_post.rowsEqual(existing, row, kinds)) return true;
    }
    return false;
}

/// Walk every FROM term (including subqueries / setop branches) of
/// `ps` and return true when any `.table_ref` matches `name`
/// (case-insensitive). Used by `materialize` to decide whether a CTE
/// body is recursive.
fn selectReferencesName(ps: stmt_mod.ParsedSelect, name: []const u8) bool {
    for (ps.from) |term| {
        switch (term.source) {
            .table_ref => |tr| if (func_util.eqlIgnoreCase(tr.name, name)) return true,
            .subquery => |sq| if (selectReferencesName(sq.select, name)) return true,
            .inline_values => {},
        }
    }
    for (ps.branches) |branch| {
        if (selectReferencesName(branch.select, name)) return true;
    }
    return false;
}
