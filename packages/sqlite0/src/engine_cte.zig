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
const database = @import("database.zig");
const engine = @import("engine.zig");
const stmt_from = @import("stmt_from.zig");

const Value = value_mod.Value;
const Database = database.Database;
const Error = ops.Error;

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
        const rows_const, const columns_default = try materializeOneBody(db, alloc, cte.body);
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
