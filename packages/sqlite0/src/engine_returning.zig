//! Iter31.AE — RETURNING clause projection for INSERT / UPDATE / DELETE.
//!
//! The DML execution paths (engine_dml.zig in-memory; file-mode paths
//! reject `parsed.returning != null` for now — Iter31.AE deferred (a))
//! call `projectRow` once per affected row at the right moment for the
//! statement's semantics (post-INSERT, post-UPDATE, pre-DELETE) and
//! accumulate the projected `[]Value` arrays into a list. The list is
//! handed to dispatchOne which deep-dupes to `db.allocator` before the
//! per-statement arena tears down.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const ast = @import("ast.zig");
const eval_mod = @import("eval.zig");
const select_mod = @import("select.zig");
const database = @import("database.zig");

const Value = value_mod.Value;
const Database = database.Database;
const Error = ops.Error;

/// Project a parsed RETURNING clause's items over a single row of the
/// affected table. The result `[]Value` lives in `arena` (the caller's
/// per-statement arena); contents are NOT duped — caller must deep-dupe
/// to long-lived memory before the arena tears down.
///
/// `*` expands to every column of the table (qualifier-less star). Bare
/// `t.*` with a qualifier matching the table name does the same; other
/// qualifiers reject (sqlite3: "no such table: <q>"). Each `expr` item
/// goes through `eval.evalExpr` with the row + column context, so any
/// expression valid in a SELECT projection works the same here.
pub fn projectRow(
    db: *Database,
    arena: std.mem.Allocator,
    items: []const select_mod.SelectItem,
    row: []const Value,
    table_name: []const u8,
    columns: []const []const u8,
    collations: []const ast.CollationKind,
) Error![]Value {
    // Build qualifiers parallel to columns so `<table>.<col>` references
    // resolve. Qualifier identical for every column (single-table DML).
    const qualifiers = try arena.alloc([]const u8, columns.len);
    for (qualifiers) |*q| q.* = table_name;

    var out: std.ArrayList(Value) = .empty;
    var produced: usize = 0;
    errdefer {
        for (out.items[0..produced]) |v| ops.freeValue(arena, v);
        out.deinit(arena);
    }
    for (items) |item| {
        switch (item) {
            .star => |q| {
                if (q) |want| {
                    if (!std.ascii.eqlIgnoreCase(want, table_name)) return Error.NoSuchTable;
                }
                for (row) |v| {
                    try out.append(arena, v);
                    produced += 1;
                }
            },
            .expr => |e| {
                const ctx = eval_mod.EvalContext{
                    .allocator = arena,
                    .current_row = row,
                    .columns = columns,
                    .column_qualifiers = qualifiers,
                    .column_collations = collations,
                    .db = db,
                };
                try out.append(arena, try eval_mod.evalExpr(ctx, e.expr));
                produced += 1;
            },
        }
    }
    return out.toOwnedSlice(arena);
}

/// Deep-dupe a list of arena-allocated returning rows to `long_alloc`
/// (typically `db.allocator`). Mirrors `engine.dupeRowsToLongLived` —
/// each TEXT/BLOB byte buffer is copied so the result outlives the
/// statement arena.
pub fn dupeRowsToLong(
    long_alloc: std.mem.Allocator,
    arena_rows: [][]Value,
) Error![][]Value {
    const out = try long_alloc.alloc([]Value, arena_rows.len);
    var produced: usize = 0;
    errdefer {
        for (out[0..produced]) |row| {
            for (row) |v| ops.freeValue(long_alloc, v);
            long_alloc.free(row);
        }
        long_alloc.free(out);
    }
    while (produced < arena_rows.len) : (produced += 1) {
        const src = arena_rows[produced];
        const new_row = try long_alloc.alloc(Value, src.len);
        var k: usize = 0;
        errdefer {
            for (new_row[0..k]) |v| ops.freeValue(long_alloc, v);
            long_alloc.free(new_row);
        }
        while (k < src.len) : (k += 1) {
            new_row[k] = try @import("func_util.zig").dupeValue(long_alloc, src[k]);
        }
        out[produced] = new_row;
    }
    return out;
}
