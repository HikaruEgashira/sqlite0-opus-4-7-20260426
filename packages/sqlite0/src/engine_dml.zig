//! DML execution: DELETE / UPDATE / INSERT against `Database` state.
//!
//! Split out of `engine.zig` to keep that file under the 500-line discipline
//! (CLAUDE.md "Module Splitting Rules") after Iter21 added the
//! subquery-in-FROM column-derivation helpers. The split point is the
//! statement-kind boundary: this module handles the three table-mutating
//! statements; `engine.zig` keeps `dispatchOne` (the orchestrator) plus the
//! SELECT execution path.
//!
//! The all-or-nothing semantics for INSERT (rollback already-appended rows
//! on error, ADR-0003 §1) lives here. UPDATE intentionally stops at row-
//! level atomicity (consistent with sqlite3); DELETE is atomic at the
//! statement level via the survivors-list swap.
//!
//! File-mode (Pager-backed) DML lives in `engine_dml_file.zig`; the
//! public entry points here dispatch on `Table.root_page`.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const stmt_dml = @import("stmt_dml.zig");
const eval = @import("eval.zig");
const database = @import("database.zig");
const engine = @import("engine.zig");
const func_util = @import("func_util.zig");
const engine_dml_file = @import("engine_dml_file.zig");
const engine_returning = @import("engine_returning.zig");

const Value = value_mod.Value;
const Database = database.Database;
const Table = database.Table;
const Error = ops.Error;

/// Iter31.AE — internal DML result. `returning` lives in `arena`
/// (per-statement) when non-null; dispatchOne deep-dupes to
/// `db.allocator` before returning to the caller.
pub const DmlInternal = struct {
    rowcount: u64,
    returning: ?[][]Value = null,
};

/// Remove rows from `parsed.table` for which the WHERE predicate is truthy
/// (or all rows when WHERE is absent). Returns the count of deleted rows.
/// Mutation is performed by building a survivor list, so partial WHERE
/// failures leave the table unchanged (all-or-nothing — ADR-0003 §1).
pub fn executeDelete(db: *Database, arena: std.mem.Allocator, parsed: stmt_dml.ParsedDelete) !DmlInternal {
    const t = try engine.lookupTable(db, db.allocator, parsed.table);
    if (t.is_system) return Error.UnsupportedFeature; // Iter29.A
    // File-mode tables (root_page != 0) take the Pager + rebuild-page
    // path. The fork lives here so engine.dispatchOne stays oblivious
    // to backend choice — same shape as executeInsert.
    if (t.root_page != 0) {
        if (parsed.returning != null) return Error.UnsupportedFeature; // Iter31.AE deferred (a)
        const rc = try engine_dml_file.executeDeleteFile(db, t, parsed);
        return .{ .rowcount = rc };
    }
    // Build the per-table-row qualifier vector once: each column is
    // qualified by the (unaliased) table name so correlated subqueries can
    // reference `<table>.<col>` from the WHERE predicate's outer frame.
    const dml_qualifiers = try arena.alloc([]const u8, t.columns.len);
    for (dml_qualifiers) |*q| q.* = parsed.table;

    // Iter31.AE — accumulate projected RETURNING rows for each pre-delete
    // row state. Lives in arena; dispatchOne dupes to long-lived memory.
    var returning_rows: std.ArrayList([]Value) = .empty;
    errdefer returning_rows.deinit(arena);

    if (parsed.where) |w_ast| {
        var survivors: std.ArrayList([]Value) = .empty;
        errdefer survivors.deinit(arena);
        var survivor_rowids: std.ArrayList(i64) = .empty;
        errdefer survivor_rowids.deinit(arena);
        var to_free: std.ArrayList([]Value) = .empty;
        errdefer to_free.deinit(arena);

        const track_rowids = t.ipk_column == null;
        for (t.rows.items, 0..) |row, idx| {
            const ctx = eval.EvalContext{
                .allocator = arena,
                .current_row = row,
                .columns = t.columns,
                .column_qualifiers = dml_qualifiers,
                .column_collations = t.collations,
                .db = db,
            };
            const cond = try eval.evalExpr(ctx, w_ast);
            defer ops.freeValue(arena, cond);
            if (ops.truthy(cond) orelse false) {
                if (parsed.returning) |items| {
                    const projected = try engine_returning.projectRow(db, arena, items, row, parsed.table, t.columns, t.collations);
                    try returning_rows.append(arena, projected);
                }
                try to_free.append(arena, row);
            } else {
                try survivors.append(arena, row);
                if (track_rowids) {
                    try survivor_rowids.append(arena, t.rowids.items[idx]);
                }
            }
        }

        // Atomic swap: install survivors as the new row list, then free the
        // dropped rows. Iter29.T — non-IPK tables also swap the parallel
        // rowids list so DELETE-from-end correctly lowers max(rowid).
        const new_rows = try db.allocator.alloc([]Value, survivors.items.len);
        @memcpy(new_rows, survivors.items);
        const old_storage = t.rows;
        t.rows = .empty;
        try t.rows.ensureTotalCapacity(db.allocator, new_rows.len);
        for (new_rows) |row| t.rows.appendAssumeCapacity(row);
        var os = old_storage;
        os.deinit(db.allocator);
        db.allocator.free(new_rows);

        if (track_rowids) {
            const new_rowids = try db.allocator.alloc(i64, survivor_rowids.items.len);
            @memcpy(new_rowids, survivor_rowids.items);
            const old_rowid_storage = t.rowids;
            t.rowids = .empty;
            try t.rowids.ensureTotalCapacity(db.allocator, new_rowids.len);
            for (new_rowids) |rid| t.rowids.appendAssumeCapacity(rid);
            var ors = old_rowid_storage;
            ors.deinit(db.allocator);
            db.allocator.free(new_rowids);
        }

        for (to_free.items) |row| {
            for (row) |v| ops.freeValue(db.allocator, v);
            db.allocator.free(row);
        }
        const ret_slice: ?[][]Value = if (parsed.returning != null)
            try returning_rows.toOwnedSlice(arena)
        else
            null;
        return .{ .rowcount = to_free.items.len, .returning = ret_slice };
    } else {
        // DELETE without WHERE — drop everything. Capture RETURNING
        // rows over each row before tear-down.
        const count: u64 = t.rows.items.len;
        if (parsed.returning) |items| {
            for (t.rows.items) |row| {
                const projected = try engine_returning.projectRow(db, arena, items, row, parsed.table, t.columns, t.collations);
                try returning_rows.append(arena, projected);
            }
        }
        for (t.rows.items) |row| {
            for (row) |v| ops.freeValue(db.allocator, v);
            db.allocator.free(row);
        }
        t.rows.clearAndFree(db.allocator);
        if (t.ipk_column == null) {
            t.rowids.clearAndFree(db.allocator);
        }
        const ret_slice: ?[][]Value = if (parsed.returning != null)
            try returning_rows.toOwnedSlice(arena)
        else
            null;
        return .{ .rowcount = count, .returning = ret_slice };
    }
}

/// Apply assignments to rows where WHERE matches. New values are eagerly
/// evaluated into a scratch row, the old values are then freed and the
/// scratch row is moved into place. Errors during evaluation leave the
/// table unchanged for that row (and previously updated rows stay updated —
/// matching sqlite3's per-row UPDATE behavior, not per-statement).
pub fn executeUpdate(db: *Database, arena: std.mem.Allocator, parsed: stmt_dml.ParsedUpdate) !DmlInternal {
    const t = try engine.lookupTable(db, db.allocator, parsed.table);
    if (t.is_system) return Error.UnsupportedFeature; // Iter29.A

    // Resolve column indices once (validation precedes mutation per ADR-0003).
    const indices = try arena.alloc(usize, parsed.assignments.len);
    for (parsed.assignments, indices) |a, *idx| {
        idx.* = findTableColumn(t, a.column) orelse return Error.SyntaxError;
    }

    // File-mode tables (root_page != 0) take the rebuild-page path
    // (Iter26.A.2.b). The dispatch happens after column-index
    // validation so parse-time errors still surface uniformly.
    if (t.root_page != 0) {
        if (parsed.returning != null) return Error.UnsupportedFeature; // Iter31.AE deferred (a)
        const rc = try engine_dml_file.executeUpdateFile(db, t, parsed, indices);
        return .{ .rowcount = rc };
    }

    const dml_qualifiers = try arena.alloc([]const u8, t.columns.len);
    for (dml_qualifiers) |*q| q.* = parsed.table;

    // Iter31.AE — RETURNING accumulator. Projected POST-update rows.
    var returning_rows: std.ArrayList([]Value) = .empty;
    errdefer returning_rows.deinit(arena);

    // Iter31.AF — pre-locate the assignment slot (if any) that targets
    // the IPK column. `null` => UPDATE doesn't touch IPK; no UNIQUE check.
    const ipk_assignment_slot: ?usize = if (t.ipk_column) |ipk| blk: {
        for (indices, 0..) |col_idx, i| {
            if (col_idx == ipk) break :blk i;
        }
        break :blk null;
    } else null;

    var changed: u64 = 0;
    for (t.rows.items, 0..) |*row_ptr, row_idx| {
        if (parsed.where) |w_ast| {
            const ctx = eval.EvalContext{
                .allocator = arena,
                .current_row = row_ptr.*,
                .columns = t.columns,
                .column_qualifiers = dml_qualifiers,
                .column_collations = t.collations,
                .db = db,
            };
            const cond = try eval.evalExpr(ctx, w_ast);
            defer ops.freeValue(arena, cond);
            if (!(ops.truthy(cond) orelse false)) continue;
        }
        const ctx = eval.EvalContext{
            .allocator = arena,
            .current_row = row_ptr.*,
            .columns = t.columns,
            .column_qualifiers = dml_qualifiers,
            .column_collations = t.collations,
            .db = db,
        };
        const new_values = try arena.alloc(Value, parsed.assignments.len);
        var produced: usize = 0;
        errdefer {
            for (new_values[0..produced]) |v| ops.freeValue(arena, v);
        }
        while (produced < parsed.assignments.len) : (produced += 1) {
            new_values[produced] = try eval.evalExpr(ctx, parsed.assignments[produced].value);
        }
        // Iter29.B — NOT NULL check on the projected post-assignment
        // value of every NOT NULL column. Assignments can only land
        // NULL into the indices listed in `indices`; columns not in
        // `indices` keep their pre-UPDATE value (already valid). The
        // errdefer above frees the partially-populated `new_values`.
        for (indices, new_values) |col_idx, new_v| {
            if (t.not_null[col_idx] and new_v == .null) {
                return Error.ConstraintNotNull;
            }
        }
        // Iter31.AF — IPK UNIQUE check. Self-update (new == old) is fine;
        // otherwise scan every other row for the new IPK value.
        if (ipk_assignment_slot) |ai| {
            const ipk = t.ipk_column.?;
            const new_ipk_v = new_values[ai];
            if (new_ipk_v == .integer) {
                const new_ipk = new_ipk_v.integer;
                const cur_ipk_v = row_ptr.*[ipk];
                const same_as_self = cur_ipk_v == .integer and cur_ipk_v.integer == new_ipk;
                if (!same_as_self) {
                    for (t.rows.items, 0..) |other_row, j| {
                        if (j == row_idx) continue;
                        if (other_row[ipk] == .integer and other_row[ipk].integer == new_ipk) {
                            return Error.UniqueConstraint;
                        }
                    }
                }
            }
        }
        for (indices, new_values) |col_idx, new_v| {
            const duped = try func_util.dupeValue(db.allocator, new_v);
            ops.freeValue(db.allocator, row_ptr.*[col_idx]);
            row_ptr.*[col_idx] = duped;
        }
        for (new_values) |v| ops.freeValue(arena, v);
        // Iter31.AE — capture POST-update row state for RETURNING.
        if (parsed.returning) |items| {
            const projected = try engine_returning.projectRow(db, arena, items, row_ptr.*, parsed.table, t.columns, t.collations);
            try returning_rows.append(arena, projected);
        }
        changed += 1;
    }
    const ret_slice: ?[][]Value = if (parsed.returning != null)
        try returning_rows.toOwnedSlice(arena)
    else
        null;
    return .{ .rowcount = changed, .returning = ret_slice };
}

/// Re-export — Iter31.AG split executeInsert + helpers into
/// `engine_dml_insert.zig` to keep this file under the 500-line discipline.
pub const executeInsert = @import("engine_dml_insert.zig").executeInsert;
pub const enforceNotNull = @import("engine_dml_insert.zig").enforceNotNull;

fn findTableColumn(t: *const Table, name: []const u8) ?usize {
    for (t.columns, 0..) |col, i| {
        if (func_util.eqlIgnoreCase(name, col)) return i;
    }
    return null;
}


