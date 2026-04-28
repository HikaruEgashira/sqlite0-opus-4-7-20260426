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
const stmt_mod = @import("stmt.zig");
const stmt_dml = @import("stmt_dml.zig");
const eval = @import("eval.zig");
const database = @import("database.zig");
const engine = @import("engine.zig");
const func_util = @import("func_util.zig");
const engine_dml_file = @import("engine_dml_file.zig");
const engine_dml_insert_file = @import("engine_dml_insert_file.zig");
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

    var changed: u64 = 0;
    for (t.rows.items) |*row_ptr| {
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

/// Append rows from a parsed INSERT into the target table. Source rows come
/// from either eagerly-evaluated VALUES tuples or a per-row SELECT result;
/// both live in arena memory until `func_util.dupeValue` moves them.
///
/// When `parsed.columns` is non-null, source-row columns are projected into
/// the table-schema-shaped row by name (case-insensitive); table columns
/// not mentioned in the column list become NULL. Unknown column names or
/// arity mismatches return `SyntaxError`/`ColumnCountMismatch` before any
/// rows are appended (validation precedes mutation).
pub fn executeInsert(db: *Database, arena: std.mem.Allocator, parsed: stmt_mod.ParsedInsert) !DmlInternal {
    const t = try engine.lookupTable(db, db.allocator, parsed.table);
    // Iter29.A — sqlite_schema / sqlite_master are engine-managed.
    // Direct SQL mutation would corrupt page 1 (silent corruption);
    // sqlite3 rejects with "table sqlite_master may not be modified".
    if (t.is_system) return Error.UnsupportedFeature;

    const source_rows: [][]Value = switch (parsed.source) {
        .values => |rows| rows,
        .select => |ps| try engine.executeSelect(db, arena, ps),
    };

    const target_indices = try resolveColumnTargets(arena, t, parsed.columns);
    const source_arity: usize = if (parsed.columns) |cs| cs.len else t.columns.len;
    for (source_rows) |row| {
        if (row.len != source_arity) return Error.ColumnCountMismatch;
    }

    // File-mode tables route through the Pager + B-tree mutation path.
    // In-memory tables (root_page == 0) keep the existing ArrayList
    // append behaviour. The dispatch lives here so the caller-side
    // (engine.dispatchOne) doesn't need to know about the backend.
    if (t.root_page != 0) {
        if (parsed.returning != null) return Error.UnsupportedFeature; // Iter31.AE deferred (a)
        const rc = try engine_dml_insert_file.executeInsertFile(db, t, target_indices, source_rows);
        return .{ .rowcount = rc };
    }

    try t.rows.ensureUnusedCapacity(db.allocator, source_rows.len);
    if (t.ipk_column == null) {
        try t.rowids.ensureUnusedCapacity(db.allocator, source_rows.len);
    }
    var inserted: u64 = 0;
    // Iter28.fix — in-memory IPK auto-rowid. Mirrors Iter28's file-mode
    // chooseRowid: scan existing rows for the highest integer value in
    // the IPK column (default 0 for empty table), then bump for each
    // row whose IPK column is NULL after column-target resolution.
    // Explicit IPK values bump max_rowid forward so subsequent NULL
    // entries continue from there. sqlite3 returns 1 for the first
    // auto-assigned rowid in a fresh table; we match. Iter29.T —
    // non-IPK path computes max from the parallel `rowids` list, so
    // DELETE-from-end correctly reduces `max+1` next iteration.
    var max_rowid: i64 = if (t.ipk_column) |ipk|
        computeMaxIpkValue(t, ipk)
    else
        currentMaxImplicitRowid(t);
    errdefer {
        // Roll back any rows already appended in this call (ADR-0003 §1
        // all-or-nothing). Schema mutations from registerTable are not
        // rolled back; only same-call row appends are. For non-IPK
        // tables the parallel rowids entry is popped in lockstep.
        var i: u64 = 0;
        while (i < inserted) : (i += 1) {
            const last_idx = t.rows.items.len - 1;
            const undone_row = t.rows.items[last_idx];
            t.rows.items.len = last_idx;
            for (undone_row) |v| ops.freeValue(db.allocator, v);
            db.allocator.free(undone_row);
            if (t.ipk_column == null) {
                t.rowids.items.len -= 1;
            }
        }
    }
    // Iter29.S — track last-inserted rowid in a local that we commit
    // to `db.last_insert_rowid` only after the full for-loop succeeds.
    var last_rowid: i64 = db.last_insert_rowid;
    for (source_rows) |row| {
        const new_row = try db.allocator.alloc(Value, t.columns.len);
        var k: usize = 0;
        errdefer {
            for (new_row[0..k]) |v| ops.freeValue(db.allocator, v);
            db.allocator.free(new_row);
        }
        while (k < t.columns.len) : (k += 1) {
            new_row[k] = if (target_indices[k]) |src_idx|
                try func_util.dupeValue(db.allocator, row[src_idx])
            else
                Value.null;
        }
        if (t.ipk_column) |ipk| switch (new_row[ipk]) {
            .null => {
                max_rowid += 1;
                new_row[ipk] = .{ .integer = max_rowid };
                last_rowid = max_rowid;
            },
            .integer => |explicit| {
                if (explicit > max_rowid) max_rowid = explicit;
                last_rowid = explicit;
            },
            else => {},
        } else {
            max_rowid += 1;
            last_rowid = max_rowid;
        }
        try enforceNotNull(t, new_row);
        t.rows.appendAssumeCapacity(new_row);
        if (t.ipk_column == null) {
            t.rowids.appendAssumeCapacity(max_rowid);
        }
        inserted += 1;
    }
    if (inserted > 0) {
        db.last_insert_rowid = last_rowid;
    }

    // Iter31.AE — RETURNING projection over each newly-inserted row.
    // Done after the loop so partial-failure rollback (errdefer above)
    // doesn't leave behind rows whose RETURNING projection ran but
    // whose insertion was rolled back.
    var returning_rows: ?[][]Value = null;
    if (parsed.returning) |items| {
        const start_idx = t.rows.items.len - inserted;
        const arr = try arena.alloc([]Value, inserted);
        var i: usize = 0;
        while (i < inserted) : (i += 1) {
            const row = t.rows.items[start_idx + i];
            arr[i] = try engine_returning.projectRow(db, arena, items, row, parsed.table, t.columns, t.collations);
        }
        returning_rows = arr;
    }
    return .{ .rowcount = inserted, .returning = returning_rows };
}

fn currentMaxImplicitRowid(t: *const Table) i64 {
    var max_val: i64 = 0;
    for (t.rowids.items) |rid| {
        if (rid > max_val) max_val = rid;
    }
    return max_val;
}

fn computeMaxIpkValue(t: *const Table, ipk: usize) i64 {
    var max_val: i64 = 0;
    for (t.rows.items) |row| {
        if (ipk >= row.len) continue;
        switch (row[ipk]) {
            .integer => |n| if (n > max_val) {
                max_val = n;
            },
            else => {},
        }
    }
    return max_val;
}

/// Build a per-table-column slice mapping each table column index to either
/// the source row position that provides it (`?usize`) or null (meaning
/// "column omitted; use NULL"). When `cols` is null the mapping is the
/// identity (`[0, 1, 2, ...]`).
fn resolveColumnTargets(
    arena: std.mem.Allocator,
    t: *const Table,
    cols: ?[][]const u8,
) ![]?usize {
    const targets = try arena.alloc(?usize, t.columns.len);
    if (cols) |column_list| {
        @memset(targets, null);
        // Duplicate column names: sqlite3 silently keeps the FIRST mapping
        // and ignores later occurrences. Match that behavior — verified
        // against sqlite3 3.51.0 (`INSERT INTO t (a, a, b) VALUES (1, 2, 3)`
        // → row `(1, 3)`).
        for (column_list, 0..) |name, src_idx| {
            const tcol_idx = findTableColumn(t, name) orelse return Error.SyntaxError;
            if (targets[tcol_idx] == null) targets[tcol_idx] = src_idx;
        }
    } else {
        for (targets, 0..) |*slot, i| slot.* = i;
    }
    return targets;
}

fn findTableColumn(t: *const Table, name: []const u8) ?usize {
    for (t.columns, 0..) |col, i| {
        if (func_util.eqlIgnoreCase(name, col)) return i;
    }
    return null;
}

/// Iter29.B — reject if any column declared `NOT NULL` carries a NULL
/// value at this point. Called AFTER IPK auto-assignment so an IPK
/// column whose source NULL was rewritten to the next rowid passes.
/// Mirrors the file-mode equivalent in `engine_dml_insert_file` so
/// both backends produce the same rejection signal.
pub fn enforceNotNull(t: *const Table, row: []const Value) Error!void {
    for (t.not_null, row) |required, v| {
        if (required and v == .null) return Error.ConstraintNotNull;
    }
}

