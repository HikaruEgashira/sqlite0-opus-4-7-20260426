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

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const stmt_mod = @import("stmt.zig");
const stmt_dml = @import("stmt_dml.zig");
const eval = @import("eval.zig");
const database = @import("database.zig");
const engine = @import("engine.zig");
const func_util = @import("func_util.zig");
const btree = @import("btree.zig");
const btree_insert = @import("btree_insert.zig");
const record = @import("record.zig");
const record_encode = @import("record_encode.zig");
const pager_mod = @import("pager.zig");

const Value = value_mod.Value;
const Database = database.Database;
const Table = database.Table;
const Error = ops.Error;

/// Remove rows from `parsed.table` for which the WHERE predicate is truthy
/// (or all rows when WHERE is absent). Returns the count of deleted rows.
/// Mutation is performed by building a survivor list, so partial WHERE
/// failures leave the table unchanged (all-or-nothing — ADR-0003 §1).
pub fn executeDelete(db: *Database, arena: std.mem.Allocator, parsed: stmt_dml.ParsedDelete) !u64 {
    const t = try engine.lookupTable(db, db.allocator, parsed.table);
    // Build the per-table-row qualifier vector once: each column is
    // qualified by the (unaliased) table name so correlated subqueries can
    // reference `<table>.<col>` from the WHERE predicate's outer frame.
    const dml_qualifiers = try arena.alloc([]const u8, t.columns.len);
    for (dml_qualifiers) |*q| q.* = parsed.table;
    if (parsed.where) |w_ast| {
        var survivors: std.ArrayList([]Value) = .empty;
        errdefer survivors.deinit(arena);
        var to_free: std.ArrayList([]Value) = .empty;
        errdefer to_free.deinit(arena);

        for (t.rows.items) |row| {
            const ctx = eval.EvalContext{
                .allocator = arena,
                .current_row = row,
                .columns = t.columns,
                .column_qualifiers = dml_qualifiers,
                .db = db,
            };
            const cond = try eval.evalExpr(ctx, w_ast);
            defer ops.freeValue(arena, cond);
            if (ops.truthy(cond) orelse false) {
                try to_free.append(arena, row);
            } else {
                try survivors.append(arena, row);
            }
        }

        // Atomic swap: install survivors as the new row list, then free the
        // dropped rows.
        const new_rows = try db.allocator.alloc([]Value, survivors.items.len);
        @memcpy(new_rows, survivors.items);
        const old_storage = t.rows;
        t.rows = .empty;
        try t.rows.ensureTotalCapacity(db.allocator, new_rows.len);
        for (new_rows) |row| t.rows.appendAssumeCapacity(row);
        var os = old_storage;
        os.deinit(db.allocator);
        db.allocator.free(new_rows);

        for (to_free.items) |row| {
            for (row) |v| ops.freeValue(db.allocator, v);
            db.allocator.free(row);
        }
        return to_free.items.len;
    } else {
        // DELETE without WHERE — drop everything.
        const count: u64 = t.rows.items.len;
        for (t.rows.items) |row| {
            for (row) |v| ops.freeValue(db.allocator, v);
            db.allocator.free(row);
        }
        t.rows.clearAndFree(db.allocator);
        return count;
    }
}

/// Apply assignments to rows where WHERE matches. New values are eagerly
/// evaluated into a scratch row, the old values are then freed and the
/// scratch row is moved into place. Errors during evaluation leave the
/// table unchanged for that row (and previously updated rows stay updated —
/// matching sqlite3's per-row UPDATE behavior, not per-statement).
pub fn executeUpdate(db: *Database, arena: std.mem.Allocator, parsed: stmt_dml.ParsedUpdate) !u64 {
    const t = try engine.lookupTable(db, db.allocator, parsed.table);

    // Resolve column indices once (validation precedes mutation per ADR-0003).
    const indices = try arena.alloc(usize, parsed.assignments.len);
    for (parsed.assignments, indices) |a, *idx| {
        idx.* = findTableColumn(t, a.column) orelse return Error.SyntaxError;
    }

    const dml_qualifiers = try arena.alloc([]const u8, t.columns.len);
    for (dml_qualifiers) |*q| q.* = parsed.table;

    var changed: u64 = 0;
    for (t.rows.items) |*row_ptr| {
        if (parsed.where) |w_ast| {
            const ctx = eval.EvalContext{
                .allocator = arena,
                .current_row = row_ptr.*,
                .columns = t.columns,
                .column_qualifiers = dml_qualifiers,
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
        for (indices, new_values) |col_idx, new_v| {
            const duped = try func_util.dupeValue(db.allocator, new_v);
            ops.freeValue(db.allocator, row_ptr.*[col_idx]);
            row_ptr.*[col_idx] = duped;
        }
        for (new_values) |v| ops.freeValue(arena, v);
        changed += 1;
    }
    return changed;
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
pub fn executeInsert(db: *Database, arena: std.mem.Allocator, parsed: stmt_mod.ParsedInsert) !u64 {
    const t = try engine.lookupTable(db, db.allocator, parsed.table);

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
        return executeInsertFile(db, arena, t, target_indices, source_rows);
    }

    try t.rows.ensureUnusedCapacity(db.allocator, source_rows.len);
    var inserted: u64 = 0;
    errdefer {
        // Roll back any rows already appended in this call (ADR-0003 §1
        // all-or-nothing). Schema mutations from registerTable are not
        // rolled back; only same-call row appends are.
        var i: u64 = 0;
        while (i < inserted) : (i += 1) {
            const last_idx = t.rows.items.len - 1;
            const undone_row = t.rows.items[last_idx];
            t.rows.items.len = last_idx;
            for (undone_row) |v| ops.freeValue(db.allocator, v);
            db.allocator.free(undone_row);
        }
    }
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
        t.rows.appendAssumeCapacity(new_row);
        inserted += 1;
    }
    return inserted;
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

/// File-mode INSERT (Iter26.A.1): build a working copy of the table's
/// root page, insert each new row's cell into it via
/// `btree_insert.insertLeafTableCell`, then commit the whole batch with
/// one `Pager.writePage`. All-or-nothing per ADR-0003 §1: if any row
/// triggers `.page_full` (or any other failure), the working buffer is
/// discarded and the on-disk page is untouched.
///
/// Restrictions for Iter26.A.1:
///   - root page must be a leaf-table (multi-page B-trees → split path
///     not yet implemented; returns `Error.UnsupportedFeature`).
///   - no per-page split (single-page tables only); `.page_full`
///     returns `Error.UnsupportedFeature` so the user knows the limit.
///   - rowid auto-assigned as `max(existing_rowids) + 1` (or 1 if the
///     leaf is empty). Explicit rowid via INTEGER PRIMARY KEY alias is
///     a future iteration.
fn executeInsertFile(
    db: *Database,
    _: std.mem.Allocator,
    t: *Table,
    target_indices: []?usize,
    source_rows: []const []Value,
) !u64 {
    const pager = if (db.pager) |*pp| pp else return Error.IoError;

    // The "arena" param threading from `Database.execute` is actually
    // `db.allocator` (no per-statement arena yet — see ADR-0003 §8 for
    // the planned shape). Until that lands, manage our own scratch
    // arena locally so the work buffer + encoded records get released
    // even on the error paths.
    var scratch = std.heap.ArenaAllocator.init(db.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    const original = try pager.getPage(t.root_page);
    const work = try a.alloc(u8, original.len);
    @memcpy(work, original);

    const header_offset = btree.pageHeaderOffset(t.root_page);
    const header = try btree.parsePageHeader(work, header_offset);
    if (header.page_type != .leaf_table) return Error.UnsupportedFeature;

    // Find current max rowid by parsing existing cells. Linear scan over
    // the cell pointer array is fine for Iter26.A.1's small fixtures.
    var max_rowid: i64 = 0;
    {
        const cells = try btree.parseLeafTablePage(a, work, header_offset, pager_mod.PAGE_SIZE);
        for (cells) |c| {
            if (c.rowid > max_rowid) max_rowid = c.rowid;
        }
    }

    var inserted: u64 = 0;
    for (source_rows) |row| {
        const new_values = try a.alloc(Value, t.columns.len);
        for (new_values, 0..) |*slot, k| {
            slot.* = if (target_indices[k]) |src_idx| row[src_idx] else Value.null;
        }

        const rec = try record_encode.encodeRecord(a, new_values);
        max_rowid += 1;
        const outcome = try btree_insert.insertLeafTableCell(
            work,
            header_offset,
            pager_mod.PAGE_SIZE,
            max_rowid,
            rec,
        );
        switch (outcome) {
            .ok => inserted += 1,
            .page_full => return Error.UnsupportedFeature, // Iter26.B
        }
    }

    // Commit: one writePage call lands the entire batch atomically with
    // respect to the cache. The on-disk write is a single pwrite — torn
    // writes are still possible at the OS layer, which is the price we
    // pay for "no fsync" until Phase 4.
    try pager.writePage(t.root_page, work);
    return inserted;
}
