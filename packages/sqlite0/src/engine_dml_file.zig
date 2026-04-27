//! File-mode DML helpers (Iter26.A.1 / .A.2). All three functions
//! share the same shape:
//!
//!   1. Open a scratch arena (Database.execute doesn't yet provide a
//!      per-statement arena — see ADR-0003 §8 for the planned shape).
//!   2. Snapshot the table's root page into a local working buffer.
//!   3. Mutate the working buffer (insert / rebuild).
//!   4. Single `Pager.writePage` commit. All-or-nothing per ADR-0003
//!      §1: any error before step 4 leaves the on-disk page untouched.
//!
//! Split out of `engine_dml.zig` to keep that file under the 500-line
//! discipline (CLAUDE.md "Module Splitting Rules") after Iter26.A.2
//! added DELETE/UPDATE bodies. The in-memory DML paths stay in
//! `engine_dml.zig`; this module is reached only through
//! `executeInsertFile` / `executeDeleteFile` / `executeUpdateFile` from
//! the public DML entry points there.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const stmt_mod = @import("stmt.zig");
const stmt_dml = @import("stmt_dml.zig");
const eval = @import("eval.zig");
const database = @import("database.zig");
const btree = @import("btree.zig");
const btree_insert = @import("btree_insert.zig");
const btree_split = @import("btree_split.zig");
const record = @import("record.zig");
const record_encode = @import("record_encode.zig");
const pager_mod = @import("pager.zig");

const Value = value_mod.Value;
const Database = database.Database;
const Table = database.Table;
const Error = ops.Error;

/// File-mode INSERT (Iter26.A.1 / .B.1): merge new rows into the
/// existing rowid-sorted cell list, then either rebuild the root leaf
/// in place (single-page case) or balance-deeper into a new interior
/// root with two leaf children (Iter26.B.1).
///
/// Restrictions:
///   - root page must currently be a leaf-table. Multi-page B-trees
///     (interior root) need non-root leaf split, which is Iter26.B.2.
///   - rowid auto-assigned as `max(existing_rowids) + 1` (or 1 if the
///     leaf is empty). Explicit rowid via INTEGER PRIMARY KEY alias is
///     a future iteration.
///   - records exceeding `usable_size − 35` need an overflow chain
///     (Iter26.C); rejected with `Error.UnsupportedFeature` here so the
///     user sees the limit instead of a cryptic IoError.
pub fn executeInsertFile(
    db: *Database,
    t: *Table,
    target_indices: []const ?usize,
    source_rows: []const []Value,
) !u64 {
    const pager = if (db.pager) |*pp| pp else return Error.IoError;

    var scratch = std.heap.ArenaAllocator.init(db.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    // Real usable area: PAGE_SIZE − reserved tail (file header byte 20).
    // sqlite3's CLI emits 12 bytes of reserved space by default; passing
    // PAGE_SIZE here would let us write cells into the reserved region
    // and trip integrity_check.
    const usable_size = try pager.usableSize();

    const original = try pager.getPage(t.root_page);
    const work = try a.alloc(u8, original.len);
    @memcpy(work, original);

    const header_offset = btree.pageHeaderOffset(t.root_page);
    const header = try btree.parsePageHeader(work, header_offset);
    if (header.page_type != .leaf_table) return Error.UnsupportedFeature;

    // Parse existing cells and dupe their record bytes into the scratch
    // arena: `work` will be overwritten by the rebuild / balance-deeper
    // path, and the source slices borrow from it.
    const existing = try btree.parseLeafTablePage(a, work, header_offset, usable_size);
    var combined: std.ArrayList(btree_insert.RebuildCell) = .empty;
    var max_rowid: i64 = 0;
    for (existing) |c| {
        const dup = try a.dupe(u8, c.record_bytes);
        try combined.append(a, .{ .rowid = c.rowid, .record_bytes = dup });
        if (c.rowid > max_rowid) max_rowid = c.rowid;
    }

    // Encode each new row, assign monotonically-increasing rowid, append
    // (still sorted because rowid > all existing).
    var inserted: u64 = 0;
    for (source_rows) |row| {
        const new_values = try a.alloc(Value, t.columns.len);
        for (new_values, 0..) |*slot, k| {
            slot.* = if (target_indices[k]) |src_idx| row[src_idx] else Value.null;
        }
        const rec = try record_encode.encodeRecord(a, new_values);
        max_rowid += 1;
        try combined.append(a, .{ .rowid = max_rowid, .record_bytes = rec });
        inserted += 1;
    }

    switch (btree_split.classifyForLeaf(combined.items, header_offset, usable_size)) {
        .fits => {
            try btree_insert.rebuildLeafTablePage(work, header_offset, usable_size, combined.items);
            try pager.writePage(t.root_page, work);
        },
        .needs_split => {
            // Iter26.B.1: balance-deeper. Page 1 (sqlite_schema) growth
            // is forbidden through this path — it would require splicing
            // the 100-byte file header into the new interior root and
            // race with allocatePage's own page-1 mutations.
            if (header_offset != 0) return Error.UnsupportedFeature;
            try btree_split.balanceDeeperRoot(pager, t.root_page, combined.items);
        },
        .oversize_record => return Error.UnsupportedFeature, // Iter26.C
    }
    return inserted;
}

/// File-mode DELETE (Iter26.A.2.a): walk the table's root leaf page,
/// decode each row, evaluate the WHERE predicate, then rebuild the
/// page in place from the survivors.
///
/// Restrictions for Iter26.A.2.a:
///   - root page must be a leaf-table — multi-page B-trees return
///     `Error.UnsupportedFeature` (Iter26.B will widen this).
pub fn executeDeleteFile(db: *Database, t: *Table, parsed: stmt_dml.ParsedDelete) !u64 {
    const pager = if (db.pager) |*pp| pp else return Error.IoError;

    var scratch = std.heap.ArenaAllocator.init(db.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    const usable_size = try pager.usableSize();

    const original = try pager.getPage(t.root_page);
    const work = try a.alloc(u8, original.len);
    @memcpy(work, original);

    const header_offset = btree.pageHeaderOffset(t.root_page);
    const header = try btree.parsePageHeader(work, header_offset);
    if (header.page_type != .leaf_table) return Error.UnsupportedFeature;

    const cells = try btree.parseLeafTablePage(a, work, header_offset, usable_size);

    const dml_qualifiers = try a.alloc([]const u8, t.columns.len);
    for (dml_qualifiers) |*q| q.* = parsed.table;

    var survivors: std.ArrayList(btree_insert.RebuildCell) = .empty;
    var deleted: u64 = 0;
    for (cells) |c| {
        if (parsed.where) |w_ast| {
            const row_values = try decodeRowPadded(a, c.record_bytes, t.columns.len);
            const ctx = eval.EvalContext{
                .allocator = a,
                .current_row = row_values,
                .columns = t.columns,
                .column_qualifiers = dml_qualifiers,
                .db = db,
            };
            const cond = try eval.evalExpr(ctx, w_ast);
            if (ops.truthy(cond) orelse false) {
                deleted += 1;
                continue;
            }
        } else {
            deleted += 1;
            continue;
        }
        // Dupe survivor record bytes into the scratch arena: the source
        // slice borrows from `work`, and rebuildLeafTablePage will
        // overwrite `work` with the new cell layout.
        const dup = try a.dupe(u8, c.record_bytes);
        try survivors.append(a, .{ .rowid = c.rowid, .record_bytes = dup });
    }

    try btree_insert.rebuildLeafTablePage(work, header_offset, usable_size, survivors.items);
    try pager.writePage(t.root_page, work);
    return deleted;
}

/// File-mode UPDATE (Iter26.A.2.b): walk the table's root leaf page,
/// decode each row, evaluate the WHERE predicate, then re-encode the
/// matched rows with assignment values applied and rebuild the page in
/// place. Stricter all-or-nothing than the in-memory path (which
/// commits per-row): the rebuild primitive doesn't allow partial
/// mutation, and that strictness is harmless for the small fixtures
/// Iter26.A.2 covers.
///
/// Restrictions for Iter26.A.2.b:
///   - root page must be a leaf-table — multi-page B-trees return
///     `Error.UnsupportedFeature` (Iter26.B will widen this).
///   - new record bytes must fit (≤ usable_size − 35 each); larger
///     records return `Error.IoError` from rebuildLeafTablePage.
pub fn executeUpdateFile(
    db: *Database,
    t: *Table,
    parsed: stmt_dml.ParsedUpdate,
    indices: []const usize,
) !u64 {
    const pager = if (db.pager) |*pp| pp else return Error.IoError;

    var scratch = std.heap.ArenaAllocator.init(db.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    const usable_size = try pager.usableSize();

    const original = try pager.getPage(t.root_page);
    const work = try a.alloc(u8, original.len);
    @memcpy(work, original);

    const header_offset = btree.pageHeaderOffset(t.root_page);
    const header = try btree.parsePageHeader(work, header_offset);
    if (header.page_type != .leaf_table) return Error.UnsupportedFeature;

    const cells = try btree.parseLeafTablePage(a, work, header_offset, usable_size);

    const dml_qualifiers = try a.alloc([]const u8, t.columns.len);
    for (dml_qualifiers) |*q| q.* = parsed.table;

    var rebuilt: std.ArrayList(btree_insert.RebuildCell) = .empty;
    var changed: u64 = 0;
    for (cells) |c| {
        const row_values = try decodeRowPadded(a, c.record_bytes, t.columns.len);
        const ctx = eval.EvalContext{
            .allocator = a,
            .current_row = row_values,
            .columns = t.columns,
            .column_qualifiers = dml_qualifiers,
            .db = db,
        };
        const matches = if (parsed.where) |w_ast| blk: {
            const cond = try eval.evalExpr(ctx, w_ast);
            break :blk ops.truthy(cond) orelse false;
        } else true;

        if (matches) {
            // Two-pass per ADR-0003 / sqlite3 invariant: every RHS sees
            // the OLD row, then we splice all new values in. The
            // in-memory `executeUpdate` does the same; mirror it here so
            // `UPDATE t SET a = a+10, b = a` evaluates b using the old
            // a (otherwise b would observe a's already-updated value).
            const new_values = try a.alloc(Value, parsed.assignments.len);
            for (parsed.assignments, 0..) |asgn, k| {
                new_values[k] = try eval.evalExpr(ctx, asgn.value);
            }
            for (indices, new_values) |col_idx, new_v| {
                row_values[col_idx] = new_v;
            }
            const new_rec = try record_encode.encodeRecord(a, row_values);
            try rebuilt.append(a, .{ .rowid = c.rowid, .record_bytes = new_rec });
            changed += 1;
        } else {
            const dup = try a.dupe(u8, c.record_bytes);
            try rebuilt.append(a, .{ .rowid = c.rowid, .record_bytes = dup });
        }
    }

    try btree_insert.rebuildLeafTablePage(work, header_offset, usable_size, rebuilt.items);
    try pager.writePage(t.root_page, work);
    return changed;
}

/// Decode `record_bytes` and pad the result to `expected_columns` by
/// appending NULLs. ALTER TABLE ADD COLUMN can leave older rows with
/// fewer serial types than the table's current column list, and
/// EvalContext indexing requires positional alignment with `t.columns`.
fn decodeRowPadded(
    a: std.mem.Allocator,
    record_bytes: []const u8,
    expected_columns: usize,
) ![]Value {
    const decoded = try record.decodeRecord(a, record_bytes);
    if (decoded.len >= expected_columns) return decoded;
    const padded = try a.alloc(Value, expected_columns);
    for (padded, 0..) |*slot, i| {
        slot.* = if (i < decoded.len) decoded[i] else Value.null;
    }
    return padded;
}
