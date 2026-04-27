//! File-mode INSERT (Iter26.A.1 / .B.1 / .B.2). Split out of
//! `engine_dml_file.zig` ahead of Iter26.C (overflow chain) so neither
//! file crosses the 500-line discipline once chain-allocation wiring
//! lands at the leaf-root + interior-root call sites.
//!
//! The DELETE / UPDATE walker still lives in `engine_dml_file.zig`
//! because the per-leaf rebuild loop is materially different from
//! INSERT's "merge into rightmost subtree" shape.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const database = @import("database.zig");
const btree = @import("btree.zig");
const btree_insert = @import("btree_insert.zig");
const btree_split = @import("btree_split.zig");
const record_encode = @import("record_encode.zig");
const pager_mod = @import("pager.zig");

const Value = value_mod.Value;
const Database = database.Database;
const Table = database.Table;
const Error = ops.Error;

/// File-mode INSERT (Iter26.A.1 / .B.1 / .B.2): merge new rows into the
/// existing rowid-sorted cell list, then either rebuild the affected
/// leaf in place, balance-deeper a leaf root (B.1), or split the
/// rightmost leaf of an interior root (B.2).
///
/// Restrictions:
///   - root page must be either a leaf-table (any depth-0 case) or an
///     interior-table (depth-1 case). Deeper trees and recursive
///     interior split are Iter26.B.3 scope.
///   - INSERTs always grow the rightmost subtree because the rowid is
///     auto-assigned as `max(seen) + 1`. Mid-tree splits aren't
///     reachable through this entry point.
///   - rowid auto-assigned as `max(existing) + 1` (or 1 if empty).
///     Explicit rowid via INTEGER PRIMARY KEY alias is a future
///     iteration.
///   - records exceeding `usable_size − 35` need an overflow chain
///     (Iter26.C); rejected with `Error.UnsupportedFeature`.
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

    const root_orig = try pager.getPage(t.root_page);
    const root_work = try a.alloc(u8, root_orig.len);
    @memcpy(root_work, root_orig);

    const root_header_offset = btree.pageHeaderOffset(t.root_page);
    const root_header = try btree.parsePageHeader(root_work, root_header_offset);

    return switch (root_header.page_type) {
        .leaf_table => try insertIntoLeafRoot(
            a,
            db,
            pager,
            t,
            target_indices,
            source_rows,
            usable_size,
            root_work,
            root_header_offset,
        ),
        .interior_table => try insertIntoInteriorRoot(
            a,
            db,
            pager,
            t,
            target_indices,
            source_rows,
            usable_size,
            root_work,
            root_header_offset,
        ),
        else => Error.UnsupportedFeature,
    };
}

fn insertIntoLeafRoot(
    a: std.mem.Allocator,
    db: *Database,
    pager: *pager_mod.Pager,
    t: *Table,
    target_indices: []const ?usize,
    source_rows: []const []Value,
    usable_size: usize,
    work: []u8,
    header_offset: usize,
) !u64 {
    _ = db;
    // Parse existing cells and dupe their record bytes into the scratch
    // arena: `work` will be overwritten by the rebuild / balance-deeper
    // path, and the source slices borrow from it.
    const existing = try btree.parseLeafTablePage(a, work, header_offset, usable_size);
    var combined: std.ArrayList(btree_insert.RebuildCell) = .empty;
    var max_rowid: i64 = 0;
    for (existing) |c| {
        // Iter26.C.1: existing oversize cells (overflow_head != 0)
        // must round-trip through chain re-emit, which lands in C.2
        // when RebuildCell gains an overflow_head field.
        if (c.overflow_head != 0) return Error.UnsupportedFeature;
        const dup = try a.dupe(u8, c.inline_bytes);
        try combined.append(a, .{ .rowid = c.rowid, .record_bytes = dup });
        if (c.rowid > max_rowid) max_rowid = c.rowid;
    }

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

/// Iter26.B.2: depth-1 (interior root → leaves) INSERT path. We always
/// fall through to the rightmost leaf because rowids are
/// monotonically-increasing. The rightmost leaf either fits the new
/// rows (cheap rebuild) or splits via `splitRightmostLeaf`.
fn insertIntoInteriorRoot(
    a: std.mem.Allocator,
    db: *Database,
    pager: *pager_mod.Pager,
    t: *Table,
    target_indices: []const ?usize,
    source_rows: []const []Value,
    usable_size: usize,
    root_work: []u8,
    root_header_offset: usize,
) !u64 {
    _ = db;
    // Page 1 as interior root would mean sqlite_schema overflowed and
    // balance-deeper'd — currently impossible (B.1 rejects page 1) but
    // belt-and-braces in case a future iteration changes that.
    if (root_header_offset != 0) return Error.UnsupportedFeature;

    const parent = try btree.parseInteriorTablePage(a, root_work, root_header_offset);

    // Read & snapshot the rightmost leaf. Mutation will go to this
    // page in the .fits path; in the .needs_split path it's freed.
    const old_right_child = parent.right_child;
    const right_orig = try pager.getPage(old_right_child);
    const right_work = try a.alloc(u8, right_orig.len);
    @memcpy(right_work, right_orig);

    const right_header = try btree.parsePageHeader(right_work, 0);
    if (right_header.page_type != .leaf_table) return Error.UnsupportedFeature;

    const right_cells = try btree.parseLeafTablePage(a, right_work, 0, usable_size);

    // max_rowid spans both parent's last divider key and the rightmost
    // leaf's cells. Using the larger guards against an empty rightmost
    // leaf (theoretically possible if a previous DELETE chain ran;
    // harmless in B.2 since DELETE on multi-page tables is still
    // UnsupportedFeature, but cheap to be correct).
    var max_rowid: i64 = 0;
    for (parent.cells) |c| {
        if (c.key > max_rowid) max_rowid = c.key;
    }
    var combined: std.ArrayList(btree_insert.RebuildCell) = .empty;
    for (right_cells) |c| {
        if (c.overflow_head != 0) return Error.UnsupportedFeature; // C.2 scope
        const dup = try a.dupe(u8, c.inline_bytes);
        try combined.append(a, .{ .rowid = c.rowid, .record_bytes = dup });
        if (c.rowid > max_rowid) max_rowid = c.rowid;
    }

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

    switch (btree_split.classifyForLeaf(combined.items, 0, usable_size)) {
        .fits => {
            try btree_insert.rebuildLeafTablePage(right_work, 0, usable_size, combined.items);
            try pager.writePage(old_right_child, right_work);
        },
        .needs_split => {
            // Convert btree.InteriorTableCell → btree_split.InteriorCell.
            const parent_cells = try a.alloc(btree_split.InteriorCell, parent.cells.len);
            for (parent_cells, parent.cells) |*dst, src| {
                dst.* = .{ .left_child = src.left_child, .key = src.key };
            }
            try btree_split.splitRightmostLeaf(
                pager,
                t.root_page,
                old_right_child,
                parent_cells,
                combined.items,
            );
        },
        .oversize_record => return Error.UnsupportedFeature, // Iter26.C
    }
    return inserted;
}

