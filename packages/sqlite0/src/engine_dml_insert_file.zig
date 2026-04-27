//! File-mode INSERT (Iter26.A.1 / .B.1 / .B.2 / .B.3). Split out of
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
const btree_overflow = @import("btree_overflow.zig");
const btree_split = @import("btree_split.zig");
const btree_split_interior = @import("btree_split_interior.zig");
const record_encode = @import("record_encode.zig");
const pager_mod = @import("pager.zig");

const Value = value_mod.Value;
const Database = database.Database;
const Table = database.Table;
const Error = ops.Error;

/// File-mode INSERT (Iter26.A.1 / .B.1 / .B.2 / .B.3): merge new rows
/// into the existing rowid-sorted cell list, then dispatch by root
/// page type. Leaf root → rebuild in place or balance-deeper (B.1).
/// Interior root → spine-descend to the rightmost leaf and propagate
/// any split bottom-up; non-root interior overflow becomes a recursive
/// interior split (B.3), root overflow becomes balance-deeper-interior.
///
/// Restrictions:
///   - INSERTs always grow the rightmost subtree. Auto-assigned rowids
///     are `max(seen) + 1`; explicit IPK rowids (Iter28) are accepted
///     only when strictly greater than the current max, preserving the
///     monotonic-rightmost invariant. An explicit IPK ≤ max would
///     require mid-tree insertion (a future iteration); we reject with
///     `Error.UnsupportedFeature` BEFORE any pwrite (fail-loud).
///   - records exceeding `usable_size − 35` use the overflow chain
///     allocated via `btree_overflow.allocateOverflowChain` (Iter26.C).
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
        .interior_table => try insertIntoDeepTree(
            a,
            db,
            pager,
            t,
            target_indices,
            source_rows,
            usable_size,
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
        const dup = try a.dupe(u8, c.inline_bytes);
        try combined.append(a, .{
            .rowid = c.rowid,
            .record_bytes = dup,
            .overflow_head = c.overflow_head,
            .payload_len = if (c.overflow_head != 0) c.payload_len else 0,
        });
        if (c.rowid > max_rowid) max_rowid = c.rowid;
    }

    const prepared = try prepareNewCells(a, t, target_indices, source_rows, &max_rowid);
    var inserted: u64 = 0;
    for (prepared) |row_info| {
        const rec = try record_encode.encodeRecord(a, row_info.values);
        try combined.append(a, try buildRebuildCellWithOverflow(pager, row_info.rowid, rec, usable_size));
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
        .oversize_record => return Error.IoError, // invariant: handled above
    }
    return inserted;
}

/// One row's resolved insert state: the rowid the cell will carry and
/// the value array (with IPK column already nulled for IPK tables).
const PreparedRow = struct {
    rowid: i64,
    values: []Value,
};

/// Pre-pass that allocates every new row's value array and resolves
/// every rowid BEFORE any caller-side encode or chain allocation.
///
/// Why two passes (Iter28 chain-leak fix): `chooseRowid` can return
/// `Error.UnsupportedFeature` for out-of-order explicit IPK. If we
/// interleave it with `buildRebuildCellWithOverflow`, a row N that
/// allocates an overflow chain followed by a row N+1 that fails IPK
/// validation leaves the chain pages orphaned on disk. Pre-validating
/// every row first ensures any failure short-circuits before the first
/// pwrite to a chain page — preserving byte-identical-on-failure.
fn prepareNewCells(
    a: std.mem.Allocator,
    t: *const Table,
    target_indices: []const ?usize,
    source_rows: []const []Value,
    max_rowid: *i64,
) ![]PreparedRow {
    const out = try a.alloc(PreparedRow, source_rows.len);
    for (source_rows, out) |row, *slot| {
        const new_values = try a.alloc(Value, t.columns.len);
        for (new_values, 0..) |*v, k| {
            v.* = if (target_indices[k]) |src_idx| row[src_idx] else Value.null;
        }
        const rowid = try chooseRowid(t, max_rowid, new_values);
        slot.* = .{ .rowid = rowid, .values = new_values };
    }
    return out;
}

/// IPK-aware rowid resolver (Iter28). When the table has an INTEGER
/// PRIMARY KEY column and the user supplied a non-NULL integer value
/// for it, that value becomes the cell's rowid AND the IPK column in
/// `new_values` is rewritten to NULL (sqlite3 invariant: the record
/// body always stores NULL for an aliased rowid). Otherwise the rowid
/// is auto-assigned as `max(seen) + 1`.
///
/// `max_rowid` is updated in place so subsequent rows in the same
/// INSERT see the running max.
///
/// Monotonic-rightmost guard: explicit IPK ≤ current max would force
/// mid-tree insertion. The spine walker only descends to the rightmost
/// leaf, so we reject with `Error.UnsupportedFeature`. Always called
/// from `prepareNewCells` (pre-pass) so the failure short-circuits
/// before any chain allocation (Iter28 chain-leak fix).
fn chooseRowid(
    t: *const Table,
    max_rowid: *i64,
    new_values: []Value,
) !i64 {
    const ipk = t.ipk_column orelse {
        max_rowid.* += 1;
        return max_rowid.*;
    };
    switch (new_values[ipk]) {
        .integer => |explicit| {
            if (explicit <= max_rowid.*) return Error.UnsupportedFeature;
            new_values[ipk] = Value.null;
            max_rowid.* = explicit;
            return explicit;
        },
        else => {
            // NULL or non-integer → auto-rowid. (sqlite3 also coerces
            // text/real here in some cases; deferred — explicit-NULL
            // is the common path for omitted-IPK INSERTs.)
            new_values[ipk] = Value.null;
            max_rowid.* += 1;
            return max_rowid.*;
        },
    }
}

/// Build a `RebuildCell` for a freshly-encoded record. When the record
/// exceeds the inline threshold X, allocates the overflow chain BEFORE
/// returning so the caller's leaf rebuild commits the head pointer last
/// (parent-last invariant — chain pages exist on disk, but no leaf
/// references them until the rebuild lands).
fn buildRebuildCellWithOverflow(
    pager: *pager_mod.Pager,
    rowid: i64,
    rec: []const u8,
    usable_size: usize,
) !btree_insert.RebuildCell {
    const split = btree_overflow.inlineSplitForPayload(rec.len, usable_size);
    if (split.spill_len == 0) {
        return .{ .rowid = rowid, .record_bytes = rec };
    }
    const head = try btree_overflow.allocateOverflowChain(pager, rec, split.inline_len);
    return .{
        .rowid = rowid,
        .record_bytes = rec[0..split.inline_len],
        .overflow_head = head,
        .payload_len = rec.len,
    };
}

/// One frame of the spine descend. Interior page snapshot the
/// orchestrator keeps so it can rebuild the page in place (`.fits`)
/// or hand a `merged_cells` view to `splitInteriorPage` (`.needs_split`).
/// Cells are duped into the scratch arena — a pager getPage between
/// snapshot and write may evict the original buffer.
const SpineFrame = struct {
    page_no: u32,
    cells: []btree_split.InteriorCell,
    right_child: u32,
};

/// Iter26.B.3 INSERT path for any depth ≥ 1 (interior root). The
/// generalisation of B.2's `insertIntoInteriorRoot`: spine-descend
/// from the root via `right_child` to the rightmost leaf, attempt
/// the leaf rebuild; on `.needs_split` propagate the split up the
/// spine, splitting interior pages where they can't fit and
/// balance-deeper-interior'ing the root if it would overflow.
///
/// The whole flow is rightmost-only because the auto-assigned rowid
/// is `max + 1` — every new row lands in the rightmost subtree.
///
/// Page 1 as interior root is rejected (sqlite_schema growth path
/// is its own iteration; `balanceDeeperRoot` and
/// `balanceDeeperInterior` both reject page 1).
fn insertIntoDeepTree(
    a: std.mem.Allocator,
    db: *Database,
    pager: *pager_mod.Pager,
    t: *Table,
    target_indices: []const ?usize,
    source_rows: []const []Value,
    usable_size: usize,
) !u64 {
    _ = db;
    if (btree.pageHeaderOffset(t.root_page) != 0) return Error.UnsupportedFeature;

    // -- Spine descend: root → ... → parent_of_leaf, leaf is `cur` after loop.
    var spine: std.ArrayList(SpineFrame) = .empty;
    var cur: u32 = t.root_page;
    while (true) {
        const page_bytes = try pager.getPage(cur);
        const header_offset = btree.pageHeaderOffset(cur);
        const header = try btree.parsePageHeader(page_bytes, header_offset);
        switch (header.page_type) {
            .leaf_table => break, // leaf reached; `cur` is its page_no
            .interior_table => {},
            else => return Error.UnsupportedFeature,
        }
        if (header_offset != 0) return Error.UnsupportedFeature; // non-root page 1 impossible; defensive
        const info = try btree.parseInteriorTablePage(a, page_bytes, header_offset);
        // parseInteriorTablePage already alloc'd `info.cells` from `a`.
        const cells_split = try a.alloc(btree_split.InteriorCell, info.cells.len);
        for (cells_split, info.cells) |*dst, src| {
            dst.* = .{ .left_child = src.left_child, .key = src.key };
        }
        try spine.append(a, .{
            .page_no = cur,
            .cells = cells_split,
            .right_child = info.right_child,
        });
        cur = info.right_child;
    }

    const leaf_page_no = cur;
    const leaf_orig = try pager.getPage(leaf_page_no);
    const leaf_work = try a.alloc(u8, leaf_orig.len);
    @memcpy(leaf_work, leaf_orig);
    const leaf_cells = try btree.parseLeafTablePage(a, leaf_work, 0, usable_size);

    // -- max_rowid: include every spine divider key + leaf cell rowid.
    var max_rowid: i64 = 0;
    for (spine.items) |frame| {
        for (frame.cells) |c| {
            if (c.key > max_rowid) max_rowid = c.key;
        }
    }
    for (leaf_cells) |c| {
        if (c.rowid > max_rowid) max_rowid = c.rowid;
    }

    var combined: std.ArrayList(btree_insert.RebuildCell) = .empty;
    for (leaf_cells) |c| {
        const dup = try a.dupe(u8, c.inline_bytes);
        try combined.append(a, .{
            .rowid = c.rowid,
            .record_bytes = dup,
            .overflow_head = c.overflow_head,
            .payload_len = if (c.overflow_head != 0) c.payload_len else 0,
        });
    }

    const prepared = try prepareNewCells(a, t, target_indices, source_rows, &max_rowid);
    var inserted: u64 = 0;
    for (prepared) |row_info| {
        const rec = try record_encode.encodeRecord(a, row_info.values);
        try combined.append(a, try buildRebuildCellWithOverflow(pager, row_info.rowid, rec, usable_size));
        inserted += 1;
    }

    // -- Leaf classify. The .fits path is the common case (no split),
    //    .needs_split kicks off the bottom-up propagation.
    switch (btree_split.classifyForLeaf(combined.items, 0, usable_size)) {
        .fits => {
            try btree_insert.rebuildLeafTablePage(leaf_work, 0, usable_size, combined.items);
            try pager.writePage(leaf_page_no, leaf_work);
            return inserted;
        },
        .needs_split => {
            // Deferred-free list: the OLD leaf and any OLD interior
            // pages that get split during spine ascent. Drained
            // ONLY after the topmost commit (either an ancestor's
            // .fits rewrite or balanceDeeperInterior on the root).
            // Until then those pages are still reachable from the
            // not-yet-updated grandparent, so freelist insertion
            // would race integrity_check.
            var deferred_free: std.ArrayList(u32) = .empty;

            const ps_leaf = try btree_split_interior.splitLeafProduceChildren(pager, combined.items);
            try deferred_free.append(a, leaf_page_no);

            try propagateSplitUpSpine(a, pager, t.root_page, &spine, ps_leaf, usable_size, &deferred_free);
            return inserted;
        },
        .oversize_record => return Error.IoError, // invariant: handled by buildRebuildCellWithOverflow
    }
}

/// Walk back up the spine from leaf to root, inserting the carried
/// `PromotedSplit` at each level and propagating further splits when
/// interior pages would overflow. Drains `deferred_free` after the
/// topmost commit.
///
/// Spine layout: `spine[0]` is the root, `spine[len-1]` is the parent
/// of the leaf. We iterate from len-1 down to 0.
fn propagateSplitUpSpine(
    a: std.mem.Allocator,
    pager: *pager_mod.Pager,
    root_page_no: u32,
    spine: *std.ArrayList(SpineFrame),
    initial_carry: btree_split_interior.PromotedSplit,
    usable_size: usize,
    deferred_free: *std.ArrayList(u32),
) !void {
    var carry = initial_carry;
    var i: usize = spine.items.len;
    while (i > 0) {
        i -= 1;
        const frame = &spine.items[i];

        // Compose merged cells: existing frame.cells ++ [carry.new_cell].
        // `carry.new_cell.left_child` was the OLD frame.right_child's
        // replacement L-half; the new frame.right_child is carry.new_right_child.
        const merged = try a.alloc(btree_split.InteriorCell, frame.cells.len + 1);
        for (merged[0..frame.cells.len], frame.cells) |*dst, src| {
            dst.* = src;
        }
        merged[frame.cells.len] = carry.new_cell;
        const merged_right_child = carry.new_right_child;

        switch (btree_split.classifyForInterior(merged, 0, usable_size)) {
            .fits => {
                // Rewrite this interior page in place (page_no preserved).
                const allocator = pager.allocator;
                const buf = try allocator.alloc(u8, pager_mod.PAGE_SIZE);
                defer allocator.free(buf);
                @memset(buf, 0);
                try btree_split.writeInteriorTablePage(buf, 0, usable_size, merged, merged_right_child);
                try pager.writePage(frame.page_no, buf);

                // Topmost write committed — safe to drain deferred frees.
                for (deferred_free.items) |pn| try pager.freePage(pn);
                return;
            },
            .needs_split => {
                if (i == 0) {
                    // Root level — balance-deeper-interior. Page 1
                    // root is rejected inside the helper. root_page_no
                    // is reused (no freePage of root).
                    try btree_split_interior.balanceDeeperInterior(
                        pager,
                        root_page_no,
                        merged,
                        merged_right_child,
                    );
                    for (deferred_free.items) |pn| try pager.freePage(pn);
                    return;
                }
                // Non-root interior split. Old frame page joins the
                // deferred-free list (still reachable from grandparent
                // until we update it on the next iteration / ancestor's
                // .fits write).
                const ps_interior = try btree_split_interior.splitInteriorPage(
                    pager,
                    merged,
                    merged_right_child,
                );
                try deferred_free.append(a, frame.page_no);
                carry = ps_interior;
            },
        }
    }
    // Spine exhausted without committing — should not happen because
    // i == 0 lands in balanceDeeperInterior. Defensive: if we get
    // here, something corrupt happened.
    return Error.IoError;
}
