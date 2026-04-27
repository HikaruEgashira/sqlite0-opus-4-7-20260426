//! File-mode DELETE / UPDATE walker (Iter26.A.2 / .B.2.b). The
//! file-mode INSERT path lives next door in `engine_dml_insert_file.zig`
//! — split there ahead of Iter26.C so neither file crosses the 500-line
//! discipline once chain-allocation wires into both shapes.
//!
//! All three file-mode mutations share the recipe:
//!
//!   1. Open a scratch arena (Database.execute doesn't yet provide a
//!      per-statement arena — see ADR-0003 §8 for the planned shape).
//!   2. Snapshot the affected page(s) into local working buffers.
//!   3. Mutate the working buffers (rebuild from survivors or merged
//!      cell lists).
//!   4. Single `Pager.writePage` commit per page. All-or-nothing per
//!      ADR-0003 §1: any error before step 4 leaves the on-disk page
//!      untouched.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const stmt_dml = @import("stmt_dml.zig");
const eval = @import("eval.zig");
const database = @import("database.zig");
const btree = @import("btree.zig");
const btree_insert = @import("btree_insert.zig");
const btree_overflow = @import("btree_overflow.zig");
const record = @import("record.zig");
const record_encode = @import("record_encode.zig");
const pager_mod = @import("pager.zig");

const Value = value_mod.Value;
const Database = database.Database;
const Table = database.Table;
const Error = ops.Error;

/// File-mode DELETE (Iter26.A.2.a / .B.2.b / .B.3.d / .B.3.f): walk
/// every leaf page of the table at any depth, decode each row,
/// evaluate the WHERE predicate, then rebuild each leaf in place from
/// the survivors.
///
/// Restrictions:
///   - empty leaves are rejected at depth ≥ 2. sqlite3 only tolerates
///     an empty leaf when its parent is the **root** ("only possible
///     for a root page of a table that contains no rows"); at deeper
///     depths sqlite3 raises `SQLITE_CORRUPT` reading the file. Until
///     proper underfull rebalance (analogous to sqlite3's
///     `balance_quick`) lands, a DELETE that would empty any single
///     leaf returns `Error.UnsupportedFeature` BEFORE writing — the
///     on-disk file is left intact rather than silently corrupted.
pub fn executeDeleteFile(db: *Database, t: *Table, parsed: stmt_dml.ParsedDelete) !u64 {
    const op: ModifyOp = .{ .delete = .{ .where = parsed.where } };
    return modifyAllLeaves(db, t, parsed.table, op);
}

/// File-mode UPDATE (Iter26.A.2.b / .B.2.b / .B.3.d / .B.3.f): same
/// per-leaf shape as DELETE — walk leaves at any depth, decode +
/// evaluate + re-encode matched rows, rebuild each leaf. Stricter
/// all-or-nothing than the in-memory path (per-row commit isn't
/// possible without a freeblock chain).
///
/// Restrictions:
///   - same empty-leaf-at-depth-≥-2 rejection as DELETE (UPDATE never
///     drops cells, so this only trips when a future code path adds
///     conditional-delete semantics).
///   - per-leaf size growth that would force a leaf split is not
///     handled — `rebuildLeafTablePage` returns IoError when the new
///     cell content exceeds the usable area. UPDATE-driven splits
///     are deferred (would need to integrate with the spine walker
///     in `engine_dml_insert_file`).
pub fn executeUpdateFile(
    db: *Database,
    t: *Table,
    parsed: stmt_dml.ParsedUpdate,
    indices: []const usize,
) !u64 {
    const op: ModifyOp = .{ .update = .{
        .where = parsed.where,
        .assignments = parsed.assignments,
        .indices = indices,
    } };
    return modifyAllLeaves(db, t, parsed.table, op);
}

/// Per-leaf modification recipe. DELETE drops matched cells; UPDATE
/// rewrites them. Sharing the walker keeps both paths in lockstep
/// when we extend leaf collection (already covers depth-1 here) or
/// add per-leaf split detection later.
const ModifyOp = union(enum) {
    delete: struct {
        where: ?*const @import("ast.zig").Expr,
    },
    update: struct {
        where: ?*const @import("ast.zig").Expr,
        assignments: []const stmt_dml.ParsedUpdate.Assignment,
        indices: []const usize,
    },
};

fn modifyAllLeaves(db: *Database, t: *Table, table_name: []const u8, op: ModifyOp) !u64 {
    const pager = if (db.pager) |*pp| pp else return Error.IoError;

    var scratch = std.heap.ArenaAllocator.init(db.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();

    const usable_size = try pager.usableSize();

    const dml_qualifiers = try a.alloc([]const u8, t.columns.len);
    for (dml_qualifiers) |*q| q.* = table_name;

    const leaves = try collectLeafPages(a, pager, t.root_page);

    var total: u64 = 0;
    for (leaves.pages) |page_no| {
        const header_offset = btree.pageHeaderOffset(page_no);
        total += try modifyOneLeaf(a, db, pager, t, op, dml_qualifiers, page_no, header_offset, usable_size, leaves.tree_is_deep);
    }
    return total;
}

/// Result of leaf collection. `tree_is_deep` is set when at least one
/// leaf sits below an interior page that is NOT the root — i.e. the
/// tree has ≥ 3 levels (root → interior → leaf). The flag drives the
/// fail-loud guard in `modifyOneLeaf`: empty leaves are spec-legal at
/// depth-1 (root is the parent) but corrupt the file at depth ≥ 2.
const LeafCollection = struct {
    pages: []u32,
    tree_is_deep: bool,
};

/// Resolve `root_page` to the list of every leaf page that holds the
/// table's rows. Recursive, supporting any depth produced by Iter26.B.3:
/// depth-0 (leaf root) → `[root]`; deeper trees descend each interior
/// child left-to-right (cells then right_child) and recurse.
///
/// Interior cells are duped from the page bytes before any recursive
/// `pager.getPage` because the LRU may evict the original page slice
/// out from under us.
fn collectLeafPages(a: std.mem.Allocator, pager: *pager_mod.Pager, root_page: u32) !LeafCollection {
    var pages: std.ArrayList(u32) = .empty;
    var tree_is_deep = false;
    try collectLeafPagesRecursive(a, pager, root_page, 0, &pages, &tree_is_deep);
    return .{
        .pages = try pages.toOwnedSlice(a),
        .tree_is_deep = tree_is_deep,
    };
}

fn collectLeafPagesRecursive(
    a: std.mem.Allocator,
    pager: *pager_mod.Pager,
    page_no: u32,
    depth_from_root: u32,
    pages: *std.ArrayList(u32),
    tree_is_deep: *bool,
) !void {
    const page = try pager.getPage(page_no);
    const header_offset = btree.pageHeaderOffset(page_no);
    const header = try btree.parsePageHeader(page, header_offset);

    switch (header.page_type) {
        .leaf_table => {
            try pages.append(a, page_no);
            if (depth_from_root >= 2) tree_is_deep.* = true;
        },
        .interior_table => {
            const info = try btree.parseInteriorTablePage(a, page, header_offset);
            // Snapshot child pointers — the recursive `getPage` calls
            // below may evict `page`, invalidating `info.cells`'s
            // backing slice.
            const left_children = try a.alloc(u32, info.cells.len);
            for (left_children, info.cells) |*dst, src| dst.* = src.left_child;
            const right_child = info.right_child;

            for (left_children) |child| {
                try collectLeafPagesRecursive(a, pager, child, depth_from_root + 1, pages, tree_is_deep);
            }
            try collectLeafPagesRecursive(a, pager, right_child, depth_from_root + 1, pages, tree_is_deep);
        },
        else => return Error.UnsupportedFeature,
    }
}

fn modifyOneLeaf(
    a: std.mem.Allocator,
    db: *Database,
    pager: *pager_mod.Pager,
    t: *Table,
    op: ModifyOp,
    dml_qualifiers: []const []const u8,
    page_no: u32,
    header_offset: usize,
    usable_size: usize,
    tree_is_deep: bool,
) !u64 {
    const original = try pager.getPage(page_no);
    const work = try a.alloc(u8, original.len);
    @memcpy(work, original);

    const header = try btree.parsePageHeader(work, header_offset);
    if (header.page_type != .leaf_table) return Error.UnsupportedFeature;

    const cells = try btree.parseLeafTablePage(a, work, header_offset, usable_size);

    // Iter28: UPDATE that targets the IPK column would change the cell
    // rowid (= move the cell to a different leaf), which the per-leaf
    // rebuild walker can't express. Reject up-front, before any pwrite,
    // so the file stays byte-identical.
    if (t.ipk_column) |ipk| {
        switch (op) {
            .update => |u| for (u.indices) |col_idx| {
                if (col_idx == ipk) return Error.UnsupportedFeature;
            },
            .delete => {},
        }
    }

    var rebuilt: std.ArrayList(btree_insert.RebuildCell) = .empty;
    // Old chain heads to free AFTER the leaf rebuild commits — leaf is
    // the source of truth for chain reachability, so we mustn't put a
    // chain on the freelist while a leaf cell still points at it.
    // Crash between rebuild + free leaves orphan chain pages; same
    // accepted window as B.1/B.2 (Phase 4 / WAL absorbs it).
    var chains_to_free: std.ArrayList(u32) = .empty;
    var changed: u64 = 0;
    for (cells) |c| {
        // Decode against the FULL payload (assemble the chain if any) —
        // WHERE predicates and UPDATE RHS expressions need every column.
        const full = try btree_overflow.assemblePayload(a, pager, c, usable_size);
        const row_values = try decodeRowPadded(a, full, t.columns.len);
        // IPK substitution mirrors btree_cursor.decodeCurrentRow: the
        // record stores NULL for the IPK column; the rowid header is
        // the source of truth. WHERE clauses (`WHERE n=5`) need to see
        // the rowid, not NULL.
        if (t.ipk_column) |ipk| {
            if (ipk < row_values.len) row_values[ipk] = .{ .integer = c.rowid };
        }
        const ctx = eval.EvalContext{
            .allocator = a,
            .current_row = row_values,
            .columns = t.columns,
            .column_qualifiers = dml_qualifiers,
            .db = db,
        };
        switch (op) {
            .delete => |d| {
                const matches = if (d.where) |w| blk: {
                    const cond = try eval.evalExpr(ctx, w);
                    break :blk ops.truthy(cond) orelse false;
                } else true;
                if (matches) {
                    if (c.overflow_head != 0) try chains_to_free.append(a, c.overflow_head);
                    changed += 1;
                    continue;
                }
                // Survivor — dupe inline bytes out of `work` (rebuild
                // overwrites the source) and preserve the chain
                // reference so the existing overflow pages stay
                // reachable through the new leaf.
                const dup = try a.dupe(u8, c.inline_bytes);
                try rebuilt.append(a, .{
                    .rowid = c.rowid,
                    .record_bytes = dup,
                    .overflow_head = c.overflow_head,
                    .payload_len = if (c.overflow_head != 0) c.payload_len else 0,
                });
            },
            .update => |u| {
                const matches = if (u.where) |w| blk: {
                    const cond = try eval.evalExpr(ctx, w);
                    break :blk ops.truthy(cond) orelse false;
                } else true;
                if (matches) {
                    // Two-pass per ADR-0003: every RHS sees the OLD row,
                    // then splice all new values in. Mirrors the in-memory
                    // path so `UPDATE t SET a = a+10, b = a` evaluates
                    // b using the old a.
                    const new_values = try a.alloc(Value, u.assignments.len);
                    for (u.assignments, 0..) |asgn, k| {
                        new_values[k] = try eval.evalExpr(ctx, asgn.value);
                    }
                    for (u.indices, new_values) |col_idx, new_v| {
                        row_values[col_idx] = new_v;
                    }
                    // Restore IPK column to NULL before encoding —
                    // sqlite3 invariant: aliased rowid is stored only
                    // in the cell header, never in the record body.
                    if (t.ipk_column) |ipk| {
                        if (ipk < row_values.len) row_values[ipk] = Value.null;
                    }
                    const new_rec = try record_encode.encodeRecord(a, row_values);
                    // Always free old chain on UPDATE — incremental chain
                    // mutation is out-of-scope per the C plan; new chain
                    // (if needed) is allocated fresh below. May reuse the
                    // freed pages once \`Pager.allocatePage\` honours the
                    // freelist, currently grows end-of-file.
                    if (c.overflow_head != 0) try chains_to_free.append(a, c.overflow_head);
                    try rebuilt.append(a, try buildUpdateRebuildCell(pager, c.rowid, new_rec, usable_size));
                    changed += 1;
                } else {
                    const dup = try a.dupe(u8, c.inline_bytes);
                    try rebuilt.append(a, .{
                        .rowid = c.rowid,
                        .record_bytes = dup,
                        .overflow_head = c.overflow_head,
                        .payload_len = if (c.overflow_head != 0) c.payload_len else 0,
                    });
                }
            },
        }
    }

    // Fail-loud guard (Iter26.B.3.f): emptying a leaf at depth ≥ 2
    // produces a sqlite3-malformed file (spec only allows empty leaves
    // when the parent is the root). Refuse BEFORE any write so the
    // on-disk file stays intact. Proper underfull rebalance is a
    // future iteration; until then this surfaces the gap explicitly
    // rather than silently corrupting.
    if (tree_is_deep and rebuilt.items.len == 0) return Error.UnsupportedFeature;

    try btree_insert.rebuildLeafTablePage(work, header_offset, usable_size, rebuilt.items);
    try pager.writePage(page_no, work);

    // Leaf is committed — chain pages we held a reference to are now
    // unreachable from any leaf and safe to add to the freelist.
    for (chains_to_free.items) |head| try btree_overflow.freeOverflowChain(pager, head);
    return changed;
}

/// Allocate an overflow chain for an UPDATE-replaced record when it
/// exceeds the inline threshold. Mirrors `buildRebuildCellWithOverflow`
/// in `engine_dml_insert_file.zig` — kept here as a small helper so
/// the two file-mode DML modules don't need a cross-import for one
/// 12-line function.
fn buildUpdateRebuildCell(
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
