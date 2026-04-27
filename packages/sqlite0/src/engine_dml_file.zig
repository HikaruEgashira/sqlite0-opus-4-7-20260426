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
const record = @import("record.zig");
const record_encode = @import("record_encode.zig");
const pager_mod = @import("pager.zig");

const Value = value_mod.Value;
const Database = database.Database;
const Table = database.Table;
const Error = ops.Error;

/// File-mode DELETE (Iter26.A.2.a / .B.2.b): walk every leaf page of
/// the table, decode each row, evaluate the WHERE predicate, then
/// rebuild each leaf in place from the survivors.
///
/// Restrictions:
///   - depth-1 max (leaf root or interior root → leaves). Recursive
///     interior trees are Iter26.B.3 scope.
///   - empty leaves left in place: sqlite3 traversal tolerates them
///     and `PRAGMA integrity_check` accepts them, so the harness
///     stays "ok" even when DELETE wipes a leaf clean. Reclaiming
///     the now-empty page (re-balancing the B-tree) is a future
///     iteration.
pub fn executeDeleteFile(db: *Database, t: *Table, parsed: stmt_dml.ParsedDelete) !u64 {
    const op: ModifyOp = .{ .delete = .{ .where = parsed.where } };
    return modifyAllLeaves(db, t, parsed.table, op);
}

/// File-mode UPDATE (Iter26.A.2.b / .B.2.b): same per-leaf shape as
/// DELETE — walk leaves, decode + evaluate + re-encode matched rows,
/// rebuild each leaf. Stricter all-or-nothing than the in-memory
/// path (per-row commit isn't possible without a freeblock chain).
///
/// Restrictions: same as DELETE plus
///   - new record bytes must fit (`≤ usable_size − 35` each); larger
///     records return `Error.IoError` from rebuildLeafTablePage.
///   - per-leaf size growth that would force a leaf split is not
///     handled — `rebuildLeafTablePage` returns IoError when the new
///     cell content exceeds the usable area. UPDATE-driven splits
///     are out of B.2.b scope.
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

    const leaf_pages = try collectLeafPages(a, pager, t.root_page);

    var total: u64 = 0;
    for (leaf_pages) |page_no| {
        const header_offset = btree.pageHeaderOffset(page_no);
        total += try modifyOneLeaf(a, db, pager, t, op, dml_qualifiers, page_no, header_offset, usable_size);
    }
    return total;
}

/// Resolve `root_page` to the list of every leaf page that holds the
/// table's rows. Depth-0 (leaf root) → `[root]`; depth-1 (interior
/// root) → all `left_child` pointers + `right_child`. Deeper trees
/// return `Error.UnsupportedFeature` (B.3 scope).
fn collectLeafPages(a: std.mem.Allocator, pager: *pager_mod.Pager, root_page: u32) ![]u32 {
    const root = try pager.getPage(root_page);
    const header_offset = btree.pageHeaderOffset(root_page);
    const header = try btree.parsePageHeader(root, header_offset);

    return switch (header.page_type) {
        .leaf_table => blk: {
            const result = try a.alloc(u32, 1);
            result[0] = root_page;
            break :blk result;
        },
        .interior_table => blk: {
            const interior = try btree.parseInteriorTablePage(a, root, header_offset);
            const result = try a.alloc(u32, interior.cells.len + 1);
            for (interior.cells, 0..) |c, i| result[i] = c.left_child;
            result[interior.cells.len] = interior.right_child;
            break :blk result;
        },
        else => Error.UnsupportedFeature,
    };
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
) !u64 {
    const original = try pager.getPage(page_no);
    const work = try a.alloc(u8, original.len);
    @memcpy(work, original);

    const header = try btree.parsePageHeader(work, header_offset);
    if (header.page_type != .leaf_table) return Error.UnsupportedFeature;

    const cells = try btree.parseLeafTablePage(a, work, header_offset, usable_size);

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
        switch (op) {
            .delete => |d| {
                const matches = if (d.where) |w| blk: {
                    const cond = try eval.evalExpr(ctx, w);
                    break :blk ops.truthy(cond) orelse false;
                } else true;
                if (matches) {
                    changed += 1;
                    continue;
                }
                // Survivor — dupe out of `work` since rebuildLeafTablePage
                // will overwrite the source bytes.
                const dup = try a.dupe(u8, c.record_bytes);
                try rebuilt.append(a, .{ .rowid = c.rowid, .record_bytes = dup });
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
                    const new_rec = try record_encode.encodeRecord(a, row_values);
                    try rebuilt.append(a, .{ .rowid = c.rowid, .record_bytes = new_rec });
                    changed += 1;
                } else {
                    const dup = try a.dupe(u8, c.record_bytes);
                    try rebuilt.append(a, .{ .rowid = c.rowid, .record_bytes = dup });
                }
            },
        }
    }

    try btree_insert.rebuildLeafTablePage(work, header_offset, usable_size, rebuilt.items);
    try pager.writePage(page_no, work);
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
