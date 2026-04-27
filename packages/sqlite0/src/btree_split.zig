//! Page split primitives (Iter26.B.1, ADR-0005 §2).
//!
//! Iter26.B.1 covers `balance-deeper` only: when a leaf root grows past
//! page capacity, allocate two new leaves, distribute cells, and convert
//! the root in place to an interior page with one divider cell. The
//! `root_page_no` value stays stable so `sqlite_schema.rootpage`
//! references remain valid — that's the single hardest invariant of the
//! SQLite balance algorithm and why we can't just allocate a new root.
//!
//! ## Crash window (accepted, will be absorbed by Phase 4 / WAL)
//!
//! `Pager.allocatePage` bumps the on-disk dbsize before any content
//! lands in the new page. `balanceDeeperRoot` writes the new children
//! first (left, then right) and the new interior root LAST. A crash
//! between any of these leaves the most-recoverable failure shape:
//! the old leaf root content remains intact at `root_page_no`, the
//! orphan child pages contain either zeros (allocatePage default) or
//! their freshly-written content. `PRAGMA integrity_check` may flag
//! the orphans but the table is still queryable through the unchanged
//! root.
//!
//! ## Page 1 not supported
//!
//! `root_page_no == 1` is rejected with `Error.UnsupportedFeature`.
//! Page 1 carries the 100-byte file header AND its dbsize counter is
//! the very value `Pager.allocatePage` mutates concurrently — splitting
//! it during balance-deeper would race the snapshot/write pair. User
//! tables created via `engine_ddl_file.executeCreateTableFile` get
//! `root_page` allocated from page 2 onward, so this never fires
//! through the INSERT path. sqlite_schema growth past one page is
//! deferred to a later iteration.

const std = @import("std");
const ops = @import("ops.zig");
const btree = @import("btree.zig");
const btree_insert = @import("btree_insert.zig");
const record = @import("record.zig");
const record_encode = @import("record_encode.zig");
const pager_mod = @import("pager.zig");

pub const Error = ops.Error;

/// One interior-table-page cell to write. `key` follows the sqlite3
/// invariant: every rowid in `left_child`'s subtree is ≤ `key`.
pub const InteriorCell = struct {
    left_child: u32,
    key: i64,
};

pub const SplitResult = struct {
    /// Cells whose rowids are ≤ `divider_key`. Sub-slice of input.
    left: []const btree_insert.RebuildCell,
    /// Cells whose rowids are > `divider_key`. Sub-slice of input.
    right: []const btree_insert.RebuildCell,
    /// Largest rowid in `left` — the value that goes into the parent's
    /// interior cell `key` field.
    divider_key: i64,
};

/// Split a rowid-sorted cell list at the midpoint. Returns sub-slices
/// borrowing from the input plus the divider key (= max rowid in the
/// left half). The textbook midpoint split is sufficient for B.1 — the
/// differential test only checks SELECT output, not split symmetry.
///
/// Known limitation (B.2 will fix): non-uniform cell sizes can leave
/// the right half larger than `usable_size` even when the left half
/// fits, surfacing as `rebuildLeafTablePage` IoError. Switch to a
/// byte-cumulative split (split where running total of cell bytes
/// crosses `usable_size / 2`) when generalising to non-root splits.
pub fn splitLeafCells(cells: []const btree_insert.RebuildCell) Error!SplitResult {
    if (cells.len < 2) return Error.IoError;
    const mid = cells.len / 2;
    return .{
        .left = cells[0..mid],
        .right = cells[mid..],
        .divider_key = cells[mid - 1].rowid,
    };
}

pub const InteriorFitClass = enum { fits, needs_split };

/// Predicate over a candidate list of interior cells. `fits` means
/// `writeInteriorTablePage` will succeed at the given header offset and
/// usable size; `needs_split` means the cell content + pointer array
/// would not fit. Used by `splitRightmostLeaf` to pre-check the parent
/// root before committing the child writes — a parent that wouldn't fit
/// requires recursive interior split (Iter26.B.3).
pub fn classifyForInterior(
    cells: []const InteriorCell,
    header_offset: usize,
    usable_size: usize,
) InteriorFitClass {
    var total: usize = header_offset + 12 + cells.len * 2;
    for (cells) |c| {
        total += 4 + record_encode.varintLen(@as(u64, @bitCast(c.key)));
        if (total > usable_size) return .needs_split;
    }
    return .fits;
}

pub const FitClass = enum { fits, needs_split, oversize_record };

/// Predicate over a candidate list of leaf cells. `fits` means
/// `rebuildLeafTablePage` will succeed; `needs_split` means total cell
/// content exceeds the contiguous gap (caller should balance-deeper);
/// `oversize_record` means at least one record exceeds `usable_size−35`
/// and would need an overflow chain (Iter26.C scope).
pub fn classifyForLeaf(
    cells: []const btree_insert.RebuildCell,
    header_offset: usize,
    usable_size: usize,
) FitClass {
    const x: usize = usable_size - 35;
    for (cells) |c| {
        if (c.record_bytes.len > x) return .oversize_record;
    }
    var total: usize = header_offset + 8 + cells.len * 2;
    for (cells) |c| {
        total += record_encode.varintLen(c.record_bytes.len);
        total += record_encode.varintLen(@as(u64, @bitCast(c.rowid)));
        total += c.record_bytes.len;
        if (total > usable_size) return .needs_split;
    }
    return .fits;
}

/// Write a fresh interior-table-page (type 0x05) into `page`. Pure
/// mutation — no Pager dependency. `cells` must be sorted by `key`
/// ascending (caller's responsibility, matching parseInteriorTablePage's
/// invariant). Bytes BEFORE `header_offset` (the 100-byte file header on
/// page 1) are left untouched.
pub fn writeInteriorTablePage(
    page: []u8,
    header_offset: usize,
    usable_size: usize,
    cells: []const InteriorCell,
    right_child: u32,
) Error!void {
    if (page.len < usable_size) return Error.IoError;
    // Interior page header is 12 bytes (the extra 4 carry right_child).
    const ptr_array_offset = header_offset + 12;
    const required_ptr_array: usize = cells.len * 2;
    if (ptr_array_offset + required_ptr_array > usable_size) return Error.IoError;

    var content_start: usize = usable_size;
    var k: usize = cells.len;
    while (k > 0) {
        k -= 1;
        const c = cells[k];
        const key_n = record_encode.varintLen(@as(u64, @bitCast(c.key)));
        const cell_size: usize = 4 + key_n;
        if (cell_size > content_start - (ptr_array_offset + required_ptr_array)) {
            return Error.IoError;
        }
        content_start -= cell_size;
        // left_child u32 BE.
        page[content_start] = @intCast((c.left_child >> 24) & 0xff);
        page[content_start + 1] = @intCast((c.left_child >> 16) & 0xff);
        page[content_start + 2] = @intCast((c.left_child >> 8) & 0xff);
        page[content_start + 3] = @intCast(c.left_child & 0xff);
        _ = record.encodeVarint(@as(u64, @bitCast(c.key)), page[content_start + 4 ..]);

        const ptr_slot = ptr_array_offset + k * 2;
        page[ptr_slot] = @intCast((content_start >> 8) & 0xff);
        page[ptr_slot + 1] = @intCast(content_start & 0xff);
    }

    const ptr_array_end = ptr_array_offset + required_ptr_array;
    @memset(page[ptr_array_end..content_start], 0);

    // Header (12 bytes for interior).
    page[header_offset] = 0x05;
    page[header_offset + 1] = 0x00;
    page[header_offset + 2] = 0x00;
    page[header_offset + 3] = @intCast((cells.len >> 8) & 0xff);
    page[header_offset + 4] = @intCast(cells.len & 0xff);
    page[header_offset + 5] = @intCast((content_start >> 8) & 0xff);
    page[header_offset + 6] = @intCast(content_start & 0xff);
    page[header_offset + 7] = 0x00;
    page[header_offset + 8] = @intCast((right_child >> 24) & 0xff);
    page[header_offset + 9] = @intCast((right_child >> 16) & 0xff);
    page[header_offset + 10] = @intCast((right_child >> 8) & 0xff);
    page[header_offset + 11] = @intCast(right_child & 0xff);
}

/// Convert a leaf root at `root_page_no` into an interior root with two
/// leaf children. `all_cells` MUST be the complete rowid-sorted cell
/// list including any new INSERT — caller has already merged. The root
/// page number stays stable across the call.
///
/// Page 1 is rejected (see module doc). `all_cells` shorter than 2 is
/// `Error.IoError` — there's nothing to split.
pub fn balanceDeeperRoot(
    pager: *pager_mod.Pager,
    root_page_no: u32,
    all_cells: []const btree_insert.RebuildCell,
) Error!void {
    if (root_page_no == 1) return Error.UnsupportedFeature;
    if (all_cells.len < 2) return Error.IoError;

    const usable_size = try pager.usableSize();
    const split = try splitLeafCells(all_cells);

    // Allocate the two new leaves. Order: left then right so child page
    // numbers ascend with rowid (matches sqlite3's layout convention
    // and makes integrity_check output easier to reason about).
    const left_page_no = try pager.allocatePage();
    const right_page_no = try pager.allocatePage();

    const allocator = pager.allocator;

    // Build the leaf bodies in scratch buffers. Children get written
    // before the new interior root so a crash between writes leaves
    // the OLD leaf root intact (queryable, just stale) — orphan
    // children are tolerated by sqlite3's traversal.
    const left_buf = try allocator.alloc(u8, pager_mod.PAGE_SIZE);
    defer allocator.free(left_buf);
    @memset(left_buf, 0);
    try btree_insert.rebuildLeafTablePage(left_buf, 0, usable_size, split.left);

    const right_buf = try allocator.alloc(u8, pager_mod.PAGE_SIZE);
    defer allocator.free(right_buf);
    @memset(right_buf, 0);
    try btree_insert.rebuildLeafTablePage(right_buf, 0, usable_size, split.right);

    try pager.writePage(left_page_no, left_buf);
    try pager.writePage(right_page_no, right_buf);

    // Build the new interior root. header_offset is 0 because we
    // already excluded root_page_no == 1 above.
    const root_buf = try allocator.alloc(u8, pager_mod.PAGE_SIZE);
    defer allocator.free(root_buf);
    @memset(root_buf, 0);
    const interior_cells = [_]InteriorCell{
        .{ .left_child = left_page_no, .key = split.divider_key },
    };
    try writeInteriorTablePage(root_buf, 0, usable_size, &interior_cells, right_page_no);

    try pager.writePage(root_page_no, root_buf);
}

/// Split the rightmost leaf of an interior root (Iter26.B.2). The
/// monotonic-INSERT path always grows into the rightmost leaf, so this
/// is the only split shape the B.2 differential surface exercises.
///
/// Inputs:
///   - `root_page_no`: interior root (page 1 rejected — see B.1 doc).
///   - `old_right_child`: the leaf that currently sits in the parent's
///     `right_child` slot and is overflowing.
///   - `parent_cells`: existing interior cells of the parent (sub-slice
///     borrow OK; copied into the new parent before any pager mutation).
///   - `all_combined`: rowid-sorted list of every cell that should live
///     across the two new leaves (= existing right_child cells + new
///     INSERT rows merged by the caller).
///
/// Sequence (parent-last invariant matches B.1):
///   1. allocate L_new, R_new (bumps page 1 dbsize twice).
///   2. classifyForInterior on the proposed new parent → if needs_split,
///      bail with `Error.UnsupportedFeature` (Iter26.B.3 scope). The two
///      newly-allocated pages stay zeroed and orphaned in this branch;
///      acceptable for the same reason B.1 accepts orphans.
///   3. write children: L_new, then R_new.
///   4. write parent_root LAST (now pointing at L_new + R_new).
///   5. freePage(old_right_child) — the OLD leaf is now unreachable from
///      the parent and must enter the freelist or `PRAGMA
///      integrity_check` would flag it as "never used".
pub fn splitRightmostLeaf(
    pager: *pager_mod.Pager,
    root_page_no: u32,
    old_right_child: u32,
    parent_cells: []const InteriorCell,
    all_combined: []const btree_insert.RebuildCell,
) Error!void {
    if (root_page_no == 1) return Error.UnsupportedFeature;
    if (all_combined.len < 2) return Error.IoError;

    const usable_size = try pager.usableSize();
    const split = try splitLeafCells(all_combined);

    const allocator = pager.allocator;

    // Allocate the two new leaves up front so we know their page numbers
    // when we build the new parent. allocatePage's own page-1 mutation
    // is independent of the parent root we're about to rewrite.
    const left_page_no = try pager.allocatePage();
    const right_page_no = try pager.allocatePage();

    // Compose the new parent's interior cell list = old cells +
    // (left=L_new, key=divider). right_child becomes R_new.
    const new_parent_cells = try allocator.alloc(InteriorCell, parent_cells.len + 1);
    defer allocator.free(new_parent_cells);
    @memcpy(new_parent_cells[0..parent_cells.len], parent_cells);
    new_parent_cells[parent_cells.len] = .{
        .left_child = left_page_no,
        .key = split.divider_key,
    };

    // Pre-check parent fit. root_page_no != 1 (rejected above) so
    // header_offset = 0. If the new interior cell would push the parent
    // past usable_size, this is recursive-interior-split territory
    // (B.3); fail fast and let the caller surface UnsupportedFeature.
    if (classifyForInterior(new_parent_cells, 0, usable_size) != .fits) {
        return Error.UnsupportedFeature;
    }

    // Build leaf bodies in scratch buffers; rebuildLeafTablePage may
    // still reject if a single record is oversize, in which case the
    // newly-allocated pages stay orphaned (acceptable per module doc).
    const left_buf = try allocator.alloc(u8, pager_mod.PAGE_SIZE);
    defer allocator.free(left_buf);
    @memset(left_buf, 0);
    try btree_insert.rebuildLeafTablePage(left_buf, 0, usable_size, split.left);

    const right_buf = try allocator.alloc(u8, pager_mod.PAGE_SIZE);
    defer allocator.free(right_buf);
    @memset(right_buf, 0);
    try btree_insert.rebuildLeafTablePage(right_buf, 0, usable_size, split.right);

    try pager.writePage(left_page_no, left_buf);
    try pager.writePage(right_page_no, right_buf);

    const root_buf = try allocator.alloc(u8, pager_mod.PAGE_SIZE);
    defer allocator.free(root_buf);
    @memset(root_buf, 0);
    try writeInteriorTablePage(root_buf, 0, usable_size, new_parent_cells, right_page_no);
    try pager.writePage(root_page_no, root_buf);

    // OLD right child is now unreachable from the parent — reclaim it
    // through the freelist so integrity_check stays "ok".
    try pager.freePage(old_right_child);
}

// -- tests --

const testing = std.testing;

test "splitLeafCells: midpoint split, divider = max(left)" {
    const r1 = [_]u8{ 0x02, 0x01, 0x05 };
    const cells = [_]btree_insert.RebuildCell{
        .{ .rowid = 1, .record_bytes = &r1 },
        .{ .rowid = 2, .record_bytes = &r1 },
        .{ .rowid = 3, .record_bytes = &r1 },
        .{ .rowid = 4, .record_bytes = &r1 },
    };
    const s = try splitLeafCells(&cells);
    try testing.expectEqual(@as(usize, 2), s.left.len);
    try testing.expectEqual(@as(usize, 2), s.right.len);
    try testing.expectEqual(@as(i64, 2), s.divider_key);
    try testing.expectEqual(@as(i64, 1), s.left[0].rowid);
    try testing.expectEqual(@as(i64, 3), s.right[0].rowid);
}

test "splitLeafCells: rejects 0/1 cell" {
    const empty = [_]btree_insert.RebuildCell{};
    try testing.expectError(Error.IoError, splitLeafCells(&empty));
    const r1 = [_]u8{ 0x02, 0x01, 0x05 };
    const one = [_]btree_insert.RebuildCell{.{ .rowid = 1, .record_bytes = &r1 }};
    try testing.expectError(Error.IoError, splitLeafCells(&one));
}

test "classifyForLeaf: tiny cells fit" {
    const r = [_]u8{ 0x02, 0x01, 0x07 };
    const cells = [_]btree_insert.RebuildCell{
        .{ .rowid = 1, .record_bytes = &r },
        .{ .rowid = 2, .record_bytes = &r },
    };
    try testing.expectEqual(FitClass.fits, classifyForLeaf(&cells, 0, 4096));
}

test "classifyForLeaf: oversize record" {
    const big = try testing.allocator.alloc(u8, 5000);
    defer testing.allocator.free(big);
    @memset(big, 0);
    const cells = [_]btree_insert.RebuildCell{.{ .rowid = 1, .record_bytes = big }};
    try testing.expectEqual(FitClass.oversize_record, classifyForLeaf(&cells, 0, 4096));
}

test "classifyForLeaf: many medium cells require split" {
    // ~50 byte record × 100 cells = ~5200 bytes content + 200 ptr + 8 header
    // = ~5400 bytes > 4084 usable.
    const rec = try testing.allocator.alloc(u8, 50);
    defer testing.allocator.free(rec);
    @memset(rec, 0);
    var arr: std.ArrayList(btree_insert.RebuildCell) = .empty;
    defer arr.deinit(testing.allocator);
    var i: i64 = 1;
    while (i <= 100) : (i += 1) {
        try arr.append(testing.allocator, .{ .rowid = i, .record_bytes = rec });
    }
    try testing.expectEqual(FitClass.needs_split, classifyForLeaf(arr.items, 0, 4084));
}

test "writeInteriorTablePage: round-trip via parseInteriorTablePage" {
    const page = try testing.allocator.alloc(u8, 4096);
    defer testing.allocator.free(page);
    @memset(page, 0xab);

    const cells = [_]InteriorCell{
        .{ .left_child = 5, .key = 100 },
        .{ .left_child = 7, .key = 200 },
    };
    try writeInteriorTablePage(page, 0, 4096, &cells, 9);

    const info = try btree.parseInteriorTablePage(testing.allocator, page, 0);
    defer testing.allocator.free(info.cells);
    try testing.expectEqual(@as(u32, 9), info.right_child);
    try testing.expectEqual(@as(usize, 2), info.cells.len);
    try testing.expectEqual(@as(u32, 5), info.cells[0].left_child);
    try testing.expectEqual(@as(i64, 100), info.cells[0].key);
    try testing.expectEqual(@as(u32, 7), info.cells[1].left_child);
    try testing.expectEqual(@as(i64, 200), info.cells[1].key);
}

test "classifyForInterior: small cell list fits" {
    const cells = [_]InteriorCell{
        .{ .left_child = 2, .key = 100 },
        .{ .left_child = 3, .key = 200 },
    };
    try testing.expectEqual(InteriorFitClass.fits, classifyForInterior(&cells, 0, 4096));
}

test "classifyForInterior: huge cell list needs_split" {
    var arr: std.ArrayList(InteriorCell) = .empty;
    defer arr.deinit(testing.allocator);
    var i: i64 = 1;
    while (i <= 1000) : (i += 1) {
        try arr.append(testing.allocator, .{ .left_child = @intCast(i + 1), .key = i * 1000 });
    }
    try testing.expectEqual(InteriorFitClass.needs_split, classifyForInterior(arr.items, 0, 4084));
}

test "writeInteriorTablePage: empty cells with only right_child" {
    const page = try testing.allocator.alloc(u8, 4096);
    defer testing.allocator.free(page);
    @memset(page, 0);

    const cells = [_]InteriorCell{};
    try writeInteriorTablePage(page, 0, 4096, &cells, 42);

    const info = try btree.parseInteriorTablePage(testing.allocator, page, 0);
    defer testing.allocator.free(info.cells);
    try testing.expectEqual(@as(u32, 42), info.right_child);
    try testing.expectEqual(@as(usize, 0), info.cells.len);
}
