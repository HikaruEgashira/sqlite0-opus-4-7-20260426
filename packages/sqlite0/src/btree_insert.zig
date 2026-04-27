//! Leaf-table page cell insertion (Iter26.A.1, ADR-0005 §2). Pure
//! `[]u8` → `[]u8` mutation; no Pager / no walker / no encode logic.
//! Higher layers compose this with `Pager.writePage` and
//! `record_encode.encodeRecord` to land an INSERT.
//!
//! ## Free-space accounting
//!
//! We use the contiguous gap between the cell pointer array and the
//! cell content area only — fragmented freeblocks / `fragmented_free_bytes`
//! are ignored. A page with enough total free space spread across
//! freeblocks but not enough contiguous would return `.page_full` here.
//! That's a deliberate Iter26.A.1 simplification (no defrag / vacuum
//! pass yet); the next layer treats `.page_full` as "split needed",
//! and Iter26.B implements split.
//!
//! ## Overflow rejection
//!
//! Records whose payload exceeds `usable_size - 35` would need overflow
//! chain pages. We surface `Error.IoError` here rather than silently
//! truncating; Iter26.C will add the chain.

const std = @import("std");
const ops = @import("ops.zig");
const btree = @import("btree.zig");
const record = @import("record.zig");
const record_encode = @import("record_encode.zig");

pub const Error = ops.Error;

pub const InsertOutcome = enum { ok, page_full };

/// Insert one new cell (rowid + record bytes) into the leaf-table page
/// at `page` in rowid-sorted position. Returns `.page_full` if the
/// contiguous gap cannot fit the new cell. Mutates `page` in place;
/// header cell_count and cell_content_area are updated.
///
/// `header_offset` is 0 for normal pages, 100 for page 1.
/// `usable_size` is page_size − reserved_bytes (4096 for Iter25.A
/// fixtures; sqlite3 default is also 4096).
///
/// Errors:
///   - `Error.IoError` for: malformed page header, non-leaf-table page,
///     duplicate rowid (sqlite3 raises `SQLITE_CONSTRAINT_PRIMARYKEY`),
///     record exceeding overflow threshold (Iter26.C scope).
pub fn insertLeafTableCell(
    page: []u8,
    header_offset: usize,
    usable_size: usize,
    rowid: i64,
    record_bytes: []const u8,
) Error!InsertOutcome {
    const h = try btree.parsePageHeader(page, header_offset);
    if (h.page_type != .leaf_table) return Error.IoError;

    // Overflow rejection — same threshold as parseLeafTablePage.
    const x: usize = usable_size - 35;
    if (record_bytes.len > x) return Error.IoError;

    // Cell layout: payload_len varint + rowid varint + record bytes.
    const payload_len_n = record_encode.varintLen(record_bytes.len);
    const rowid_n = record_encode.varintLen(@as(u64, @bitCast(rowid)));
    const cell_size: usize = payload_len_n + rowid_n + record_bytes.len;

    // Free-space check (contiguous only).
    const ptr_array_offset = header_offset + h.size();
    const ptr_array_end = ptr_array_offset + @as(usize, h.cell_count) * 2;
    const content_start: usize = if (h.cell_content_area == 0) 65536 else h.cell_content_area;
    if (content_start < ptr_array_end) return Error.IoError; // page already over-full
    const free_contiguous = content_start - ptr_array_end;
    if (free_contiguous < cell_size + 2) return .page_full;

    // Find insertion index via linear scan over existing cell pointers.
    // Cells are stored in rowid-ascending order; we read each pointer's
    // rowid varint to compare. (Linear over cell_count is fine for
    // Iter26.A.1's small fixtures; binary search is a follow-up
    // optimisation when the differential surface forces it.)
    var insert_idx: usize = h.cell_count;
    var i: usize = 0;
    while (i < h.cell_count) : (i += 1) {
        const cp: usize = readU16(page, ptr_array_offset + i * 2);
        // Skip the payload_len varint, then read the rowid varint.
        const pl_v = try record.decodeVarint(page[cp..]);
        const rid_v = try record.decodeVarint(page[cp + pl_v.bytes_consumed ..]);
        const existing_rowid: i64 = @bitCast(rid_v.value);
        if (existing_rowid == rowid) return Error.IoError; // duplicate
        if (existing_rowid > rowid) {
            insert_idx = i;
            break;
        }
    }

    // Write the new cell into the content area (back-to-front layout).
    const new_content_start = content_start - cell_size;
    var pos = new_content_start;
    pos += record.encodeVarint(record_bytes.len, page[pos..]);
    pos += record.encodeVarint(@as(u64, @bitCast(rowid)), page[pos..]);
    @memcpy(page[pos .. pos + record_bytes.len], record_bytes);

    // Shift later cell pointer entries up by 2 to make room. Overlapping
    // copy with destination above source — copyBackwards is the correct
    // primitive (it walks from end to start so we don't trample bytes
    // we haven't yet copied).
    if (insert_idx < h.cell_count) {
        const src_start = ptr_array_offset + insert_idx * 2;
        const src_end = ptr_array_offset + @as(usize, h.cell_count) * 2;
        std.mem.copyBackwards(u8, page[src_start + 2 .. src_end + 2], page[src_start..src_end]);
    }
    // Write the new pointer.
    const ptr_slot = ptr_array_offset + insert_idx * 2;
    page[ptr_slot] = @intCast((new_content_start >> 8) & 0xff);
    page[ptr_slot + 1] = @intCast(new_content_start & 0xff);

    // Update header: cell_count and cell_content_area.
    const new_cell_count: u16 = h.cell_count + 1;
    page[header_offset + 3] = @intCast((new_cell_count >> 8) & 0xff);
    page[header_offset + 4] = @intCast(new_cell_count & 0xff);
    page[header_offset + 5] = @intCast((new_content_start >> 8) & 0xff);
    page[header_offset + 6] = @intCast(new_content_start & 0xff);

    return .ok;
}

fn readU16(bytes: []const u8, off: usize) u16 {
    return (@as(u16, bytes[off]) << 8) | bytes[off + 1];
}

// -- tests --

const testing = std.testing;
const test_util = @import("btree_test_util.zig");

test "insertLeafTableCell: into empty page" {
    const empty_inputs = [_]test_util.TestCellInput{};
    const page = try test_util.buildLeafTablePage(testing.allocator, 4096, 0, &empty_inputs);
    defer testing.allocator.free(page);

    const rec = [_]u8{ 0x02, 0x01, 0x07 }; // record: 1 col INT 7
    const r = try insertLeafTableCell(page, 0, 4096, 1, &rec);
    try testing.expectEqual(InsertOutcome.ok, r);

    const cells = try btree.parseLeafTablePage(testing.allocator, page, 0, 4096);
    defer testing.allocator.free(cells);
    try testing.expectEqual(@as(usize, 1), cells.len);
    try testing.expectEqual(@as(i64, 1), cells[0].rowid);
    try testing.expectEqualSlices(u8, &rec, cells[0].record_bytes);
}

test "insertLeafTableCell: maintains rowid sort order on out-of-order insert" {
    const r1 = [_]u8{ 0x02, 0x01, 0x05 };
    const r3 = [_]u8{ 0x02, 0x01, 0x0f };
    const inputs = [_]test_util.TestCellInput{
        .{ .rowid = 1, .record = &r1 },
        .{ .rowid = 3, .record = &r3 },
    };
    const page = try test_util.buildLeafTablePage(testing.allocator, 4096, 0, &inputs);
    defer testing.allocator.free(page);

    // Insert a cell with rowid 2 — should land between the existing two.
    const r2 = [_]u8{ 0x02, 0x01, 0x0a };
    const out = try insertLeafTableCell(page, 0, 4096, 2, &r2);
    try testing.expectEqual(InsertOutcome.ok, out);

    const cells = try btree.parseLeafTablePage(testing.allocator, page, 0, 4096);
    defer testing.allocator.free(cells);
    try testing.expectEqual(@as(usize, 3), cells.len);
    try testing.expectEqual(@as(i64, 1), cells[0].rowid);
    try testing.expectEqual(@as(i64, 2), cells[1].rowid);
    try testing.expectEqual(@as(i64, 3), cells[2].rowid);
    try testing.expectEqualSlices(u8, &r2, cells[1].record_bytes);
}

test "insertLeafTableCell: appends to non-empty page (rowid > all)" {
    const r1 = [_]u8{ 0x02, 0x01, 0x05 };
    const inputs = [_]test_util.TestCellInput{.{ .rowid = 5, .record = &r1 }};
    const page = try test_util.buildLeafTablePage(testing.allocator, 4096, 0, &inputs);
    defer testing.allocator.free(page);

    const r2 = [_]u8{ 0x02, 0x01, 0x06 };
    _ = try insertLeafTableCell(page, 0, 4096, 7, &r2);

    const cells = try btree.parseLeafTablePage(testing.allocator, page, 0, 4096);
    defer testing.allocator.free(cells);
    try testing.expectEqual(@as(usize, 2), cells.len);
    try testing.expectEqual(@as(i64, 5), cells[0].rowid);
    try testing.expectEqual(@as(i64, 7), cells[1].rowid);
}

test "insertLeafTableCell: duplicate rowid → IoError" {
    const r1 = [_]u8{ 0x02, 0x01, 0x05 };
    const inputs = [_]test_util.TestCellInput{.{ .rowid = 1, .record = &r1 }};
    const page = try test_util.buildLeafTablePage(testing.allocator, 4096, 0, &inputs);
    defer testing.allocator.free(page);

    const r2 = [_]u8{ 0x02, 0x01, 0x06 };
    try testing.expectError(Error.IoError, insertLeafTableCell(page, 0, 4096, 1, &r2));
}

test "insertLeafTableCell: returns page_full when no contiguous room" {
    // Build a tiny page (256 bytes) that is mostly full, then attempt
    // an insert that exceeds the remaining contiguous space.
    const big_record_bytes = try testing.allocator.alloc(u8, 200);
    defer testing.allocator.free(big_record_bytes);
    @memset(big_record_bytes, 0);
    big_record_bytes[0] = 0x02;
    big_record_bytes[1] = 0x17 + 198; // huge text serial type
    // The record content is irrelevant for the free-space test.

    const inputs = [_]test_util.TestCellInput{.{ .rowid = 1, .record = big_record_bytes }};
    const page = try test_util.buildLeafTablePage(testing.allocator, 256, 0, &inputs);
    defer testing.allocator.free(page);

    // Now try inserting another big record — usable_size 256 → X = 221.
    const second = try testing.allocator.alloc(u8, 100);
    defer testing.allocator.free(second);
    @memset(second, 0);
    second[0] = 0x02;
    second[1] = 0x17;

    const r = try insertLeafTableCell(page, 0, 256, 2, second);
    try testing.expectEqual(InsertOutcome.page_full, r);
}

test "insertLeafTableCell: header_offset=100 (page 1)" {
    const empty_inputs = [_]test_util.TestCellInput{};
    const page = try test_util.buildLeafTablePage(testing.allocator, 4096, 100, &empty_inputs);
    defer testing.allocator.free(page);

    const rec = [_]u8{ 0x02, 0x01, 0x42 };
    _ = try insertLeafTableCell(page, 100, 4096, 1, &rec);

    const cells = try btree.parseLeafTablePage(testing.allocator, page, 100, 4096);
    defer testing.allocator.free(cells);
    try testing.expectEqual(@as(usize, 1), cells.len);
    try testing.expectEqual(@as(i64, 1), cells[0].rowid);
}
