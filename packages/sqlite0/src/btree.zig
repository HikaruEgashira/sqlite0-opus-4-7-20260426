//! SQLite3 B-tree page parsing (Iter25.B.2, ADR-0005 §2).
//!
//! Iter25.B.2 covers **leaf table pages** only (page header type 0x0d).
//! Interior tables (0x05) and index B-tree pages (0x02 / 0x0a) are
//! deferred to Iter25.B.3 / B.6.
//!
//! Pure-logic module: takes `[]const u8` page slices and returns parsed
//! cell metadata. No Pager dependency — callers feed `pager.getPage(n)`
//! results in. This keeps the module reusable for both the Pager-backed
//! production path and unit tests with hand-constructed bytes.
//!
//! ## Page header offset (page 1 special case)
//!
//! Page 1 carries the 100-byte database file header before the B-tree
//! page header begins. Every other page starts the B-tree header at
//! offset 0. Callers pass `page_header_offset` (0 or 100) — DO NOT
//! hardcode 0. The cell-pointer-array offsets are still absolute within
//! the page (counted from byte 0), so cell iteration is uniform once
//! the header is located.
//!
//! ## Overflow stance (Iter26.C)
//!
//! Table-leaf cells whose total payload exceeds X = `usable_size − 35`
//! spill into overflow pages with a 4-byte trailing page number. The
//! parser detects the chain (returning `inline_bytes` of length K and
//! a non-zero `overflow_head`); chain walking lives in
//! `btree_overflow.zig` so consumers that don't need the full payload
//! (e.g. cell-list iterators that only count rows) avoid pager I/O.
//! Cells with an inline-only payload land in the same struct with
//! `overflow_head == 0`; a corrupt cell that claims a longer payload
//! than fits inline but has no overflow head is rejected with
//! `Error.IoError`.
//!
//! ## Reference
//!
//! <https://www.sqlite.org/fileformat.html> §1.6 ("B-tree Pages") and
//! §1.5 ("Table B-Tree Leaf Cell Format").

const std = @import("std");
const ops = @import("ops.zig");
const record = @import("record.zig");
const btree_overflow = @import("btree_overflow.zig");

pub const Error = ops.Error;

pub const PageType = enum(u8) {
    interior_index = 0x02,
    interior_table = 0x05,
    leaf_index = 0x0a,
    leaf_table = 0x0d,
    _,
};

/// Parsed B-tree page header for any of the four page types. Sizes:
/// leaf pages have 8-byte headers; interior pages have a 12-byte header
/// (extra 4 bytes for the right-child pointer).
pub const PageHeader = struct {
    page_type: PageType,
    first_freeblock: u16,
    cell_count: u16,
    /// Offset within the page where the cell content area starts. The
    /// SQLite spec quirk: a value of 0 means 65536 (only relevant for
    /// page sizes ≥ 64K). Iter25.A hardcodes 4096, so this never fires;
    /// we still surface the raw 0 here and let the caller decide.
    cell_content_area: u16,
    fragmented_free_bytes: u8,
    /// Right-child pointer for interior pages; 0 for leaf pages (the
    /// header is 4 bytes shorter on leaves).
    right_child: u32 = 0,
    /// Total bytes occupied by the page header (8 for leaf, 12 for
    /// interior). Cell-pointer array starts at `header_offset + size`.
    pub fn size(self: PageHeader) usize {
        return switch (self.page_type) {
            .interior_index, .interior_table => 12,
            .leaf_index, .leaf_table => 8,
            _ => 8,
        };
    }
};

/// Parse the B-tree page header at `page[header_offset..]`. Validates the
/// type byte and returns `Error.IoError` for unknown types.
pub fn parsePageHeader(page: []const u8, header_offset: usize) Error!PageHeader {
    if (page.len < header_offset + 8) return Error.IoError;
    const t = page[header_offset];
    const page_type: PageType = switch (t) {
        0x02, 0x05, 0x0a, 0x0d => @enumFromInt(t),
        else => return Error.IoError,
    };
    var h: PageHeader = .{
        .page_type = page_type,
        .first_freeblock = readU16(page, header_offset + 1),
        .cell_count = readU16(page, header_offset + 3),
        .cell_content_area = readU16(page, header_offset + 5),
        .fragmented_free_bytes = page[header_offset + 7],
    };
    if (page_type == .interior_index or page_type == .interior_table) {
        if (page.len < header_offset + 12) return Error.IoError;
        h.right_child = readU32(page, header_offset + 8);
    }

    // Sanity check: the cell content area must not start inside the page
    // header (or before the file header on page 1). Catches misaligned
    // reads and corrupt pages early.
    const min_content_start: usize = header_offset + h.size();
    const content_start: usize = if (h.cell_content_area == 0) 65536 else h.cell_content_area;
    if (content_start < min_content_start) return Error.IoError;

    return h;
}

/// One leaf-table-page cell. `inline_bytes` slices the K (or full P)
/// payload bytes that live inline on the page; `payload_len` is the
/// full P; `overflow_head` is 0 when there's no chain, otherwise the
/// first overflow page number (caller walks via `btree_overflow.assemblePayload`).
/// Lifetime of `inline_bytes` matches the `page` slice — callers keeping
/// it past page eviction must dupe.
pub const LeafTableCell = struct {
    rowid: i64,
    inline_bytes: []const u8,
    payload_len: usize,
    overflow_head: u32 = 0,
};

/// Parse a leaf-table page (type 0x0d) and return its cells in pointer-
/// array order (which is sorted by rowid per sqlite3 invariant). The
/// returned slice is owned by `alloc` (length = `header.cell_count`);
/// individual `inline_bytes` slices borrow from `page`.
///
/// `header_offset` is 0 for every page except page 1 (which has 100).
/// `usable_size` is page_size − reserved_space (4096 default; sqlite3
/// CLI emits 12 reserved → 4084). Drives the K/M overflow split per
/// sqlite.org/fileformat.html §1.6 — see `btree_overflow.inlineSplitForPayload`.
pub fn parseLeafTablePage(
    alloc: std.mem.Allocator,
    page: []const u8,
    header_offset: usize,
    usable_size: usize,
) Error![]LeafTableCell {
    const h = try parsePageHeader(page, header_offset);
    if (h.page_type != .leaf_table) return Error.IoError;

    const ptr_array_offset = header_offset + h.size();
    if (page.len < ptr_array_offset + @as(usize, h.cell_count) * 2) return Error.IoError;

    const cells = try alloc.alloc(LeafTableCell, h.cell_count);
    errdefer alloc.free(cells);

    var i: usize = 0;
    while (i < h.cell_count) : (i += 1) {
        const cell_offset: usize = readU16(page, ptr_array_offset + i * 2);
        if (cell_offset >= page.len) return Error.IoError;

        const cell_slice = page[cell_offset..];
        const payload_len_v = try record.decodeVarint(cell_slice);
        const payload_len: usize = @intCast(payload_len_v.value);

        const rowid_v = try record.decodeVarint(cell_slice[payload_len_v.bytes_consumed..]);
        const rowid: i64 = @bitCast(rowid_v.value);

        const record_start = cell_offset + payload_len_v.bytes_consumed + rowid_v.bytes_consumed;

        // Spec K/M split decides how much of the payload lives inline
        // and whether a 4-byte overflow head pointer follows.
        const split = btree_overflow.inlineSplitForPayload(payload_len, usable_size);
        const inline_len = split.inline_len;

        const tail_size: usize = if (split.spill_len > 0) 4 else 0;
        if (record_start + inline_len + tail_size > page.len) return Error.IoError;

        var overflow_head: u32 = 0;
        if (split.spill_len > 0) {
            const ohead_off = record_start + inline_len;
            overflow_head = (@as(u32, page[ohead_off]) << 24) |
                (@as(u32, page[ohead_off + 1]) << 16) |
                (@as(u32, page[ohead_off + 2]) << 8) |
                @as(u32, page[ohead_off + 3]);
            // A spill-bearing cell with a zero head is corrupt — sqlite3
            // would never emit this and it would silently truncate the
            // payload on assemble. Fail loudly.
            if (overflow_head == 0) return Error.IoError;
        }

        cells[i] = .{
            .rowid = rowid,
            .inline_bytes = page[record_start .. record_start + inline_len],
            .payload_len = payload_len,
            .overflow_head = overflow_head,
        };
    }
    return cells;
}

/// One interior-table-page cell: pointer to a left child page plus the
/// largest rowid in that subtree. The right child of the page (anything
/// with key > all interior cells) lives in `PageHeader.right_child`, not
/// in this struct.
pub const InteriorTableCell = struct {
    left_child: u32,
    key: i64,
};

pub const InteriorTableInfo = struct {
    cells: []InteriorTableCell,
    right_child: u32,
};

/// Parse an interior-table page (type 0x05). Returns the cells in
/// pointer-array order (sorted by key ascending) plus the page's
/// right_child pointer. `cells` is owned by `alloc`.
pub fn parseInteriorTablePage(
    alloc: std.mem.Allocator,
    page: []const u8,
    header_offset: usize,
) Error!InteriorTableInfo {
    const h = try parsePageHeader(page, header_offset);
    if (h.page_type != .interior_table) return Error.IoError;

    const ptr_array_offset = header_offset + h.size();
    if (page.len < ptr_array_offset + @as(usize, h.cell_count) * 2) return Error.IoError;

    const cells = try alloc.alloc(InteriorTableCell, h.cell_count);
    errdefer alloc.free(cells);

    var i: usize = 0;
    while (i < h.cell_count) : (i += 1) {
        const cell_offset: usize = readU16(page, ptr_array_offset + i * 2);
        if (cell_offset + 4 > page.len) return Error.IoError;

        const left_child = readU32(page, cell_offset);
        const key_v = try record.decodeVarint(page[cell_offset + 4 ..]);
        cells[i] = .{
            .left_child = left_child,
            .key = @bitCast(key_v.value),
        };
    }
    return .{ .cells = cells, .right_child = h.right_child };
}

/// Page-1 has the 100-byte sqlite3 file header before the B-tree page
/// header begins. Every other page starts the header at offset 0.
pub fn pageHeaderOffset(page_no: u32) usize {
    return if (page_no == 1) 100 else 0;
}

fn readU16(bytes: []const u8, off: usize) u16 {
    return (@as(u16, bytes[off]) << 8) | bytes[off + 1];
}

fn readU32(bytes: []const u8, off: usize) u32 {
    return (@as(u32, bytes[off]) << 24) |
        (@as(u32, bytes[off + 1]) << 16) |
        (@as(u32, bytes[off + 2]) << 8) |
        bytes[off + 3];
}

// -- tests --

const testing = std.testing;
const test_util = @import("btree_test_util.zig");
const TestCellInput = test_util.TestCellInput;
const TestInteriorCellInput = test_util.TestInteriorCellInput;
const buildLeafTablePage = test_util.buildLeafTablePage;
const buildInteriorTablePage = test_util.buildInteriorTablePage;

test "parsePageHeader: leaf table" {
    const page = [_]u8{
        0x0d, // type
        0x00, 0x00, // first_freeblock
        0x00, 0x05, // cell_count = 5
        0x00, 0x80, // cell_content_area = 128
        0x00, // fragmented
    };
    const h = try parsePageHeader(&page, 0);
    try testing.expectEqual(PageType.leaf_table, h.page_type);
    try testing.expectEqual(@as(u16, 5), h.cell_count);
    try testing.expectEqual(@as(u16, 128), h.cell_content_area);
    try testing.expectEqual(@as(usize, 8), h.size());
}

test "parsePageHeader: interior table includes right child" {
    const page = [_]u8{
        0x05, 0x00, 0x00, 0x00, 0x02, 0x01, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x07, // right_child = 7
    };
    const h = try parsePageHeader(&page, 0);
    try testing.expectEqual(PageType.interior_table, h.page_type);
    try testing.expectEqual(@as(u32, 7), h.right_child);
    try testing.expectEqual(@as(usize, 12), h.size());
}

test "parsePageHeader: invalid type → IoError" {
    const page = [_]u8{ 0xab, 0, 0, 0, 0, 0, 0, 0 };
    try testing.expectError(Error.IoError, parsePageHeader(&page, 0));
}

test "parsePageHeader: short buffer → IoError" {
    const page = [_]u8{0x0d};
    try testing.expectError(Error.IoError, parsePageHeader(&page, 0));
}

test "parsePageHeader: content_start < header_end → IoError" {
    // cell_content_area claims 4 (inside the 8-byte header) — corrupt.
    const page = [_]u8{ 0x0d, 0, 0, 0, 1, 0, 4, 0 };
    try testing.expectError(Error.IoError, parsePageHeader(&page, 0));
}

test "parseLeafTablePage: empty cell list" {
    const page = [_]u8{ 0x0d, 0, 0, 0, 0, 0x01, 0x00, 0 } ++ [_]u8{0} ** 248;
    const cells = try parseLeafTablePage(testing.allocator, &page, 0, 256);
    defer testing.allocator.free(cells);
    try testing.expectEqual(@as(usize, 0), cells.len);
}

test "parseLeafTablePage: three cells in rowid order" {
    const r1 = [_]u8{ 0x02, 0x01, 0x07 }; // record: 1 column INT 7
    const r2 = [_]u8{ 0x02, 0x01, 0x0e }; // record: 1 column INT 14
    const r3 = [_]u8{ 0x02, 0x01, 0x15 }; // record: 1 column INT 21
    const inputs = [_]TestCellInput{
        .{ .rowid = 1, .record = &r1 },
        .{ .rowid = 2, .record = &r2 },
        .{ .rowid = 3, .record = &r3 },
    };
    const buf = try buildLeafTablePage(testing.allocator, 512, 0, &inputs);
    defer testing.allocator.free(buf);

    const cells = try parseLeafTablePage(testing.allocator, buf, 0, 512);
    defer testing.allocator.free(cells);
    try testing.expectEqual(@as(usize, 3), cells.len);
    try testing.expectEqual(@as(i64, 1), cells[0].rowid);
    try testing.expectEqual(@as(i64, 2), cells[1].rowid);
    try testing.expectEqual(@as(i64, 3), cells[2].rowid);

    // Decode the record bytes for cell 1 to verify the slice points at
    // the right place. No overflow → inline_bytes IS the full payload.
    try testing.expectEqual(@as(u32, 0), cells[1].overflow_head);
    try testing.expectEqual(cells[1].inline_bytes.len, cells[1].payload_len);
    const decoded = try record.decodeRecord(testing.allocator, cells[1].inline_bytes);
    defer testing.allocator.free(decoded);
    try testing.expectEqual(@as(i64, 14), decoded[0].integer);
}

test "parseLeafTablePage: header at offset 100 (page 1)" {
    const r1 = [_]u8{ 0x02, 0x01, 0x42 };
    const inputs = [_]TestCellInput{
        .{ .rowid = 99, .record = &r1 },
    };
    const buf = try buildLeafTablePage(testing.allocator, 512, 100, &inputs);
    defer testing.allocator.free(buf);

    const cells = try parseLeafTablePage(testing.allocator, buf, 100, 512);
    defer testing.allocator.free(cells);
    try testing.expectEqual(@as(usize, 1), cells.len);
    try testing.expectEqual(@as(i64, 99), cells[0].rowid);
}

test "parseLeafTablePage: oversize payload with zero overflow head → IoError" {
    // Hand-build a page where the cell claims a payload length larger
    // than X but leaves the overflow head pointer (4 bytes after the
    // inline_len portion) as zeros. Iter26.C requires a non-zero head
    // for any spilled payload — silent zero would truncate on assemble.
    const usable: usize = 256;
    const page = try testing.allocator.alloc(u8, usable);
    defer testing.allocator.free(page);
    @memset(page, 0);

    page[0] = 0x0d;
    page[3] = 0x00;
    page[4] = 0x01; // cell_count = 1
    page[5] = 0x00;
    page[6] = 0x10; // cell_content_area = 16
    // Cell pointer array: ptr 0 = 16
    page[8] = 0x00;
    page[9] = 0x10;
    // Cell at offset 16: payload_len varint = 1000 (overflow), rowid = 1.
    var tmp: [9]u8 = undefined;
    const pl_n = record.encodeVarint(1000, &tmp);
    @memcpy(page[16 .. 16 + pl_n], tmp[0..pl_n]);
    _ = record.encodeVarint(1, page[16 + pl_n ..]);

    try testing.expectError(Error.IoError, parseLeafTablePage(testing.allocator, page, 0, usable));
}

test "parseLeafTablePage: oversize payload with non-zero overflow head parses" {
    // Same hand-built page as above but with a non-zero overflow head
    // pointer in the 4 bytes following the inline portion. We're not
    // walking the chain here — just confirming parse accepts the cell.
    const usable: usize = 256;
    const page = try testing.allocator.alloc(u8, usable);
    defer testing.allocator.free(page);
    @memset(page, 0);

    page[0] = 0x0d;
    page[3] = 0x00;
    page[4] = 0x01; // cell_count = 1
    page[5] = 0x00;
    page[6] = 0x10; // cell_content_area = 16
    page[8] = 0x00;
    page[9] = 0x10;
    var tmp: [9]u8 = undefined;
    const pl_n = record.encodeVarint(1000, &tmp);
    @memcpy(page[16 .. 16 + pl_n], tmp[0..pl_n]);
    const rid_n_v = record.encodeVarint(1, page[16 + pl_n ..]);
    const record_start = 16 + pl_n + rid_n_v;
    // inlineSplitForPayload(1000, 256) → inline_len = M = 7, spill = 993.
    // Place a non-zero head pointer at record_start + 7.
    const head_off = record_start + 7;
    page[head_off + 3] = 42; // u32 BE = 42

    const cells = try parseLeafTablePage(testing.allocator, page, 0, usable);
    defer testing.allocator.free(cells);
    try testing.expectEqual(@as(usize, 1), cells.len);
    try testing.expectEqual(@as(usize, 1000), cells[0].payload_len);
    try testing.expectEqual(@as(usize, 7), cells[0].inline_bytes.len);
    try testing.expectEqual(@as(u32, 42), cells[0].overflow_head);
}

test "parseLeafTablePage: cell pointer beyond page → IoError" {
    var page = [_]u8{ 0x0d, 0, 0, 0, 1, 0x01, 0, 0, 0xff, 0xff } ++ [_]u8{0} ** 246;
    try testing.expectError(Error.IoError, parseLeafTablePage(testing.allocator, &page, 0, 256));
}

test "parseLeafTablePage: rejects non-leaf page type" {
    const page = [_]u8{ 0x05, 0, 0, 0, 0, 0x01, 0, 0, 0, 0, 0, 0 } ++ [_]u8{0} ** 244;
    try testing.expectError(Error.IoError, parseLeafTablePage(testing.allocator, &page, 0, 256));
}

test "parseInteriorTablePage: two cells + right_child" {
    const inputs = [_]TestInteriorCellInput{
        .{ .left_child = 5, .key = 100 },
        .{ .left_child = 7, .key = 200 },
    };
    const buf = try buildInteriorTablePage(testing.allocator, 512, 0, 9, &inputs);
    defer testing.allocator.free(buf);

    const info = try parseInteriorTablePage(testing.allocator, buf, 0);
    defer testing.allocator.free(info.cells);
    try testing.expectEqual(@as(u32, 9), info.right_child);
    try testing.expectEqual(@as(usize, 2), info.cells.len);
    try testing.expectEqual(@as(u32, 5), info.cells[0].left_child);
    try testing.expectEqual(@as(i64, 100), info.cells[0].key);
    try testing.expectEqual(@as(u32, 7), info.cells[1].left_child);
    try testing.expectEqual(@as(i64, 200), info.cells[1].key);
}

test "parseInteriorTablePage: rejects leaf page" {
    const inputs = [_]TestCellInput{};
    const buf = try buildLeafTablePage(testing.allocator, 512, 0, &inputs);
    defer testing.allocator.free(buf);
    try testing.expectError(Error.IoError, parseInteriorTablePage(testing.allocator, buf, 0));
}

test "pageHeaderOffset: page 1 = 100, others = 0" {
    try testing.expectEqual(@as(usize, 100), pageHeaderOffset(1));
    try testing.expectEqual(@as(usize, 0), pageHeaderOffset(2));
    try testing.expectEqual(@as(usize, 0), pageHeaderOffset(42));
}
