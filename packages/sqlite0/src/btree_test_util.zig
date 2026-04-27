//! Test-only helpers for constructing hand-built B-tree page bytes.
//! Shared by `btree.zig` parser tests and `btree_walk.zig` traversal
//! tests. Production code MUST NOT depend on this module.
//!
//! The page-construction routines mirror the byte layout described in
//! sqlite3 file-format §1.6 / §1.5, so a parser bug surfaces against the
//! same on-disk shape sqlite3 itself emits.

const std = @import("std");
const record = @import("record.zig");

pub const TestCellInput = struct { rowid: i64, record: []const u8 };
pub const TestInteriorCellInput = struct { left_child: u32, key: i64 };

/// Build a leaf-table page (type 0x0d). Cells are laid out back-to-front
/// from `page_size`. Cell pointer array follows the 8-byte page header
/// in pointer-array order (= input order).
pub fn buildLeafTablePage(
    alloc: std.mem.Allocator,
    page_size: usize,
    header_offset: usize,
    cells: []const TestCellInput,
) ![]u8 {
    const buf = try alloc.alloc(u8, page_size);
    @memset(buf, 0);

    var cell_positions = try alloc.alloc(usize, cells.len);
    defer alloc.free(cell_positions);

    var content_start: usize = page_size;
    var k: usize = cells.len;
    while (k > 0) {
        k -= 1;
        const c = cells[k];
        var tmp: [9]u8 = undefined;
        const pl_n = record.encodeVarint(c.record.len, &tmp);
        const rid_n = record.encodeVarint(@as(u64, @bitCast(c.rowid)), &tmp);
        const cell_size = pl_n + rid_n + c.record.len;
        content_start -= cell_size;
        cell_positions[k] = content_start;

        var pos = content_start;
        pos += record.encodeVarint(c.record.len, buf[pos..]);
        pos += record.encodeVarint(@as(u64, @bitCast(c.rowid)), buf[pos..]);
        @memcpy(buf[pos .. pos + c.record.len], c.record);
    }

    buf[header_offset] = 0x0d; // leaf table
    buf[header_offset + 1] = 0x00;
    buf[header_offset + 2] = 0x00;
    buf[header_offset + 3] = @intCast((cells.len >> 8) & 0xff);
    buf[header_offset + 4] = @intCast(cells.len & 0xff);
    buf[header_offset + 5] = @intCast((content_start >> 8) & 0xff);
    buf[header_offset + 6] = @intCast(content_start & 0xff);
    buf[header_offset + 7] = 0x00;

    const ptr_off = header_offset + 8;
    for (cell_positions, 0..) |cp, i| {
        buf[ptr_off + i * 2] = @intCast((cp >> 8) & 0xff);
        buf[ptr_off + i * 2 + 1] = @intCast(cp & 0xff);
    }

    return buf;
}

/// Build an interior-table page (type 0x05) with the given cells and
/// right_child pointer. Cells are laid out back-to-front.
pub fn buildInteriorTablePage(
    alloc: std.mem.Allocator,
    page_size: usize,
    header_offset: usize,
    right_child: u32,
    cells: []const TestInteriorCellInput,
) ![]u8 {
    const buf = try alloc.alloc(u8, page_size);
    @memset(buf, 0);

    var cell_positions = try alloc.alloc(usize, cells.len);
    defer alloc.free(cell_positions);

    var content_start: usize = page_size;
    var k: usize = cells.len;
    while (k > 0) {
        k -= 1;
        const c = cells[k];
        var tmp: [9]u8 = undefined;
        const key_n = record.encodeVarint(@as(u64, @bitCast(c.key)), &tmp);
        const cell_size = 4 + key_n;
        content_start -= cell_size;
        cell_positions[k] = content_start;

        buf[content_start] = @intCast((c.left_child >> 24) & 0xff);
        buf[content_start + 1] = @intCast((c.left_child >> 16) & 0xff);
        buf[content_start + 2] = @intCast((c.left_child >> 8) & 0xff);
        buf[content_start + 3] = @intCast(c.left_child & 0xff);
        _ = record.encodeVarint(@as(u64, @bitCast(c.key)), buf[content_start + 4 ..]);
    }

    buf[header_offset] = 0x05; // interior table
    buf[header_offset + 1] = 0x00;
    buf[header_offset + 2] = 0x00;
    buf[header_offset + 3] = @intCast((cells.len >> 8) & 0xff);
    buf[header_offset + 4] = @intCast(cells.len & 0xff);
    buf[header_offset + 5] = @intCast((content_start >> 8) & 0xff);
    buf[header_offset + 6] = @intCast(content_start & 0xff);
    buf[header_offset + 7] = 0x00;
    buf[header_offset + 8] = @intCast((right_child >> 24) & 0xff);
    buf[header_offset + 9] = @intCast((right_child >> 16) & 0xff);
    buf[header_offset + 10] = @intCast((right_child >> 8) & 0xff);
    buf[header_offset + 11] = @intCast(right_child & 0xff);

    const ptr_off = header_offset + 12;
    for (cell_positions, 0..) |cp, i| {
        buf[ptr_off + i * 2] = @intCast((cp >> 8) & 0xff);
        buf[ptr_off + i * 2 + 1] = @intCast(cp & 0xff);
    }

    return buf;
}
