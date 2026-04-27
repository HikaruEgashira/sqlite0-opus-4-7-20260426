//! Unit tests for `btree_split.zig`, split out so the production
//! module stays under the 500-line discipline (CLAUDE.md "Module
//! Splitting Rules"). Mirrors the `pager_write_test.zig` pattern.

const std = @import("std");
const btree = @import("btree.zig");
const btree_insert = @import("btree_insert.zig");
const btree_split = @import("btree_split.zig");

const testing = std.testing;
const Error = btree_split.Error;
const FitClass = btree_split.FitClass;
const InteriorFitClass = btree_split.InteriorFitClass;
const InteriorCell = btree_split.InteriorCell;
const splitLeafCells = btree_split.splitLeafCells;
const classifyForLeaf = btree_split.classifyForLeaf;
const classifyForInterior = btree_split.classifyForInterior;
const writeInteriorTablePage = btree_split.writeInteriorTablePage;

test "splitLeafCells: uniform cells split at positional midpoint" {
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
}

test "splitLeafCells: heterogeneous cells split by byte mass, not position" {
    // Three tiny cells (3 bytes record + 4 overhead = 7 each = 21 bytes)
    // followed by one giant (100 bytes record + 4 overhead = 104).
    // Positional midpoint would be index 2 → left=2 tiny (14B),
    // right=1 tiny + 1 giant (111B). Byte-cumulative midpoint sees
    // total=125, half=62; running 7,14,21,125 → split_at=4 clamped to 3.
    // Left = 3 tiny (21B), right = 1 giant (104B). Heavier side gets
    // the dominant cell alone.
    const tiny = [_]u8{ 0x02, 0x01, 0x07 };
    var giant: [100]u8 = undefined;
    @memset(&giant, 0xab);
    const cells = [_]btree_insert.RebuildCell{
        .{ .rowid = 1, .record_bytes = &tiny },
        .{ .rowid = 2, .record_bytes = &tiny },
        .{ .rowid = 3, .record_bytes = &tiny },
        .{ .rowid = 4, .record_bytes = &giant },
    };
    const s = try splitLeafCells(&cells);
    try testing.expectEqual(@as(usize, 3), s.left.len);
    try testing.expectEqual(@as(usize, 1), s.right.len);
    try testing.expectEqual(@as(i64, 3), s.divider_key);
    try testing.expectEqual(@as(i64, 4), s.right[0].rowid);
}

test "splitLeafCells: rejects 0/1 cell" {
    const empty = [_]btree_insert.RebuildCell{};
    try testing.expectError(Error.IoError, splitLeafCells(&empty));
    const r1 = [_]u8{ 0x02, 0x01, 0x05 };
    const one = [_]btree_insert.RebuildCell{.{ .rowid = 1, .record_bytes = &r1 }};
    try testing.expectError(Error.IoError, splitLeafCells(&one));
}

test "splitLeafCells: 2 cells always split 1/1" {
    const r1 = [_]u8{ 0x02, 0x01, 0x05 };
    var huge: [200]u8 = undefined;
    @memset(&huge, 0);
    const cells = [_]btree_insert.RebuildCell{
        .{ .rowid = 1, .record_bytes = &r1 },
        .{ .rowid = 2, .record_bytes = &huge },
    };
    const s = try splitLeafCells(&cells);
    try testing.expectEqual(@as(usize, 1), s.left.len);
    try testing.expectEqual(@as(usize, 1), s.right.len);
    try testing.expectEqual(@as(i64, 1), s.divider_key);
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
