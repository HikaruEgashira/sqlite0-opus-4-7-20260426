//! Unit tests for `btree_split_interior.zig`. Split out so the
//! production module stays under the 500-line discipline once
//! Iter26.B.3.b adds the pager-touching helpers.

const std = @import("std");
const btree_split = @import("btree_split.zig");
const btree_split_interior = @import("btree_split_interior.zig");

const testing = std.testing;
const Error = btree_split_interior.Error;
const InteriorCell = btree_split_interior.InteriorCell;
const splitInteriorCells = btree_split_interior.splitInteriorCells;

test "splitInteriorCells: 4 uniform cells split 2/1 with cells[2] promoted" {
    // Standard balanced interior split. 4 cells with equal byte mass:
    // pivot = 2, left = [c0, c1], right = [c3], promoted = c2.
    const cells = [_]InteriorCell{
        .{ .left_child = 10, .key = 100 },
        .{ .left_child = 11, .key = 200 },
        .{ .left_child = 12, .key = 300 },
        .{ .left_child = 13, .key = 400 },
    };
    const s = try splitInteriorCells(&cells, 14);
    try testing.expectEqual(@as(usize, 2), s.left_cells.len);
    try testing.expectEqual(@as(u32, 10), s.left_cells[0].left_child);
    try testing.expectEqual(@as(u32, 11), s.left_cells[1].left_child);
    try testing.expectEqual(@as(u32, 12), s.left_right_child);
    try testing.expectEqual(@as(i64, 300), s.promoted_key);
    try testing.expectEqual(@as(usize, 1), s.right_cells.len);
    try testing.expectEqual(@as(u32, 13), s.right_cells[0].left_child);
    try testing.expectEqual(@as(u32, 14), s.right_right_child);
}

test "splitInteriorCells: 2 cells minimal — left=1, right=0, promoted=cells[1]" {
    // Smallest valid input. The right half has zero cells but a non-
    // zero right_child = the input right_child_in. parseInteriorTablePage
    // already accepts this degenerate (cell_count=0 + right_child) form.
    const cells = [_]InteriorCell{
        .{ .left_child = 10, .key = 100 },
        .{ .left_child = 11, .key = 200 },
    };
    const s = try splitInteriorCells(&cells, 99);
    try testing.expectEqual(@as(usize, 1), s.left_cells.len);
    try testing.expectEqual(@as(u32, 10), s.left_cells[0].left_child);
    try testing.expectEqual(@as(i64, 100), s.left_cells[0].key);
    // Promoted cells[1].left_child becomes left_half.right_child:
    try testing.expectEqual(@as(u32, 11), s.left_right_child);
    try testing.expectEqual(@as(i64, 200), s.promoted_key);
    try testing.expectEqual(@as(usize, 0), s.right_cells.len);
    try testing.expectEqual(@as(u32, 99), s.right_right_child);
}

test "splitInteriorCells: 3 cells split symmetrically — promote the middle" {
    const cells = [_]InteriorCell{
        .{ .left_child = 10, .key = 100 },
        .{ .left_child = 11, .key = 200 },
        .{ .left_child = 12, .key = 300 },
    };
    const s = try splitInteriorCells(&cells, 13);
    try testing.expectEqual(@as(usize, 1), s.left_cells.len);
    try testing.expectEqual(@as(u32, 10), s.left_cells[0].left_child);
    try testing.expectEqual(@as(u32, 11), s.left_right_child);
    try testing.expectEqual(@as(i64, 200), s.promoted_key);
    try testing.expectEqual(@as(usize, 1), s.right_cells.len);
    try testing.expectEqual(@as(u32, 12), s.right_cells[0].left_child);
    try testing.expectEqual(@as(u32, 13), s.right_right_child);
}

test "splitInteriorCells: heterogeneous keys — pivot biased by varint width" {
    // 3 cells with single-byte keys (varint=1) followed by 1 cell
    // with a 9-byte varint key. Per-cell byte cost = 4 + 2 + varintLen(key):
    //   tiny: 7 each, big: 15. total = 7*3 + 15 = 36, half = 18.
    //   running: 7, 14, 21 (>=18 → split_at=3). pivot = 3 (last cell).
    //   left = cells[0..3], right = [], promoted = cells[3].
    // Without byte-cumulative bias, a positional midpoint at index 2
    // would put the giant cell on the right with heavy imbalance.
    const cells = [_]InteriorCell{
        .{ .left_child = 10, .key = 1 }, // varintLen(1) = 1
        .{ .left_child = 11, .key = 2 }, // varintLen(2) = 1
        .{ .left_child = 12, .key = 3 }, // varintLen(3) = 1
        .{ .left_child = 13, .key = std.math.maxInt(i64) }, // varintLen(maxInt) = 9
    };
    const s = try splitInteriorCells(&cells, 14);
    try testing.expectEqual(@as(usize, 3), s.left_cells.len);
    try testing.expectEqual(@as(u32, 13), s.left_right_child);
    try testing.expectEqual(@as(i64, std.math.maxInt(i64)), s.promoted_key);
    try testing.expectEqual(@as(usize, 0), s.right_cells.len);
    try testing.expectEqual(@as(u32, 14), s.right_right_child);
}

test "splitInteriorCells: rejects 0/1 cell" {
    const empty = [_]InteriorCell{};
    try testing.expectError(Error.IoError, splitInteriorCells(&empty, 99));
    const one = [_]InteriorCell{.{ .left_child = 10, .key = 100 }};
    try testing.expectError(Error.IoError, splitInteriorCells(&one, 99));
}

test "splitInteriorCells: promoted cell appears in NEITHER half" {
    // Sanity check on the consume-not-copy contract. Promoted key
    // must not appear in left_cells or right_cells.
    const cells = [_]InteriorCell{
        .{ .left_child = 10, .key = 100 },
        .{ .left_child = 11, .key = 200 },
        .{ .left_child = 12, .key = 300 },
        .{ .left_child = 13, .key = 400 },
        .{ .left_child = 14, .key = 500 },
    };
    const s = try splitInteriorCells(&cells, 15);
    for (s.left_cells) |c| try testing.expect(c.key != s.promoted_key);
    for (s.right_cells) |c| try testing.expect(c.key != s.promoted_key);
    // Sum of cell counts in both halves = total - 1 (one consumed).
    try testing.expectEqual(@as(usize, 4), s.left_cells.len + s.right_cells.len);
}
