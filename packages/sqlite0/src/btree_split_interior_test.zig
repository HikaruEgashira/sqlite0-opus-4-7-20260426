//! Unit tests for `btree_split_interior.zig`. Split out so the
//! production module stays under the 500-line discipline once
//! Iter26.B.3.b adds the pager-touching helpers.

const std = @import("std");
const btree = @import("btree.zig");
const btree_split = @import("btree_split.zig");
const btree_split_interior = @import("btree_split_interior.zig");
const pager_mod = @import("pager.zig");
const test_db_util = @import("test_db_util.zig");

const testing = std.testing;
const Error = btree_split_interior.Error;
const InteriorCell = btree_split_interior.InteriorCell;
const splitInteriorCells = btree_split_interior.splitInteriorCells;
const splitInteriorPage = btree_split_interior.splitInteriorPage;
const balanceDeeperInterior = btree_split_interior.balanceDeeperInterior;
const PAGE_SIZE = pager_mod.PAGE_SIZE;
const Pager = pager_mod.Pager;
const makeTempPath = test_db_util.makeTempPath;
const unlinkPath = test_db_util.unlinkPath;
const writeFixture = test_db_util.writeFixture;

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

// -- Pager-touching helpers (Iter26.B.3.b) --

/// Build a fixture file with a 1-page database (dbsize set to
/// `initial_dbsize`). Higher page numbers are extended via
/// `allocatePage` during the test.
fn writeMinimalFixture(path: []const u8, initial_dbsize: u32) !void {
    const initial = try testing.allocator.alloc(u8, PAGE_SIZE * initial_dbsize);
    defer testing.allocator.free(initial);
    @memset(initial, 0);
    initial[28] = @intCast((initial_dbsize >> 24) & 0xff);
    initial[29] = @intCast((initial_dbsize >> 16) & 0xff);
    initial[30] = @intCast((initial_dbsize >> 8) & 0xff);
    initial[31] = @intCast(initial_dbsize & 0xff);
    try writeFixture(path, initial);
}

test "splitInteriorPage: 5 cells split 2/2 with cells[2] promoted, both children well-formed" {
    const path = try makeTempPath("split-interior");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    // dbsize = 1 — allocatePage will hand out page 2, 3 to L_new, R_new.
    try writeMinimalFixture(path, 1);
    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    // Five interior cells with uniform single-byte keys. cellByteCost
    // = 7 each → total 35, half 17. Min-distance pivot = 2 (left=2,
    // right=2, promoted=cells[2]).
    const merged = [_]InteriorCell{
        .{ .left_child = 100, .key = 10 },
        .{ .left_child = 101, .key = 20 },
        .{ .left_child = 102, .key = 30 },
        .{ .left_child = 103, .key = 40 },
        .{ .left_child = 104, .key = 50 },
    };
    const merged_right_child: u32 = 105;

    const ps = try splitInteriorPage(&p, &merged, merged_right_child);

    // PromotedSplit shape: new_cell.left_child = L_new (= page 2),
    // new_right_child = R_new (= page 3), key = promoted (= 30).
    try testing.expectEqual(@as(u32, 2), ps.new_cell.left_child);
    try testing.expectEqual(@as(i64, 30), ps.new_cell.key);
    try testing.expectEqual(@as(u32, 3), ps.new_right_child);

    // L_new (page 2) holds cells[0..2], right_child = cells[2].left_child = 102.
    const left_bytes = try p.getPage(2);
    const left_info = try btree.parseInteriorTablePage(testing.allocator, left_bytes, 0);
    defer testing.allocator.free(left_info.cells);
    try testing.expectEqual(@as(u32, 102), left_info.right_child);
    try testing.expectEqual(@as(usize, 2), left_info.cells.len);
    try testing.expectEqual(@as(u32, 100), left_info.cells[0].left_child);
    try testing.expectEqual(@as(i64, 10), left_info.cells[0].key);
    try testing.expectEqual(@as(u32, 101), left_info.cells[1].left_child);
    try testing.expectEqual(@as(i64, 20), left_info.cells[1].key);

    // R_new (page 3) holds cells[3..5], right_child = merged_right_child = 105.
    const right_bytes = try p.getPage(3);
    const right_info = try btree.parseInteriorTablePage(testing.allocator, right_bytes, 0);
    defer testing.allocator.free(right_info.cells);
    try testing.expectEqual(@as(u32, 105), right_info.right_child);
    try testing.expectEqual(@as(usize, 2), right_info.cells.len);
    try testing.expectEqual(@as(u32, 103), right_info.cells[0].left_child);
    try testing.expectEqual(@as(i64, 40), right_info.cells[0].key);
    try testing.expectEqual(@as(u32, 104), right_info.cells[1].left_child);
    try testing.expectEqual(@as(i64, 50), right_info.cells[1].key);
}

test "splitInteriorPage: 2-cell minimal — right half is degenerate (0 cells, only right_child)" {
    const path = try makeTempPath("split-interior-2cell");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    try writeMinimalFixture(path, 1);
    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    const merged = [_]InteriorCell{
        .{ .left_child = 100, .key = 10 },
        .{ .left_child = 101, .key = 20 },
    };
    const ps = try splitInteriorPage(&p, &merged, 999);

    try testing.expectEqual(@as(u32, 2), ps.new_cell.left_child);
    try testing.expectEqual(@as(i64, 20), ps.new_cell.key);
    try testing.expectEqual(@as(u32, 3), ps.new_right_child);

    // Right half is 0 cells + right_child only — `parseInteriorTablePage`
    // accepts this degenerate shape (already covered by B.1's
    // `writeInteriorTablePage: empty cells` test).
    const right_bytes = try p.getPage(3);
    const right_info = try btree.parseInteriorTablePage(testing.allocator, right_bytes, 0);
    defer testing.allocator.free(right_info.cells);
    try testing.expectEqual(@as(usize, 0), right_info.cells.len);
    try testing.expectEqual(@as(u32, 999), right_info.right_child);
}

test "splitInteriorPage: rejects 0/1 cells" {
    const path = try makeTempPath("split-interior-bad");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    try writeMinimalFixture(path, 1);
    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    const empty = [_]InteriorCell{};
    try testing.expectError(Error.IoError, splitInteriorPage(&p, &empty, 99));
    const one = [_]InteriorCell{.{ .left_child = 100, .key = 10 }};
    try testing.expectError(Error.IoError, splitInteriorPage(&p, &one, 99));
}

test "balanceDeeperInterior: root_page_no preserved, root rewritten as 1-cell interior" {
    const path = try makeTempPath("balance-deeper-interior");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    // dbsize = 2 so the existing root sits at page 2. Child allocations
    // will land on pages 3 (L_new) and 4 (R_new).
    try writeMinimalFixture(path, 2);
    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    const root_page_no: u32 = 2;
    // Snapshot the original page-2 bytes so we can verify the rewrite
    // actually changed something.
    const before = try p.getPage(root_page_no);
    var before_copy: [PAGE_SIZE]u8 = undefined;
    @memcpy(&before_copy, before);

    const merged = [_]InteriorCell{
        .{ .left_child = 200, .key = 100 },
        .{ .left_child = 201, .key = 200 },
        .{ .left_child = 202, .key = 300 },
        .{ .left_child = 203, .key = 400 },
    };
    const merged_right_child: u32 = 204;

    try balanceDeeperInterior(&p, root_page_no, &merged, merged_right_child);

    // Root still at page_no = 2 — the bytes changed but the page slot
    // is reused. This is the critical invariant for sqlite_schema.rootpage.
    const after = try p.getPage(root_page_no);
    try testing.expect(!std.mem.eql(u8, &before_copy, after));

    // After balance-deeper, root is interior with EXACTLY 1 cell.
    // Per B.1 worked example: pivot of 4 uniform cells is index 2, so
    // promoted_key = 300, L_new gets cells[0..2], R_new gets cells[3..].
    // L_new lands on page 3, R_new on page 4.
    const root_info = try btree.parseInteriorTablePage(testing.allocator, after, 0);
    defer testing.allocator.free(root_info.cells);
    try testing.expectEqual(@as(usize, 1), root_info.cells.len);
    try testing.expectEqual(@as(u32, 3), root_info.cells[0].left_child);
    try testing.expectEqual(@as(i64, 300), root_info.cells[0].key);
    try testing.expectEqual(@as(u32, 4), root_info.right_child);

    // L_new (page 3) = cells[0..2], right_child = cells[2].left_child = 202.
    const left_bytes = try p.getPage(3);
    const left_info = try btree.parseInteriorTablePage(testing.allocator, left_bytes, 0);
    defer testing.allocator.free(left_info.cells);
    try testing.expectEqual(@as(u32, 202), left_info.right_child);
    try testing.expectEqual(@as(usize, 2), left_info.cells.len);
    try testing.expectEqual(@as(u32, 200), left_info.cells[0].left_child);
    try testing.expectEqual(@as(u32, 201), left_info.cells[1].left_child);

    // R_new (page 4) = cells[3..], right_child = merged_right_child = 204.
    const right_bytes = try p.getPage(4);
    const right_info = try btree.parseInteriorTablePage(testing.allocator, right_bytes, 0);
    defer testing.allocator.free(right_info.cells);
    try testing.expectEqual(@as(u32, 204), right_info.right_child);
    try testing.expectEqual(@as(usize, 1), right_info.cells.len);
    try testing.expectEqual(@as(u32, 203), right_info.cells[0].left_child);
    try testing.expectEqual(@as(i64, 400), right_info.cells[0].key);
}

test "balanceDeeperInterior: page 1 root rejected (sqlite_schema growth deferred)" {
    const path = try makeTempPath("balance-deeper-interior-page1");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    try writeMinimalFixture(path, 1);
    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    const merged = [_]InteriorCell{
        .{ .left_child = 100, .key = 10 },
        .{ .left_child = 101, .key = 20 },
    };
    try testing.expectError(Error.UnsupportedFeature, balanceDeeperInterior(&p, 1, &merged, 102));
}
