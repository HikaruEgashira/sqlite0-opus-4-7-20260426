//! Multi-page table B-tree traversal (Iter25.B.3, ADR-0005 §2).
//!
//! `TableLeafWalker` drives a depth-first, left-to-right walk of a table
//! B-tree rooted at any page number, yielding one leaf-table page at a
//! time. The traversal uses `Pager` for actual I/O — this is the first
//! place where Pager + btree integrate end-to-end.
//!
//! ## Why a leaf-page (not row) iterator
//!
//! Iter25.B.4 will layer a `BtreeCursor` on top that wraps this walker
//! with `parseLeafTablePage` to produce per-row iteration. Keeping the
//! walker at page granularity makes it easy to test independently and
//! lets the cursor own the row-decoding lifetime separately.
//!
//! ## LRU eviction safety
//!
//! Pager.getPage's returned slice is borrowed and may be invalidated by
//! the next getPage call. The walker copies parsed interior cells into
//! its own stack frames, so descending arbitrarily deep without a cache
//! pin is safe. The yielded leaf page bytes themselves remain borrowed:
//! callers must consume them before the next `next()` call (or dupe).
//!
//! ## sqlite3 quirk
//!
//! Page 1 has the 100-byte file header before the B-tree page header.
//! `btree.pageHeaderOffset(page_no)` handles this; the walker calls it
//! at every level so a sqlite_schema scan rooted at page 1 works the
//! same way as any user-table scan.

const std = @import("std");
const ops = @import("ops.zig");
const btree = @import("btree.zig");
const pager_mod = @import("pager.zig");

pub const Error = ops.Error;
pub const Pager = pager_mod.Pager;

/// One frame of the descent stack. Holds the parsed interior cells for
/// a level of the tree plus the index of the next child to descend
/// into. Cells are owned by `alloc`.
const InteriorFrame = struct {
    cells: []btree.InteriorTableCell,
    right_child: u32,
    /// Index into `cells` of the next child to visit. When equal to
    /// `cells.len`, the next descent target is `right_child`. When it
    /// exceeds `cells.len`, the frame is exhausted and gets popped.
    next_index: usize,
};

pub const LeafPage = struct {
    page_no: u32,
    /// Borrowed from the Pager. Valid only until the next `next()` call.
    bytes: []const u8,
    /// 100 if `page_no == 1`, else 0. Pre-computed so callers don't have
    /// to know about the file-header quirk.
    header_offset: usize,
};

pub const TableLeafWalker = struct {
    alloc: std.mem.Allocator,
    pager: *Pager,
    stack: std.ArrayList(InteriorFrame) = .empty,
    /// Page to visit on the next `next()` call. `null` once the walk
    /// has been driven to completion.
    pending: ?u32,

    pub fn init(alloc: std.mem.Allocator, p: *Pager, root_page_no: u32) TableLeafWalker {
        return .{
            .alloc = alloc,
            .pager = p,
            .pending = root_page_no,
        };
    }

    pub fn deinit(self: *TableLeafWalker) void {
        for (self.stack.items) |frame| self.alloc.free(frame.cells);
        self.stack.deinit(self.alloc);
    }

    /// Yield the next leaf page in left-to-right order. Returns `null`
    /// when the entire subtree rooted at `root_page_no` has been
    /// visited. Returned `bytes` are borrowed from the Pager.
    pub fn next(self: *TableLeafWalker) Error!?LeafPage {
        while (true) {
            if (self.pending) |page_no| {
                self.pending = null;
                const page_bytes = try self.pager.getPage(page_no);
                const header_offset = btree.pageHeaderOffset(page_no);
                const header = try btree.parsePageHeader(page_bytes, header_offset);

                switch (header.page_type) {
                    .leaf_table => return .{
                        .page_no = page_no,
                        .bytes = page_bytes,
                        .header_offset = header_offset,
                    },
                    .interior_table => {
                        const info = try btree.parseInteriorTablePage(self.alloc, page_bytes, header_offset);
                        // After this point the page_bytes slice may be
                        // evicted by future getPage calls — we've copied
                        // everything we need into `info`.
                        try self.stack.append(self.alloc, .{
                            .cells = info.cells,
                            .right_child = info.right_child,
                            .next_index = 0,
                        });
                        // Descend into the leftmost child.
                        self.pending = info.cells[0].left_child;
                        if (info.cells.len == 0) {
                            // Degenerate interior page (cell_count == 0):
                            // descend into right_child only.
                            self.pending = info.right_child;
                        }
                    },
                    else => return Error.IoError, // index pages not supported in Iter25.B.3
                }
                continue;
            }

            // No pending page → ascend the stack to find the next sibling.
            while (self.stack.items.len > 0) {
                const top = &self.stack.items[self.stack.items.len - 1];
                top.next_index += 1;
                if (top.next_index < top.cells.len) {
                    self.pending = top.cells[top.next_index].left_child;
                    break;
                } else if (top.next_index == top.cells.len) {
                    self.pending = top.right_child;
                    break;
                } else {
                    // Frame exhausted; pop and continue ascending.
                    const popped = self.stack.pop().?;
                    self.alloc.free(popped.cells);
                }
            }

            if (self.pending == null) return null;
        }
    }
};

// -- tests --

const testing = std.testing;
const test_util = @import("btree_test_util.zig");
const PAGE_SIZE = pager_mod.PAGE_SIZE;

/// Test plumbing: write a sequence of pages to a fixture file. Pages are
/// 1-indexed; index 0 in the slice corresponds to page 1.
fn writeMultiPageFixture(path: []const u8, pages: []const []const u8) !void {
    const path_z = try testing.allocator.dupeZ(u8, path);
    defer testing.allocator.free(path_z);
    const flags: std.c.O = .{ .ACCMODE = .RDWR, .CREAT = true, .TRUNC = true };
    const fd = std.c.open(path_z.ptr, flags, @as(std.c.mode_t, 0o644));
    if (fd < 0) return error.OpenFailed;
    defer _ = std.c.close(fd);

    const total = pages.len * PAGE_SIZE;
    const buf = try testing.allocator.alloc(u8, total);
    defer testing.allocator.free(buf);
    @memset(buf, 0);
    for (pages, 0..) |p, i| {
        std.debug.assert(p.len == PAGE_SIZE);
        @memcpy(buf[i * PAGE_SIZE .. (i + 1) * PAGE_SIZE], p);
    }
    const w = std.c.write(fd, buf.ptr, total);
    if (w != @as(isize, @intCast(total))) return error.WriteFailed;
}

var tlw_test_counter: std.atomic.Value(u32) = .init(0);

fn makeTempPath(suffix: []const u8) ![]u8 {
    const tmpdir_raw = std.c.getenv("TMPDIR");
    const tmpdir_slice: []const u8 = if (tmpdir_raw) |p| std.mem.span(@as([*:0]const u8, p)) else "/tmp";
    const trimmed = std.mem.trimEnd(u8, tmpdir_slice, "/");
    const pid = std.c.getpid();
    const seq = tlw_test_counter.fetchAdd(1, .seq_cst);
    return std.fmt.allocPrint(testing.allocator, "{s}/sqlite0-walk-test-{d}-{d}-{s}.db", .{ trimmed, pid, seq, suffix });
}

fn unlinkPath(path: []const u8) void {
    const path_z = testing.allocator.dupeZ(u8, path) catch return;
    defer testing.allocator.free(path_z);
    _ = std.c.unlink(path_z.ptr);
}

/// Make a page-sized leaf with one record per rowid (encoded as 1-byte
/// integer = rowid). For tests only.
fn makeLeafPage(rowids: []const i64) ![]u8 {
    var inputs = try testing.allocator.alloc(test_util.TestCellInput, rowids.len);
    defer testing.allocator.free(inputs);
    var records = try testing.allocator.alloc([3]u8, rowids.len);
    defer testing.allocator.free(records);
    for (rowids, 0..) |rid, i| {
        // serial type 1 (1-byte int), value = low byte of rowid.
        records[i] = .{ 0x02, 0x01, @intCast(@as(u8, @intCast(rid & 0xff))) };
        inputs[i] = .{ .rowid = rid, .record = &records[i] };
    }
    return try test_util.buildLeafTablePage(testing.allocator, PAGE_SIZE, 0, inputs);
}

test "TableLeafWalker: root is itself a leaf (page 1, header_offset=100)" {
    const path = try makeTempPath("rootleaf");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    // Page 1 layout: 100-byte sqlite3 file header (left zeroed for the
    // walker test — Iter25.C will validate the magic), then leaf header.
    const records = [_][3]u8{
        .{ 0x02, 0x01, 0x01 },
        .{ 0x02, 0x01, 0x02 },
        .{ 0x02, 0x01, 0x03 },
    };
    const inputs = [_]test_util.TestCellInput{
        .{ .rowid = 1, .record = &records[0] },
        .{ .rowid = 2, .record = &records[1] },
        .{ .rowid = 3, .record = &records[2] },
    };
    const page1 = try test_util.buildLeafTablePage(testing.allocator, PAGE_SIZE, 100, &inputs);
    defer testing.allocator.free(page1);
    try writeMultiPageFixture(path, &[_][]const u8{page1});

    var p = try Pager.open(testing.allocator, path);
    defer p.close();
    var w = TableLeafWalker.init(testing.allocator, &p, 1);
    defer w.deinit();

    const leaf = (try w.next()).?;
    try testing.expectEqual(@as(u32, 1), leaf.page_no);
    try testing.expectEqual(@as(usize, 100), leaf.header_offset);
    try testing.expect((try w.next()) == null);
}

test "TableLeafWalker: interior root → 3 leaves left-to-right" {
    const path = try makeTempPath("3leaves");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    // Layout:
    //   page 1 = unused (sqlite3 reserves page 1 for sqlite_schema; we
    //           leave it zero-padded so opening works)
    //   page 2 = interior root, cells = [(left=3, key=10), (left=4, key=20)], right=5
    //   page 3 = leaf with rowids 1,2
    //   page 4 = leaf with rowids 11,12
    //   page 5 = leaf with rowids 21,22,23
    const blank_page1 = try testing.allocator.alloc(u8, PAGE_SIZE);
    defer testing.allocator.free(blank_page1);
    @memset(blank_page1, 0);

    const interior_cells = [_]test_util.TestInteriorCellInput{
        .{ .left_child = 3, .key = 10 },
        .{ .left_child = 4, .key = 20 },
    };
    const page2 = try test_util.buildInteriorTablePage(testing.allocator, PAGE_SIZE, 0, 5, &interior_cells);
    defer testing.allocator.free(page2);

    const page3 = try makeLeafPage(&[_]i64{ 1, 2 });
    defer testing.allocator.free(page3);
    const page4 = try makeLeafPage(&[_]i64{ 11, 12 });
    defer testing.allocator.free(page4);
    const page5 = try makeLeafPage(&[_]i64{ 21, 22, 23 });
    defer testing.allocator.free(page5);

    try writeMultiPageFixture(path, &[_][]const u8{ blank_page1, page2, page3, page4, page5 });

    var p = try Pager.open(testing.allocator, path);
    defer p.close();
    var w = TableLeafWalker.init(testing.allocator, &p, 2);
    defer w.deinit();

    const a = (try w.next()).?;
    try testing.expectEqual(@as(u32, 3), a.page_no);
    const b = (try w.next()).?;
    try testing.expectEqual(@as(u32, 4), b.page_no);
    const c = (try w.next()).?;
    try testing.expectEqual(@as(u32, 5), c.page_no);
    try testing.expect((try w.next()) == null);
}

test "TableLeafWalker: nested interior → 4 leaves" {
    const path = try makeTempPath("nested");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    // page 1 = blank
    // page 2 = interior root: cells=[(left=3, key=10)], right=4
    // page 3 = interior:      cells=[(left=5, key=5)],  right=6
    // page 4 = interior:      cells=[(left=7, key=15)], right=8
    // pages 5,6,7,8 = leaves
    const blank_page1 = try testing.allocator.alloc(u8, PAGE_SIZE);
    defer testing.allocator.free(blank_page1);
    @memset(blank_page1, 0);

    const root_cells = [_]test_util.TestInteriorCellInput{.{ .left_child = 3, .key = 10 }};
    const page2 = try test_util.buildInteriorTablePage(testing.allocator, PAGE_SIZE, 0, 4, &root_cells);
    defer testing.allocator.free(page2);

    const left_cells = [_]test_util.TestInteriorCellInput{.{ .left_child = 5, .key = 5 }};
    const page3 = try test_util.buildInteriorTablePage(testing.allocator, PAGE_SIZE, 0, 6, &left_cells);
    defer testing.allocator.free(page3);

    const right_cells = [_]test_util.TestInteriorCellInput{.{ .left_child = 7, .key = 15 }};
    const page4 = try test_util.buildInteriorTablePage(testing.allocator, PAGE_SIZE, 0, 8, &right_cells);
    defer testing.allocator.free(page4);

    const page5 = try makeLeafPage(&[_]i64{1});
    defer testing.allocator.free(page5);
    const page6 = try makeLeafPage(&[_]i64{6});
    defer testing.allocator.free(page6);
    const page7 = try makeLeafPage(&[_]i64{11});
    defer testing.allocator.free(page7);
    const page8 = try makeLeafPage(&[_]i64{16});
    defer testing.allocator.free(page8);

    try writeMultiPageFixture(path, &[_][]const u8{ blank_page1, page2, page3, page4, page5, page6, page7, page8 });

    var p = try Pager.open(testing.allocator, path);
    defer p.close();
    var w = TableLeafWalker.init(testing.allocator, &p, 2);
    defer w.deinit();

    const expected = [_]u32{ 5, 6, 7, 8 };
    for (expected) |e| {
        const leaf = (try w.next()).?;
        try testing.expectEqual(e, leaf.page_no);
    }
    try testing.expect((try w.next()) == null);
}

test "TableLeafWalker: rejects index page (Iter25.B.3 scope)" {
    const path = try makeTempPath("idxreject");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    // page 1 = leaf-index (0x0a). Should reject with IoError.
    const buf = try testing.allocator.alloc(u8, PAGE_SIZE);
    defer testing.allocator.free(buf);
    @memset(buf, 0);
    buf[0] = 0x0a; // leaf_index — not supported by walker
    buf[5] = 0x01; // cell_content_area = 256 (anything past 8)
    buf[6] = 0x00;
    try writeMultiPageFixture(path, &[_][]const u8{buf});

    var p = try Pager.open(testing.allocator, path);
    defer p.close();
    var w = TableLeafWalker.init(testing.allocator, &p, 1);
    defer w.deinit();

    try testing.expectError(Error.IoError, w.next());
}
