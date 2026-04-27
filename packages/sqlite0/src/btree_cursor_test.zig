//! Unit tests for `btree_cursor.zig`, split out so the production
//! module stays under the 500-line discipline (CLAUDE.md "Module
//! Splitting Rules"). Mirrors the `pager_write_test.zig` and
//! `btree_split_test.zig` patterns.

const std = @import("std");
const ops = @import("ops.zig");
const pager_mod = @import("pager.zig");
const btree_cursor = @import("btree_cursor.zig");
const test_util = @import("btree_test_util.zig");

const testing = std.testing;
const Pager = pager_mod.Pager;
const PAGE_SIZE = pager_mod.PAGE_SIZE;
const BtreeCursor = btree_cursor.BtreeCursor;
const Error = ops.Error;

var bc_test_counter: std.atomic.Value(u32) = .init(0);

fn makeTempPath(suffix: []const u8) ![]u8 {
    const tmpdir_raw = std.c.getenv("TMPDIR");
    const tmpdir_slice: []const u8 = if (tmpdir_raw) |p| std.mem.span(@as([*:0]const u8, p)) else "/tmp";
    const trimmed = std.mem.trimEnd(u8, tmpdir_slice, "/");
    const pid = std.c.getpid();
    const seq = bc_test_counter.fetchAdd(1, .seq_cst);
    return std.fmt.allocPrint(testing.allocator, "{s}/sqlite0-bcursor-test-{d}-{d}-{s}.db", .{ trimmed, pid, seq, suffix });
}

fn unlinkPath(path: []const u8) void {
    const path_z = testing.allocator.dupeZ(u8, path) catch return;
    defer testing.allocator.free(path_z);
    _ = std.c.unlink(path_z.ptr);
}

fn writePages(path: []const u8, pages: []const []const u8) !void {
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

test "BtreeCursor: empty B-tree (root leaf, 0 cells)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const path = try makeTempPath("empty");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    const empty_inputs = [_]test_util.TestCellInput{};
    const page1 = try test_util.buildLeafTablePage(testing.allocator, PAGE_SIZE, 100, &empty_inputs);
    defer testing.allocator.free(page1);
    try writePages(path, &[_][]const u8{page1});

    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    const names = [_][]const u8{"x"};
    var bc = BtreeCursor.open(a, &p, 1, &names, null);
    defer bc.deinit();
    const c = bc.cursor();

    try c.rewind();
    try testing.expect(c.isEof());
}

test "BtreeCursor: walks rows in rowid order, decodes columns" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const path = try makeTempPath("walk");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    // 3 rows on page 1: each has one INTEGER column.
    const r1 = [_]u8{ 0x02, 0x01, 0x05 };
    const r2 = [_]u8{ 0x02, 0x01, 0x0a };
    const r3 = [_]u8{ 0x02, 0x01, 0x0f };
    const inputs = [_]test_util.TestCellInput{
        .{ .rowid = 1, .record = &r1 },
        .{ .rowid = 2, .record = &r2 },
        .{ .rowid = 3, .record = &r3 },
    };
    const page1 = try test_util.buildLeafTablePage(testing.allocator, PAGE_SIZE, 100, &inputs);
    defer testing.allocator.free(page1);
    try writePages(path, &[_][]const u8{page1});

    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    const names = [_][]const u8{"x"};
    var bc = BtreeCursor.open(a, &p, 1, &names, null);
    defer bc.deinit();
    const c = bc.cursor();

    try c.rewind();
    var seen: usize = 0;
    while (!c.isEof()) : (try c.next()) {
        const v = try c.column(0);
        const expected: i64 = @intCast(5 * (seen + 1));
        try testing.expectEqual(expected, v.integer);
        seen += 1;
    }
    try testing.expectEqual(@as(usize, 3), seen);
}

test "BtreeCursor: TEXT survives page churn (arena dupe)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const path = try makeTempPath("textchurn");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    // page 1 = blank (sqlite_schema slot — unused for this test)
    // page 2 = interior, single cell pointing left=3, right=4
    // page 3 = leaf with rowid 1, record = 1 column TEXT "alpha"
    // page 4 = leaf with rowid 2, record = 1 column TEXT "bravo"
    // Repeating both leaves under one interior forces the walker to
    // pull page 3, then page 4 — which would evict page 3's bytes
    // from the cache once we lower cache_capacity to 1.
    const blank = try testing.allocator.alloc(u8, PAGE_SIZE);
    defer testing.allocator.free(blank);
    @memset(blank, 0);

    const interior_cells = [_]test_util.TestInteriorCellInput{.{ .left_child = 3, .key = 1 }};
    const page2 = try test_util.buildInteriorTablePage(testing.allocator, PAGE_SIZE, 0, 4, &interior_cells);
    defer testing.allocator.free(page2);

    // serial type for TEXT of length n: 13 + 2n. "alpha" = len 5 → 23.
    const r3 = [_]u8{ 0x02, 0x17, 'a', 'l', 'p', 'h', 'a' };
    const r4 = [_]u8{ 0x02, 0x17, 'b', 'r', 'a', 'v', 'o' };
    const in3 = [_]test_util.TestCellInput{.{ .rowid = 1, .record = &r3 }};
    const in4 = [_]test_util.TestCellInput{.{ .rowid = 2, .record = &r4 }};
    const page3 = try test_util.buildLeafTablePage(testing.allocator, PAGE_SIZE, 0, &in3);
    defer testing.allocator.free(page3);
    const page4 = try test_util.buildLeafTablePage(testing.allocator, PAGE_SIZE, 0, &in4);
    defer testing.allocator.free(page4);

    try writePages(path, &[_][]const u8{ blank, page2, page3, page4 });

    var p = try Pager.open(testing.allocator, path);
    defer p.close();
    p.cache_capacity = 1; // force eviction between leaves

    const names = [_][]const u8{"name"};
    var bc = BtreeCursor.open(a, &p, 2, &names, null);
    defer bc.deinit();
    const c = bc.cursor();

    try c.rewind();
    const first = try c.column(0);
    // Read first BEFORE advancing so we can hold it across next().
    try testing.expectEqualStrings("alpha", first.text);
    try c.next();
    try testing.expect(!c.isEof());
    const second = try c.column(0);
    try testing.expectEqualStrings("bravo", second.text);
    // Critical: after advancing to row 2, the bytes the FIRST Value
    // points at are no longer in the page cache. Because BtreeCursor
    // dupes into the arena, `first.text` is still "alpha".
    try testing.expectEqualStrings("alpha", first.text);
    try c.next();
    try testing.expect(c.isEof());
}

test "BtreeCursor: rewind re-reads from the start" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const path = try makeTempPath("rewind");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    const r1 = [_]u8{ 0x02, 0x01, 0x07 };
    const r2 = [_]u8{ 0x02, 0x01, 0x0e };
    const inputs = [_]test_util.TestCellInput{
        .{ .rowid = 1, .record = &r1 },
        .{ .rowid = 2, .record = &r2 },
    };
    const page1 = try test_util.buildLeafTablePage(testing.allocator, PAGE_SIZE, 100, &inputs);
    defer testing.allocator.free(page1);
    try writePages(path, &[_][]const u8{page1});

    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    const names = [_][]const u8{"x"};
    var bc = BtreeCursor.open(a, &p, 1, &names, null);
    defer bc.deinit();
    const c = bc.cursor();

    try c.rewind();
    try c.next();
    try c.next();
    try testing.expect(c.isEof());

    try c.rewind();
    try testing.expect(!c.isEof());
    const v = try c.column(0);
    try testing.expectEqual(@as(i64, 7), v.integer);
}

test "BtreeCursor: short record yields NULL for missing trailing columns" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const path = try makeTempPath("short");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    // Schema = 3 columns; record only encodes 1.
    const r = [_]u8{ 0x02, 0x01, 0x2a };
    const inputs = [_]test_util.TestCellInput{.{ .rowid = 1, .record = &r }};
    const page1 = try test_util.buildLeafTablePage(testing.allocator, PAGE_SIZE, 100, &inputs);
    defer testing.allocator.free(page1);
    try writePages(path, &[_][]const u8{page1});

    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    const names = [_][]const u8{ "a", "b", "c" };
    var bc = BtreeCursor.open(a, &p, 1, &names, null);
    defer bc.deinit();
    const c = bc.cursor();

    try c.rewind();
    try testing.expectEqual(@as(usize, 3), c.columns().len);
    const v0 = try c.column(0);
    try testing.expectEqual(@as(i64, 0x2a), v0.integer);
    const v1 = try c.column(1);
    try testing.expect(v1 == .null);
    const v2 = try c.column(2);
    try testing.expect(v2 == .null);
    try testing.expectError(Error.SyntaxError, c.column(3));
}

test "BtreeCursor: column() before rewind returns SyntaxError" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const path = try makeTempPath("noread");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    const inputs = [_]test_util.TestCellInput{};
    const page1 = try test_util.buildLeafTablePage(testing.allocator, PAGE_SIZE, 100, &inputs);
    defer testing.allocator.free(page1);
    try writePages(path, &[_][]const u8{page1});

    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    const names = [_][]const u8{"x"};
    var bc = BtreeCursor.open(a, &p, 1, &names, null);
    defer bc.deinit();
    const c = bc.cursor();
    // Note: eof defaults to true before rewind() is called. column() on
    // an EOF cursor returns SyntaxError per the contract.
    try testing.expectError(Error.SyntaxError, c.column(0));
}
