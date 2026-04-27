//! Read-side `Pager` tests (open / close / getPage / LRU). Separated
//! from `pager.zig` to keep the production module under the 500-line
//! discipline. Write-side tests live in `pager_write_test.zig`,
//! truncate / journal-recovery tests in `journal_test.zig`.

const std = @import("std");
const pager_mod = @import("pager.zig");
const test_db_util = @import("test_db_util.zig");

const Pager = pager_mod.Pager;
const PAGE_SIZE = pager_mod.PAGE_SIZE;
const Error = pager_mod.Error;

const testing = std.testing;
const makeTempPath = test_db_util.makeTempPath;
const unlinkPath = test_db_util.unlinkPath;
const writeFixture = test_db_util.writeFixture;

test "Pager.open: rejects nonexistent file" {
    const path = try makeTempPath("missing");
    defer testing.allocator.free(path);
    try testing.expectError(Error.IoError, Pager.open(testing.allocator, path));
}

test "Pager.getPage: reads page 1 contents" {
    const path = try makeTempPath("page1");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    var content: [16]u8 = .{ 'H', 'e', 'l', 'l', 'o', '!', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    try writeFixture(path, &content);

    var p = try Pager.open(testing.allocator, path);
    defer p.close();
    const data = try p.getPage(1);
    try testing.expectEqual(@as(usize, PAGE_SIZE), data.len);
    try testing.expectEqualStrings("Hello!", data[0..6]);
    try testing.expectEqual(@as(u8, 0), data[6]);
}

test "Pager.getPage: page 0 is invalid" {
    const path = try makeTempPath("p0");
    defer testing.allocator.free(path);
    defer unlinkPath(path);
    try writeFixture(path, "x");

    var p = try Pager.open(testing.allocator, path);
    defer p.close();
    try testing.expectError(Error.IoError, p.getPage(0));
}

test "Pager.getPage: cache hit returns same backing slice" {
    const path = try makeTempPath("hit");
    defer testing.allocator.free(path);
    defer unlinkPath(path);
    try writeFixture(path, "abc");

    var p = try Pager.open(testing.allocator, path);
    defer p.close();
    const a = try p.getPage(1);
    const b = try p.getPage(1);
    try testing.expect(a.ptr == b.ptr);
}

test "Pager.getPage: LRU eviction at capacity" {
    const path = try makeTempPath("lru");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    const n_pages: u32 = 20;
    const content = try testing.allocator.alloc(u8, PAGE_SIZE * n_pages);
    defer testing.allocator.free(content);
    @memset(content, 0);
    for (0..n_pages) |i| {
        content[i * PAGE_SIZE] = @intCast(i + 1);
    }
    try writeFixture(path, content);

    var p = try Pager.open(testing.allocator, path);
    defer p.close();
    p.cache_capacity = 4;

    var i: u32 = 1;
    while (i <= 6) : (i += 1) {
        const data = try p.getPage(i);
        try testing.expectEqual(@as(u8, @intCast(i)), data[0]);
    }
    try testing.expectEqual(@as(usize, 4), p.cache.items.len);
    try testing.expectEqual(@as(u32, 6), p.cache.items[0].page_no);
    try testing.expectEqual(@as(u32, 3), p.cache.items[3].page_no);

    const page1 = try p.getPage(1);
    try testing.expectEqual(@as(u8, 1), page1[0]);
    try testing.expectEqual(@as(u32, 1), p.cache.items[0].page_no);
    try testing.expectEqual(@as(u32, 4), p.cache.items[3].page_no);
}

test "Pager.open: second instance fails with DatabaseLocked" {
    const path = try makeTempPath("lock");
    defer testing.allocator.free(path);
    defer unlinkPath(path);
    try writeFixture(path, "x");

    var p1 = try Pager.open(testing.allocator, path);
    defer p1.close();
    try testing.expectError(Error.DatabaseLocked, Pager.open(testing.allocator, path));
}

test "Pager.getPage: LRU promotion on hit" {
    const path = try makeTempPath("promote");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    const n_pages: u32 = 4;
    const content = try testing.allocator.alloc(u8, PAGE_SIZE * n_pages);
    defer testing.allocator.free(content);
    @memset(content, 0);
    for (0..n_pages) |i| content[i * PAGE_SIZE] = @intCast(i + 1);
    try writeFixture(path, content);

    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    _ = try p.getPage(1);
    _ = try p.getPage(2);
    _ = try p.getPage(3);
    try testing.expectEqual(@as(u32, 3), p.cache.items[0].page_no);
    try testing.expectEqual(@as(u32, 1), p.cache.items[2].page_no);

    _ = try p.getPage(1);
    try testing.expectEqual(@as(u32, 1), p.cache.items[0].page_no);
    try testing.expectEqual(@as(u32, 3), p.cache.items[1].page_no);
    try testing.expectEqual(@as(u32, 2), p.cache.items[2].page_no);
}
