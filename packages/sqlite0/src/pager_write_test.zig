//! `Pager.writePage` + `Pager.allocatePage` tests, split out of
//! `pager.zig` to keep that file under the 500-line discipline
//! (CLAUDE.md "Module Splitting Rules"). Production code lives
//! exclusively in `pager.zig`; this file is test-only.

const std = @import("std");
const pager_mod = @import("pager.zig");
const test_db_util = @import("test_db_util.zig");

const testing = std.testing;
const Pager = pager_mod.Pager;
const Error = pager_mod.Error;
const PAGE_SIZE = pager_mod.PAGE_SIZE;
const makeTempPath = test_db_util.makeTempPath;
const unlinkPath = test_db_util.unlinkPath;
const writeFixture = test_db_util.writeFixture;

test "Pager.writePage: round-trip through close + reopen" {
    const path = try makeTempPath("write");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    // Pre-create a 2-page file (sparse zeros) so page 1 and 2 exist
    // before we open the Pager (open requires the file to exist).
    const content = try testing.allocator.alloc(u8, PAGE_SIZE * 2);
    defer testing.allocator.free(content);
    @memset(content, 0);
    try writeFixture(path, content);

    // Construct a non-trivial payload for page 1.
    const payload = try testing.allocator.alloc(u8, PAGE_SIZE);
    defer testing.allocator.free(payload);
    @memset(payload, 0);
    @memcpy(payload[0..6], "hello!");
    payload[PAGE_SIZE - 1] = 0xab; // mark the tail too

    {
        var p = try Pager.open(testing.allocator, path);
        defer p.close();
        try p.writePage(1, payload);
    }

    // Reopen the file; verify the bytes survived the close.
    var p2 = try Pager.open(testing.allocator, path);
    defer p2.close();
    const got = try p2.getPage(1);
    try testing.expectEqualStrings("hello!", got[0..6]);
    try testing.expectEqual(@as(u8, 0xab), got[PAGE_SIZE - 1]);
}

test "Pager.writePage: rejects page 0 and wrong-length buffers" {
    const path = try makeTempPath("rejects");
    defer testing.allocator.free(path);
    defer unlinkPath(path);
    try writeFixture(path, "x");

    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    const buf = try testing.allocator.alloc(u8, PAGE_SIZE);
    defer testing.allocator.free(buf);
    @memset(buf, 0);
    try testing.expectError(Error.IoError, p.writePage(0, buf));

    const short = try testing.allocator.alloc(u8, 32);
    defer testing.allocator.free(short);
    try testing.expectError(Error.IoError, p.writePage(1, short));
}

test "Pager.writePage: cache write-through reflects in subsequent getPage" {
    const path = try makeTempPath("wt");
    defer testing.allocator.free(path);
    defer unlinkPath(path);
    try writeFixture(path, "init");

    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    // Prime the cache with the initial bytes.
    const initial = try p.getPage(1);
    try testing.expectEqualStrings("init", initial[0..4]);

    // Write new bytes; cache must reflect them WITHOUT another disk
    // read (the test trusts the write-through path because we haven't
    // reopened the file).
    const fresh = try testing.allocator.alloc(u8, PAGE_SIZE);
    defer testing.allocator.free(fresh);
    @memset(fresh, 0);
    @memcpy(fresh[0..5], "fresh");
    try p.writePage(1, fresh);

    const after = try p.getPage(1);
    try testing.expectEqualStrings("fresh", after[0..5]);
    // Pointer identity: same cache slot, in-place memcpy.
    try testing.expect(initial.ptr == after.ptr);
}

test "Pager.allocatePage: extends file and bumps in-header dbsize" {
    const path = try makeTempPath("alloc");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    // Build a 1-page file with a minimally valid header: dbsize at
    // bytes 28..31 = 1. We don't care about the rest for this test.
    const initial = try testing.allocator.alloc(u8, PAGE_SIZE);
    defer testing.allocator.free(initial);
    @memset(initial, 0);
    initial[28] = 0;
    initial[29] = 0;
    initial[30] = 0;
    initial[31] = 1; // dbsize = 1
    try writeFixture(path, initial);

    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    const new_page = try p.allocatePage();
    try testing.expectEqual(@as(u32, 2), new_page);

    // Page 1's dbsize bumped to 2.
    const page1 = try p.getPage(1);
    try testing.expectEqual(@as(u8, 0), page1[28]);
    try testing.expectEqual(@as(u8, 0), page1[29]);
    try testing.expectEqual(@as(u8, 0), page1[30]);
    try testing.expectEqual(@as(u8, 2), page1[31]);

    // Page 2 exists and is zeroed.
    const page2 = try p.getPage(2);
    try testing.expectEqual(@as(usize, PAGE_SIZE), page2.len);
    for (page2) |b| try testing.expectEqual(@as(u8, 0), b);

    // A second allocatePage call advances to page 3.
    const next = try p.allocatePage();
    try testing.expectEqual(@as(u32, 3), next);
}

test "Pager.allocatePage: rejects file with zero in-header dbsize (malformed)" {
    const path = try makeTempPath("alloc-bad");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    const initial = try testing.allocator.alloc(u8, PAGE_SIZE);
    defer testing.allocator.free(initial);
    @memset(initial, 0); // dbsize = 0 (invalid)
    try writeFixture(path, initial);

    var p = try Pager.open(testing.allocator, path);
    defer p.close();
    try testing.expectError(Error.IoError, p.allocatePage());
}

test "Pager.freePage: empty freelist promotes page to new trunk" {
    const path = try makeTempPath("free-empty");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    // Build a 3-page file. Page 1 dbsize=3, freelist trunk=0, count=0.
    const initial = try testing.allocator.alloc(u8, PAGE_SIZE * 3);
    defer testing.allocator.free(initial);
    @memset(initial, 0);
    initial[31] = 3;
    try writeFixture(path, initial);

    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    try p.freePage(3);

    const page1 = try p.getPage(1);
    // freelist trunk = 3
    try testing.expectEqual(@as(u8, 0), page1[32]);
    try testing.expectEqual(@as(u8, 0), page1[33]);
    try testing.expectEqual(@as(u8, 0), page1[34]);
    try testing.expectEqual(@as(u8, 3), page1[35]);
    // freelist count = 1
    try testing.expectEqual(@as(u8, 1), page1[39]);

    // Page 3 zero-filled (next_trunk=0, leaf_count=0).
    const page3 = try p.getPage(3);
    try testing.expectEqual(@as(u8, 0), page3[0]);
    try testing.expectEqual(@as(u8, 0), page3[7]);
}

test "Pager.freePage: appends leaf to existing trunk" {
    const path = try makeTempPath("free-append");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    const initial = try testing.allocator.alloc(u8, PAGE_SIZE * 4);
    defer testing.allocator.free(initial);
    @memset(initial, 0);
    initial[31] = 4; // dbsize = 4
    try writeFixture(path, initial);

    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    try p.freePage(3); // page 3 becomes trunk
    try p.freePage(4); // page 4 appended to trunk's leaf array

    const page1 = try p.getPage(1);
    try testing.expectEqual(@as(u8, 3), page1[35]); // trunk still page 3
    try testing.expectEqual(@as(u8, 2), page1[39]); // count = 2

    const trunk = try p.getPage(3);
    try testing.expectEqual(@as(u8, 0), trunk[0]); // next_trunk = 0
    try testing.expectEqual(@as(u8, 1), trunk[7]); // leaf_count = 1
    try testing.expectEqual(@as(u8, 4), trunk[11]); // first leaf entry = 4
}

test "Pager.freePage: rejects page 0 and 1" {
    const path = try makeTempPath("free-bad");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    const initial = try testing.allocator.alloc(u8, PAGE_SIZE);
    defer testing.allocator.free(initial);
    @memset(initial, 0);
    initial[31] = 1;
    try writeFixture(path, initial);

    var p = try Pager.open(testing.allocator, path);
    defer p.close();
    try testing.expectError(Error.IoError, p.freePage(0));
    try testing.expectError(Error.IoError, p.freePage(1));
}
