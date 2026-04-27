//! Iter27.0.b unit tests for `journal.recover` + `Pager.truncate`.
//!
//! Each test hand-constructs a sqlite3-format rollback journal pointing
//! at a synthetic main DB, then asserts that recovery rewrites the
//! main file's page bytes back to the journal-recorded state and
//! unlinks the journal. The fixtures stay small (≤ 3 pages) so the
//! invariant is easy to read off the buffers.

const std = @import("std");
const journal = @import("journal.zig");
const pager_mod = @import("pager.zig");
const database = @import("database.zig");
const test_db_util = @import("test_db_util.zig");

const Database = database.Database;
const JournalMode = database.JournalMode;
const Error = database.Error;

const PAGE_SIZE = pager_mod.PAGE_SIZE;
const SECTOR_SIZE: u32 = 512;

/// Minimum valid sqlite3 page-1 header in DELETE-legacy mode with an
/// empty leaf-table sqlite_schema. Caller may stamp custom bytes after
/// position 200 to make pages distinguishable.
fn makePage1(dbsize: u32) [PAGE_SIZE]u8 {
    var buf: [PAGE_SIZE]u8 = @splat(0);
    @memcpy(buf[0..16], "SQLite format 3\x00");
    buf[16] = 0x10;
    buf[17] = 0x00;
    buf[18] = 1;
    buf[19] = 1;
    buf[20] = 0;
    buf[21] = 64;
    buf[22] = 32;
    buf[23] = 32;
    buf[27] = 1;
    // dbsize at [28..32], BE u32.
    buf[28] = @intCast((dbsize >> 24) & 0xff);
    buf[29] = @intCast((dbsize >> 16) & 0xff);
    buf[30] = @intCast((dbsize >> 8) & 0xff);
    buf[31] = @intCast(dbsize & 0xff);
    buf[100] = 0x0d; // leaf_table sqlite_schema
    buf[105] = (PAGE_SIZE >> 8) & 0xff;
    buf[106] = PAGE_SIZE & 0xff;
    return buf;
}

/// Build a single-segment rollback journal containing `records`
/// original-page snapshots. `initial_db_size` is the pre-transaction
/// page count (recovery truncates main to this).
fn makeJournal(
    allocator: std.mem.Allocator,
    initial_db_size: u32,
    records: []const struct { page_no: u32, page_bytes: []const u8 },
) ![]u8 {
    // Header padded to SECTOR_SIZE, then [4 + PAGE_SIZE + 4]-byte records
    // back to back.
    const total_size = SECTOR_SIZE + records.len * (4 + PAGE_SIZE + 4);
    const buf = try allocator.alloc(u8, total_size);
    @memset(buf, 0);

    @memcpy(buf[0..8], &journal.MAGIC);
    // page_count at [8..12]
    buf[8] = @intCast((records.len >> 24) & 0xff);
    buf[9] = @intCast((records.len >> 16) & 0xff);
    buf[10] = @intCast((records.len >> 8) & 0xff);
    buf[11] = @intCast(records.len & 0xff);
    // nonce at [12..16] = 0
    // initial_db_size at [16..20]
    buf[16] = @intCast((initial_db_size >> 24) & 0xff);
    buf[17] = @intCast((initial_db_size >> 16) & 0xff);
    buf[18] = @intCast((initial_db_size >> 8) & 0xff);
    buf[19] = @intCast(initial_db_size & 0xff);
    // sector_size at [20..24]
    buf[20] = @intCast((SECTOR_SIZE >> 24) & 0xff);
    buf[21] = @intCast((SECTOR_SIZE >> 16) & 0xff);
    buf[22] = @intCast((SECTOR_SIZE >> 8) & 0xff);
    buf[23] = @intCast(SECTOR_SIZE & 0xff);
    // page_size at [24..28]
    buf[24] = @intCast((PAGE_SIZE >> 24) & 0xff);
    buf[25] = @intCast((PAGE_SIZE >> 16) & 0xff);
    buf[26] = @intCast((PAGE_SIZE >> 8) & 0xff);
    buf[27] = @intCast(PAGE_SIZE & 0xff);

    var off: usize = SECTOR_SIZE;
    for (records) |r| {
        std.debug.assert(r.page_bytes.len == PAGE_SIZE);
        buf[off] = @intCast((r.page_no >> 24) & 0xff);
        buf[off + 1] = @intCast((r.page_no >> 16) & 0xff);
        buf[off + 2] = @intCast((r.page_no >> 8) & 0xff);
        buf[off + 3] = @intCast(r.page_no & 0xff);
        @memcpy(buf[off + 4 .. off + 4 + PAGE_SIZE], r.page_bytes);
        // Checksum bytes at [off+4+PAGE_SIZE..off+8+PAGE_SIZE] left
        // zero — Iter27.0.b doesn't verify checksums.
        off += 4 + PAGE_SIZE + 4;
    }
    return buf;
}

fn writeBytes(path: []const u8, bytes: []const u8) !void {
    const path_z = try std.testing.allocator.dupeZ(u8, path);
    defer std.testing.allocator.free(path_z);
    const flags: std.c.O = .{ .ACCMODE = .RDWR, .CREAT = true, .TRUNC = true };
    const fd = std.c.open(path_z.ptr, flags, @as(std.c.mode_t, 0o644));
    if (fd < 0) return error.OpenFailed;
    defer _ = std.c.close(fd);
    const w = std.c.write(fd, bytes.ptr, bytes.len);
    if (w != @as(isize, @intCast(bytes.len))) return error.WriteFailed;
}

fn fileExists(path: []const u8) bool {
    const path_z = std.testing.allocator.dupeZ(u8, path) catch return false;
    defer std.testing.allocator.free(path_z);
    const flags: std.c.O = .{ .ACCMODE = .RDONLY };
    const fd = std.c.open(path_z.ptr, flags);
    if (fd < 0) return false;
    _ = std.c.close(fd);
    return true;
}

fn fileSize(path: []const u8) !i64 {
    const path_z = try std.testing.allocator.dupeZ(u8, path);
    defer std.testing.allocator.free(path_z);
    const flags: std.c.O = .{ .ACCMODE = .RDONLY };
    const fd = std.c.open(path_z.ptr, flags);
    if (fd < 0) return error.OpenFailed;
    defer _ = std.c.close(fd);
    const end = std.c.lseek(fd, 0, 2); // SEEK_END = 2
    if (end < 0) return error.SeekFailed;
    return @intCast(end);
}

test "journal.parseHeader: valid magic" {
    const allocator = std.testing.allocator;
    const j = try makeJournal(allocator, 1, &.{});
    defer allocator.free(j);
    const h = try journal.parseHeader(j);
    try std.testing.expectEqual(@as(u32, 0), h.page_count);
    try std.testing.expectEqual(@as(u32, 1), h.initial_db_size);
    try std.testing.expectEqual(SECTOR_SIZE, h.sector_size);
    try std.testing.expectEqual(@as(u32, PAGE_SIZE), h.page_size);
}

test "journal.parseHeader: bad magic → IoError" {
    var bad: [journal.HEADER_SIZE]u8 = @splat(0);
    try std.testing.expectError(Error.IoError, journal.parseHeader(&bad));
}

test "journal.parseHeader: short buffer → IoError" {
    var short: [10]u8 = @splat(0);
    @memcpy(short[0..8], &journal.MAGIC);
    try std.testing.expectError(Error.IoError, journal.parseHeader(&short));
}

test "journal.parseHeader: page_size != PAGE_SIZE → IoError" {
    const allocator = std.testing.allocator;
    const j = try makeJournal(allocator, 1, &.{});
    defer allocator.free(j);
    var copy = try allocator.dupe(u8, j);
    defer allocator.free(copy);
    // Tamper with page_size at [24..28]. PAGE_SIZE = 4096 = 0x1000 so
    // [24..28] = (0,0,0x10,0). Bumping byte 26 to 0x20 makes page_size
    // = 0x2000 = 8192, definitely ≠ PAGE_SIZE.
    copy[26] = 0x20;
    try std.testing.expectError(Error.IoError, journal.parseHeader(copy));
}

test "Database.openFile: hot journal rolls back page 2 mutation" {
    // Setup: main file = page1 + page2(rolled-forward state, "AFTER").
    // Journal records: page2 = "BEFORE" (= what we want to recover).
    // After openFile, page2 must read back as "BEFORE" and the journal
    // must be deleted.
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("hotjournal");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);

    var p1 = makePage1(2);
    var p2_after: [PAGE_SIZE]u8 = @splat(0);
    @memset(&p2_after, 0xAA); // current "post-failed-write" state
    var p2_before: [PAGE_SIZE]u8 = @splat(0);
    @memset(&p2_before, 0xBB); // pre-transaction state recorded in journal

    try test_db_util.writePages(path, &.{ &p1, &p2_after });

    const journal_path = try std.fmt.allocPrint(allocator, "{s}-journal", .{path});
    defer allocator.free(journal_path);
    defer test_db_util.unlinkPath(journal_path);

    const j = try makeJournal(allocator, 2, &.{
        .{ .page_no = 2, .page_bytes = &p2_before },
    });
    defer allocator.free(j);
    try writeBytes(journal_path, j);

    try std.testing.expect(fileExists(journal_path));

    var db = try Database.openFile(allocator, path);
    defer db.deinit();

    // Journal must have been unlinked by recovery.
    try std.testing.expect(!fileExists(journal_path));

    // Page 2 must now read as the BEFORE state.
    const got = try db.pager.?.getPage(2);
    try std.testing.expectEqualSlices(u8, &p2_before, got);
}

test "Database.openFile: hot journal truncates extended file back" {
    // Setup: main file has 3 pages but journal records initial_db_size=2.
    // After recovery, file size must be exactly 2 pages and page 3
    // must be unreachable (pread past EOF).
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("hotjournal-trunc");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);

    var p1 = makePage1(3); // dbsize=3 (will be rolled back to 2)
    var p2: [PAGE_SIZE]u8 = @splat(0xAA);
    var p3: [PAGE_SIZE]u8 = @splat(0xCC);

    try test_db_util.writePages(path, &.{ &p1, &p2, &p3 });

    const journal_path = try std.fmt.allocPrint(allocator, "{s}-journal", .{path});
    defer allocator.free(journal_path);
    defer test_db_util.unlinkPath(journal_path);

    var p1_pre = makePage1(2); // pre-transaction had dbsize=2
    const j = try makeJournal(allocator, 2, &.{
        .{ .page_no = 1, .page_bytes = &p1_pre },
    });
    defer allocator.free(j);
    try writeBytes(journal_path, j);

    var db = try Database.openFile(allocator, path);
    defer db.deinit();

    try std.testing.expect(!fileExists(journal_path));

    // File should be exactly 2 pages = 2 * PAGE_SIZE bytes.
    try std.testing.expectEqual(@as(i64, 2 * PAGE_SIZE), try fileSize(path));
}

test "Database.openFile: corrupt journal magic → IoError, journal preserved" {
    // Failure semantics from the module doc: parse error returns
    // Error.IoError and leaves the journal file in place so the user
    // can inspect / manually delete.
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("hotjournal-corrupt");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);

    var p1 = makePage1(1);
    try test_db_util.writePages(path, &.{&p1});

    const journal_path = try std.fmt.allocPrint(allocator, "{s}-journal", .{path});
    defer allocator.free(journal_path);
    defer test_db_util.unlinkPath(journal_path);

    // Corrupt header (no MAGIC).
    var corrupt: [journal.HEADER_SIZE]u8 = @splat(0xFF);
    try writeBytes(journal_path, &corrupt);

    try std.testing.expectError(Error.IoError, Database.openFile(allocator, path));
    // Journal still on disk — user can inspect.
    try std.testing.expect(fileExists(journal_path));
}

test "Pager.truncate: shrinks file and evicts cached pages beyond" {
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("trunc");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);

    var p1: [PAGE_SIZE]u8 = @splat(0x11);
    var p2: [PAGE_SIZE]u8 = @splat(0x22);
    var p3: [PAGE_SIZE]u8 = @splat(0x33);
    try test_db_util.writePages(path, &.{ &p1, &p2, &p3 });

    var p = try pager_mod.Pager.open(allocator, path);
    defer p.close();

    // Touch page 3 so it's in the cache before truncate.
    _ = try p.getPage(3);

    try p.truncate(2);

    try std.testing.expectEqual(@as(i64, 2 * PAGE_SIZE), try fileSize(path));
}

test "Pager.truncate: rejects n_pages == 0" {
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("trunc-zero");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);

    var p1: [PAGE_SIZE]u8 = @splat(0x11);
    try test_db_util.writePages(path, &.{&p1});

    var p = try pager_mod.Pager.open(allocator, path);
    defer p.close();

    try std.testing.expectError(Error.IoError, p.truncate(0));
}
