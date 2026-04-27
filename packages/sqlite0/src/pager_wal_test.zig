//! Iter27.C unit tests for `pager_wal.checkpoint`.
//!
//! Discriminating round-trip: build a fixture with N committed WAL
//! frames over multiple pages, call checkpoint via `Pager`, then
//! assert that (a) the main file holds the new bytes, (b) the WAL
//! file shrunk to a fresh 32-byte header, (c) the in-memory WalState
//! is empty (wal_dbsize 0, index empty), and (d) the new WAL
//! header's checkpoint_seq advanced. A subsequent reader sees the
//! same logical row set.

const std = @import("std");
const wal = @import("wal.zig");
const wal_writer = @import("wal_writer.zig");
const wal_recovery = @import("wal_recovery.zig");
const pager_mod = @import("pager.zig");
const pager_wal = @import("pager_wal.zig");
const test_db_util = @import("test_db_util.zig");

const PAGE_SIZE = pager_mod.PAGE_SIZE;
const Error = @import("ops.zig").Error;

fn page1Header(buf: *[PAGE_SIZE]u8, dbsize: u32) void {
    @memset(buf, 0);
    // sqlite3 file-format magic so detectJournalMode + parser don't
    // bail. Bytes [0..16) = "SQLite format 3\x00".
    const magic = "SQLite format 3\x00";
    @memcpy(buf[0..magic.len], magic);
    // Page size at [16..18] big-endian — sqlite3 encodes 4096 as 0x10 0x00.
    buf[16] = @intCast((PAGE_SIZE >> 8) & 0xff);
    buf[17] = @intCast(PAGE_SIZE & 0xff);
    // Journal mode bytes [18] and [19]: WAL is (2, 2).
    buf[18] = 2;
    buf[19] = 2;
    // Dbsize-in-pages at [28..32] big-endian.
    buf[28] = @intCast((dbsize >> 24) & 0xff);
    buf[29] = @intCast((dbsize >> 16) & 0xff);
    buf[30] = @intCast((dbsize >> 8) & 0xff);
    buf[31] = @intCast(dbsize & 0xff);
}

test "checkpoint: replays committed frames to main + truncates WAL + bumps checkpoint_seq" {
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("ckpt-basic");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);

    // Main file: 3 pages. Page 1 = WAL header + dbsize=3, others stale.
    var main_p1: [PAGE_SIZE]u8 = undefined;
    page1Header(&main_p1, 3);
    var main_p2: [PAGE_SIZE]u8 = @splat(0xAA);
    var main_p3: [PAGE_SIZE]u8 = @splat(0xBB);
    try test_db_util.writePages(path, &.{ &main_p1, &main_p2, &main_p3 });

    // WAL file: write a fresh chain via WalWriter, then commit two
    // pages. Page 2 → 0xC2, page 3 → 0xC3, last frame carries
    // commit_size = 3 so dbsize stays 3.
    {
        var w = try wal_writer.create(allocator, path, 7); // start at seq 7
        defer w.close();
        const p2: [PAGE_SIZE]u8 = @splat(0xC2);
        const p3: [PAGE_SIZE]u8 = @splat(0xC3);
        _ = try w.appendFrame(2, 0, &p2);
        _ = try w.appendFrame(3, 3, &p3);
        if (std.c.fsync(w.fd) != 0) return error.FsyncFailed;
    }

    // Open the Pager + attach WAL the same way Database.openFile does.
    var p = try pager_mod.Pager.open(allocator, path);
    defer p.close();
    try pager_wal.openWal(&p, path);

    // Sanity: pre-checkpoint, getPage on page 2 returns the WAL bytes.
    {
        const got2 = try p.getPage(2);
        try std.testing.expectEqual(@as(u8, 0xC2), got2[0]);
    }

    // Run checkpoint.
    _ = try pager_wal.checkpoint(&p, .truncate);

    // Post-conditions on the in-memory state.
    try std.testing.expect(p.wal != null);
    try std.testing.expect(p.wal_writer != null);
    try std.testing.expectEqual(@as(u32, 0), p.wal.?.wal_dbsize);
    try std.testing.expectEqual(@as(usize, 0), p.wal.?.index.count());

    // Post-conditions on main file: pages 2/3 now hold the WAL bytes.
    var got2_main: [PAGE_SIZE]u8 = undefined;
    const r2 = std.c.pread(p.fd, &got2_main, PAGE_SIZE, PAGE_SIZE);
    try std.testing.expectEqual(@as(isize, PAGE_SIZE), r2);
    try std.testing.expectEqual(@as(u8, 0xC2), got2_main[0]);
    var got3_main: [PAGE_SIZE]u8 = undefined;
    const r3 = std.c.pread(p.fd, &got3_main, PAGE_SIZE, PAGE_SIZE * 2);
    try std.testing.expectEqual(@as(isize, PAGE_SIZE), r3);
    try std.testing.expectEqual(@as(u8, 0xC3), got3_main[0]);

    // -wal file is back to a 32-byte header.
    const wal_path_z = try allocator.dupeZ(u8, wal_path);
    defer allocator.free(wal_path_z);
    const flags: std.c.O = .{ .ACCMODE = .RDONLY };
    const wfd = std.c.open(wal_path_z.ptr, flags);
    try std.testing.expect(wfd >= 0);
    defer _ = std.c.close(wfd);
    const wal_size = std.c.lseek(wfd, 0, std.c.SEEK.END);
    try std.testing.expectEqual(@as(std.c.off_t, wal.HEADER_SIZE), wal_size);
}

test "checkpoint: post-checkpoint reopen sees fresh WAL with bumped seq" {
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("ckpt-reopen");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);

    var main_p1: [PAGE_SIZE]u8 = undefined;
    page1Header(&main_p1, 2);
    var main_p2: [PAGE_SIZE]u8 = @splat(0xAA);
    try test_db_util.writePages(path, &.{ &main_p1, &main_p2 });

    {
        var w = try wal_writer.create(allocator, path, 42);
        defer w.close();
        const p2: [PAGE_SIZE]u8 = @splat(0xD2);
        _ = try w.appendFrame(2, 2, &p2);
        if (std.c.fsync(w.fd) != 0) return error.FsyncFailed;
    }

    {
        var p = try pager_mod.Pager.open(allocator, path);
        defer p.close();
        try pager_wal.openWal(&p, path);
        _ = try pager_wal.checkpoint(&p, .truncate);
    }

    // Reopen from disk only — verify the new WAL header has seq 43.
    var header_buf: [wal.HEADER_SIZE]u8 = undefined;
    const wal_path_z = try allocator.dupeZ(u8, wal_path);
    defer allocator.free(wal_path_z);
    const flags: std.c.O = .{ .ACCMODE = .RDONLY };
    const fd = std.c.open(wal_path_z.ptr, flags);
    try std.testing.expect(fd >= 0);
    defer _ = std.c.close(fd);
    const r = std.c.pread(fd, &header_buf, wal.HEADER_SIZE, 0);
    try std.testing.expectEqual(@as(isize, wal.HEADER_SIZE), r);
    const hdr = try wal.parseHeader(&header_buf);
    try std.testing.expectEqual(@as(u32, 43), hdr.checkpoint_seq);
}

test "checkpoint: noop when no WAL attached" {
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("ckpt-noop");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);

    var main_p1: [PAGE_SIZE]u8 = undefined;
    page1Header(&main_p1, 1);
    main_p1[18] = 1;
    main_p1[19] = 1; // delete_legacy mode
    try test_db_util.writePages(path, &.{&main_p1});

    var p = try pager_mod.Pager.open(allocator, path);
    defer p.close();
    // No openWal call → wal == null. Checkpoint is a silent no-op.
    _ = try pager_wal.checkpoint(&p, .truncate);
    try std.testing.expect(p.wal == null);
    try std.testing.expect(p.wal_writer == null);
}

test "checkpoint: rejects mid-tx call (staged frames present)" {
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("ckpt-staged");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);

    var main_p1: [PAGE_SIZE]u8 = undefined;
    page1Header(&main_p1, 1);
    try test_db_util.writePages(path, &.{&main_p1});

    var p = try pager_mod.Pager.open(allocator, path);
    defer p.close();
    try pager_wal.openWal(&p, path);

    const p2: [PAGE_SIZE]u8 = @splat(0xE2);
    try pager_wal.writeFrame(&p, 2, &p2); // stages

    try std.testing.expectError(Error.IoError, pager_wal.checkpoint(&p, .truncate));
}

test "checkpoint(.passive): backfills main but leaves WAL bytes intact" {
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("ckpt-passive");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);

    var main_p1: [PAGE_SIZE]u8 = undefined;
    page1Header(&main_p1, 2);
    var main_p2: [PAGE_SIZE]u8 = @splat(0xAA);
    try test_db_util.writePages(path, &.{ &main_p1, &main_p2 });

    {
        var w = try wal_writer.create(allocator, path, 0);
        defer w.close();
        const p2: [PAGE_SIZE]u8 = @splat(0xC2);
        _ = try w.appendFrame(2, 2, &p2);
        if (std.c.fsync(w.fd) != 0) return error.FsyncFailed;
    }

    var p = try pager_mod.Pager.open(allocator, path);
    defer p.close();
    try pager_wal.openWal(&p, path);

    const result = try pager_wal.checkpoint(&p, .passive);
    try std.testing.expectEqual(@as(i64, 0), result.busy);
    try std.testing.expectEqual(@as(i64, 1), result.log);
    try std.testing.expectEqual(@as(i64, 1), result.ckpt);

    // Main file got the new page-2 bytes.
    var got2_main: [PAGE_SIZE]u8 = undefined;
    const r = std.c.pread(p.fd, &got2_main, PAGE_SIZE, PAGE_SIZE);
    try std.testing.expectEqual(@as(isize, PAGE_SIZE), r);
    try std.testing.expectEqual(@as(u8, 0xC2), got2_main[0]);

    // WAL file size is unchanged — passive doesn't truncate.
    const wal_path_z = try allocator.dupeZ(u8, wal_path);
    defer allocator.free(wal_path_z);
    const flags: std.c.O = .{ .ACCMODE = .RDONLY };
    const wfd = std.c.open(wal_path_z.ptr, flags);
    try std.testing.expect(wfd >= 0);
    defer _ = std.c.close(wfd);
    const wal_size = std.c.lseek(wfd, 0, std.c.SEEK.END);
    try std.testing.expectEqual(@as(std.c.off_t, wal.HEADER_SIZE + wal.FRAME_SIZE), wal_size);

    // WAL state still attached, index still has the entry.
    try std.testing.expect(p.wal != null);
    try std.testing.expectEqual(@as(usize, 1), p.wal.?.index.count());
}

test "checkpoint: returns -1|-1 when WAL not attached" {
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("ckpt-no-wal");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);

    var main_p1: [PAGE_SIZE]u8 = undefined;
    page1Header(&main_p1, 1);
    main_p1[18] = 1;
    main_p1[19] = 1; // delete_legacy
    try test_db_util.writePages(path, &.{&main_p1});

    var p = try pager_mod.Pager.open(allocator, path);
    defer p.close();
    const result = try pager_wal.checkpoint(&p, .passive);
    try std.testing.expectEqual(@as(i64, 0), result.busy);
    try std.testing.expectEqual(@as(i64, -1), result.log);
    try std.testing.expectEqual(@as(i64, -1), result.ckpt);
}
