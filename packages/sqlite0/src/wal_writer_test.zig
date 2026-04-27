//! Iter27.B.1 unit tests for `wal_writer.zig`.
//!
//! The headline test is the **discriminating round-trip** the advisor
//! singled out: `WalWriter.create` + N×`appendFrame` (last with
//! `commit_size > 0`) → close → `wal_recovery.openIfPresent` → assert
//! the recovered index, dbsize, and chain seed match what was written.
//! This catches chain-seed and salt-mirror mistakes more reliably than
//! a synthetic encode-then-parse test (the LE/BE magic bug from
//! Iter27.A would have been caught by this round-trip on any host).
//!
//! Plus: `fromExistingState` round-trip — open existing WAL, append
//! more frames, reopen, see all of them. Confirms the writer
//! correctly inherits the chain from a prior session.

const std = @import("std");
const wal = @import("wal.zig");
const wal_writer = @import("wal_writer.zig");
const wal_recovery = @import("wal_recovery.zig");
const pager_mod = @import("pager.zig");
const test_db_util = @import("test_db_util.zig");

const PAGE_SIZE = pager_mod.PAGE_SIZE;
const Error = @import("ops.zig").Error;

test "wal_writer.create + appendFrame round-trips through openIfPresent" {
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("wal-writer-create");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    var p1: [PAGE_SIZE]u8 = @splat(0x11);
    try test_db_util.writePages(path, &.{&p1});

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);

    var w = try wal_writer.create(allocator, path, 0);
    defer w.close();

    const p2: [PAGE_SIZE]u8 = @splat(0xC2);
    const p3: [PAGE_SIZE]u8 = @splat(0xC3);

    const off2 = try w.appendFrame(2, 0, &p2); // mid-tx
    try std.testing.expectEqual(@as(u64, wal.HEADER_SIZE), off2);
    const off3 = try w.appendFrame(3, 3, &p3); // commit dbsize=3
    try std.testing.expectEqual(@as(u64, wal.HEADER_SIZE + wal.FRAME_SIZE), off3);

    const final_chain = w.chain;
    const final_off = w.next_frame_offset;
    const expected_salt1 = w.salt1;
    const expected_salt2 = w.salt2;
    w.close();

    var state = (try wal_recovery.openIfPresent(allocator, path)).?;
    defer state.deinit();

    try std.testing.expectEqual(@as(u32, 3), state.wal_dbsize);
    try std.testing.expect(state.index.contains(2));
    try std.testing.expect(state.index.contains(3));
    try std.testing.expectEqual(off2, state.index.get(2).?);
    try std.testing.expectEqual(off3, state.index.get(3).?);

    try std.testing.expectEqual(expected_salt1, state.salt1);
    try std.testing.expectEqual(expected_salt2, state.salt2);
    try std.testing.expectEqual(wal.Endianness.le, state.endian);
    try std.testing.expectEqual(final_chain, state.last_checksum);
    try std.testing.expectEqual(final_off, state.next_frame_offset);

    // Read-back through WalState confirms page bytes round-trip too.
    var got2: [PAGE_SIZE]u8 = undefined;
    try state.readPage(2, &got2);
    try std.testing.expectEqualSlices(u8, &p2, &got2);
    var got3: [PAGE_SIZE]u8 = undefined;
    try state.readPage(3, &got3);
    try std.testing.expectEqualSlices(u8, &p3, &got3);
}

test "wal_writer.create: uncommitted-only writer leaves no readable index" {
    // No commit frame ever appended → recovery sees frames as
    // tentative, discards them all, index empty, wal_dbsize 0.
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("wal-writer-uncommit");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    var p1: [PAGE_SIZE]u8 = @splat(0x11);
    try test_db_util.writePages(path, &.{&p1});

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);

    var w = try wal_writer.create(allocator, path, 0);
    defer w.close();
    const p2: [PAGE_SIZE]u8 = @splat(0xA2);
    _ = try w.appendFrame(2, 0, &p2);
    w.close();

    var state = (try wal_recovery.openIfPresent(allocator, path)).?;
    defer state.deinit();
    try std.testing.expectEqual(@as(u32, 0), state.wal_dbsize);
    try std.testing.expectEqual(@as(usize, 0), state.index.count());
}

test "wal_writer.fromExistingState: appends extend prior chain" {
    // Round 1: create + commit frame for page 2.
    // Round 2: reopen state, fromExistingState, append commit frame
    //          for page 3 (with dbsize=3). Reopen and check both pages
    //          visible.
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("wal-writer-inherit");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    var p1: [PAGE_SIZE]u8 = @splat(0x11);
    try test_db_util.writePages(path, &.{&p1});

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);

    {
        var w = try wal_writer.create(allocator, path, 0);
        defer w.close();
        const p2: [PAGE_SIZE]u8 = @splat(0xC2);
        _ = try w.appendFrame(2, 2, &p2); // commit dbsize=2
    }

    var state1 = (try wal_recovery.openIfPresent(allocator, path)).?;
    {
        defer state1.deinit();
        try std.testing.expectEqual(@as(u32, 2), state1.wal_dbsize);

        var w = try wal_writer.fromExistingState(allocator, path, &state1);
        defer w.close();
        const p3: [PAGE_SIZE]u8 = @splat(0xC3);
        _ = try w.appendFrame(3, 3, &p3); // commit dbsize=3

        // After append, the writer's internal salts should match what
        // was inherited (no rotation mid-session).
        try std.testing.expectEqual(state1.salt1, w.salt1);
        try std.testing.expectEqual(state1.salt2, w.salt2);
    }

    // Final reopen: both pages should be visible, dbsize 3.
    var state2 = (try wal_recovery.openIfPresent(allocator, path)).?;
    defer state2.deinit();
    try std.testing.expectEqual(@as(u32, 3), state2.wal_dbsize);
    try std.testing.expect(state2.index.contains(2));
    try std.testing.expect(state2.index.contains(3));
}

test "wal_writer.fromExistingState: overwrites uncommitted tail" {
    // Round 1: create + commit page 2 + uncommitted tentative page 3.
    // Round 2: reopen — wal_dbsize=2, next_frame_offset rewinds past
    //   the tentative frame. fromExistingState appends commit page 4
    //   AT the rewound offset, overwriting the tentative bytes.
    // Final reopen: pages 2 and 4 visible; page 3 NOT (its frame got
    //   overwritten by page 4's chain-valid frame).
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("wal-writer-overwrite");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    var p1: [PAGE_SIZE]u8 = @splat(0x11);
    try test_db_util.writePages(path, &.{&p1});

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);

    {
        var w = try wal_writer.create(allocator, path, 0);
        defer w.close();
        const p2: [PAGE_SIZE]u8 = @splat(0xC2);
        const p3: [PAGE_SIZE]u8 = @splat(0xC3);
        _ = try w.appendFrame(2, 2, &p2); // commit
        _ = try w.appendFrame(3, 0, &p3); // tentative
    }

    var state1 = (try wal_recovery.openIfPresent(allocator, path)).?;
    {
        defer state1.deinit();
        try std.testing.expectEqual(@as(u32, 2), state1.wal_dbsize);
        // Inherited offset rewinds past the tentative frame.
        try std.testing.expectEqual(
            @as(u64, wal.HEADER_SIZE + wal.FRAME_SIZE),
            state1.next_frame_offset,
        );

        var w = try wal_writer.fromExistingState(allocator, path, &state1);
        defer w.close();
        const p4: [PAGE_SIZE]u8 = @splat(0xC4);
        _ = try w.appendFrame(4, 3, &p4); // commit dbsize=3
    }

    var state2 = (try wal_recovery.openIfPresent(allocator, path)).?;
    defer state2.deinit();
    try std.testing.expectEqual(@as(u32, 3), state2.wal_dbsize);
    try std.testing.expect(state2.index.contains(2));
    try std.testing.expect(state2.index.contains(4));
    try std.testing.expect(!state2.index.contains(3));
}

test "wal_writer.create: produces sqlite3-compatible bytes (golden header check)" {
    // Encode through WalWriter.create, then read the header back from
    // disk and verify magic + file_format + page_size + checksum-self.
    // This is the single test that catches "create() forgot to write
    // the header" or "create() wrote it with the wrong endianness".
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("wal-writer-golden");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    var p1: [PAGE_SIZE]u8 = @splat(0);
    try test_db_util.writePages(path, &.{&p1});

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);

    var w = try wal_writer.create(allocator, path, 7);
    w.close();

    // Read the header bytes back.
    const path_z = try allocator.dupeZ(u8, wal_path);
    defer allocator.free(path_z);
    const flags: std.c.O = .{ .ACCMODE = .RDONLY };
    const fd = std.c.open(path_z.ptr, flags);
    try std.testing.expect(fd >= 0);
    defer _ = std.c.close(fd);

    var buf: [wal.HEADER_SIZE]u8 = undefined;
    const n = std.c.pread(fd, &buf, wal.HEADER_SIZE, 0);
    try std.testing.expectEqual(@as(isize, @intCast(wal.HEADER_SIZE)), n);

    const h = try wal.parseHeader(&buf);
    try std.testing.expectEqual(wal.MAGIC_LE, h.magic);
    try std.testing.expectEqual(@as(u32, wal.FILE_FORMAT), h.file_format);
    try std.testing.expectEqual(@as(u32, PAGE_SIZE), h.page_size);
    try std.testing.expectEqual(@as(u32, 7), h.checkpoint_seq);
    try std.testing.expect(wal.verifyHeaderChecksum(&buf, h));
}
