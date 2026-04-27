//! Iter27.A unit tests for `wal.zig` + `wal_recovery.zig`.
//!
//! Two layers:
//!   1. **Golden bytes** from a real sqlite3-emitted -wal file (the
//!      Iter27.A spike) anchor `walChecksum` to spec-conformant output.
//!      Without these we'd be testing the implementation against
//!      itself — the recovery tests below all use `walChecksum` to
//!      synthesise frames, so a bug in the algorithm would be invisible.
//!   2. **Synthetic recovery fixtures** built in-memory exercise the
//!      tentative-then-promoted index algorithm: empty, single commit,
//!      multi-commit, partial transaction discarded, salt mismatch,
//!      checksum mismatch.

const std = @import("std");
const wal = @import("wal.zig");
const wal_recovery = @import("wal_recovery.zig");
const pager_mod = @import("pager.zig");
const test_db_util = @import("test_db_util.zig");

const PAGE_SIZE = pager_mod.PAGE_SIZE;
const Error = @import("ops.zig").Error;

// Captured from a real sqlite3-emitted -wal file (PRAGMA journal_mode=WAL;
// PRAGMA wal_autocheckpoint=0; CREATE TABLE t(x); INSERT INTO t VALUES(1)
// on a darwin LE host). Magic = 0x377f0682 = WRITER WAS LE → checksum
// algorithm consumes words in LE order.
const REAL_HEADER_BYTES = [_]u8{
    0x37, 0x7f, 0x06, 0x82, // magic (LE host)
    0x00, 0x2d, 0xe2, 0x18, // file_format = 3007000
    0x00, 0x00, 0x10, 0x00, // page_size = 4096
    0x00, 0x00, 0x00, 0x00, // checkpoint_seq = 0
    0xc3, 0xad, 0x9c, 0x0b, // salt-1
    0x9b, 0xe2, 0xd7, 0x9e, // salt-2
    0xe0, 0x83, 0xb0, 0xd6, // checksum-1 (over bytes [0..24] in LE word order)
    0x37, 0x43, 0x6b, 0x16, // checksum-2
};

test "wal.walChecksum: golden vs sqlite3-real header bytes" {
    const result = wal.walChecksum(.{ 0, 0 }, REAL_HEADER_BYTES[0..24], .le);
    try std.testing.expectEqual(@as(u32, 0xe083b0d6), result[0]);
    try std.testing.expectEqual(@as(u32, 0x37436b16), result[1]);
}

test "wal.parseHeader: golden bytes decode" {
    const h = try wal.parseHeader(&REAL_HEADER_BYTES);
    try std.testing.expectEqual(wal.MAGIC_LE, h.magic);
    try std.testing.expectEqual(@as(u32, 3007000), h.file_format);
    try std.testing.expectEqual(@as(u32, 4096), h.page_size);
    try std.testing.expectEqual(@as(u32, 0), h.checkpoint_seq);
    try std.testing.expectEqual(@as(u32, 0xc3ad9c0b), h.salt1);
    try std.testing.expectEqual(@as(u32, 0x9be2d79e), h.salt2);
    try std.testing.expectEqual(wal.Endianness.le, h.endianness());
}

test "wal.verifyHeaderChecksum: golden bytes pass" {
    const h = try wal.parseHeader(&REAL_HEADER_BYTES);
    try std.testing.expect(wal.verifyHeaderChecksum(&REAL_HEADER_BYTES, h));
}

test "wal.verifyHeaderChecksum: tampered bytes fail" {
    var bytes = REAL_HEADER_BYTES;
    bytes[16] = 0x00; // mutate salt-1
    const h = try wal.parseHeader(&bytes);
    try std.testing.expect(!wal.verifyHeaderChecksum(&bytes, h));
}

test "wal.parseHeader: bad magic → IoError" {
    var bytes = REAL_HEADER_BYTES;
    bytes[0] = 0xff;
    try std.testing.expectError(Error.IoError, wal.parseHeader(&bytes));
}

test "wal.parseHeader: bad file_format → IoError" {
    var bytes = REAL_HEADER_BYTES;
    bytes[7] = 0xff;
    try std.testing.expectError(Error.IoError, wal.parseHeader(&bytes));
}

test "wal.parseHeader: wrong page_size → IoError" {
    var bytes = REAL_HEADER_BYTES;
    bytes[10] = 0x20; // page_size = 0x2000 = 8192
    try std.testing.expectError(Error.IoError, wal.parseHeader(&bytes));
}

test "wal.parseHeader: short → IoError" {
    var bytes: [10]u8 = @splat(0);
    try std.testing.expectError(Error.IoError, wal.parseHeader(&bytes));
}

// ---------- WAL fixture builder ----------

const WAL_HEADER_SIZE = wal.HEADER_SIZE;
const WAL_FRAME_HEADER_SIZE = wal.FRAME_HEADER_SIZE;
const WAL_FRAME_SIZE = wal.FRAME_SIZE;

const FixtureFrame = struct {
    page_no: u32,
    commit_size: u32,
    page_bytes: *const [PAGE_SIZE]u8,
    /// When set, the frame's checksum field is filled with garbage
    /// instead of the cumulative chain. Used to test "scan stops at
    /// invalid frame".
    corrupt_checksum: bool = false,
    /// When set, the frame's salt fields are zeroed instead of mirroring
    /// the header's. Used to test "scan stops at salt mismatch".
    bad_salt: bool = false,
};

/// Build a WAL byte buffer with the given salts and frames. Computes a
/// valid header checksum and (per-frame) cumulative chain. Frames flagged
/// `corrupt_checksum` / `bad_salt` deliberately break the chain.
fn buildWal(
    allocator: std.mem.Allocator,
    salt1: u32,
    salt2: u32,
    frames: []const FixtureFrame,
) ![]u8 {
    const total = WAL_HEADER_SIZE + frames.len * WAL_FRAME_SIZE;
    const buf = try allocator.alloc(u8, total);
    @memset(buf, 0);

    // Header
    wal.writeU32BE(buf[0..4], wal.MAGIC_LE); // pick LE — checksum uses LE word order
    wal.writeU32BE(buf[4..8], wal.FILE_FORMAT);
    wal.writeU32BE(buf[8..12], PAGE_SIZE);
    wal.writeU32BE(buf[12..16], 0);
    wal.writeU32BE(buf[16..20], salt1);
    wal.writeU32BE(buf[20..24], salt2);
    const header_cksum = wal.walChecksum(.{ 0, 0 }, buf[0..24], .le);
    wal.writeU32BE(buf[24..28], header_cksum[0]);
    wal.writeU32BE(buf[28..32], header_cksum[1]);

    var running: [2]u32 = header_cksum;
    var off: usize = WAL_HEADER_SIZE;
    for (frames) |f| {
        wal.writeU32BE(buf[off .. off + 4][0..4], f.page_no);
        wal.writeU32BE(buf[off + 4 .. off + 8][0..4], f.commit_size);
        if (f.bad_salt) {
            wal.writeU32BE(buf[off + 8 .. off + 12][0..4], 0);
            wal.writeU32BE(buf[off + 12 .. off + 16][0..4], 0);
        } else {
            wal.writeU32BE(buf[off + 8 .. off + 12][0..4], salt1);
            wal.writeU32BE(buf[off + 12 .. off + 16][0..4], salt2);
        }
        @memcpy(buf[off + WAL_FRAME_HEADER_SIZE .. off + WAL_FRAME_SIZE], f.page_bytes);
        // Compute cumulative checksum over [page_no, commit_size, page_bytes].
        var next = wal.walChecksum(running, buf[off .. off + 8], .le);
        next = wal.walChecksum(next, buf[off + WAL_FRAME_HEADER_SIZE .. off + WAL_FRAME_SIZE], .le);
        if (f.corrupt_checksum) {
            wal.writeU32BE(buf[off + 16 .. off + 20][0..4], 0xdeadbeef);
            wal.writeU32BE(buf[off + 20 .. off + 24][0..4], 0xcafebabe);
            // Don't advance running — the next frame's chain would still
            // need to follow the legitimate `next`, but verifyFrame will
            // reject this frame and stop. Leave running unchanged.
        } else {
            wal.writeU32BE(buf[off + 16 .. off + 20][0..4], next[0]);
            wal.writeU32BE(buf[off + 20 .. off + 24][0..4], next[1]);
            running = next;
        }
        off += WAL_FRAME_SIZE;
    }
    return buf;
}

fn writeFile(path: []const u8, bytes: []const u8) !void {
    const path_z = try std.testing.allocator.dupeZ(u8, path);
    defer std.testing.allocator.free(path_z);
    const flags: std.c.O = .{ .ACCMODE = .RDWR, .CREAT = true, .TRUNC = true };
    const fd = std.c.open(path_z.ptr, flags, @as(std.c.mode_t, 0o644));
    if (fd < 0) return error.OpenFailed;
    defer _ = std.c.close(fd);
    const w = std.c.write(fd, bytes.ptr, bytes.len);
    if (w != @as(isize, @intCast(bytes.len))) return error.WriteFailed;
}

// ---------- Recovery scan tests ----------

test "wal_recovery.openIfPresent: missing -wal returns null" {
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("wal-missing");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    var p1: [PAGE_SIZE]u8 = @splat(0);
    try test_db_util.writePages(path, &.{&p1});
    const state = try wal_recovery.openIfPresent(allocator, path);
    try std.testing.expect(state == null);
}

test "wal_recovery.openIfPresent: empty -wal returns null" {
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("wal-empty");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    var p1: [PAGE_SIZE]u8 = @splat(0);
    try test_db_util.writePages(path, &.{&p1});

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);
    try writeFile(wal_path, "");

    const state = try wal_recovery.openIfPresent(allocator, path);
    try std.testing.expect(state == null);
}

test "wal_recovery.openIfPresent: single commit frame builds index" {
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("wal-single");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    var p1_main: [PAGE_SIZE]u8 = @splat(0x11);
    try test_db_util.writePages(path, &.{&p1_main});

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);

    const p2_wal: [PAGE_SIZE]u8 = @splat(0xAA);
    const wal_bytes = try buildWal(allocator, 0xdeadbeef, 0xcafebabe, &.{
        .{ .page_no = 2, .commit_size = 2, .page_bytes = &p2_wal },
    });
    defer allocator.free(wal_bytes);
    try writeFile(wal_path, wal_bytes);

    var state = (try wal_recovery.openIfPresent(allocator, path)).?;
    defer state.deinit();
    try std.testing.expectEqual(@as(u32, 2), state.wal_dbsize);
    try std.testing.expectEqual(@as(usize, 1), state.index.count());
    try std.testing.expect(state.index.contains(2));
    try std.testing.expect(!state.index.contains(1));

    var got: [PAGE_SIZE]u8 = undefined;
    try state.readPage(2, &got);
    try std.testing.expectEqualSlices(u8, &p2_wal, &got);
}

test "wal_recovery.openIfPresent: multiple commits, latest version per page wins" {
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("wal-multi");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    var p1_main: [PAGE_SIZE]u8 = @splat(0x11);
    try test_db_util.writePages(path, &.{&p1_main});

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);

    const p2_v1: [PAGE_SIZE]u8 = @splat(0xA1);
    const p2_v2: [PAGE_SIZE]u8 = @splat(0xA2);
    const wal_bytes = try buildWal(allocator, 1, 2, &.{
        .{ .page_no = 2, .commit_size = 2, .page_bytes = &p2_v1 },
        .{ .page_no = 2, .commit_size = 2, .page_bytes = &p2_v2 },
    });
    defer allocator.free(wal_bytes);
    try writeFile(wal_path, wal_bytes);

    var state = (try wal_recovery.openIfPresent(allocator, path)).?;
    defer state.deinit();

    var got: [PAGE_SIZE]u8 = undefined;
    try state.readPage(2, &got);
    try std.testing.expectEqualSlices(u8, &p2_v2, &got);
}

test "wal_recovery.openIfPresent: uncommitted tail discarded" {
    // Commit frame for page 2; then a NON-commit frame for page 3
    // (no closing commit). After scan, index has page 2 only and
    // wal_dbsize == 2.
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("wal-tentative");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    var p1_main: [PAGE_SIZE]u8 = @splat(0x11);
    try test_db_util.writePages(path, &.{&p1_main});

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);

    const p2: [PAGE_SIZE]u8 = @splat(0xA2);
    const p3: [PAGE_SIZE]u8 = @splat(0xA3);
    const wal_bytes = try buildWal(allocator, 9, 7, &.{
        .{ .page_no = 2, .commit_size = 2, .page_bytes = &p2 },
        .{ .page_no = 3, .commit_size = 0, .page_bytes = &p3 }, // tentative
    });
    defer allocator.free(wal_bytes);
    try writeFile(wal_path, wal_bytes);

    var state = (try wal_recovery.openIfPresent(allocator, path)).?;
    defer state.deinit();
    try std.testing.expectEqual(@as(u32, 2), state.wal_dbsize);
    try std.testing.expectEqual(@as(usize, 1), state.index.count());
    try std.testing.expect(state.index.contains(2));
    try std.testing.expect(!state.index.contains(3));
}

test "wal_recovery.openIfPresent: corrupt frame stops scan, prior commit visible" {
    // Three frames: page 2 commit, page 3 commit, page 4 with bad
    // checksum. After scan, index should contain pages 2 and 3 only.
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("wal-corrupt-mid");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    var p1_main: [PAGE_SIZE]u8 = @splat(0x11);
    try test_db_util.writePages(path, &.{&p1_main});

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);

    const p2: [PAGE_SIZE]u8 = @splat(0xA2);
    const p3: [PAGE_SIZE]u8 = @splat(0xA3);
    const p4: [PAGE_SIZE]u8 = @splat(0xA4);
    const wal_bytes = try buildWal(allocator, 5, 9, &.{
        .{ .page_no = 2, .commit_size = 2, .page_bytes = &p2 },
        .{ .page_no = 3, .commit_size = 3, .page_bytes = &p3 },
        .{ .page_no = 4, .commit_size = 4, .page_bytes = &p4, .corrupt_checksum = true },
    });
    defer allocator.free(wal_bytes);
    try writeFile(wal_path, wal_bytes);

    var state = (try wal_recovery.openIfPresent(allocator, path)).?;
    defer state.deinit();
    try std.testing.expectEqual(@as(u32, 3), state.wal_dbsize);
    try std.testing.expect(state.index.contains(2));
    try std.testing.expect(state.index.contains(3));
    try std.testing.expect(!state.index.contains(4));
}

test "wal_recovery.openIfPresent: salt mismatch on first frame stops scan" {
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("wal-bad-salt");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    var p1_main: [PAGE_SIZE]u8 = @splat(0x11);
    try test_db_util.writePages(path, &.{&p1_main});

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);

    const p2: [PAGE_SIZE]u8 = @splat(0xA2);
    const wal_bytes = try buildWal(allocator, 1, 2, &.{
        .{ .page_no = 2, .commit_size = 2, .page_bytes = &p2, .bad_salt = true },
    });
    defer allocator.free(wal_bytes);
    try writeFile(wal_path, wal_bytes);

    var state = (try wal_recovery.openIfPresent(allocator, path)).?;
    defer state.deinit();
    try std.testing.expectEqual(@as(u32, 0), state.wal_dbsize);
    try std.testing.expectEqual(@as(usize, 0), state.index.count());
}

test "Database.openFile + WAL: getPage returns WAL-resident bytes over main" {
    const database = @import("database.zig");
    const journal_test_helpers = struct {
        fn makePage1(dbsize: u32) [PAGE_SIZE]u8 {
            var buf: [PAGE_SIZE]u8 = @splat(0);
            @memcpy(buf[0..16], "SQLite format 3\x00");
            buf[16] = 0x10;
            buf[17] = 0x00;
            // bytes 18/19 = (2,2) → WAL mode
            buf[18] = 2;
            buf[19] = 2;
            buf[20] = 0;
            buf[21] = 64;
            buf[22] = 32;
            buf[23] = 32;
            buf[27] = 1;
            buf[28] = @intCast((dbsize >> 24) & 0xff);
            buf[29] = @intCast((dbsize >> 16) & 0xff);
            buf[30] = @intCast((dbsize >> 8) & 0xff);
            buf[31] = @intCast(dbsize & 0xff);
            buf[100] = 0x0d; // empty leaf-table sqlite_schema
            buf[105] = (PAGE_SIZE >> 8) & 0xff;
            buf[106] = PAGE_SIZE & 0xff;
            return buf;
        }
    };

    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("wal-getpage");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);

    var p1_main = journal_test_helpers.makePage1(2);
    var p2_main: [PAGE_SIZE]u8 = @splat(0x11); // pre-WAL page 2 content
    try test_db_util.writePages(path, &.{ &p1_main, &p2_main });

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);

    const p2_wal: [PAGE_SIZE]u8 = @splat(0xAA); // WAL-resident newer version
    const wal_bytes = try buildWal(allocator, 0xfeedface, 0x12345678, &.{
        .{ .page_no = 2, .commit_size = 2, .page_bytes = &p2_wal },
    });
    defer allocator.free(wal_bytes);
    try writeFile(wal_path, wal_bytes);

    var db = try database.Database.openFile(allocator, path);
    defer db.deinit();

    const got = try db.pager.?.getPage(2);
    try std.testing.expectEqualSlices(u8, &p2_wal, got);
}
