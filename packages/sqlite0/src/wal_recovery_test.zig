//! Iter27.A/B unit tests for `wal_recovery.zig` — the open-time scan.
//!
//! Synthetic in-memory fixtures exercise the tentative-then-promoted
//! index algorithm: empty, single commit, multi-commit, partial
//! transaction discarded, salt mismatch, checksum mismatch, plus
//! Iter27.B.0 writer-inheritance state (chain seed advances only on
//! commit).
//!
//! Pure `wal.zig` byte-level coverage (parse/encode/checksum) lives in
//! `wal_test.zig`.

const std = @import("std");
const wal = @import("wal.zig");
const wal_recovery = @import("wal_recovery.zig");
const pager_mod = @import("pager.zig");
const test_db_util = @import("test_db_util.zig");

const PAGE_SIZE = pager_mod.PAGE_SIZE;
const Error = @import("ops.zig").Error;

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

test "wal_recovery.openIfPresent: bad-magic header returns null (not half-init state)" {
    // sqlite3 treats a -wal with a corrupt header as nonexistent. Iter27.B.1
    // depends on this two-outcome contract: openIfPresent → null OR
    // fully-populated state. A half-initialised state here would give
    // the writer no way to tell "valid empty WAL" from "garbage WAL".
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("wal-bad-magic");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    var p1: [PAGE_SIZE]u8 = @splat(0);
    try test_db_util.writePages(path, &.{&p1});

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);

    var bytes: [WAL_HEADER_SIZE]u8 = @splat(0xff); // bogus magic
    try writeFile(wal_path, &bytes);

    const state = try wal_recovery.openIfPresent(allocator, path);
    try std.testing.expect(state == null);
}

test "wal_recovery.openIfPresent: tampered-checksum header returns null" {
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("wal-bad-cksum");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    var p1: [PAGE_SIZE]u8 = @splat(0);
    try test_db_util.writePages(path, &.{&p1});

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);

    var buf: [WAL_HEADER_SIZE]u8 = undefined;
    wal.encodeHeader(&buf, PAGE_SIZE, 0, 1, 2, wal.MAGIC_LE);
    buf[24] ^= 0xff; // mutate checksum-1 byte
    try writeFile(wal_path, &buf);

    const state = try wal_recovery.openIfPresent(allocator, path);
    try std.testing.expect(state == null);
}

test "wal_recovery.openIfPresent: valid header zero frames inherits seed" {
    // The legitimate "WAL exists, no commits yet" case — writer should
    // continue from header.checksum1/2 at HEADER_SIZE.
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("wal-zero-frames");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    var p1: [PAGE_SIZE]u8 = @splat(0);
    try test_db_util.writePages(path, &.{&p1});

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);

    var buf: [WAL_HEADER_SIZE]u8 = undefined;
    wal.encodeHeader(&buf, PAGE_SIZE, 0, 0xaaaa, 0xbbbb, wal.MAGIC_LE);
    const header = try wal.parseHeader(&buf);
    try writeFile(wal_path, &buf);

    var state = (try wal_recovery.openIfPresent(allocator, path)).?;
    defer state.deinit();
    try std.testing.expectEqual(@as(u32, 0), state.wal_dbsize);
    try std.testing.expectEqual(@as(usize, 0), state.index.count());
    try std.testing.expectEqual(header.salt1, state.salt1);
    try std.testing.expectEqual(header.salt2, state.salt2);
    try std.testing.expectEqual(@as(u32, header.checksum1), state.last_checksum[0]);
    try std.testing.expectEqual(@as(u32, header.checksum2), state.last_checksum[1]);
    try std.testing.expectEqual(@as(u64, WAL_HEADER_SIZE), state.next_frame_offset);
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

// ---------- Iter27.B.0 writer-inheritance state ----------
//
// The encode→scan round-trip is exercised end-to-end through the
// actual writer API in `wal_writer_test.zig`; we keep just the chain-
// seed-rewind property here since it's a pure recovery-side invariant.

test "wal_recovery: chain seed only advances on commit (uncommitted tail rewinds)" {
    // Frame 1: commit (dbsize=2). Frame 2: mid-tx (commit_size=0).
    // Recovered seed must point at *after frame 1*, not frame 2.
    const allocator = std.testing.allocator;
    const path = try test_db_util.makeTempPath("wal-seed-rewind");
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);
    var p1_main: [PAGE_SIZE]u8 = @splat(0x11);
    try test_db_util.writePages(path, &.{&p1_main});

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    defer test_db_util.unlinkPath(wal_path);

    const p2: [PAGE_SIZE]u8 = @splat(0xA2);
    const p3: [PAGE_SIZE]u8 = @splat(0xA3);
    const wal_bytes = try buildWal(allocator, 7, 9, &.{
        .{ .page_no = 2, .commit_size = 2, .page_bytes = &p2 },
        .{ .page_no = 3, .commit_size = 0, .page_bytes = &p3 }, // tentative
    });
    defer allocator.free(wal_bytes);
    try writeFile(wal_path, wal_bytes);

    var state = (try wal_recovery.openIfPresent(allocator, path)).?;
    defer state.deinit();
    // Seed should be HEADER + 1 frame (after frame 1's commit), not 2.
    try std.testing.expectEqual(
        @as(u64, wal.HEADER_SIZE + wal.FRAME_SIZE),
        state.next_frame_offset,
    );
}
