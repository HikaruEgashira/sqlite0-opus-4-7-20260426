//! Iter27.A/B unit tests for `wal.zig` — pure byte-level primitives.
//!
//! Two layers:
//!   1. **Golden bytes** from a real sqlite3-emitted -wal file (the
//!      Iter27.A spike) anchor `walChecksum` to spec-conformant output.
//!      Without these we'd be testing the implementation against
//!      itself — every other test that synthesises frames uses
//!      `walChecksum`, so a bug in the algorithm would be invisible.
//!   2. **Encode round-trip** tests for the Iter27.B.0 write-side
//!      primitives, pinning the symmetry property: anything
//!      `encodeHeader` / `encodeFrame` produce, the reader accepts.
//!
//! Recovery scan + Database integration tests live in
//! `wal_recovery_test.zig` (separation: the modules they exercise).

const std = @import("std");
const wal = @import("wal.zig");
const pager_mod = @import("pager.zig");

const PAGE_SIZE = pager_mod.PAGE_SIZE;
const Error = @import("ops.zig").Error;

// Captured from a real sqlite3-emitted -wal file (PRAGMA journal_mode=WAL;
// PRAGMA wal_autocheckpoint=0; CREATE TABLE t(x); INSERT INTO t VALUES(1)
// on a darwin LE host). Magic = 0x377f0682 = WRITER WAS LE → checksum
// algorithm consumes words in LE order.
pub const REAL_HEADER_BYTES = [_]u8{
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

// ---------- Iter27.B.0 encode primitive round-trip tests ----------
//
// Combined with the golden-bytes test above (which anchors
// `walChecksum` to spec output), these transitively guarantee encoded
// frames are sqlite3-readable.

test "wal.encodeHeader: round-trip parses + verifies" {
    var buf: [wal.HEADER_SIZE]u8 = undefined;
    wal.encodeHeader(&buf, PAGE_SIZE, 7, 0xc3ad9c0b, 0x9be2d79e, wal.MAGIC_LE);
    const h = try wal.parseHeader(&buf);
    try std.testing.expectEqual(wal.MAGIC_LE, h.magic);
    try std.testing.expectEqual(@as(u32, wal.FILE_FORMAT), h.file_format);
    try std.testing.expectEqual(@as(u32, PAGE_SIZE), h.page_size);
    try std.testing.expectEqual(@as(u32, 7), h.checkpoint_seq);
    try std.testing.expectEqual(@as(u32, 0xc3ad9c0b), h.salt1);
    try std.testing.expectEqual(@as(u32, 0x9be2d79e), h.salt2);
    try std.testing.expect(wal.verifyHeaderChecksum(&buf, h));
}

test "wal.encodeHeader: BE magic round-trips with BE checksum endian" {
    var buf: [wal.HEADER_SIZE]u8 = undefined;
    wal.encodeHeader(&buf, PAGE_SIZE, 0, 1, 2, wal.MAGIC_BE);
    const h = try wal.parseHeader(&buf);
    try std.testing.expectEqual(wal.MAGIC_BE, h.magic);
    try std.testing.expectEqual(wal.Endianness.be, h.endianness());
    try std.testing.expect(wal.verifyHeaderChecksum(&buf, h));
}

test "wal.encodeFrame: round-trip parses + verifies + advances chain" {
    var header_buf: [wal.HEADER_SIZE]u8 = undefined;
    wal.encodeHeader(&header_buf, PAGE_SIZE, 0, 0xfeedface, 0x12345678, wal.MAGIC_LE);
    const header = try wal.parseHeader(&header_buf);

    const page: [PAGE_SIZE]u8 = @splat(0x5a);
    var frame_buf: [wal.FRAME_SIZE]u8 = undefined;
    const new_chain = wal.encodeFrame(
        &frame_buf,
        .{ header.checksum1, header.checksum2 },
        2, // page_no
        2, // commit_size
        header.salt1,
        header.salt2,
        &page,
        .le,
    );

    const fh = try wal.parseFrameHeader(frame_buf[0..wal.FRAME_HEADER_SIZE]);
    try std.testing.expectEqual(@as(u32, 2), fh.page_no);
    try std.testing.expectEqual(@as(u32, 2), fh.commit_size);
    try std.testing.expectEqual(header.salt1, fh.salt1);
    try std.testing.expectEqual(header.salt2, fh.salt2);

    const verified = wal.verifyFrame(
        .{ header.checksum1, header.checksum2 },
        frame_buf[0..wal.FRAME_HEADER_SIZE],
        frame_buf[wal.FRAME_HEADER_SIZE..wal.FRAME_SIZE],
        header,
    );
    try std.testing.expect(verified != null);
    try std.testing.expectEqual(new_chain, verified.?);
}
