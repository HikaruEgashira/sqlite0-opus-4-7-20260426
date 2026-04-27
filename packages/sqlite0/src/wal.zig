//! Iter27.A — WAL (write-ahead log) byte-level format primitives.
//!
//! Read-only encode/decode for the SQLite3 WAL file format described
//! at <https://sqlite.org/walformat.html>. This module is `Pager`-free
//! so unit tests can synthesise headers and frames without going
//! through file I/O. The companion `wal_recovery.zig` module owns
//! `open + scan + index build` and consumes these primitives.
//!
//! ### File layout
//!   - 32-byte WAL header
//!   - Zero or more 24-byte-frame-header + PAGE_SIZE-byte-page records
//!
//! ### Two checksum byte orders
//!
//! The header magic records which host byte order the checksum was
//! *computed* in — it does NOT change the byte order of page contents
//! (those stay big-endian, matching the main file). Empirically (see
//! `wal_test.zig` golden) and from sqlite3 source `wal.c`:
//!   - `0x377f0682` (= `WAL_MAGIC | 0`) — checksum computed by LE host
//!   - `0x377f0683` (= `WAL_MAGIC | 1`) — checksum computed by BE host
//! sqlite3 picks `host_is_be ? MAGIC_BE : MAGIC_LE`. The spec page at
//! sqlite.org is misleadingly worded ("magic indicates BE checksums");
//! the actual semantics are "magic indicates the WRITER'S host byte
//! order". sqlite0 will write `MAGIC_LE` regardless of architecture in
//! Iter27.B (portable, single code path); for Iter27.A read-side we
//! accept both magics so we can read sqlite3-emitted WALs from any
//! host.
//!
//! ### Frame checksum scope (advisor caught — easy to misread the spec)
//!
//! The cumulative checksum input per frame is **8 + PAGE_SIZE bytes**:
//!   - bytes [0..8] of the frame header (page_no + commit_size)
//!   - the PAGE_SIZE page bytes immediately after the frame header
//! The salt-1 / salt-2 / checksum-1 / checksum-2 fields in the frame
//! header are **NOT** input — they are the metadata being verified.
//! Frame N seeds its checksum chain from frame N-1's output (or from
//! the WAL header's checksum if N == 1).
//!
//! ### Header checksum scope
//!
//! The header's own checksum-1/2 (bytes [24..32]) is computed over
//! header bytes [0..24] with seed `(0, 0)`. Verifying it confirms the
//! magic / file-format / page_size / checkpoint-seq / salts haven't
//! been tampered with — and the result is the seed for frame 1.

const std = @import("std");
const ops = @import("ops.zig");
const pager_mod = @import("pager.zig");

pub const Error = ops.Error;

pub const HEADER_SIZE: usize = 32;
pub const FRAME_HEADER_SIZE: usize = 24;
pub const FRAME_SIZE: usize = FRAME_HEADER_SIZE + pager_mod.PAGE_SIZE;

/// File-format version sqlite3 has stamped into every WAL since 3.7.0.
/// A different value at bytes [4..8] is treated as `Error.IoError` so
/// we never silently misparse a future format.
pub const FILE_FORMAT: u32 = 3007000;

pub const MAGIC_LE: u32 = 0x377f0682;
pub const MAGIC_BE: u32 = 0x377f0683;

/// Which byte order the checksum algorithm consumes 32-bit words in.
/// Page bytes themselves are always big-endian on disk; this only
/// affects how `walChecksum` interprets the stream.
pub const Endianness = enum { be, le };

pub const Header = struct {
    magic: u32,
    file_format: u32,
    page_size: u32,
    checkpoint_seq: u32,
    salt1: u32,
    salt2: u32,
    checksum1: u32,
    checksum2: u32,

    pub fn endianness(self: Header) Endianness {
        return if (self.magic == MAGIC_BE) .be else .le;
    }
};

pub const FrameHeader = struct {
    page_no: u32,
    /// 0 = mid-transaction frame; > 0 = commit frame, value is the
    /// post-commit dbsize in pages. The index promotion rule keys on
    /// this field — only commit frames make their preceding mutations
    /// durable to the in-memory index (advisor §3).
    commit_size: u32,
    salt1: u32,
    salt2: u32,
    checksum1: u32,
    checksum2: u32,
};

pub fn readU32BE(b: []const u8) u32 {
    return (@as(u32, b[0]) << 24) |
        (@as(u32, b[1]) << 16) |
        (@as(u32, b[2]) << 8) |
        @as(u32, b[3]);
}

pub fn writeU32BE(out: []u8, v: u32) void {
    out[0] = @intCast((v >> 24) & 0xff);
    out[1] = @intCast((v >> 16) & 0xff);
    out[2] = @intCast((v >> 8) & 0xff);
    out[3] = @intCast(v & 0xff);
}

fn readU32LE(b: []const u8) u32 {
    return @as(u32, b[0]) |
        (@as(u32, b[1]) << 8) |
        (@as(u32, b[2]) << 16) |
        (@as(u32, b[3]) << 24);
}

/// Parse + validate a WAL header. Magic must be one of the two known
/// values, file_format must equal `FILE_FORMAT`, page_size must equal
/// `PAGE_SIZE`. The two checksum fields are returned verbatim — call
/// `verifyHeaderChecksum` separately if you want to validate them.
pub fn parseHeader(buf: []const u8) Error!Header {
    if (buf.len < HEADER_SIZE) return Error.IoError;
    const magic = readU32BE(buf[0..4]);
    if (magic != MAGIC_BE and magic != MAGIC_LE) return Error.IoError;
    const file_format = readU32BE(buf[4..8]);
    if (file_format != FILE_FORMAT) return Error.IoError;
    const page_size = readU32BE(buf[8..12]);
    if (page_size != pager_mod.PAGE_SIZE) return Error.IoError;
    return .{
        .magic = magic,
        .file_format = file_format,
        .page_size = page_size,
        .checkpoint_seq = readU32BE(buf[12..16]),
        .salt1 = readU32BE(buf[16..20]),
        .salt2 = readU32BE(buf[20..24]),
        .checksum1 = readU32BE(buf[24..28]),
        .checksum2 = readU32BE(buf[28..32]),
    };
}

pub fn parseFrameHeader(buf: []const u8) Error!FrameHeader {
    if (buf.len < FRAME_HEADER_SIZE) return Error.IoError;
    return .{
        .page_no = readU32BE(buf[0..4]),
        .commit_size = readU32BE(buf[4..8]),
        .salt1 = readU32BE(buf[8..12]),
        .salt2 = readU32BE(buf[12..16]),
        .checksum1 = readU32BE(buf[16..20]),
        .checksum2 = readU32BE(buf[20..24]),
    };
}

/// SQLite3 cumulative WAL checksum (walformat.html §4.4):
///
/// ```text
///   for each pair of 32-bit words (x, y) in the input:
///     s0 += x + s1
///     s1 += y + s0
/// ```
///
/// Wraps using u32 modular arithmetic. Input length must be a multiple
/// of 8; callers already pad to that boundary by construction (header
/// is 24 bytes pre-checksum, frame input is 8 + PAGE_SIZE = 4104 bytes,
/// both ÷ 8 cleanly).
///
/// `seed` is `(0, 0)` for the header, the previous frame's running
/// checksum for frame N > 1, and the header's own `(checksum1,
/// checksum2)` for frame 1.
pub fn walChecksum(seed: [2]u32, bytes: []const u8, endian: Endianness) [2]u32 {
    std.debug.assert(bytes.len % 8 == 0);
    var s0: u32 = seed[0];
    var s1: u32 = seed[1];
    var i: usize = 0;
    while (i + 8 <= bytes.len) : (i += 8) {
        const x = if (endian == .be) readU32BE(bytes[i .. i + 4]) else readU32LE(bytes[i .. i + 4]);
        const y = if (endian == .be) readU32BE(bytes[i + 4 .. i + 8]) else readU32LE(bytes[i + 4 .. i + 8]);
        s0 = s0 +% x +% s1;
        s1 = s1 +% y +% s0;
    }
    return .{ s0, s1 };
}

/// Verify the header's own checksum: walChecksum over header[0..24]
/// with seed (0, 0) must equal the stored (checksum1, checksum2).
/// Header endianness is determined by the magic at bytes [0..4].
pub fn verifyHeaderChecksum(header_bytes: []const u8, header: Header) bool {
    const computed = walChecksum(.{ 0, 0 }, header_bytes[0..24], header.endianness());
    return computed[0] == header.checksum1 and computed[1] == header.checksum2;
}

/// Verify a frame: salts must match the WAL header (= same checkpoint
/// epoch), and the cumulative checksum over (frame_header[0..8] ++
/// page_bytes) seeded from `prev_checksum` must equal the stored
/// (checksum1, checksum2). Returns the running checksum to seed the
/// next frame on success.
///
/// `frame_header_bytes` is the raw 24-byte slice as it sits on disk —
/// `verifyFrame` indexes into bytes [0..8] for the checksum input.
/// `page_bytes` is the PAGE_SIZE payload that follows.
pub fn verifyFrame(
    prev_checksum: [2]u32,
    frame_header_bytes: []const u8,
    page_bytes: []const u8,
    header: Header,
) ?[2]u32 {
    if (frame_header_bytes.len < FRAME_HEADER_SIZE) return null;
    if (page_bytes.len != pager_mod.PAGE_SIZE) return null;
    const fh = parseFrameHeader(frame_header_bytes) catch return null;
    if (fh.salt1 != header.salt1 or fh.salt2 != header.salt2) return null;

    const endian = header.endianness();
    var running = walChecksum(prev_checksum, frame_header_bytes[0..8], endian);
    running = walChecksum(running, page_bytes, endian);
    if (running[0] != fh.checksum1 or running[1] != fh.checksum2) return null;
    return running;
}
