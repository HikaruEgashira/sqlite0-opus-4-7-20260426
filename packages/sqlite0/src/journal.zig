//! Iter27.0.b: minimal SQLite3 rollback-journal recovery.
//!
//! On `Database.openFile` for a `journal_mode=delete_legacy` file, this
//! module is invoked when a `<dbname>-journal` sidecar exists. We parse
//! the journal header, replay each "original page" record back to the
//! main file via `Pager.writePage` (cache-coherent), `Pager.truncate` to
//! the recorded `initial_db_size`, then `unlink(journal)`.
//!
//! Format reference: <https://sqlite.org/atomiccommit.html> §3 and
//! sqlite3 source `src/pager.c::pager_playback`.
//!
//! Scope (purely additive — Iter27.0.b stays read-only on the main DB
//! before recovery commits):
//!   - **Single-segment journals only, page_count must be set**.
//!     sqlite3 emits multi-segment journals when a transaction wraps
//!     past `cache_spill`; each segment has its own 28-byte header at
//!     a sector-aligned offset. Scanning straight to EOF would treat
//!     the next segment's magic bytes as a "record" and replay them as
//!     garbage page contents (silent corruption). We trust
//!     `header.page_count` and refuse `page_count == 0` (the "the
//!     process died before finalising" sentinel) with `Error.IoError`.
//!     Multi-segment recovery is Iter27.D scope.
//!   - **Per-record checksums are NOT verified**. sqlite3's checksum
//!     adds defence in depth against torn writes, but a mis-rolled-back
//!     page would be detected by `PRAGMA integrity_check` downstream.
//!     Phase 4 hardening (Iter27.D) adds verification.
//!   - **No fsync between writes** (ADR-0007 §1.6). The recovery
//!     conceptually IS a commit (on-disk state atomically changes from
//!     "pre-rollback" to "post-rollback"), but durability lands at
//!     `Pager.flushCommitFrame` in Iter27.B+. For Iter27.0.b the
//!     existing "best-effort durability" Phase 3 contract holds — a
//!     power loss mid-recovery means the next open re-runs recovery
//!     because the journal is unlinked LAST.
//!
//! Failure semantics: any parse error or short read returns
//! `Error.IoError` and **leaves the journal file in place**. A
//! subsequent open will retry recovery; a manual `rm <db>-journal`
//! lets the user accept the partial state. We never silently delete
//! a journal we couldn't parse — that's the data-loss path the
//! advisor warned against in B.3.f.

const std = @import("std");
const pager_mod = @import("pager.zig");
const ops = @import("ops.zig");

const Error = ops.Error;

/// Journal mode declared by header bytes 18 (write_format) / 19 (read_format)
/// per SQLite3 file format spec §1.6. Iter27.0.a sets this on `Database.openFile`
/// so downstream iterations can dispatch on it without re-parsing the header.
/// ADR-0007 §1.5 commits to "respect existing file's mode" — sqlite0 does not
/// silently flip the bytes.
///
/// Only the two combinations sqlite3 actually writes are recognised:
///   - `(1, 1)` = `.delete_legacy` (rollback-journal mode, sqlite3 default)
///   - `(2, 2)` = `.wal`           (write-ahead-log mode)
/// Any other pair is `Error.IoError` at open time — sqlite3 itself treats
/// unknown formats as corrupt.
pub const JournalMode = enum { delete_legacy, wal };

/// Decode the journal-mode bits at file-header bytes 18 (write_format) and
/// 19 (read_format). Both bytes must agree — sqlite3 itself writes them as
/// a pair (`(1,1)` legacy or `(2,2)` WAL). Disagreement or any other value
/// surfaces as `Error.IoError` so we never guess at half-corrupt headers.
pub fn detectJournalMode(p: *pager_mod.Pager) Error!JournalMode {
    const page1 = try p.getPage(1);
    if (page1.len < 20) return Error.IoError;
    const write_format = page1[18];
    const read_format = page1[19];
    if (write_format != read_format) return Error.IoError;
    return switch (write_format) {
        1 => .delete_legacy,
        2 => .wal,
        else => Error.IoError,
    };
}

/// Probe for `<db_path>-journal`. If it exists, hand off to `recover` which
/// replays original-page records back to the main file via the pager
/// (cache-coherent), truncates to the pre-transaction page count, then unlinks
/// the journal. Missing journal is the common case (no transaction was in
/// flight) and returns silently.
///
/// Existence probe is `open(O_RDONLY)` rather than `stat` because Zig std.c
/// on darwin doesn't expose `stat` directly (stat$INODE64 mangling).
pub fn maybeRunRecovery(p: *pager_mod.Pager, db_path: []const u8) Error!void {
    const allocator = p.allocator;
    const journal_path = std.fmt.allocPrint(allocator, "{s}-journal", .{db_path}) catch return Error.IoError;
    defer allocator.free(journal_path);

    const journal_path_z = allocator.dupeZ(u8, journal_path) catch return Error.IoError;
    defer allocator.free(journal_path_z);

    const flags: std.c.O = .{ .ACCMODE = .RDONLY };
    const probe_fd = std.c.open(journal_path_z.ptr, flags);
    if (probe_fd < 0) return; // no journal → nothing to do
    _ = std.c.close(probe_fd);

    try recover(p, journal_path);
}

/// 8-byte file-format magic that opens every rollback journal sqlite3
/// writes. Anything else at offset 0 → `Error.IoError`.
pub const MAGIC = [_]u8{ 0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7 };

pub const HEADER_SIZE = 28;

pub const Header = struct {
    /// Number of records the journal claims to contain. 0 means "the
    /// process died before finalising the count" — recovery scans
    /// forward until the first invalid record (page_no == 0 or short).
    page_count: u32,
    nonce: u32,
    /// Pre-transaction page count of the main DB. After replay the
    /// main file is `ftruncate`'d to this size in pages.
    initial_db_size: u32,
    /// Physical sector size used to align records. The header itself is
    /// padded to the first sector boundary, so the first record starts
    /// at offset `sector_size` (NOT `HEADER_SIZE`).
    sector_size: u32,
    page_size: u32,
};

fn readU32BE(b: []const u8) u32 {
    return (@as(u32, b[0]) << 24) |
        (@as(u32, b[1]) << 16) |
        (@as(u32, b[2]) << 8) |
        @as(u32, b[3]);
}

pub fn parseHeader(buf: []const u8) Error!Header {
    if (buf.len < HEADER_SIZE) return Error.IoError;
    if (!std.mem.eql(u8, buf[0..8], &MAGIC)) return Error.IoError;
    const sector_size = readU32BE(buf[20..24]);
    const page_size = readU32BE(buf[24..28]);
    if (sector_size == 0 or sector_size > 65536) return Error.IoError;
    if (page_size != pager_mod.PAGE_SIZE) return Error.IoError;
    return .{
        .page_count = readU32BE(buf[8..12]),
        .nonce = readU32BE(buf[12..16]),
        .initial_db_size = readU32BE(buf[16..20]),
        .sector_size = sector_size,
        .page_size = page_size,
    };
}

/// Replay every original-page record from `journal_path` back to the
/// main file via `pager`, truncate to the pre-transaction size, then
/// unlink the journal. See module doc for failure semantics.
pub fn recover(pager: *pager_mod.Pager, journal_path: []const u8) Error!void {
    const allocator = pager.allocator;

    const path_z = allocator.dupeZ(u8, journal_path) catch return Error.IoError;
    defer allocator.free(path_z);

    const flags: std.c.O = .{ .ACCMODE = .RDONLY };
    const fd = std.c.open(path_z.ptr, flags);
    if (fd < 0) return Error.IoError;
    defer _ = std.c.close(fd);

    // `lseek(fd, 0, SEEK_END)` is the portable way to get the file size
    // here — `std.c.fstat` isn't surfaced on darwin in this Zig version.
    const end = std.c.lseek(fd, 0, 2); // SEEK_END = 2
    if (end < @as(@TypeOf(end), HEADER_SIZE)) return Error.IoError;
    const size: usize = @intCast(end);
    _ = std.c.lseek(fd, 0, 0); // SEEK_SET = 0 — rewind for the read loop below.

    const journal_bytes = allocator.alloc(u8, size) catch return Error.IoError;
    defer allocator.free(journal_bytes);

    var pos: usize = 0;
    while (pos < size) {
        const remaining = size - pos;
        const n = std.c.read(fd, journal_bytes.ptr + pos, remaining);
        if (n <= 0) return Error.IoError;
        pos += @intCast(n);
    }

    const header = try parseHeader(journal_bytes);

    // Multi-segment journals (page_count == 0 sentinel) are Iter27.D scope.
    // Scanning straight-to-EOF here would treat the next segment's 28-byte
    // header magic (0xd9d505f9...) as a "record" and replay it as garbage
    // page contents — silent corruption of the main DB. Refuse the journal
    // and leave it on disk for manual inspection per the module's
    // fail-loud contract.
    if (header.page_count == 0) return Error.IoError;

    // Records start at the first sector boundary. Each is
    // [page_no u32 BE | page bytes | checksum u32 BE].
    const record_size: usize = 4 + header.page_size + 4;
    var rec_off: usize = header.sector_size;
    var applied: u32 = 0;
    const max_records: u32 = header.page_count;

    while (applied < max_records and rec_off + record_size <= size) {
        const page_no = readU32BE(journal_bytes[rec_off .. rec_off + 4]);
        // page_no == 0 inside a finalised segment is structural corruption,
        // not an end-of-records sentinel (that branch only applies when
        // page_count == 0, which we've already rejected above).
        if (page_no == 0) return Error.IoError;
        const page_bytes = journal_bytes[rec_off + 4 .. rec_off + 4 + header.page_size];
        try pager.writePage(page_no, page_bytes);
        rec_off += record_size;
        applied += 1;
    }

    if (applied != max_records) return Error.IoError;

    // Shrink the file back if the aborted transaction had extended it.
    try pager.truncate(header.initial_db_size);

    // Last operation: unlink the journal. A crash before this leaves
    // the next open to re-run recovery (idempotent — same records get
    // pwritten again to the same offsets).
    if (std.c.unlink(path_z.ptr) != 0) return Error.IoError;
}
