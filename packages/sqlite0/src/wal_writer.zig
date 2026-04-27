//! Iter27.B.1 — WAL append-side writer.
//!
//! Owns its own RDWR fd to `<db>-wal` (separate from the recovery
//! reader's fd in `WalState`) and appends frames built by the encode
//! primitives in `wal.zig`. The Pager (Iter27.B.3) drives this — it
//! decides what page bytes to write and when a transaction commits.
//!
//! ### Two construction paths
//!
//!   - `create(path, page_size, checkpoint_seq)` — no usable WAL on
//!     disk (missing, empty, or `openIfPresent` returned null because
//!     the header was unparseable). We open `O_CREAT|O_TRUNC`, draw
//!     fresh random salts, write a fresh header, and start the chain
//!     from `(header.checksum1, header.checksum2)` at `HEADER_SIZE`.
//!
//!   - `fromExistingState(path, state)` — `WalState` is in hand from
//!     `openIfPresent`. We open `O_RDWR` (no TRUNC — preserve the
//!     reader's view) and inherit the writer-inheritance fields:
//!     `salt1/salt2/endian/last_checksum/next_frame_offset`. Salt
//!     rotation only happens at WAL restart (post-checkpoint wrap),
//!     not per-transaction — this matches sqlite3 `wal.c`'s behaviour.
//!     Mid-session appends keep the same salts so the existing
//!     readable frames stay verifiable.
//!
//! ### State advancement rule
//!
//! `appendFrame` advances `chain` and `next_frame_offset` ONLY after
//! `pwrite` returns success. A torn frame (partial write, kernel
//! crash, ENOSPC) leaves the writer's in-memory state pointing at
//! the slot it tried to write — which means the next append
//! re-attempts the same offset, NOT the slot after. On reopen, the
//! recovery scan in `wal_recovery.zig` sees the torn frame as
//! trailing garbage and stops there — exactly the case Iter27.A
//! handles.
//!
//! ### Index coupling
//!
//! `WalWriter` is intentionally ignorant of `WalState.index`.
//! `appendFrame` returns the byte offset where the frame went; the
//! caller (Pager in B.3) does `state.index.put(page_no, offset)`,
//! and ONLY does so on commit promotion to mirror the recovery rule
//! (uncommitted frames never become reader-visible).
//!
//! ### Salt source (Iter27.B.0/B.1)
//!
//! `std.c.arc4random_buf` — kernel-provided, never blocks, never
//! fails, no fd handling. Available on darwin natively and on linux
//! via glibc 2.36+ / musl / Android. Same `std.c.*` layer the rest of
//! the codebase already binds (`std.c.open`, `std.c.pread`, …) so no
//! new dependency surface.

const std = @import("std");
const wal = @import("wal.zig");
const wal_recovery = @import("wal_recovery.zig");
const ops = @import("ops.zig");
const pager_mod = @import("pager.zig");

pub const Error = ops.Error;

pub const WalWriter = struct {
    allocator: std.mem.Allocator,
    fd: std.c.fd_t,
    salt1: u32,
    salt2: u32,
    endian: wal.Endianness,
    /// Cumulative checksum chain seed for the next frame.
    chain: [2]u32,
    /// Byte offset where the next encoded frame goes. Advances by
    /// `FRAME_SIZE` after a successful `appendFrame`.
    next_frame_offset: u64,

    pub fn close(self: *WalWriter) void {
        if (self.fd >= 0) {
            _ = std.c.close(self.fd);
            self.fd = -1;
        }
    }

    /// Append one frame at `next_frame_offset`. On success returns the
    /// byte offset where the frame's HEADER landed (caller maps this
    /// into `WalState.index` keyed by `page_no` — but only on commit
    /// promotion).
    ///
    /// `commit_size > 0` means "this frame closes a transaction; dbsize
    /// after the commit is `commit_size` pages". Mid-tx frames pass 0.
    pub fn appendFrame(
        self: *WalWriter,
        page_no: u32,
        commit_size: u32,
        page_bytes: *const [pager_mod.PAGE_SIZE]u8,
    ) Error!u64 {
        var buf: [wal.FRAME_SIZE]u8 = undefined;
        const new_chain = wal.encodeFrame(
            &buf,
            self.chain,
            page_no,
            commit_size,
            self.salt1,
            self.salt2,
            page_bytes,
            self.endian,
        );

        const off: std.c.off_t = @intCast(self.next_frame_offset);
        const n = std.c.pwrite(self.fd, &buf, wal.FRAME_SIZE, off);
        // Failure semantics: do NOT advance state. The caller can
        // retry the same call after recovering disk space; or, if the
        // writer is dropped, recovery will see a torn frame and stop
        // there.
        if (n < 0) return Error.IoError;
        if (n != @as(isize, @intCast(wal.FRAME_SIZE))) return Error.IoError;

        const written_at = self.next_frame_offset;
        self.chain = new_chain;
        self.next_frame_offset = written_at + wal.FRAME_SIZE;
        return written_at;
    }
};

/// Construct a fresh writer for a WAL that doesn't exist (or whose
/// existing bytes are unusable). Truncates any existing file at
/// `<db_path>-wal`. Picks `MAGIC_LE` regardless of host architecture
/// (see `wal.zig` doc comment); sqlite3 reads either fine.
pub fn create(
    allocator: std.mem.Allocator,
    db_path: []const u8,
    checkpoint_seq: u32,
) Error!WalWriter {
    const wal_path = std.fmt.allocPrint(allocator, "{s}-wal", .{db_path}) catch return Error.IoError;
    defer allocator.free(wal_path);
    const wal_path_z = allocator.dupeZ(u8, wal_path) catch return Error.IoError;
    defer allocator.free(wal_path_z);

    const flags: std.c.O = .{ .ACCMODE = .RDWR, .CREAT = true, .TRUNC = true };
    const fd = std.c.open(wal_path_z.ptr, flags, @as(std.c.mode_t, 0o644));
    if (fd < 0) return Error.IoError;
    errdefer _ = std.c.close(fd);

    var salt_bytes: [8]u8 = undefined;
    std.c.arc4random_buf(&salt_bytes, salt_bytes.len);
    const salt1 = wal.readU32BE(salt_bytes[0..4]);
    const salt2 = wal.readU32BE(salt_bytes[4..8]);

    var header_buf: [wal.HEADER_SIZE]u8 = undefined;
    wal.encodeHeader(&header_buf, pager_mod.PAGE_SIZE, checkpoint_seq, salt1, salt2, wal.MAGIC_LE);

    const n = std.c.pwrite(fd, &header_buf, wal.HEADER_SIZE, 0);
    if (n != @as(isize, @intCast(wal.HEADER_SIZE))) return Error.IoError;

    const header = wal.parseHeader(&header_buf) catch return Error.IoError;
    return .{
        .allocator = allocator,
        .fd = fd,
        .salt1 = salt1,
        .salt2 = salt2,
        .endian = header.endianness(),
        .chain = .{ header.checksum1, header.checksum2 },
        .next_frame_offset = wal.HEADER_SIZE,
    };
}

/// Construct a writer that continues an in-flight WAL chain. The
/// caller has already run `wal_recovery.openIfPresent` and is holding
/// a usable `WalState`. We open a *separate* RDWR fd to the same
/// path — the reader keeps its RDONLY fd live, so `getPage` and
/// `appendFrame` don't fight over a single fd. Same inode, two file
/// descriptions; reads and writes both go through the kernel page
/// cache so ordering is fine for our single-writer model.
pub fn fromExistingState(
    allocator: std.mem.Allocator,
    db_path: []const u8,
    state: *const wal_recovery.WalState,
) Error!WalWriter {
    const wal_path = std.fmt.allocPrint(allocator, "{s}-wal", .{db_path}) catch return Error.IoError;
    defer allocator.free(wal_path);
    const wal_path_z = allocator.dupeZ(u8, wal_path) catch return Error.IoError;
    defer allocator.free(wal_path_z);

    const flags: std.c.O = .{ .ACCMODE = .RDWR };
    const fd = std.c.open(wal_path_z.ptr, flags);
    if (fd < 0) return Error.IoError;

    return .{
        .allocator = allocator,
        .fd = fd,
        .salt1 = state.salt1,
        .salt2 = state.salt2,
        .endian = state.endian,
        .chain = state.last_checksum,
        .next_frame_offset = state.next_frame_offset,
    };
}
