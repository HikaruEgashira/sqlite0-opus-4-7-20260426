//! Iter27.A — open-time WAL scan + in-memory page-index.
//!
//! `WalState` owns the `<dbname>-wal` file descriptor, the
//! `page_no → frame_offset` map, and the post-recovery `wal_dbsize`
//! (= the commit_size of the last validated commit frame). `Pager`
//! holds an `?WalState` and consults it inside `getPage` before
//! falling back to the main file.
//!
//! ### Scan algorithm (advisor §3 — tentative-then-promoted)
//!
//! Forward-walk frames from offset `HEADER_SIZE`. Maintain:
//!   - `running_checksum: [2]u32` — chain seeded from header's
//!     checksum, advanced per-frame
//!   - `tentative: HashMap<page_no, frame_offset>` — mutations not yet
//!     covered by a commit frame
//!   - `index: HashMap<page_no, frame_offset>` — durable map (only
//!     promoted when a commit frame validates)
//!   - `wal_dbsize: u32` — last commit frame's `commit_size`
//!
//! For each frame:
//!   1. Read the 24-byte frame header + PAGE_SIZE payload.
//!   2. `wal.verifyFrame(running, fh_bytes, page_bytes, header)`. On
//!      failure (bad salt or bad checksum) → STOP. Discard `tentative`.
//!      Everything since the last promotion is "the writer crashed
//!      mid-tx" or "old frames from a prior epoch" — both safely
//!      ignored.
//!   3. On success, advance `running` to verifyFrame's return.
//!   4. Add `page_no → frame_offset` to `tentative` (overwriting any
//!      earlier tentative entry — the latest pre-commit write wins).
//!   5. If `commit_size > 0`: copy `tentative` into `index`, clear
//!      `tentative`, set `wal_dbsize = commit_size`.
//!
//! Final `index` contains exactly the page versions visible at the
//! last valid commit point. Frames after that are invisible — sqlite3
//! readers see the same view.

const std = @import("std");
const ops = @import("ops.zig");
const wal = @import("wal.zig");
const pager_mod = @import("pager.zig");

pub const Error = ops.Error;

pub const WalState = struct {
    allocator: std.mem.Allocator,
    fd: std.c.fd_t,
    /// Map from sqlite3 page number to the byte offset of the frame
    /// header on disk. `pread(fd, dest, PAGE_SIZE, offset + FRAME_HEADER_SIZE)`
    /// reads the page bytes for that frame.
    index: std.AutoHashMapUnmanaged(u32, u64) = .{},
    /// Post-recovery dbsize from the last valid commit frame. Pages in
    /// the range `[1..wal_dbsize]` exist in the database snapshot the
    /// reader sees; pages > wal_dbsize were truncated by the WAL.
    /// Iter27.A trusts main_file_size here for any page not in `index`;
    /// truncate-via-WAL semantics land in Iter27.C/D.
    wal_dbsize: u32 = 0,
    /// Iter27.B writer-inheritance state. After a successful scan these
    /// fields describe "where the next writer would pick up": the salt
    /// epoch (copied from the WAL header), the running checksum, the
    /// byte offset where new frames go, and which endianness the chain
    /// was computed in. Iter27.B.1's `WalWriter.fromExistingState(state)`
    /// reads these so an append continues the same chain sqlite3 would
    /// have continued.
    ///
    /// A valid header but zero frames still produces a usable seed
    /// (`last_checksum = header.checksum1/2`, `next_frame_offset =
    /// HEADER_SIZE`) — that's the legitimate "WAL exists but no commits
    /// yet" case and writers should inherit it. Only `openIfPresent`
    /// returning `null` (missing file, empty file, bad header) means
    /// "no inheritable state, take create()".
    salt1: u32 = 0,
    salt2: u32 = 0,
    endian: wal.Endianness = .le,
    /// Cumulative checksum after the LAST validated commit frame, OR
    /// the header's checksum if no commits have happened yet (= seed
    /// for the next frame the writer will encode).
    last_checksum: [2]u32 = .{ 0, 0 },
    /// Byte offset of the first slot the writer should append to.
    /// Equals `HEADER_SIZE + n_committed_frames * FRAME_SIZE` — past
    /// any uncommitted tail (which a new writer overwrites).
    next_frame_offset: u64 = 0,

    pub fn deinit(self: *WalState) void {
        self.index.deinit(self.allocator);
        if (self.fd >= 0) {
            _ = std.c.close(self.fd);
            self.fd = -1;
        }
    }

    /// Look up `page_no`. Returns the byte offset of the frame's PAGE
    /// payload (i.e. `frame_header_offset + FRAME_HEADER_SIZE`) so the
    /// caller can pread directly into a page buffer.
    pub fn lookupPagePayload(self: *const WalState, page_no: u32) ?u64 {
        if (self.index.get(page_no)) |frame_off| {
            return frame_off + wal.FRAME_HEADER_SIZE;
        }
        return null;
    }

    /// Read page bytes for `page_no` from the WAL file into `dest`.
    /// Caller must have already confirmed `lookupPagePayload` is non-null.
    pub fn readPage(self: *const WalState, page_no: u32, dest: []u8) Error!void {
        const payload_off = self.lookupPagePayload(page_no) orelse return Error.IoError;
        if (dest.len != pager_mod.PAGE_SIZE) return Error.IoError;
        const off: std.c.off_t = @intCast(payload_off);
        const n = std.c.pread(self.fd, dest.ptr, pager_mod.PAGE_SIZE, off);
        if (n < 0) return Error.IoError;
        if (n != @as(isize, @intCast(pager_mod.PAGE_SIZE))) return Error.IoError;
    }
};

/// If `<db_path>-wal` exists, open it, scan, build the index and
/// return a `WalState`. Returns null if the sidecar is absent (the
/// common case for a freshly-checkpointed DB) or empty (a 0-byte WAL
/// is the result of a successful checkpoint-then-truncate and means
/// "no recovery needed"). Any parse / checksum failure during scan is
/// treated as "trailing garbage" per the algorithm above and does NOT
/// surface as an error — recovery rules out partial writes by design.
///
/// File-not-found surfaces as null, not Error — sqlite3 treats a
/// missing -wal exactly the same as an empty one.
pub fn openIfPresent(allocator: std.mem.Allocator, db_path: []const u8) Error!?WalState {
    const wal_path = std.fmt.allocPrint(allocator, "{s}-wal", .{db_path}) catch return Error.IoError;
    defer allocator.free(wal_path);
    const wal_path_z = allocator.dupeZ(u8, wal_path) catch return Error.IoError;
    defer allocator.free(wal_path_z);

    const flags: std.c.O = .{ .ACCMODE = .RDONLY };
    const fd = std.c.open(wal_path_z.ptr, flags);
    if (fd < 0) return null;
    errdefer _ = std.c.close(fd);

    const end = std.c.lseek(fd, 0, 2); // SEEK_END
    if (end < 0) return Error.IoError;
    const size: usize = @intCast(end);

    // Empty WAL = post-checkpoint truncate. sqlite3 leaves the file
    // open at 0 bytes after `PRAGMA wal_checkpoint(TRUNCATE)`. No
    // recovery to do.
    if (size == 0) {
        _ = std.c.close(fd);
        return null;
    }

    // Header-only file: also nothing to recover (no frames). Treat as
    // empty — but keep the fd so the index can stay live for any
    // future appends Iter27.B will introduce.
    if (size < wal.HEADER_SIZE) return Error.IoError;

    var state = WalState{ .allocator = allocator, .fd = fd };
    errdefer state.deinit();

    // scanFrames returns false if the header is unparseable / fails its
    // own checksum — in that case we drop the WAL entirely (matches
    // sqlite3 wal.c behaviour: a bad WAL header = "no WAL"). We hand
    // back null instead of a half-initialised WalState so B.1 has
    // exactly two open-time outcomes: "inherit this state" or "create
    // a fresh WAL".
    if (!try scanFrames(&state, size)) {
        state.deinit();
        return null;
    }
    return state;
}

fn scanFrames(state: *WalState, file_size: usize) Error!bool {
    // pread the header in one shot.
    var header_buf: [wal.HEADER_SIZE]u8 = undefined;
    const hn = std.c.pread(state.fd, &header_buf, wal.HEADER_SIZE, 0);
    if (hn != @as(isize, @intCast(wal.HEADER_SIZE))) return Error.IoError;
    // Bad magic / file_format / page_size and a tampered header
    // checksum are all "the whole WAL is suspect" — sqlite3's recovery
    // treats them as "no WAL exists". Return false so `openIfPresent`
    // can drop the state and surface null.
    const header = wal.parseHeader(&header_buf) catch return false;
    if (!wal.verifyHeaderChecksum(&header_buf, header)) return false;

    state.salt1 = header.salt1;
    state.salt2 = header.salt2;
    state.endian = header.endianness();

    var running: [2]u32 = .{ header.checksum1, header.checksum2 };
    // The writer-inheritance seed advances only on commit promotion.
    // Uncommitted frames never become visible to readers, and an
    // appending writer must restart the chain from the last commit (any
    // frames after that are about to be overwritten).
    var commit_running: [2]u32 = running;
    var commit_off: u64 = wal.HEADER_SIZE;

    var tentative: std.AutoHashMapUnmanaged(u32, u64) = .{};
    defer tentative.deinit(state.allocator);

    var frame_off: usize = wal.HEADER_SIZE;
    var frame_buf: [wal.FRAME_HEADER_SIZE]u8 = undefined;
    var page_buf: [pager_mod.PAGE_SIZE]u8 = undefined;

    while (frame_off + wal.FRAME_SIZE <= file_size) {
        const fhn = std.c.pread(state.fd, &frame_buf, wal.FRAME_HEADER_SIZE, @intCast(frame_off));
        if (fhn != @as(isize, @intCast(wal.FRAME_HEADER_SIZE))) return Error.IoError;
        const pn = std.c.pread(state.fd, &page_buf, pager_mod.PAGE_SIZE, @intCast(frame_off + wal.FRAME_HEADER_SIZE));
        if (pn != @as(isize, @intCast(pager_mod.PAGE_SIZE))) return Error.IoError;

        const next_running = wal.verifyFrame(running, &frame_buf, &page_buf, header) orelse break;
        running = next_running;

        const fh = try wal.parseFrameHeader(&frame_buf);
        // page_no == 0 is structurally invalid in a WAL frame.
        if (fh.page_no == 0) break;

        // Tentative write-through. A later frame in the same tx may
        // overwrite the same page; the latest wins.
        tentative.put(state.allocator, fh.page_no, @intCast(frame_off)) catch return Error.IoError;

        if (fh.commit_size > 0) {
            // Promote tentative → durable index.
            var it = tentative.iterator();
            while (it.next()) |entry| {
                state.index.put(state.allocator, entry.key_ptr.*, entry.value_ptr.*) catch return Error.IoError;
            }
            tentative.clearRetainingCapacity();
            state.wal_dbsize = fh.commit_size;
            commit_running = running;
            commit_off = @as(u64, @intCast(frame_off)) + wal.FRAME_SIZE;
        }

        frame_off += wal.FRAME_SIZE;
    }
    // Anything left in `tentative` is a partial transaction — discarded
    // by virtue of never being copied to `state.index`.

    state.last_checksum = commit_running;
    state.next_frame_offset = commit_off;
    return true;
}
