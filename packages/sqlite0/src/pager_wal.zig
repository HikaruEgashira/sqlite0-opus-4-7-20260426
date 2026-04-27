//! Pager ↔ WAL coupling — `attachWal` (Iter27.A) and the write-path
//! orchestration (Iter27.B.2/3) live here so `pager.zig` stays focused
//! on cache + main-file I/O.
//!
//! All functions take `*pager.Pager` and mutate its `wal` /
//! `wal_writer` / `staged_frames` fields. We don't introduce a
//! separate "WalPager" struct — the cache, fd, and LRU are shared
//! between read and write paths and would create awkward duplication.
//!
//! ### Commit-boundary contract (advisor)
//!
//! `writeFrame(page_no, bytes)` *stages* a mutation: bytes are
//! snapshotted into the staging list and write-through into the LRU
//! cache (so subsequent `getPage` returns the new bytes). NO WAL
//! pwrite happens here.
//!
//! `commit(dbsize)` flushes the staged batch:
//!   1. For each staged page except the last: `appendFrame(page_no,
//!      commit_size=0, bytes)`.
//!   2. The last staged page: `appendFrame(page_no, commit_size=dbsize,
//!      bytes)` — the commit frame closes the transaction.
//!   3. `fsync` the WAL fd. Mid-tx frames don't need individual
//!      fsyncs — the commit-frame fsync forces all preceding bytes on
//!      the same fd by happens-before. That's the durability boundary.
//!   4. Promote staged page→offset entries into `WalState.index`.
//!      Only after this point does the next `getPage` see the new
//!      bytes via the WAL precedence branch.
//!
//! Why defer all pwrites until commit (vs. eager mid-tx writes +
//! commit-frame rewrite)? Two reasons:
//!   - Failure semantics are simpler: a mid-tx crash leaves the WAL
//!     bytes the next reader sees byte-identical to before the tx
//!     started. No torn-frame trailing garbage to skip.
//!   - The commit frame is naturally the LAST page write of the tx
//!     (the standard sqlite3 model), with no in-place rewrite of an
//!     already-pwritten frame and no second appendFrame for a frame
//!     whose page is already on disk.
//!
//! Cost: one `PAGE_SIZE` snapshot allocation per distinct staged
//! page. Acceptable for B.3's 1–3-page DML transactions; we'll
//! revisit if a workload pins this as a hotspot.

const std = @import("std");
const ops = @import("ops.zig");
const wal = @import("wal.zig");
const wal_recovery = @import("wal_recovery.zig");
const wal_writer = @import("wal_writer.zig");
const pager_mod = @import("pager.zig");

pub const Error = ops.Error;

pub const StagedFrame = struct {
    page_no: u32,
    /// Owned snapshot of the page bytes. Freed when `commit` flushes
    /// the batch or `discardStaged` aborts it.
    data: []u8,
};

/// Hand the Pager an open WAL state. The Pager takes ownership;
/// teardown happens in `Pager.close()`. Idempotent rejection of
/// double-attach is intentional: WAL recovery should run exactly once
/// at open time, and a second attach would silently leak the previous
/// state's fd.
///
/// Drops any cached page that the WAL now overrides — after the
/// schema scan ran without WAL knowledge, page 1 in particular could
/// be stale. For Iter27.A's open-time attach the cache is typically
/// empty here, but defensive eviction keeps the invariant simple:
/// "WAL takes precedence, full stop".
pub fn attachWal(self: *pager_mod.Pager, state: wal_recovery.WalState) Error!void {
    if (self.wal != null) return Error.IoError;
    self.wal = state;
    var i: usize = 0;
    while (i < self.cache.items.len) {
        if (self.wal.?.lookupPagePayload(self.cache.items[i].page_no) != null) {
            const evicted = self.cache.orderedRemove(i);
            self.allocator.free(evicted.data);
        } else {
            i += 1;
        }
    }
}

/// Attach a `WalWriter` matching the current `WalState`. Iter27.B.2 —
/// called from `Database.openFile` for `journal_mode == .wal` after
/// `attachWal` (or in lieu of it for a freshly-created WAL). Caller
/// chose between `wal_writer.create` and `wal_writer.fromExistingState`
/// based on whether a `WalState` came back from `openIfPresent`.
pub fn attachWriter(self: *pager_mod.Pager, writer: wal_writer.WalWriter) Error!void {
    if (self.wal_writer != null) return Error.IoError;
    self.wal_writer = writer;
}

/// Iter27.B.2 — open-time WAL setup for `journal_mode == .wal`.
/// Single entry point called by `Database.openFile`; encapsulates the
/// "scan + (create | inherit) writer" branch so the database module
/// stays focused on schema loading.
///
///   - openIfPresent returned a state → `fromExistingState` inherits
///     the chain (mid-session continuation).
///   - openIfPresent returned null → no usable WAL on disk; `create`
///     fresh, then re-scan to install an empty `WalState` so the
///     read-side index path works uniformly.
pub fn openWal(self: *pager_mod.Pager, db_path: []const u8) Error!void {
    if (try wal_recovery.openIfPresent(self.allocator, db_path)) |state| {
        try attachWal(self, state);
        const writer = try wal_writer.fromExistingState(self.allocator, db_path, &self.wal.?);
        try attachWriter(self, writer);
        return;
    }
    var writer = try wal_writer.create(self.allocator, db_path, 0);
    errdefer writer.close();
    if (try wal_recovery.openIfPresent(self.allocator, db_path)) |state| {
        try attachWal(self, state);
    }
    try attachWriter(self, writer);
}

/// Tear down the WAL writer, state, and any uncommitted staged
/// frames on `Pager.close()`. Order matters only loosely: writer
/// first (it has its own RDWR fd), then reader state. Both own
/// independent fds.
pub fn detach(self: *pager_mod.Pager) void {
    discardStaged(self);
    if (self.wal_writer) |*w| w.close();
    self.wal_writer = null;
    if (self.wal) |*w| w.deinit();
    self.wal = null;
}

/// Stage a mid-transaction page write. Snapshots `bytes` into the
/// staging list and write-through into the LRU cache. NO WAL pwrite
/// happens here — `commit` does the batch.
pub fn writeFrame(self: *pager_mod.Pager, page_no: u32, bytes: []const u8) Error!void {
    if (page_no == 0) return Error.IoError;
    if (bytes.len != pager_mod.PAGE_SIZE) return Error.IoError;
    if (self.wal_writer == null) return Error.IoError;

    // Restage same page within tx: replace the snapshot. Latest write
    // wins (mirrors the recovery `latest commit wins` rule).
    for (self.staged_frames.items) |*s| {
        if (s.page_no == page_no) {
            @memcpy(s.data, bytes);
            try writeThroughCache(self, page_no, bytes);
            return;
        }
    }

    const snap = self.allocator.alloc(u8, pager_mod.PAGE_SIZE) catch return Error.IoError;
    errdefer self.allocator.free(snap);
    @memcpy(snap, bytes);
    try self.staged_frames.append(self.allocator, .{ .page_no = page_no, .data = snap });
    try writeThroughCache(self, page_no, bytes);
}

/// Flush the staged batch as a single transaction:
///   - All but the last staged frame → `appendFrame(commit_size = 0)`
///   - The last frame → `appendFrame(commit_size = dbsize)`
///   - `fsync(wal_fd)`
///   - Promote every staged entry into `WalState.index`.
///
/// `dbsize == 0` (no mutations queued) is a no-op so callers can call
/// `commit` unconditionally at statement end.
pub fn commit(self: *pager_mod.Pager, dbsize: u32) Error!void {
    if (self.staged_frames.items.len == 0) return;
    if (dbsize == 0) return Error.IoError;
    const writer = if (self.wal_writer) |*w| w else return Error.IoError;
    const state = if (self.wal) |*s| s else return Error.IoError;

    const last_idx = self.staged_frames.items.len - 1;
    var offsets = std.ArrayList(u64).empty;
    defer offsets.deinit(self.allocator);
    offsets.ensureTotalCapacity(self.allocator, self.staged_frames.items.len) catch return Error.IoError;

    // The append loop. A failure mid-loop leaves prior frames with
    // commit_size=0 — they'll appear as a tentative tail to recovery,
    // which discards them. The on-disk database state remains exactly
    // the last successfully-committed snapshot.
    for (self.staged_frames.items, 0..) |s, i| {
        const commit_size: u32 = if (i == last_idx) dbsize else 0;
        const page_arr: *const [pager_mod.PAGE_SIZE]u8 = @ptrCast(s.data.ptr);
        const off = try writer.appendFrame(s.page_no, commit_size, page_arr);
        offsets.appendAssumeCapacity(off);
    }

    // Durability boundary.
    if (std.c.fsync(writer.fd) != 0) return Error.IoError;

    // Promote to reader-visible index.
    for (self.staged_frames.items, offsets.items) |s, off| {
        state.index.put(self.allocator, s.page_no, off) catch return Error.IoError;
    }
    if (dbsize > state.wal_dbsize) state.wal_dbsize = dbsize;

    discardStaged(self);
}

/// Checkpoint mode mirroring sqlite3's `PRAGMA wal_checkpoint(MODE)`.
/// We collapse RESTART/FULL into TRUNCATE for our single-connection
/// model — they only differ when concurrent readers might hold a
/// snapshot, which our exclusive flock rules out. PASSIVE backfills
/// without resetting the WAL file; TRUNCATE backfills and resets.
pub const CheckpointMode = enum { passive, truncate };

/// Three-tuple sqlite3 returns from `PRAGMA wal_checkpoint`.
///   - `busy`: 0 success, 1 lock contention (we never report 1)
///   - `log`: total frames in the WAL file post-call
///   - `ckpt`: frames backfilled into the main file
/// On a non-WAL database sqlite3 returns `0|-1|-1`; we mirror that.
pub const CheckpointResult = struct {
    busy: i64,
    log: i64,
    ckpt: i64,
};

/// Iter27.C — single-process checkpoint.
///
/// Walks `WalState.index` in arbitrary order, reads each indexed
/// frame's payload from the WAL fd, and pwrites it to the main file
/// at `(page_no - 1) * PAGE_SIZE`. After all frames are flushed and
/// fsynced, the main file is truncated to `wal_dbsize` pages.
///
/// For `.truncate`, additionally tears down the WAL writer + state
/// and recreates a fresh WAL (`O_TRUNC` via `wal_writer.create`)
/// with `checkpoint_seq + 1` — sqlite3 uses this counter to
/// distinguish reset chains; matching the bump keeps the on-disk
/// format interoperable.
///
/// `.passive` leaves the WAL file untouched after backfill so
/// existing readers (if any — there aren't, given our flock) keep
/// observing the same byte stream. The bytes are byte-identical to
/// the freshly-pwritten main file, so the cache stays correct
/// without invalidation either way.
///
/// Pre-conditions:
///   - No staged frames (mid-tx checkpoint would lose the batch)
///
/// Returns sqlite3-shape `CheckpointResult` so the PRAGMA wrapper
/// can emit `busy|log|ckpt` directly.
pub fn checkpoint(self: *pager_mod.Pager, mode: CheckpointMode) Error!CheckpointResult {
    if (self.wal == null or self.wal_writer == null) {
        return .{ .busy = 0, .log = -1, .ckpt = -1 };
    }
    if (self.staged_frames.items.len > 0) return Error.IoError;

    const state = &self.wal.?;
    const writer = &self.wal_writer.?;

    // Physical frame count: (next_frame_offset - header) / FRAME_SIZE.
    // Counts every appended frame regardless of commit status — sqlite3
    // reports the same. Our defer-all-pwrites model means uncommitted
    // frames don't exist between transactions, so this also equals the
    // committed-frame count in the steady state.
    const log_frames: i64 = @intCast((writer.next_frame_offset - wal.HEADER_SIZE) / wal.FRAME_SIZE);

    var page_buf: [pager_mod.PAGE_SIZE]u8 = undefined;
    var it = state.index.iterator();
    while (it.next()) |entry| {
        const page_no = entry.key_ptr.*;
        const frame_off = entry.value_ptr.*;
        const payload_off: std.c.off_t = @intCast(frame_off + wal.FRAME_HEADER_SIZE);
        const r = std.c.pread(writer.fd, &page_buf, pager_mod.PAGE_SIZE, payload_off);
        if (r != @as(isize, @intCast(pager_mod.PAGE_SIZE))) return Error.IoError;
        const main_off: std.c.off_t = @intCast((@as(u64, page_no) - 1) * pager_mod.PAGE_SIZE);
        const w = std.c.pwrite(self.fd, &page_buf, pager_mod.PAGE_SIZE, main_off);
        if (w != @as(isize, @intCast(pager_mod.PAGE_SIZE))) return Error.IoError;
    }

    // Shrink main when the WAL recorded a smaller dbsize (DELETE /
    // DROP TABLE released tail pages).
    if (state.wal_dbsize > 0) {
        const new_main_size: std.c.off_t = @intCast(@as(u64, state.wal_dbsize) * pager_mod.PAGE_SIZE);
        if (std.c.ftruncate(self.fd, new_main_size) != 0) return Error.IoError;
    }

    if (std.c.fsync(self.fd) != 0) return Error.IoError;

    if (mode == .passive) {
        // PASSIVE: WAL stays intact, readers (us) keep the same view.
        // sqlite3 reports log_size = log_frames, ckpt = log_frames in
        // the no-contention case.
        return .{ .busy = 0, .log = log_frames, .ckpt = log_frames };
    }

    // TRUNCATE path: bump checkpoint_seq, recreate -wal.
    var seq_buf: [4]u8 = undefined;
    const sr = std.c.pread(writer.fd, &seq_buf, 4, 12);
    if (sr != 4) return Error.IoError;
    const new_seq = wal.readU32BE(&seq_buf) +% 1;

    writer.close();
    self.wal_writer = null;
    state.deinit();
    self.wal = null;

    var new_writer = try wal_writer.create(self.allocator, self.path, new_seq);
    errdefer new_writer.close();
    self.wal_writer = new_writer;
    if (try wal_recovery.openIfPresent(self.allocator, self.path)) |new_state| {
        self.wal = new_state;
    }
    return .{ .busy = 0, .log = 0, .ckpt = 0 };
}

/// Iter27.E — explicit ROLLBACK. Discards staged frames AND evicts
/// every cached page that the rolled-back tx mutated, so subsequent
/// `getPage` reads pull from main file / WAL index again rather than
/// returning the post-mutation in-memory bytes (which write-through
/// left in the cache).
///
/// Implicit rollback on close goes through `discardStaged` via
/// `detach`; the cache disappears with the Pager either way, so the
/// eviction step matters only for the explicit-ROLLBACK path where
/// the same Pager keeps serving subsequent statements.
///
/// Idempotent: with no staged frames it's a cheap no-op (matches the
/// commit-after-rollback "loop hook fires anyway" case).
pub fn rollback(self: *pager_mod.Pager) void {
    for (self.staged_frames.items) |sf| evictCachedPage(self, sf.page_no);
    discardStaged(self);
}

fn evictCachedPage(self: *pager_mod.Pager, page_no: u32) void {
    var i: usize = 0;
    while (i < self.cache.items.len) {
        if (self.cache.items[i].page_no == page_no) {
            const evicted = self.cache.orderedRemove(i);
            self.allocator.free(evicted.data);
            return;
        }
        i += 1;
    }
}

/// Drop any uncommitted staged frames and free their snapshots.
/// Called by `detach` (= `Pager.close`) and by future ROLLBACK paths.
pub fn discardStaged(self: *pager_mod.Pager) void {
    for (self.staged_frames.items) |s| self.allocator.free(s.data);
    self.staged_frames.deinit(self.allocator);
    self.staged_frames = .empty;
}

fn writeThroughCache(self: *pager_mod.Pager, page_no: u32, bytes: []const u8) Error!void {
    for (self.cache.items, 0..) |entry, idx| {
        if (entry.page_no == page_no) {
            @memcpy(entry.data, bytes);
            if (idx != 0) {
                const tmp = self.cache.items[idx];
                var j: usize = idx;
                while (j > 0) : (j -= 1) {
                    self.cache.items[j] = self.cache.items[j - 1];
                }
                self.cache.items[0] = tmp;
            }
            return;
        }
    }
    const buf = self.allocator.alloc(u8, pager_mod.PAGE_SIZE) catch return Error.IoError;
    errdefer self.allocator.free(buf);
    @memcpy(buf, bytes);
    self.cache.insert(self.allocator, 0, .{ .page_no = page_no, .data = buf }) catch return Error.IoError;
    if (self.cache.items.len > self.cache_capacity) {
        const evicted = self.cache.pop().?;
        self.allocator.free(evicted.data);
    }
}
