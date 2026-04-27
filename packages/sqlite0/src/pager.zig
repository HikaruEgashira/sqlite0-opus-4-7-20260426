//! Page-level read access + LRU cache + advisory file lock (Iter25.A,
//! ADR-0005 §2). The Pager owns the file descriptor and the cache; it
//! does NOT understand B-tree layout or sqlite_schema — those live in
//! `btree.zig` and `schema.zig` (Iter25.B/C, not yet implemented).
//!
//! API surface (Iter25.A is read-only):
//!   - `open(allocator, path)`: open file, acquire `LOCK_EX | LOCK_NB`.
//!     Errors: `IoError` for open/lock-syscall failure, `DatabaseLocked`
//!     when EWOULDBLOCK from flock.
//!   - `getPage(n)`: 1-indexed; returns a borrowed `[]const u8` of length
//!     `PAGE_SIZE`. Bytes are valid until the next `getPage` call may
//!     evict the entry (LRU at capacity 16). Page 0 is invalid (sqlite3
//!     numbering convention).
//!   - `close()`: release lock, close fd, free cache.
//!
//! Direct C-FFI choice (`std.c.open` / `pread` / `flock`) instead of the
//! new `std.Io.Dir` API: Iter25.A is self-contained and shouldn't pull
//! the Io threading change into Database/Pager constructors. The same
//! reasoning the strftime-`'now'` task is deferred for. When Phase 4
//! lands a thoughtful Io threading refactor, this module can switch over
//! at one place (the `Pager.open` body).

const std = @import("std");
const ops = @import("ops.zig");

pub const PAGE_SIZE: usize = 4096;
pub const Error = ops.Error;

/// Per-page cache entry. `data` is one `PAGE_SIZE` byte buffer owned by
/// the Pager allocator. The order of `cache.items` encodes LRU: index 0
/// is most-recently-used; the tail is the eviction victim.
const CachedPage = struct {
    page_no: u32,
    data: []u8,
};

pub const Pager = struct {
    allocator: std.mem.Allocator,
    fd: std.c.fd_t,
    cache: std.ArrayList(CachedPage) = .empty,
    /// Maximum cache size. Hardcoded for Iter25.A — Phase 4 will surface
    /// a tunable when WAL needs a bigger working set.
    cache_capacity: usize = 16,

    /// Open `file_path` read-write and acquire an exclusive non-blocking
    /// flock. The Pager owns the fd until `close()`.
    pub fn open(allocator: std.mem.Allocator, file_path: []const u8) Error!Pager {
        const path_z = try allocator.dupeZ(u8, file_path);
        defer allocator.free(path_z);

        const flags: std.c.O = .{ .ACCMODE = .RDWR };
        const fd = std.c.open(path_z.ptr, flags);
        if (fd < 0) return Error.IoError;

        const lock_rc = std.c.flock(fd, std.posix.LOCK.EX | std.posix.LOCK.NB);
        if (lock_rc != 0) {
            // Distinguish lock-contention from generic I/O. EWOULDBLOCK
            // is the only documented flock failure that means "another
            // holder exists"; we treat any other failure as IoError.
            const errno_val = std.posix.errno(lock_rc);
            _ = std.c.close(fd);
            // Darwin only spells the "non-blocking lock would block"
            // errno as `AGAIN` (== EWOULDBLOCK on POSIX). Linux exposes
            // both names for the same value; the enum has only one.
            if (errno_val == .AGAIN) return Error.DatabaseLocked;
            return Error.IoError;
        }

        return .{
            .allocator = allocator,
            .fd = fd,
        };
    }

    pub fn close(self: *Pager) void {
        for (self.cache.items) |entry| self.allocator.free(entry.data);
        self.cache.deinit(self.allocator);
        // Best-effort lock release — close() always drops it anyway, but
        // an explicit unlock helps when the fd is being inherited by an
        // unexpected process (defensive, matches sqlite3's pager teardown).
        _ = std.c.flock(self.fd, std.posix.LOCK.UN);
        _ = std.c.close(self.fd);
        self.fd = -1;
    }

    /// Bytes reserved at the end of every page by the database header
    /// (file header byte 20). sqlite3 defaults to 0 in some builds and
    /// 12 in newer ones; sqlite_alter_table / sqlite3_analyze emit 0,
    /// while a fresh `sqlite3` CLI tends to emit 12. The usable area
    /// per page is `PAGE_SIZE - reserved_space`. Required for any
    /// page-rebuild path so cell content doesn't spill into the
    /// reserved tail (which sqlite3's `PRAGMA integrity_check` flags as
    /// "free space corruption").
    ///
    /// Returns 0 if the file is too small to have a header (shouldn't
    /// happen for an opened sqlite3 db; `Error.IoError` would have
    /// surfaced earlier).
    pub fn reservedSpace(self: *Pager) Error!u8 {
        const page1 = try self.getPage(1);
        if (page1.len < 21) return Error.IoError;
        return page1[20];
    }

    /// Convenience: PAGE_SIZE − reserved tail. Cells must not extend
    /// past byte `usableSize()`.
    pub fn usableSize(self: *Pager) Error!usize {
        return PAGE_SIZE - @as(usize, try self.reservedSpace());
    }

    /// Allocate a new page at the end of the file. Reads the
    /// in-header database size (page-1 bytes 28..31, u32 BE),
    /// increments it, writes a zero-filled page at the new index
    /// (pwrite past EOF auto-extends sparsely), and finally writes
    /// page 1 with the bumped dbsize so `PRAGMA integrity_check`
    /// finds the in-header count matching reality. Returns the
    /// freshly-allocated page number.
    ///
    /// All-or-nothing: if the page-1 update fails after the new
    /// page hits disk, the file ends up with a trailing page that
    /// the header doesn't know about — sqlite3 ignores it (treats
    /// dbsize as authoritative). Callers should treat that as
    /// recoverable: re-running allocatePage will reuse the same
    /// page index because dbsize never advanced.
    pub fn allocatePage(self: *Pager) Error!u32 {
        const page1 = try self.getPage(1);
        if (page1.len < 32) return Error.IoError;
        const cur_dbsize: u32 = (@as(u32, page1[28]) << 24) |
            (@as(u32, page1[29]) << 16) |
            (@as(u32, page1[30]) << 8) |
            @as(u32, page1[31]);
        if (cur_dbsize == 0) return Error.IoError; // malformed
        const new_page: u32 = cur_dbsize + 1;

        // Write the new page first (sparse extend). A failure here
        // leaves the on-disk dbsize unchanged.
        const zeros = try self.allocator.alloc(u8, PAGE_SIZE);
        defer self.allocator.free(zeros);
        @memset(zeros, 0);
        try self.writePage(new_page, zeros);

        // Bump dbsize on page 1. getPage may return a different
        // pointer than `page1` above if eviction happened during
        // the writePage call — re-fetch to be safe.
        const page1_now = try self.getPage(1);
        const updated = try self.allocator.alloc(u8, PAGE_SIZE);
        defer self.allocator.free(updated);
        @memcpy(updated, page1_now);
        updated[28] = @intCast((new_page >> 24) & 0xff);
        updated[29] = @intCast((new_page >> 16) & 0xff);
        updated[30] = @intCast((new_page >> 8) & 0xff);
        updated[31] = @intCast(new_page & 0xff);
        try self.writePage(1, updated);

        return new_page;
    }

    /// Add `page_no` to the freelist (Iter26.B.2). The freelist lives in
    /// page-1 bytes [32..36] (first trunk page no, u32 BE) and [36..40]
    /// (total freelist count). Each trunk page is laid out as
    ///   [0..4]  next_trunk_page_no (u32 BE; 0 = last)
    ///   [4..8]  leaf_count          (u32 BE)
    ///   [8..]   leaf_count × u32 BE leaf page numbers
    ///
    /// Two cases:
    ///   - empty freelist (cur_trunk == 0): zero-fill `page_no` as the
    ///     new (and only) trunk, then bump page-1 trunk + count.
    ///   - non-empty freelist: append `page_no` as a leaf entry on the
    ///     current trunk and bump page-1 count.
    ///
    /// Returns `Error.UnsupportedFeature` when the current trunk's leaf
    /// array is full — chaining a second trunk is Iter26.B.3 scope. The
    /// limit is `(usable_size − 8) / 4` ≈ 1019 leaves on a 4096-page
    /// fixture with 12 reserved bytes (4084 usable). Far beyond what the
    /// B.2 differential cases exercise.
    ///
    /// Write order matches `allocatePage`: trunk first, page 1 LAST. A
    /// crash between writes leaves either the trunk untouched (page 1
    /// header unchanged → integrity_check warns about a now-orphan page,
    /// matching B.1's accepted crash window) or both updated.
    pub fn freePage(self: *Pager, page_no: u32) Error!void {
        if (page_no <= 1) return Error.IoError;

        const usable = try self.usableSize();
        const max_leaves: usize = (usable - 8) / 4;

        const page1 = try self.getPage(1);
        if (page1.len < 40) return Error.IoError;
        const cur_trunk: u32 = (@as(u32, page1[32]) << 24) |
            (@as(u32, page1[33]) << 16) |
            (@as(u32, page1[34]) << 8) |
            @as(u32, page1[35]);
        const cur_count: u32 = (@as(u32, page1[36]) << 24) |
            (@as(u32, page1[37]) << 16) |
            (@as(u32, page1[38]) << 8) |
            @as(u32, page1[39]);

        const updated_p1 = try self.allocator.alloc(u8, PAGE_SIZE);
        defer self.allocator.free(updated_p1);

        if (cur_trunk == 0) {
            // Promote `page_no` to a fresh trunk: zero-fill so the leaf
            // count and next-trunk fields read as 0.
            const new_trunk = try self.allocator.alloc(u8, PAGE_SIZE);
            defer self.allocator.free(new_trunk);
            @memset(new_trunk, 0);
            try self.writePage(page_no, new_trunk);

            const page1_now = try self.getPage(1);
            @memcpy(updated_p1, page1_now);
            updated_p1[32] = @intCast((page_no >> 24) & 0xff);
            updated_p1[33] = @intCast((page_no >> 16) & 0xff);
            updated_p1[34] = @intCast((page_no >> 8) & 0xff);
            updated_p1[35] = @intCast(page_no & 0xff);
            const new_count: u32 = cur_count + 1;
            updated_p1[36] = @intCast((new_count >> 24) & 0xff);
            updated_p1[37] = @intCast((new_count >> 16) & 0xff);
            updated_p1[38] = @intCast((new_count >> 8) & 0xff);
            updated_p1[39] = @intCast(new_count & 0xff);
            try self.writePage(1, updated_p1);
            return;
        }

        // Append-to-trunk path. Snapshot the trunk, validate room, write
        // the appended copy, then bump page-1 count.
        const trunk_orig = try self.getPage(cur_trunk);
        const trunk_buf = try self.allocator.alloc(u8, PAGE_SIZE);
        defer self.allocator.free(trunk_buf);
        @memcpy(trunk_buf, trunk_orig);

        const leaf_count: u32 = (@as(u32, trunk_buf[4]) << 24) |
            (@as(u32, trunk_buf[5]) << 16) |
            (@as(u32, trunk_buf[6]) << 8) |
            @as(u32, trunk_buf[7]);
        if (@as(usize, leaf_count) >= max_leaves) return Error.UnsupportedFeature;

        const slot: usize = 8 + @as(usize, leaf_count) * 4;
        trunk_buf[slot] = @intCast((page_no >> 24) & 0xff);
        trunk_buf[slot + 1] = @intCast((page_no >> 16) & 0xff);
        trunk_buf[slot + 2] = @intCast((page_no >> 8) & 0xff);
        trunk_buf[slot + 3] = @intCast(page_no & 0xff);
        const new_leaf_count: u32 = leaf_count + 1;
        trunk_buf[4] = @intCast((new_leaf_count >> 24) & 0xff);
        trunk_buf[5] = @intCast((new_leaf_count >> 16) & 0xff);
        trunk_buf[6] = @intCast((new_leaf_count >> 8) & 0xff);
        trunk_buf[7] = @intCast(new_leaf_count & 0xff);
        try self.writePage(cur_trunk, trunk_buf);

        const page1_now = try self.getPage(1);
        @memcpy(updated_p1, page1_now);
        const new_count: u32 = cur_count + 1;
        updated_p1[36] = @intCast((new_count >> 24) & 0xff);
        updated_p1[37] = @intCast((new_count >> 16) & 0xff);
        updated_p1[38] = @intCast((new_count >> 8) & 0xff);
        updated_p1[39] = @intCast(new_count & 0xff);
        try self.writePage(1, updated_p1);
    }

    /// Write `bytes` (exactly PAGE_SIZE) to page `page_no` via pwrite,
    /// then update the LRU cache so subsequent `getPage` reads see the
    /// new contents without an extra disk roundtrip. Iter26.A.0 — the
    /// raw write primitive that all higher-level B-tree mutation layers
    /// on top of.
    ///
    /// **No fsync** by design: Phase 3c has no transaction semantics
    /// (rollback/journal are Phase 4 / WAL / ADR-0007). Adding fsync
    /// here would be cargo durability — a crash between two related
    /// page writes would still produce a torn B-tree even if both
    /// individual writes were forced to disk. Phase 4 owns the
    /// durability story end-to-end. Until then, callers should treat
    /// writes as in-memory until `close()` returns.
    ///
    /// Page 0 is invalid (sqlite3 numbering convention). pwrite to a
    /// page beyond the current file size implicitly extends the file
    /// with sparse zero pages — fine for now; Iter26.A.3 will manage
    /// the explicit page count in the header.
    pub fn writePage(self: *Pager, page_no: u32, bytes: []const u8) Error!void {
        if (page_no == 0) return Error.IoError;
        if (bytes.len != PAGE_SIZE) return Error.IoError;

        const offset: std.c.off_t = @intCast(@as(usize, page_no - 1) * PAGE_SIZE);
        const n = std.c.pwrite(self.fd, bytes.ptr, PAGE_SIZE, offset);
        if (n < 0) return Error.IoError;
        if (n != @as(isize, @intCast(PAGE_SIZE))) return Error.IoError;

        // Cache write-through: if the page is in the LRU, replace its
        // bytes (same allocation) and promote to head. If not, allocate
        // a fresh entry and insert at head, evicting the tail if over
        // capacity. This keeps `getPage` cheap immediately after a
        // mutation cycle (insert-then-select is the common pattern).
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
        const buf = try self.allocator.alloc(u8, PAGE_SIZE);
        errdefer self.allocator.free(buf);
        @memcpy(buf, bytes);
        try self.cache.insert(self.allocator, 0, .{ .page_no = page_no, .data = buf });
        if (self.cache.items.len > self.cache_capacity) {
            const evicted = self.cache.pop().?;
            self.allocator.free(evicted.data);
        }
    }

    /// Return page `page_no` (1-indexed). On cache hit, the entry moves
    /// to the head. On miss, the page is `pread`'d, inserted at head,
    /// and the LRU tail is evicted if over capacity.
    ///
    /// Returned slice is borrowed: bytes are valid until the next
    /// `getPage()` call may evict this entry. Callers needing a stable
    /// copy must `dupe` immediately.
    pub fn getPage(self: *Pager, page_no: u32) Error![]const u8 {
        if (page_no == 0) return Error.IoError;

        // Cache lookup with LRU promotion.
        for (self.cache.items, 0..) |entry, idx| {
            if (entry.page_no == page_no) {
                if (idx != 0) {
                    const tmp = self.cache.items[idx];
                    var j: usize = idx;
                    while (j > 0) : (j -= 1) {
                        self.cache.items[j] = self.cache.items[j - 1];
                    }
                    self.cache.items[0] = tmp;
                }
                return self.cache.items[0].data;
            }
        }

        // Cache miss: read from disk.
        const buf = try self.allocator.alloc(u8, PAGE_SIZE);
        errdefer self.allocator.free(buf);

        const offset: std.c.off_t = @intCast(@as(usize, page_no - 1) * PAGE_SIZE);
        const n = std.c.pread(self.fd, buf.ptr, PAGE_SIZE, offset);
        if (n < 0) return Error.IoError;
        if (n != @as(isize, @intCast(PAGE_SIZE))) return Error.IoError;

        // Insert at head, evict tail if over capacity.
        try self.cache.insert(self.allocator, 0, .{ .page_no = page_no, .data = buf });
        if (self.cache.items.len > self.cache_capacity) {
            const evicted = self.cache.pop().?;
            self.allocator.free(evicted.data);
        }
        return self.cache.items[0].data;
    }
};

// -- tests --

const testing = std.testing;
const test_db_util = @import("test_db_util.zig");
const makeTempPath = test_db_util.makeTempPath;
const unlinkPath = test_db_util.unlinkPath;
const writeFixture = test_db_util.writeFixture;

test "Pager.open: rejects nonexistent file" {
    const path = try makeTempPath("missing");
    defer testing.allocator.free(path);
    // No fixture written — open must fail.
    try testing.expectError(Error.IoError, Pager.open(testing.allocator, path));
}

test "Pager.getPage: reads page 1 contents" {
    const path = try makeTempPath("page1");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    var content: [16]u8 = .{ 'H', 'e', 'l', 'l', 'o', '!', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    try writeFixture(path, &content);

    var p = try Pager.open(testing.allocator, path);
    defer p.close();
    const data = try p.getPage(1);
    try testing.expectEqual(@as(usize, PAGE_SIZE), data.len);
    try testing.expectEqualStrings("Hello!", data[0..6]);
    try testing.expectEqual(@as(u8, 0), data[6]);
}

test "Pager.getPage: page 0 is invalid" {
    const path = try makeTempPath("p0");
    defer testing.allocator.free(path);
    defer unlinkPath(path);
    try writeFixture(path, "x");

    var p = try Pager.open(testing.allocator, path);
    defer p.close();
    try testing.expectError(Error.IoError, p.getPage(0));
}

test "Pager.getPage: cache hit returns same backing slice" {
    const path = try makeTempPath("hit");
    defer testing.allocator.free(path);
    defer unlinkPath(path);
    try writeFixture(path, "abc");

    var p = try Pager.open(testing.allocator, path);
    defer p.close();
    const a = try p.getPage(1);
    const b = try p.getPage(1);
    // Cache hit: pointer identity, not just byte equality.
    try testing.expect(a.ptr == b.ptr);
}

test "Pager.getPage: LRU eviction at capacity" {
    const path = try makeTempPath("lru");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    // 20 pages, each tagged with its page number in the first byte.
    const n_pages: u32 = 20;
    const content = try testing.allocator.alloc(u8, PAGE_SIZE * n_pages);
    defer testing.allocator.free(content);
    @memset(content, 0);
    for (0..n_pages) |i| {
        // Page (i+1)'s first byte = (i+1) so we can verify reads.
        content[i * PAGE_SIZE] = @intCast(i + 1);
    }
    try writeFixture(path, content);

    var p = try Pager.open(testing.allocator, path);
    defer p.close();
    p.cache_capacity = 4; // Force eviction quickly.

    var i: u32 = 1;
    while (i <= 6) : (i += 1) {
        const data = try p.getPage(i);
        try testing.expectEqual(@as(u8, @intCast(i)), data[0]);
    }
    // Cache should hold pages 6, 5, 4, 3 (head→tail). Page 1 and 2 evicted.
    try testing.expectEqual(@as(usize, 4), p.cache.items.len);
    try testing.expectEqual(@as(u32, 6), p.cache.items[0].page_no);
    try testing.expectEqual(@as(u32, 3), p.cache.items[3].page_no);

    // Re-read page 1: cache miss, brings it to head, evicts page 3.
    const page1 = try p.getPage(1);
    try testing.expectEqual(@as(u8, 1), page1[0]);
    try testing.expectEqual(@as(u32, 1), p.cache.items[0].page_no);
    try testing.expectEqual(@as(u32, 4), p.cache.items[3].page_no);
}

test "Pager.open: second instance fails with DatabaseLocked" {
    const path = try makeTempPath("lock");
    defer testing.allocator.free(path);
    defer unlinkPath(path);
    try writeFixture(path, "x");

    var p1 = try Pager.open(testing.allocator, path);
    defer p1.close();
    try testing.expectError(Error.DatabaseLocked, Pager.open(testing.allocator, path));
}

test "Pager.getPage: LRU promotion on hit" {
    const path = try makeTempPath("promote");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    const n_pages: u32 = 4;
    const content = try testing.allocator.alloc(u8, PAGE_SIZE * n_pages);
    defer testing.allocator.free(content);
    @memset(content, 0);
    for (0..n_pages) |i| content[i * PAGE_SIZE] = @intCast(i + 1);
    try writeFixture(path, content);

    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    _ = try p.getPage(1);
    _ = try p.getPage(2);
    _ = try p.getPage(3);
    // Order now: 3, 2, 1
    try testing.expectEqual(@as(u32, 3), p.cache.items[0].page_no);
    try testing.expectEqual(@as(u32, 1), p.cache.items[2].page_no);

    _ = try p.getPage(1); // hit; promotes 1 to head
    try testing.expectEqual(@as(u32, 1), p.cache.items[0].page_no);
    try testing.expectEqual(@as(u32, 3), p.cache.items[1].page_no);
    try testing.expectEqual(@as(u32, 2), p.cache.items[2].page_no);
}

// `Pager.writePage` and `Pager.allocatePage` tests live in
// `pager_write_test.zig` to keep this file under the 500-line discipline.
