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

/// Module-scope monotonic counter for unique temp paths.
var test_counter: std.atomic.Value(u32) = .init(0);

/// Construct a unique temp-file path under `$TMPDIR` (or `/tmp`). Caller
/// frees. Uses pid + monotonic counter so concurrent tests don't collide.
fn makeTempPath(allocator: std.mem.Allocator, suffix: []const u8) ![]u8 {
    const tmpdir_raw = std.c.getenv("TMPDIR");
    const tmpdir_slice: []const u8 = if (tmpdir_raw) |p| std.mem.span(@as([*:0]const u8, p)) else "/tmp";
    const trimmed = std.mem.trimEnd(u8, tmpdir_slice, "/");
    const pid = std.c.getpid();
    const seq = test_counter.fetchAdd(1, .seq_cst);
    return std.fmt.allocPrint(allocator, "{s}/sqlite0-pager-test-{d}-{d}-{s}.db", .{ trimmed, pid, seq, suffix });
}

/// Write `content` to a fresh file at `path` (truncating any existing).
/// Pads with zeros to the next PAGE_SIZE boundary so partial writes
/// don't surprise pread.
fn writeFixture(path: []const u8, content: []const u8) !void {
    const path_z = try testing.allocator.dupeZ(u8, path);
    defer testing.allocator.free(path_z);

    const flags: std.c.O = .{
        .ACCMODE = .RDWR,
        .CREAT = true,
        .TRUNC = true,
    };
    // Use the variadic mode arg — 0o644.
    const fd = std.c.open(path_z.ptr, flags, @as(std.c.mode_t, 0o644));
    if (fd < 0) return error.OpenFailed;
    defer _ = std.c.close(fd);

    // Round up to page boundary; final byte ensures the file is exactly
    // ceil(content.len / PAGE_SIZE) pages long.
    const n_pages = (content.len + PAGE_SIZE - 1) / PAGE_SIZE;
    const total = n_pages * PAGE_SIZE;
    const padded = try testing.allocator.alloc(u8, total);
    defer testing.allocator.free(padded);
    @memset(padded, 0);
    @memcpy(padded[0..content.len], content);

    const w = std.c.write(fd, padded.ptr, total);
    if (w != @as(isize, @intCast(total))) return error.WriteFailed;
}

fn unlinkPath(path: []const u8) void {
    const path_z = testing.allocator.dupeZ(u8, path) catch return;
    defer testing.allocator.free(path_z);
    _ = std.c.unlink(path_z.ptr);
}

test "Pager.open: rejects nonexistent file" {
    const path = try makeTempPath(testing.allocator, "missing");
    defer testing.allocator.free(path);
    // No fixture written — open must fail.
    try testing.expectError(Error.IoError, Pager.open(testing.allocator, path));
}

test "Pager.getPage: reads page 1 contents" {
    const path = try makeTempPath(testing.allocator, "page1");
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
    const path = try makeTempPath(testing.allocator, "p0");
    defer testing.allocator.free(path);
    defer unlinkPath(path);
    try writeFixture(path, "x");

    var p = try Pager.open(testing.allocator, path);
    defer p.close();
    try testing.expectError(Error.IoError, p.getPage(0));
}

test "Pager.getPage: cache hit returns same backing slice" {
    const path = try makeTempPath(testing.allocator, "hit");
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
    const path = try makeTempPath(testing.allocator, "lru");
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
    const path = try makeTempPath(testing.allocator, "lock");
    defer testing.allocator.free(path);
    defer unlinkPath(path);
    try writeFixture(path, "x");

    var p1 = try Pager.open(testing.allocator, path);
    defer p1.close();
    try testing.expectError(Error.DatabaseLocked, Pager.open(testing.allocator, path));
}

test "Pager.getPage: LRU promotion on hit" {
    const path = try makeTempPath(testing.allocator, "promote");
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

test "Pager.writePage: round-trip through close + reopen" {
    const path = try makeTempPath(testing.allocator, "write");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    // Pre-create a 2-page file (sparse zeros) so page 1 and 2 exist
    // before we open the Pager (open requires the file to exist).
    const content = try testing.allocator.alloc(u8, PAGE_SIZE * 2);
    defer testing.allocator.free(content);
    @memset(content, 0);
    try writeFixture(path, content);

    // Construct a non-trivial payload for page 1.
    const payload = try testing.allocator.alloc(u8, PAGE_SIZE);
    defer testing.allocator.free(payload);
    @memset(payload, 0);
    @memcpy(payload[0..6], "hello!");
    payload[PAGE_SIZE - 1] = 0xab; // mark the tail too

    {
        var p = try Pager.open(testing.allocator, path);
        defer p.close();
        try p.writePage(1, payload);
    }

    // Reopen the file; verify the bytes survived the close.
    var p2 = try Pager.open(testing.allocator, path);
    defer p2.close();
    const got = try p2.getPage(1);
    try testing.expectEqualStrings("hello!", got[0..6]);
    try testing.expectEqual(@as(u8, 0xab), got[PAGE_SIZE - 1]);
}

test "Pager.writePage: rejects page 0 and wrong-length buffers" {
    const path = try makeTempPath(testing.allocator, "rejects");
    defer testing.allocator.free(path);
    defer unlinkPath(path);
    try writeFixture(path, "x");

    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    const buf = try testing.allocator.alloc(u8, PAGE_SIZE);
    defer testing.allocator.free(buf);
    @memset(buf, 0);
    try testing.expectError(Error.IoError, p.writePage(0, buf));

    const short = try testing.allocator.alloc(u8, 32);
    defer testing.allocator.free(short);
    try testing.expectError(Error.IoError, p.writePage(1, short));
}

test "Pager.writePage: cache write-through reflects in subsequent getPage" {
    const path = try makeTempPath(testing.allocator, "wt");
    defer testing.allocator.free(path);
    defer unlinkPath(path);
    try writeFixture(path, "init");

    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    // Prime the cache with the initial bytes.
    const initial = try p.getPage(1);
    try testing.expectEqualStrings("init", initial[0..4]);

    // Write new bytes; cache must reflect them WITHOUT another disk
    // read (the test trusts the write-through path because we haven't
    // reopened the file).
    const fresh = try testing.allocator.alloc(u8, PAGE_SIZE);
    defer testing.allocator.free(fresh);
    @memset(fresh, 0);
    @memcpy(fresh[0..5], "fresh");
    try p.writePage(1, fresh);

    const after = try p.getPage(1);
    try testing.expectEqualStrings("fresh", after[0..5]);
    // Pointer identity: same cache slot, in-place memcpy.
    try testing.expect(initial.ptr == after.ptr);
}
