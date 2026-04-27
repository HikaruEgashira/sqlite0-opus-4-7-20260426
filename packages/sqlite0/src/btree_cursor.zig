//! `BtreeCursor` — a `Cursor` backed by `TableLeafWalker` over a Pager-
//! resident table B-tree (Iter25.B.4, ADR-0005 §2).
//!
//! Implements the unified arena lifetime contract from `cursor.zig`:
//! TEXT/BLOB Values returned by `column()` are duped into the arena at
//! decode time, so they survive any number of subsequent `next()` calls
//! and any LRU eviction in the underlying Pager. This is what makes
//! correlated-subquery OuterFrame.current_row safe — there is no
//! separate deep-copy step needed at frame boundaries.
//!
//! ## Decode strategy
//!
//! `column(i)` is called many times per row (once per WHERE-clause
//! reference). Decoding once per `next()` and caching the Values
//! amortises the work — N predicate evaluations cost one decode, not N.
//!
//! ## Schema width vs. record width
//!
//! Real SQLite3 `ALTER TABLE ADD COLUMN` leaves old rows with a record
//! shorter than the current schema. `column(i)` where `i >= decoded.len`
//! but `i < column_names.len` returns `Value.null` (sqlite3 quirk). The
//! caller's column-count is always `columns().len`.
//!
//! ## Schema injection
//!
//! `column_names` is borrowed (lifetime = Database). For Iter25.B.4
//! tests we pass a literal slice; Iter25.C will plumb the names from
//! sqlite_schema.

const std = @import("std");
const ops = @import("ops.zig");
const value_mod = @import("value.zig");
const cursor_mod = @import("cursor.zig");
const btree = @import("btree.zig");
const btree_walk = @import("btree_walk.zig");
const pager_mod = @import("pager.zig");
const record = @import("record.zig");

const Value = value_mod.Value;
const Error = ops.Error;
const Cursor = cursor_mod.Cursor;
const Pager = pager_mod.Pager;
const TableLeafWalker = btree_walk.TableLeafWalker;
const LeafTableCell = btree.LeafTableCell;

/// A read-only cursor over a table B-tree rooted at a known page number.
///
/// Lifetime: caller owns the BtreeCursor (typically stack-allocated or
/// in the per-statement arena). The wrapped `Cursor` returned by
/// `cursor()` aliases `&self`. `column_names` and `pager` are borrowed.
/// All per-row allocations live in `arena`, which the caller resets
/// at statement boundary.
pub const BtreeCursor = struct {
    arena: std.mem.Allocator,
    pager: *Pager,
    root_page: u32,
    column_names: []const []const u8,

    walker: TableLeafWalker,
    /// Cells of the current leaf, owned by `arena`. `null` before
    /// `rewind()` and after EOF.
    current_cells: ?[]LeafTableCell = null,
    /// Index of the current cell within `current_cells`.
    cell_idx: usize = 0,
    /// Decoded Values for the current row, owned by `arena`. Cleared
    /// when the cursor advances to the next row (the previous row's
    /// allocation stays in the arena until reset — that's fine).
    decoded: ?[]Value = null,
    eof: bool = true,

    pub fn open(
        arena: std.mem.Allocator,
        p: *Pager,
        root_page: u32,
        column_names: []const []const u8,
    ) BtreeCursor {
        return .{
            .arena = arena,
            .pager = p,
            .root_page = root_page,
            .column_names = column_names,
            .walker = TableLeafWalker.init(arena, p, root_page),
        };
    }

    /// Release walker stack. Per-row allocations are dropped when the
    /// arena resets — nothing to free here.
    pub fn deinit(self: *BtreeCursor) void {
        self.walker.deinit();
    }

    pub fn cursor(self: *BtreeCursor) Cursor {
        return .{ .impl = self, .vtable = &btree_vtable };
    }

    fn rewindFn(impl: *anyopaque) Error!void {
        const self: *BtreeCursor = @ptrCast(@alignCast(impl));
        // TableLeafWalker has no rewind primitive; tear down and re-init.
        self.walker.deinit();
        self.walker = TableLeafWalker.init(self.arena, self.pager, self.root_page);
        self.current_cells = null;
        self.cell_idx = 0;
        self.decoded = null;
        self.eof = false;
        try self.advanceToValidRow();
    }

    fn nextFn(impl: *anyopaque) Error!void {
        const self: *BtreeCursor = @ptrCast(@alignCast(impl));
        if (self.eof) return; // no-op past EOF (matches Cursor contract)
        self.cell_idx += 1;
        self.decoded = null;
        try self.advanceToValidRow();
    }

    fn isEofFn(impl: *anyopaque) bool {
        const self: *BtreeCursor = @ptrCast(@alignCast(impl));
        return self.eof;
    }

    fn columnFn(impl: *anyopaque, idx: usize) Error!Value {
        const self: *BtreeCursor = @ptrCast(@alignCast(impl));
        if (self.eof) return Error.SyntaxError;
        if (idx >= self.column_names.len) return Error.SyntaxError;
        const decoded = self.decoded orelse return Error.SyntaxError;
        // Schema-width quirk: short records (post-ALTER TABLE) yield
        // NULL for trailing columns the row never had.
        if (idx >= decoded.len) return Value.null;
        return decoded[idx];
    }

    fn columnsFn(impl: *anyopaque) []const []const u8 {
        const self: *BtreeCursor = @ptrCast(@alignCast(impl));
        return self.column_names;
    }

    /// After advancing `cell_idx` (or starting fresh in rewind), pull
    /// new leaves from the walker until either we land on a non-empty
    /// cell or the walker reports EOF. Decode the cell's record into
    /// `self.decoded`, duping TEXT/BLOB into the arena.
    fn advanceToValidRow(self: *BtreeCursor) Error!void {
        while (true) {
            if (self.current_cells) |cells| {
                if (self.cell_idx < cells.len) {
                    try self.decodeCurrentRow();
                    return;
                }
            }
            // Need a new leaf.
            const next_leaf = try self.walker.next();
            if (next_leaf == null) {
                self.eof = true;
                return;
            }
            const leaf = next_leaf.?;
            // PAGE_SIZE is also the usable_size in Iter25 (no reserved
            // space yet — Phase 4 may surface a tunable).
            const cells = try btree.parseLeafTablePage(
                self.arena,
                leaf.bytes,
                leaf.header_offset,
                pager_mod.PAGE_SIZE,
            );
            self.current_cells = cells;
            self.cell_idx = 0;
        }
    }

    fn decodeCurrentRow(self: *BtreeCursor) Error!void {
        const cells = self.current_cells.?;
        const cell = cells[self.cell_idx];
        const raw = try record.decodeRecord(self.arena, cell.record_bytes);
        // Dupe TEXT/BLOB into the arena so the Values survive page
        // eviction (the unified arena lifetime contract).
        for (raw) |*v| {
            switch (v.*) {
                .text => |t| {
                    const copy = try self.arena.dupe(u8, t);
                    v.* = .{ .text = copy };
                },
                .blob => |b| {
                    const copy = try self.arena.dupe(u8, b);
                    v.* = .{ .blob = copy };
                },
                else => {},
            }
        }
        self.decoded = raw;
    }

    const btree_vtable: Cursor.VTable = .{
        .rewind = rewindFn,
        .next = nextFn,
        .is_eof = isEofFn,
        .column = columnFn,
        .columns = columnsFn,
    };
};

// -- tests --

const testing = std.testing;
const test_util = @import("btree_test_util.zig");
const PAGE_SIZE = pager_mod.PAGE_SIZE;

var bc_test_counter: std.atomic.Value(u32) = .init(0);

fn makeTempPath(suffix: []const u8) ![]u8 {
    const tmpdir_raw = std.c.getenv("TMPDIR");
    const tmpdir_slice: []const u8 = if (tmpdir_raw) |p| std.mem.span(@as([*:0]const u8, p)) else "/tmp";
    const trimmed = std.mem.trimEnd(u8, tmpdir_slice, "/");
    const pid = std.c.getpid();
    const seq = bc_test_counter.fetchAdd(1, .seq_cst);
    return std.fmt.allocPrint(testing.allocator, "{s}/sqlite0-bcursor-test-{d}-{d}-{s}.db", .{ trimmed, pid, seq, suffix });
}

fn unlinkPath(path: []const u8) void {
    const path_z = testing.allocator.dupeZ(u8, path) catch return;
    defer testing.allocator.free(path_z);
    _ = std.c.unlink(path_z.ptr);
}

fn writePages(path: []const u8, pages: []const []const u8) !void {
    const path_z = try testing.allocator.dupeZ(u8, path);
    defer testing.allocator.free(path_z);
    const flags: std.c.O = .{ .ACCMODE = .RDWR, .CREAT = true, .TRUNC = true };
    const fd = std.c.open(path_z.ptr, flags, @as(std.c.mode_t, 0o644));
    if (fd < 0) return error.OpenFailed;
    defer _ = std.c.close(fd);
    const total = pages.len * PAGE_SIZE;
    const buf = try testing.allocator.alloc(u8, total);
    defer testing.allocator.free(buf);
    @memset(buf, 0);
    for (pages, 0..) |p, i| {
        std.debug.assert(p.len == PAGE_SIZE);
        @memcpy(buf[i * PAGE_SIZE .. (i + 1) * PAGE_SIZE], p);
    }
    const w = std.c.write(fd, buf.ptr, total);
    if (w != @as(isize, @intCast(total))) return error.WriteFailed;
}

test "BtreeCursor: empty B-tree (root leaf, 0 cells)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const path = try makeTempPath("empty");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    const empty_inputs = [_]test_util.TestCellInput{};
    const page1 = try test_util.buildLeafTablePage(testing.allocator, PAGE_SIZE, 100, &empty_inputs);
    defer testing.allocator.free(page1);
    try writePages(path, &[_][]const u8{page1});

    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    const names = [_][]const u8{"x"};
    var bc = BtreeCursor.open(a, &p, 1, &names);
    defer bc.deinit();
    const c = bc.cursor();

    try c.rewind();
    try testing.expect(c.isEof());
}

test "BtreeCursor: walks rows in rowid order, decodes columns" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const path = try makeTempPath("walk");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    // 3 rows on page 1: each has one INTEGER column.
    const r1 = [_]u8{ 0x02, 0x01, 0x05 };
    const r2 = [_]u8{ 0x02, 0x01, 0x0a };
    const r3 = [_]u8{ 0x02, 0x01, 0x0f };
    const inputs = [_]test_util.TestCellInput{
        .{ .rowid = 1, .record = &r1 },
        .{ .rowid = 2, .record = &r2 },
        .{ .rowid = 3, .record = &r3 },
    };
    const page1 = try test_util.buildLeafTablePage(testing.allocator, PAGE_SIZE, 100, &inputs);
    defer testing.allocator.free(page1);
    try writePages(path, &[_][]const u8{page1});

    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    const names = [_][]const u8{"x"};
    var bc = BtreeCursor.open(a, &p, 1, &names);
    defer bc.deinit();
    const c = bc.cursor();

    try c.rewind();
    var seen: usize = 0;
    while (!c.isEof()) : (try c.next()) {
        const v = try c.column(0);
        const expected: i64 = @intCast(5 * (seen + 1));
        try testing.expectEqual(expected, v.integer);
        seen += 1;
    }
    try testing.expectEqual(@as(usize, 3), seen);
}

test "BtreeCursor: TEXT survives page churn (arena dupe)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const path = try makeTempPath("textchurn");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    // page 1 = blank (sqlite_schema slot — unused for this test)
    // page 2 = interior, single cell pointing left=3, right=4
    // page 3 = leaf with rowid 1, record = 1 column TEXT "alpha"
    // page 4 = leaf with rowid 2, record = 1 column TEXT "bravo"
    // Repeating both leaves under one interior forces the walker to
    // pull page 3, then page 4 — which would evict page 3's bytes
    // from the cache once we lower cache_capacity to 1.
    const blank = try testing.allocator.alloc(u8, PAGE_SIZE);
    defer testing.allocator.free(blank);
    @memset(blank, 0);

    const interior_cells = [_]test_util.TestInteriorCellInput{.{ .left_child = 3, .key = 1 }};
    const page2 = try test_util.buildInteriorTablePage(testing.allocator, PAGE_SIZE, 0, 4, &interior_cells);
    defer testing.allocator.free(page2);

    // serial type for TEXT of length n: 13 + 2n. "alpha" = len 5 → 23.
    // "bravo" = len 5 → 23. Header (1-byte varint = 1 byte total payload?
    //   header_len varint + serial-type varint(s) → 1 + 1 = 2 bytes header.
    //   Bytes: header_len = 0x02, serial_type = 0x17, payload = "alpha".
    const r3 = [_]u8{ 0x02, 0x17, 'a', 'l', 'p', 'h', 'a' };
    const r4 = [_]u8{ 0x02, 0x17, 'b', 'r', 'a', 'v', 'o' };
    const in3 = [_]test_util.TestCellInput{.{ .rowid = 1, .record = &r3 }};
    const in4 = [_]test_util.TestCellInput{.{ .rowid = 2, .record = &r4 }};
    const page3 = try test_util.buildLeafTablePage(testing.allocator, PAGE_SIZE, 0, &in3);
    defer testing.allocator.free(page3);
    const page4 = try test_util.buildLeafTablePage(testing.allocator, PAGE_SIZE, 0, &in4);
    defer testing.allocator.free(page4);

    try writePages(path, &[_][]const u8{ blank, page2, page3, page4 });

    var p = try Pager.open(testing.allocator, path);
    defer p.close();
    p.cache_capacity = 1; // force eviction between leaves

    const names = [_][]const u8{"name"};
    var bc = BtreeCursor.open(a, &p, 2, &names);
    defer bc.deinit();
    const c = bc.cursor();

    try c.rewind();
    const first = try c.column(0);
    // Read first BEFORE advancing so we can hold it across next().
    try testing.expectEqualStrings("alpha", first.text);
    try c.next();
    try testing.expect(!c.isEof());
    const second = try c.column(0);
    try testing.expectEqualStrings("bravo", second.text);
    // Critical: after advancing to row 2, the bytes the FIRST Value
    // points at are no longer in the page cache. If column() returned
    // a borrowed slice into page 3, the next assertion would read freed
    // memory (or the same bytes as page 4). Because BtreeCursor dupes
    // into the arena, `first.text` is still "alpha".
    try testing.expectEqualStrings("alpha", first.text);
    try c.next();
    try testing.expect(c.isEof());
}

test "BtreeCursor: rewind re-reads from the start" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const path = try makeTempPath("rewind");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    const r1 = [_]u8{ 0x02, 0x01, 0x07 };
    const r2 = [_]u8{ 0x02, 0x01, 0x0e };
    const inputs = [_]test_util.TestCellInput{
        .{ .rowid = 1, .record = &r1 },
        .{ .rowid = 2, .record = &r2 },
    };
    const page1 = try test_util.buildLeafTablePage(testing.allocator, PAGE_SIZE, 100, &inputs);
    defer testing.allocator.free(page1);
    try writePages(path, &[_][]const u8{page1});

    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    const names = [_][]const u8{"x"};
    var bc = BtreeCursor.open(a, &p, 1, &names);
    defer bc.deinit();
    const c = bc.cursor();

    try c.rewind();
    try c.next();
    try c.next();
    try testing.expect(c.isEof());

    try c.rewind();
    try testing.expect(!c.isEof());
    const v = try c.column(0);
    try testing.expectEqual(@as(i64, 7), v.integer);
}

test "BtreeCursor: short record yields NULL for missing trailing columns" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const path = try makeTempPath("short");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    // Schema = 3 columns; record only encodes 1.
    // header_len = 2, serial_type = 1 (1-byte int), value = 0x2a
    const r = [_]u8{ 0x02, 0x01, 0x2a };
    const inputs = [_]test_util.TestCellInput{.{ .rowid = 1, .record = &r }};
    const page1 = try test_util.buildLeafTablePage(testing.allocator, PAGE_SIZE, 100, &inputs);
    defer testing.allocator.free(page1);
    try writePages(path, &[_][]const u8{page1});

    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    const names = [_][]const u8{ "a", "b", "c" };
    var bc = BtreeCursor.open(a, &p, 1, &names);
    defer bc.deinit();
    const c = bc.cursor();

    try c.rewind();
    try testing.expectEqual(@as(usize, 3), c.columns().len);
    const v0 = try c.column(0);
    try testing.expectEqual(@as(i64, 0x2a), v0.integer);
    const v1 = try c.column(1);
    try testing.expect(v1 == .null);
    const v2 = try c.column(2);
    try testing.expect(v2 == .null);
    try testing.expectError(Error.SyntaxError, c.column(3));
}

test "BtreeCursor: column() before rewind returns SyntaxError" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const path = try makeTempPath("noread");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    const inputs = [_]test_util.TestCellInput{};
    const page1 = try test_util.buildLeafTablePage(testing.allocator, PAGE_SIZE, 100, &inputs);
    defer testing.allocator.free(page1);
    try writePages(path, &[_][]const u8{page1});

    var p = try Pager.open(testing.allocator, path);
    defer p.close();

    const names = [_][]const u8{"x"};
    var bc = BtreeCursor.open(a, &p, 1, &names);
    defer bc.deinit();
    const c = bc.cursor();
    // Note: eof defaults to true before rewind() is called. column() on
    // an EOF cursor returns SyntaxError per the contract.
    try testing.expectError(Error.SyntaxError, c.column(0));
}
