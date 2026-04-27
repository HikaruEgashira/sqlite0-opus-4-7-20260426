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
const btree_overflow = @import("btree_overflow.zig");
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
    /// Cached usable_size (PAGE_SIZE − reserved). Read once on first
    /// advance and reused — evaluating `pager.usableSize()` on every
    /// leaf would re-fetch page 1, which evicts the just-loaded leaf
    /// when `cache_capacity` is at the minimum (the page-churn test
    /// exercises exactly this).
    usable_size: ?usize = null,
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

    /// Memoised `pager.usableSize()`. Computing it eagerly per leaf
    /// would re-fetch page 1 between every parse, which evicts the
    /// just-loaded leaf when `cache_capacity == 1` (the page-churn
    /// test exercises exactly this).
    fn cachedUsableSize(self: *BtreeCursor) Error!usize {
        if (self.usable_size) |u| return u;
        const u = try self.pager.usableSize();
        self.usable_size = u;
        return u;
    }

    pub fn cursor(self: *BtreeCursor) Cursor {
        return .{ .impl = self, .vtable = &btree_vtable };
    }

    fn rewindFn(impl: *anyopaque) Error!void {
        const self: *BtreeCursor = @ptrCast(@alignCast(impl));
        // Prime the cached usable_size BEFORE any walker.next() call.
        // The first cachedUsableSize() pulls page 1, which can evict
        // a freshly-cached leaf when cache_capacity == 1 — and the
        // walker hands back a borrowed slice into that buffer.
        _ = try self.cachedUsableSize();
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
            const cells = try btree.parseLeafTablePage(
                self.arena,
                leaf.bytes,
                leaf.header_offset,
                try self.cachedUsableSize(),
            );
            self.current_cells = cells;
            self.cell_idx = 0;
        }
    }

    fn decodeCurrentRow(self: *BtreeCursor) Error!void {
        const cells = self.current_cells.?;
        const cell = cells[self.cell_idx];
        // Assemble the full payload — for inline-only cells this returns
        // `cell.inline_bytes` directly (no copy); for overflow cells it
        // walks the chain into an arena buffer.
        const full = try btree_overflow.assemblePayload(
            self.arena,
            self.pager,
            cell,
            try self.cachedUsableSize(),
        );
        const raw = try record.decodeRecord(self.arena, full);
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

// Unit tests live in `btree_cursor_test.zig` to keep this file under
// the 500-line discipline (CLAUDE.md "Module Splitting Rules").
