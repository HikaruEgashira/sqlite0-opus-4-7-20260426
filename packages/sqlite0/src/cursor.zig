//! Row-source cursor abstraction (ADR-0004 §2, Iter24.A).
//!
//! `Cursor` is the **only** row source the evaluator path consumes. Phase 3a
//! (this module) backs cursors with the in-memory `Database.Table` ArrayList;
//! Phase 3b (ADR-0005) will add a `BtreeCursor` implementation that traverses
//! Pager pages with the same vtable shape. Code that consumes `Cursor` does
//! not branch on the backend.
//!
//! Iteration model mirrors SQLite3's VM cursors: open → rewind → loop
//! { read columns, next } → close. `is_eof()` is true after rewind on an
//! empty source and after the last `next()` past the final row. `column(idx)`
//! returns a borrowed Value — the bytes inside TEXT/BLOB belong to the
//! cursor's backing storage and are valid only until the next `next()` /
//! `rewind()`. Long-lived consumers (e.g. `dupeRowsToLongLived`) must dupe.
//!
//! `materializeRows` is the bridge from the per-row cursor model to the
//! `[][]Value` shape the legacy `cartesianFromSources` / `select.zig` code
//! still expects. Each row is freshly allocated in `alloc` (the caller's
//! arena) — Value contents are aliased by reference for in-memory backends
//! (zero-cost) and will need explicit dupe when Phase 3b's BtreeCursor lands
//! (page eviction can invalidate the bytes between rows).

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const database = @import("database.zig");

const Value = value_mod.Value;
const Error = ops.Error;

/// Type-erased cursor handle. The `impl` pointer is opaque — only the vtable
/// knows its concrete type. Callers always go through the `vtable` thunks
/// (or the inline pass-through methods below).
pub const Cursor = struct {
    impl: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Reset to the position before the first row. After `rewind()`,
        /// `is_eof()` reflects whether the source is empty.
        rewind: *const fn (impl: *anyopaque) Error!void,
        /// Advance past the current row. After `next()`, `is_eof()` is true
        /// iff the source has been fully consumed. Calling `next()` while
        /// `is_eof()` is already true is a no-op.
        next: *const fn (impl: *anyopaque) Error!void,
        /// True if the cursor is positioned past the last row (or before the
        /// first row of an empty source).
        is_eof: *const fn (impl: *anyopaque) bool,
        /// Read column `idx` (0-based) from the current row. Returns a
        /// borrowed Value — TEXT/BLOB bytes belong to the cursor's backing
        /// storage and must not outlive the next mutation of the cursor.
        column: *const fn (impl: *anyopaque, idx: usize) Error!Value,
        /// Schema column names. Lifetime = Database (independent of cursor
        /// position). Caller does not free.
        columns: *const fn (impl: *anyopaque) []const []const u8,
    };

    pub inline fn rewind(self: Cursor) Error!void {
        return self.vtable.rewind(self.impl);
    }
    pub inline fn next(self: Cursor) Error!void {
        return self.vtable.next(self.impl);
    }
    pub inline fn isEof(self: Cursor) bool {
        return self.vtable.is_eof(self.impl);
    }
    pub inline fn column(self: Cursor, idx: usize) Error!Value {
        return self.vtable.column(self.impl, idx);
    }
    pub inline fn columns(self: Cursor) []const []const u8 {
        return self.vtable.columns(self.impl);
    }
};

/// In-memory `Database.Table` cursor. Holds a borrowed pointer to the table
/// and a row-index position. Owns nothing — the table outlives the cursor.
///
/// Caller must keep the `TableCursor` alive (typically on the stack or in
/// the per-statement arena) while the resulting `Cursor` is in use; the
/// `cursor()` method returns a handle whose `impl` aliases `&self`.
pub const TableCursor = struct {
    table: *const database.Table,
    pos: usize = 0,

    pub fn open(table: *const database.Table) TableCursor {
        return .{ .table = table };
    }

    pub fn cursor(self: *TableCursor) Cursor {
        return .{ .impl = self, .vtable = &table_vtable };
    }

    fn rewindFn(impl: *anyopaque) Error!void {
        const self: *TableCursor = @ptrCast(@alignCast(impl));
        self.pos = 0;
    }
    fn nextFn(impl: *anyopaque) Error!void {
        const self: *TableCursor = @ptrCast(@alignCast(impl));
        if (self.pos < self.table.rows.items.len) self.pos += 1;
    }
    fn isEofFn(impl: *anyopaque) bool {
        const self: *TableCursor = @ptrCast(@alignCast(impl));
        return self.pos >= self.table.rows.items.len;
    }
    fn columnFn(impl: *anyopaque, idx: usize) Error!Value {
        const self: *TableCursor = @ptrCast(@alignCast(impl));
        if (self.pos >= self.table.rows.items.len) return Error.SyntaxError;
        const row = self.table.rows.items[self.pos];
        if (idx >= row.len) return Error.SyntaxError;
        return row[idx];
    }
    fn columnsFn(impl: *anyopaque) []const []const u8 {
        const self: *TableCursor = @ptrCast(@alignCast(impl));
        return self.table.columns;
    }

    const table_vtable: Cursor.VTable = .{
        .rewind = rewindFn,
        .next = nextFn,
        .is_eof = isEofFn,
        .column = columnFn,
        .columns = columnsFn,
    };
};

/// Walk `cursor` from the first row to EOF, materialising every row into a
/// freshly-allocated `[]Value` in `alloc`. Each row is one allocation; the
/// outer slice is one allocation. Value contents are copied by **value**
/// (Zig copy semantics): integers/reals/null inline; TEXT/BLOB pointer
/// + len. For in-memory backends this aliases the cursor's backing bytes —
/// safe because the cursor (and thus the Database.Table behind it) outlives
/// the materialised slice. For Phase 3b (BtreeCursor, page eviction) the
/// caller must dupe Value bytes before page churn.
pub fn materializeRows(alloc: std.mem.Allocator, c: Cursor) Error![][]Value {
    var rows: std.ArrayList([]Value) = .empty;
    errdefer rows.deinit(alloc);

    try c.rewind();
    while (!c.isEof()) {
        const cols = c.columns();
        const row = try alloc.alloc(Value, cols.len);
        var produced: usize = 0;
        errdefer {
            // Borrowed Values; nothing to free per element. Just release
            // the row slice itself.
            alloc.free(row);
        }
        while (produced < cols.len) : (produced += 1) {
            row[produced] = try c.column(produced);
        }
        try rows.append(alloc, row);
        try c.next();
    }
    return rows.toOwnedSlice(alloc);
}

test "TableCursor: empty table yields no rows" {
    const allocator = std.testing.allocator;
    var db = database.Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("CREATE TABLE t(x)");
    er.deinit();

    const t = db.tables.getPtr("t").?;
    var tc = TableCursor.open(t);
    const c = tc.cursor();

    try c.rewind();
    try std.testing.expect(c.isEof());

    const rows = try materializeRows(allocator, c);
    defer {
        for (rows) |r| allocator.free(r);
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, 0), rows.len);
}

test "TableCursor: walks all rows in order" {
    const allocator = std.testing.allocator;
    var db = database.Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("CREATE TABLE t(x, y); INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    er.deinit();

    const t = db.tables.getPtr("t").?;
    var tc = TableCursor.open(t);
    const c = tc.cursor();

    var seen: usize = 0;
    try c.rewind();
    while (!c.isEof()) : (try c.next()) {
        const x = try c.column(0);
        const y = try c.column(1);
        try std.testing.expectEqual(@as(i64, @intCast(seen + 1)), x.integer);
        try std.testing.expectEqualStrings(switch (seen) {
            0 => "a",
            1 => "b",
            2 => "c",
            else => unreachable,
        }, y.text);
        seen += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), seen);
}

test "TableCursor: rewind resets position" {
    const allocator = std.testing.allocator;
    var db = database.Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("CREATE TABLE t(x); INSERT INTO t VALUES (10), (20)");
    er.deinit();

    const t = db.tables.getPtr("t").?;
    var tc = TableCursor.open(t);
    const c = tc.cursor();

    try c.rewind();
    try c.next();
    try c.next();
    try std.testing.expect(c.isEof());

    try c.rewind();
    try std.testing.expect(!c.isEof());
    const v = try c.column(0);
    try std.testing.expectEqual(@as(i64, 10), v.integer);
}

test "TableCursor: column out of range returns SyntaxError" {
    const allocator = std.testing.allocator;
    var db = database.Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
    er.deinit();

    const t = db.tables.getPtr("t").?;
    var tc = TableCursor.open(t);
    const c = tc.cursor();
    try c.rewind();
    try std.testing.expectError(Error.SyntaxError, c.column(99));
}

test "TableCursor: columns() returns table schema" {
    const allocator = std.testing.allocator;
    var db = database.Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("CREATE TABLE t(alpha, beta, gamma)");
    er.deinit();

    const t = db.tables.getPtr("t").?;
    var tc = TableCursor.open(t);
    const c = tc.cursor();
    const cols = c.columns();
    try std.testing.expectEqual(@as(usize, 3), cols.len);
    try std.testing.expectEqualStrings("alpha", cols[0]);
    try std.testing.expectEqualStrings("gamma", cols[2]);
}

test "materializeRows: produces row-shaped output" {
    const allocator = std.testing.allocator;
    var db = database.Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("CREATE TABLE t(x, y); INSERT INTO t VALUES (1, 'a'), (2, 'b')");
    er.deinit();

    const t = db.tables.getPtr("t").?;
    var tc = TableCursor.open(t);
    const c = tc.cursor();
    const rows = try materializeRows(allocator, c);
    defer {
        for (rows) |r| allocator.free(r);
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqual(@as(i64, 1), rows[0][0].integer);
    try std.testing.expectEqualStrings("b", rows[1][1].text);
}
