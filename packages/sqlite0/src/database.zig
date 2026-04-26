//! `Database` — state-holding execution context (ADR-0003).
//!
//! Single source of truth for tables and other persistent state. Iter14.A
//! delivered the multi-statement shell + arena boundaries; Iter14.B added
//! `CREATE TABLE` schema registration; Iter14.C wires `INSERT INTO t VALUES`
//! and `SELECT ... FROM t` so a full round-trip is observable end-to-end.
//!
//! Each `execute` call accepts one or more `;`-separated statements, runs
//! them in order against `self`, and returns one `StatementResult` per
//! statement. Errors are all-or-nothing (ADR-0003 §1) — Zig's `!T` cannot
//! return a partial list and an error simultaneously, so on failure the
//! already-accumulated results are freed and only the error propagates.
//!
//! Memory model (ADR-0003 §8): every statement's AST and intermediate row
//! buffers live in a per-statement `ArenaAllocator`. Surviving rows are
//! deep-duped to `db.allocator` before the arena tears down so the returned
//! `StatementResult` outlives the parser/AST that produced it. The dupe
//! boundary is `dupeRowsToLongLived` — every TEXT/BLOB byte buffer in the
//! result must pass through it once.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const ast = @import("ast.zig");
const stmt_mod = @import("stmt.zig");
const parser_mod = @import("parser.zig");
const select_mod = @import("select.zig");

const Value = value_mod.Value;
pub const Error = ops.Error;

/// One executed statement's outcome. Row-producing statements carry their
/// rows; `create_table` and `insert` carry no rows (their effect is on
/// `Database` state). `insert` reports the rowcount for symmetry with sqlite3
/// but the CLI doesn't print it (sqlite3 stays silent too).
pub const StatementResult = union(enum) {
    select: [][]Value,
    values: [][]Value,
    create_table,
    insert: struct { rowcount: u64 },

    pub fn deinit(self: StatementResult, allocator: std.mem.Allocator) void {
        switch (self) {
            .select, .values => |rows| freeRows(allocator, rows),
            .create_table, .insert => {},
        }
    }
};

/// In-memory table. All strings (name keys, column names) and `Value`
/// payloads inside `rows` are owned by `Database.allocator`. ADR-0003 §2:
/// no type affinity tracking in Phase 2; Iter14.B leaves `rows` empty and
/// Iter14.C populates it via INSERT.
pub const Table = struct {
    columns: [][]const u8,
    rows: std.ArrayListUnmanaged([]Value) = .empty,

    pub fn deinit(self: *Table, allocator: std.mem.Allocator) void {
        for (self.rows.items) |row| {
            for (row) |v| ops.freeValue(allocator, v);
            allocator.free(row);
        }
        self.rows.deinit(allocator);
        for (self.columns) |c| allocator.free(c);
        allocator.free(self.columns);
    }
};

pub const ExecResult = struct {
    statements: []StatementResult,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ExecResult) void {
        for (self.statements) |s| s.deinit(self.allocator);
        self.allocator.free(self.statements);
    }
};

pub const Database = struct {
    allocator: std.mem.Allocator,
    tables: std.StringHashMapUnmanaged(Table) = .{},

    pub fn init(allocator: std.mem.Allocator) Database {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Database) void {
        var it = self.tables.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.tables.deinit(self.allocator);
    }

    /// Register a new empty table. Returns `Error.TableAlreadyExists` if a
    /// table with the (case-insensitive) name is already in `tables`. Both
    /// `name` and `cols` (and each entry of `cols`) are taken into ownership
    /// by `self` on success and freed at `deinit`. On failure ownership of
    /// the inputs returns to the caller (this function frees nothing it
    /// didn't allocate itself).
    fn registerTable(self: *Database, parsed: stmt_mod.ParsedCreateTable) !void {
        const key = try lowerCaseDupe(self.allocator, parsed.name);
        errdefer self.allocator.free(key);

        if (self.tables.contains(key)) return Error.TableAlreadyExists;

        const cols = try self.allocator.alloc([]const u8, parsed.columns.len);
        var produced: usize = 0;
        errdefer {
            for (cols[0..produced]) |c| self.allocator.free(c);
            self.allocator.free(cols);
        }
        while (produced < parsed.columns.len) : (produced += 1) {
            cols[produced] = try lowerCaseDupe(self.allocator, parsed.columns[produced]);
        }

        try self.tables.put(self.allocator, key, .{ .columns = cols });
    }

    /// Execute `sql` (one or more `;`-separated statements) against `self`.
    /// On success, returns one `StatementResult` per statement that ran. On
    /// any error, frees already-accumulated results and propagates the error.
    pub fn execute(self: *Database, sql: []const u8) !ExecResult {
        var p = parser_mod.Parser.init(self.allocator, sql);
        var statements: std.ArrayList(StatementResult) = .empty;
        errdefer {
            for (statements.items) |s| s.deinit(self.allocator);
            statements.deinit(self.allocator);
        }
        while (p.cur.kind != .eof) {
            if (p.cur.kind == .semicolon) {
                p.advance();
                continue;
            }
            const sr = try dispatchOne(self, &p);
            try statements.append(self.allocator, sr);
            if (p.cur.kind == .semicolon) p.advance();
        }
        return .{
            .statements = try statements.toOwnedSlice(self.allocator),
            .allocator = self.allocator,
        };
    }
};

/// Execute one statement against `db` using a per-statement arena. The arena
/// holds AST nodes and intermediate row buffers (TEXT/BLOB included); the
/// returned `StatementResult` is deep-duped to `db.allocator` before the
/// arena tears down.
fn dispatchOne(db: *Database, p: *parser_mod.Parser) !StatementResult {
    var arena = std.heap.ArenaAllocator.init(db.allocator);
    defer arena.deinit();
    const arena_alloc = arena.allocator();

    const saved = p.allocator;
    p.allocator = arena_alloc;
    defer p.allocator = saved;

    switch (p.cur.kind) {
        .keyword_select => {
            const parsed = try stmt_mod.parseSelectStatement(p);
            const arena_rows = try executeSelect(db, p.allocator, parsed);
            const long_rows = try dupeRowsToLongLived(db.allocator, arena_rows);
            return .{ .select = long_rows };
        },
        .keyword_values => {
            const arena_rows = try stmt_mod.parseValuesStatement(p);
            const long_rows = try dupeRowsToLongLived(db.allocator, arena_rows);
            return .{ .values = long_rows };
        },
        .keyword_create => {
            const parsed = try stmt_mod.parseCreateTableStatement(p);
            try db.registerTable(parsed);
            return .create_table;
        },
        .keyword_insert => {
            const parsed = try stmt_mod.parseInsertStatement(p);
            const rowcount = try executeInsert(db, parsed);
            return .{ .insert = .{ .rowcount = rowcount } };
        },
        else => return Error.SyntaxError,
    }
}

/// Run a parsed SELECT against `db` state. Result rows are allocated in
/// `alloc` (the per-statement arena); `dupeRowsToLongLived` later moves them
/// to long-lived memory.
fn executeSelect(db: *Database, alloc: std.mem.Allocator, ps: stmt_mod.ParsedSelect) ![][]Value {
    if (ps.from) |from| switch (from) {
        .inline_values => |iv| return select_mod.executeWithFrom(alloc, ps.items, iv.rows, iv.columns, ps.where),
        .table_ref => |name| {
            const t = try lookupTable(db, alloc, name);
            return select_mod.executeWithFrom(alloc, ps.items, t.rows.items, t.columns, ps.where);
        },
    };
    if (select_mod.containsStar(ps.items)) return Error.SyntaxError;
    return select_mod.executeWithoutFrom(alloc, ps.items, ps.where);
}

/// Append rows from a parsed INSERT into the target table. The Values in
/// `parsed.rows` live in arena memory; this helper deep-dupes each Value to
/// `db.allocator` before storing in the table.
fn executeInsert(db: *Database, parsed: stmt_mod.ParsedInsert) !u64 {
    const t = try lookupTable(db, db.allocator, parsed.table);
    for (parsed.rows) |row| {
        if (row.len != t.columns.len) return Error.ColumnCountMismatch;
    }
    try t.rows.ensureUnusedCapacity(db.allocator, parsed.rows.len);
    var inserted: u64 = 0;
    errdefer {
        // Roll back any rows already appended in this call. (Schema state
        // changes are durable but row-level partial writes shouldn't survive
        // the all-or-nothing error contract — undo the partial append here.)
        var i: u64 = 0;
        while (i < inserted) : (i += 1) {
            const last_idx = t.rows.items.len - 1;
            const undone_row = t.rows.items[last_idx];
            t.rows.items.len = last_idx;
            for (undone_row) |v| ops.freeValue(db.allocator, v);
            db.allocator.free(undone_row);
        }
    }
    for (parsed.rows) |row| {
        const new_row = try db.allocator.alloc(Value, row.len);
        var k: usize = 0;
        errdefer {
            for (new_row[0..k]) |v| ops.freeValue(db.allocator, v);
            db.allocator.free(new_row);
        }
        while (k < row.len) : (k += 1) {
            new_row[k] = try dupeValueDeep(db.allocator, row[k]);
        }
        t.rows.appendAssumeCapacity(new_row);
        inserted += 1;
    }
    return inserted;
}

/// Look up a table by user-supplied (possibly mixed-case) name. `scratch` is
/// used for the temporary lower-cased key buffer.
fn lookupTable(db: *Database, scratch: std.mem.Allocator, name: []const u8) !*Table {
    const lower = try lowerCaseDupe(scratch, name);
    defer scratch.free(lower);
    return db.tables.getPtr(lower) orelse Error.NoSuchTable;
}

fn lowerCaseDupe(allocator: std.mem.Allocator, src: []const u8) ![]u8 {
    const buf = try allocator.alloc(u8, src.len);
    for (src, buf) |c, *out| out.* = std.ascii.toLower(c);
    return buf;
}

/// Deep-copy `rows` from arena-backed memory into `long`. Each TEXT/BLOB
/// payload is duped; INTEGER/REAL/NULL copy by value. After this call the
/// arena can be torn down without affecting the returned slices.
fn dupeRowsToLongLived(long: std.mem.Allocator, rows: [][]Value) ![][]Value {
    const out = try long.alloc([]Value, rows.len);
    var produced: usize = 0;
    errdefer {
        for (out[0..produced]) |row| {
            for (row) |v| ops.freeValue(long, v);
            long.free(row);
        }
        long.free(out);
    }
    while (produced < rows.len) : (produced += 1) {
        const src = rows[produced];
        const new_row = try long.alloc(Value, src.len);
        var k: usize = 0;
        errdefer {
            for (new_row[0..k]) |v| ops.freeValue(long, v);
            long.free(new_row);
        }
        while (k < src.len) : (k += 1) {
            new_row[k] = try dupeValueDeep(long, src[k]);
        }
        out[produced] = new_row;
    }
    return out;
}

fn dupeValueDeep(allocator: std.mem.Allocator, v: Value) !Value {
    return switch (v) {
        .text => |t| Value{ .text = try allocator.dupe(u8, t) },
        .blob => |b| Value{ .blob = try allocator.dupe(u8, b) },
        else => v,
    };
}

fn freeRows(allocator: std.mem.Allocator, rows: [][]Value) void {
    for (rows) |row| {
        for (row) |v| ops.freeValue(allocator, v);
        allocator.free(row);
    }
    allocator.free(rows);
}

test "Database.execute: single SELECT" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("SELECT 1");
    defer er.deinit();
    try std.testing.expectEqual(@as(usize, 1), er.statements.len);
    const rows = er.statements[0].select;
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(i64, 1), rows[0][0].integer);
}

test "Database.execute: SELECT 1; SELECT 2 — two statements" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("SELECT 1; SELECT 2");
    defer er.deinit();
    try std.testing.expectEqual(@as(usize, 2), er.statements.len);
    try std.testing.expectEqual(@as(i64, 1), er.statements[0].select[0][0].integer);
    try std.testing.expectEqual(@as(i64, 2), er.statements[1].select[0][0].integer);
}

test "Database.execute: VALUES then SELECT" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("VALUES (1, 'a'); SELECT 99");
    defer er.deinit();
    try std.testing.expectEqual(@as(usize, 2), er.statements.len);
    try std.testing.expectEqualStrings("a", er.statements[0].values[0][1].text);
    try std.testing.expectEqual(@as(i64, 99), er.statements[1].select[0][0].integer);
}

test "Database.execute: empty input → no statements" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("");
    defer er.deinit();
    try std.testing.expectEqual(@as(usize, 0), er.statements.len);
}

test "Database.execute: empty stmt between (;;)" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("SELECT 1;; SELECT 2");
    defer er.deinit();
    try std.testing.expectEqual(@as(usize, 2), er.statements.len);
}

test "Database.execute: trailing semicolon" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("SELECT 1;");
    defer er.deinit();
    try std.testing.expectEqual(@as(usize, 1), er.statements.len);
}

test "Database.execute: leading semicolon" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    var er = try db.execute(";SELECT 1");
    defer er.deinit();
    try std.testing.expectEqual(@as(usize, 1), er.statements.len);
}

test "Database.execute: error frees partial results (all-or-nothing)" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    try std.testing.expectError(Error.UnknownFunction, db.execute("SELECT 1; SELECT garbage_function()"));
    try std.testing.expectError(Error.SyntaxError, db.execute("SELECT 1; INVALID_KEYWORD"));
}

test "Database.execute: text result survives arena teardown" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("SELECT 'hello'; SELECT 'world'");
    defer er.deinit();
    try std.testing.expectEqualStrings("hello", er.statements[0].select[0][0].text);
    try std.testing.expectEqualStrings("world", er.statements[1].select[0][0].text);
}

test "Database.execute: instances are independent" {
    const allocator = std.testing.allocator;
    var db1 = Database.init(allocator);
    defer db1.deinit();
    var db2 = Database.init(allocator);
    defer db2.deinit();
    var er1 = try db1.execute("SELECT 1");
    defer er1.deinit();
    var er2 = try db2.execute("SELECT 2");
    defer er2.deinit();
    try std.testing.expectEqual(@as(i64, 1), er1.statements[0].select[0][0].integer);
    try std.testing.expectEqual(@as(i64, 2), er2.statements[0].select[0][0].integer);
}

test "Database.execute: CREATE TABLE registers schema, no rows" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("CREATE TABLE t(x, y)");
    defer er.deinit();
    try std.testing.expectEqual(@as(usize, 1), er.statements.len);
    try std.testing.expect(er.statements[0] == .create_table);
    try std.testing.expect(db.tables.contains("t"));
    const t = db.tables.get("t").?;
    try std.testing.expectEqual(@as(usize, 2), t.columns.len);
    try std.testing.expectEqualStrings("x", t.columns[0]);
    try std.testing.expectEqualStrings("y", t.columns[1]);
}

test "Database.execute: CREATE TABLE with type annotations" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("CREATE TABLE users(id INTEGER, name TEXT NOT NULL)");
    defer er.deinit();
    const t = db.tables.get("users").?;
    try std.testing.expectEqualStrings("id", t.columns[0]);
    try std.testing.expectEqualStrings("name", t.columns[1]);
}

test "Database.execute: CREATE TABLE name and column case-insensitive" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("CREATE TABLE T(X, Y)");
    defer er.deinit();
    try std.testing.expect(db.tables.contains("t"));
    const t = db.tables.get("t").?;
    try std.testing.expectEqualStrings("x", t.columns[0]);
}

test "Database.execute: duplicate CREATE TABLE → TableAlreadyExists" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    try std.testing.expectError(Error.TableAlreadyExists, db.execute("CREATE TABLE t(x); CREATE TABLE t(y)"));
    // First CREATE was rolled back as part of all-or-nothing? No — schema
    // changes aren't rolled back; the table from the first stmt persists on
    // self even though the ExecResult is discarded. ADR-0003 §1 covers
    // statement-level ExecResult rollback, not Database state. Verify:
    try std.testing.expect(db.tables.contains("t"));
}

test "Database.execute: CREATE then SELECT in one batch" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("CREATE TABLE t(x); SELECT 42");
    defer er.deinit();
    try std.testing.expectEqual(@as(usize, 2), er.statements.len);
    try std.testing.expect(er.statements[0] == .create_table);
    try std.testing.expectEqual(@as(i64, 42), er.statements[1].select[0][0].integer);
}

test "Database.execute: CREATE persists across execute calls" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    var er1 = try db.execute("CREATE TABLE t(x)");
    er1.deinit();
    var er2 = try db.execute("SELECT 1");
    defer er2.deinit();
    try std.testing.expect(db.tables.contains("t"));
}

test "Database.execute: INSERT + SELECT * FROM t roundtrip with TEXT" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("CREATE TABLE t(x, y); INSERT INTO t VALUES (1, 'hello'); SELECT * FROM t");
    defer er.deinit();
    try std.testing.expectEqual(@as(usize, 3), er.statements.len);
    try std.testing.expectEqual(@as(u64, 1), er.statements[1].insert.rowcount);
    const rows = er.statements[2].select;
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(i64, 1), rows[0][0].integer);
    try std.testing.expectEqualStrings("hello", rows[0][1].text);
}

test "Database.execute: INSERT into nonexistent table → NoSuchTable" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    try std.testing.expectError(Error.NoSuchTable, db.execute("INSERT INTO nope VALUES (1)"));
}

test "Database.execute: INSERT arity mismatch → ColumnCountMismatch" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    try std.testing.expectError(Error.ColumnCountMismatch, db.execute("CREATE TABLE t(x); INSERT INTO t VALUES (1, 2)"));
}
