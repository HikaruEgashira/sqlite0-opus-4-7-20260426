//! `Database` — state-holding execution context (ADR-0003).
//!
//! Single source of truth for tables and other persistent state. Phase 2 will
//! grow an in-memory `tables` map; Iter14.A only delivers the multi-statement
//! execution shell so the dispatch / arena / Value-dupe boundaries are in
//! place before storage lands.
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
const stmt_mod = @import("stmt.zig");
const parser_mod = @import("parser.zig");

const Value = value_mod.Value;
pub const Error = ops.Error;

/// One executed statement's outcome. Phase 2 only emits row-producing kinds
/// (`select` / `values`); CREATE / INSERT join in Iter14.B/C with their own
/// variants (no rows, just side effects).
pub const StatementResult = union(enum) {
    select: [][]Value,
    values: [][]Value,

    pub fn deinit(self: StatementResult, allocator: std.mem.Allocator) void {
        switch (self) {
            .select, .values => |rows| freeRows(allocator, rows),
        }
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

    pub fn init(allocator: std.mem.Allocator) Database {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Database) void {
        _ = self;
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
            const arena_rows = try stmt_mod.parseSelectStatement(p);
            const long_rows = try dupeRowsToLongLived(db.allocator, arena_rows);
            return .{ .select = long_rows };
        },
        .keyword_values => {
            const arena_rows = try stmt_mod.parseValuesStatement(p);
            const long_rows = try dupeRowsToLongLived(db.allocator, arena_rows);
            return .{ .values = long_rows };
        },
        else => return Error.SyntaxError,
    }
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
