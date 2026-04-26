//! Single-statement execution wrapper retained for legacy callers (unit
//! tests, library consumers that haven't moved to `Database`). Implemented
//! as a thin shim over `Database.execute` per ADR-0003 §1: input must be
//! exactly one statement (a multi-statement string raises `SyntaxError`).
//! New code should use `Database` directly.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const database = @import("database.zig");

const Value = value_mod.Value;

pub const Error = ops.Error;

pub const Row = struct {
    values: []Value,

    pub fn deinit(self: *Row, allocator: std.mem.Allocator) void {
        for (self.values) |v| ops.freeValue(allocator, v);
        allocator.free(self.values);
    }
};

pub const Result = struct {
    rows: []Row,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *Result) void {
        for (self.rows) |*row| row.deinit(self.allocator);
        self.allocator.free(self.rows);
    }
};

pub fn execute(allocator: std.mem.Allocator, sql: []const u8) !Result {
    var db = database.Database.init(allocator);
    defer db.deinit();

    var er = try db.execute(sql);
    if (er.statements.len != 1) {
        er.deinit();
        return Error.SyntaxError;
    }

    // Steal the rows from the single StatementResult, then let er.deinit free
    // the (now empty-payload) outer slice. Non-row statements (CREATE TABLE)
    // produce no rows; the resulting Result has zero entries.
    const raw_rows: [][]Value = switch (er.statements[0]) {
        .select, .values => |r| r,
        .create_table, .insert, .delete, .update => &.{},
    };
    er.statements[0] = .{ .select = &.{} };
    er.deinit();

    errdefer {
        for (raw_rows) |row| {
            for (row) |v| ops.freeValue(allocator, v);
            allocator.free(row);
        }
        allocator.free(raw_rows);
    }
    var rows = try allocator.alloc(Row, raw_rows.len);
    for (raw_rows, 0..) |row, i| rows[i] = .{ .values = row };
    allocator.free(raw_rows);
    return .{ .rows = rows, .allocator = allocator };
}

test "execute: SELECT 1" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT 1");
    defer r.deinit();
    try std.testing.expectEqual(@as(usize, 1), r.rows.len);
    try std.testing.expectEqual(@as(usize, 1), r.rows[0].values.len);
    try std.testing.expectEqual(@as(i64, 1), r.rows[0].values[0].integer);
}

test "execute: SELECT 1+2*3" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT 1+2*3");
    defer r.deinit();
    try std.testing.expectEqual(@as(i64, 7), r.rows[0].values[0].integer);
}

test "execute: SELECT NULL" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT NULL");
    defer r.deinit();
    try std.testing.expectEqual(Value.null, r.rows[0].values[0]);
}

test "execute: SELECT 'it''s'" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT 'it''s'");
    defer r.deinit();
    try std.testing.expectEqualStrings("it's", r.rows[0].values[0].text);
}

test "execute: division by zero is NULL" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT 1/0");
    defer r.deinit();
    try std.testing.expectEqual(Value.null, r.rows[0].values[0]);
}

test "execute: NOT NULL is NULL" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT NOT NULL");
    defer r.deinit();
    try std.testing.expectEqual(Value.null, r.rows[0].values[0]);
}

test "execute: BETWEEN inclusive endpoints" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT 1 BETWEEN 1 AND 10, 10 BETWEEN 1 AND 10");
    defer r.deinit();
    try std.testing.expectEqual(@as(i64, 1), r.rows[0].values[0].integer);
    try std.testing.expectEqual(@as(i64, 1), r.rows[0].values[1].integer);
}

test "execute: NOT BETWEEN" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT 0 NOT BETWEEN 1 AND 10");
    defer r.deinit();
    try std.testing.expectEqual(@as(i64, 1), r.rows[0].values[0].integer);
}

test "execute: IN list" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT 2 IN (1, 2, 3), 4 IN (1, 2, 3)");
    defer r.deinit();
    try std.testing.expectEqual(@as(i64, 1), r.rows[0].values[0].integer);
    try std.testing.expectEqual(@as(i64, 0), r.rows[0].values[1].integer);
}

test "execute: NULL IS DISTINCT FROM NULL is 0" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT NULL IS DISTINCT FROM NULL");
    defer r.deinit();
    try std.testing.expectEqual(@as(i64, 0), r.rows[0].values[0].integer);
}

test "execute: simple CASE" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT CASE 2 WHEN 1 THEN 'a' WHEN 2 THEN 'b' END");
    defer r.deinit();
    try std.testing.expectEqualStrings("b", r.rows[0].values[0].text);
}

test "execute: searched CASE no match no ELSE returns NULL" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT CASE WHEN 0 THEN 'a' END");
    defer r.deinit();
    try std.testing.expectEqual(Value.null, r.rows[0].values[0]);
}

test "execute: CASE inside arithmetic" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT 1 + CASE WHEN 1=1 THEN 10 ELSE 0 END");
    defer r.deinit();
    try std.testing.expectEqual(@as(i64, 11), r.rows[0].values[0].integer);
}

test "execute: VALUES emits one row" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "VALUES (1, 'a')");
    defer r.deinit();
    try std.testing.expectEqual(@as(usize, 1), r.rows.len);
    try std.testing.expectEqual(@as(i64, 1), r.rows[0].values[0].integer);
    try std.testing.expectEqualStrings("a", r.rows[0].values[1].text);
}

test "execute: VALUES multi-row" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    defer r.deinit();
    try std.testing.expectEqual(@as(usize, 3), r.rows.len);
    try std.testing.expectEqual(@as(i64, 2), r.rows[1].values[0].integer);
    try std.testing.expectEqualStrings("c", r.rows[2].values[1].text);
}

test "execute: VALUES with mismatched arity is SyntaxError" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(Error.SyntaxError, execute(allocator, "VALUES (1, 2), (3)"));
}
