//! Integration tests for `Database.execute` — a battery of small
//! end-to-end shapes covering single SELECT, multi-statement batches,
//! CREATE TABLE registration, INSERT round-trips, and error
//! propagation. Carved out of `database.zig` (Iter28-pre-E split) to
//! keep the implementation file under the 500-line discipline ahead
//! of the BEGIN/COMMIT/ROLLBACK additions.

const std = @import("std");
const database = @import("database.zig");
const ops = @import("ops.zig");

const Database = database.Database;
const Error = database.Error;

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

test "Database.execute: CREATE TABLE duplicate column → DuplicateColumnName" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    try std.testing.expectError(Error.DuplicateColumnName, db.execute("CREATE TABLE t(a, a, b)"));
    // Failed CREATE must not leave a partial table behind.
    try std.testing.expect(!db.tables.contains("t"));
}

test "Database.execute: CREATE TABLE duplicate column case-insensitive" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    try std.testing.expectError(Error.DuplicateColumnName, db.execute("CREATE TABLE t(name TEXT, NAME INTEGER)"));
}

test "Database.execute: SELECT alias parses (AS form)" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("SELECT 1+2 AS sum");
    defer er.deinit();
    try std.testing.expectEqual(@as(i64, 3), er.statements[0].select[0][0].integer);
}

test "Database.execute: SELECT bare alias parses" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("SELECT 1 foo");
    defer er.deinit();
    try std.testing.expectEqual(@as(i64, 1), er.statements[0].select[0][0].integer);
}

test "Database.execute: alias preserved across FROM and WHERE" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("CREATE TABLE t(x); INSERT INTO t VALUES (5), (10); SELECT x AS n FROM t WHERE x > 5");
    defer er.deinit();
    const rows = er.statements[2].select;
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(i64, 10), rows[0][0].integer);
}

// Iter31.AJ regression — `registerTable` runs the CHECK re-parse before
// transferring `lowered` ownership to `cols[produced]`, so a parse
// failure must not leak the just-allocated lowered name. The testing
// allocator panics on any leak.
test "Database.execute: CHECK with bad expr fails cleanly without leaking" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    try std.testing.expectError(Error.SyntaxError, db.execute("CREATE TABLE t(x CHECK(@@@))"));
}

test "Database.execute: CHECK truthy on INSERT round-trips the row" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    var er = try db.execute("CREATE TABLE t(x CHECK(x > 0)); INSERT INTO t VALUES(5); SELECT * FROM t");
    defer er.deinit();
    try std.testing.expectEqual(@as(i64, 5), er.statements[2].select[0][0].integer);
}

test "Database.execute: CHECK falsy rejects with ConstraintCheck" {
    const allocator = std.testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();
    try std.testing.expectError(
        Error.ConstraintCheck,
        db.execute("CREATE TABLE t(x CHECK(x > 0)); INSERT INTO t VALUES(-1)"),
    );
}
