//! Iter29.A unit tests for `engine_ddl_file.executeCreateTableFile`'s
//! pre-mutation validation. Without `Database.validateNewTable` running
//! before any pager call, a rejected CREATE TABLE leaves page 1 with an
//! orphan `sqlite_schema` row (because the pager already allocated a
//! root page and `appendSchemaRow` already wrote the entry by the time
//! `registerTable` rejects). sqlite3 then refuses to open the file with
//! "malformed database schema". Each test below opens a fresh file-mode
//! db, captures page 1, attempts a CREATE that must reject, and asserts
//! page 1 is byte-identical afterwards.

const std = @import("std");
const database = @import("database.zig");
const pager_mod = @import("pager.zig");
const test_db_util = @import("test_db_util.zig");

const Database = database.Database;
const Error = database.Error;

const PAGE_SIZE = pager_mod.PAGE_SIZE;

/// Minimal valid sqlite3 page-1 buffer with empty sqlite_schema. Mirrors
/// the helper in database_journal_mode_test.zig but pinned to
/// delete_legacy mode so `openFile` doesn't need a -wal sidecar.
fn buildEmptyPage1() [PAGE_SIZE]u8 {
    var buf: [PAGE_SIZE]u8 = @splat(0);
    @memcpy(buf[0..16], "SQLite format 3\x00");
    buf[16] = 0x10;
    buf[17] = 0x00; // page_size = 4096
    buf[18] = 1; // write_format = legacy (delete journal)
    buf[19] = 1;
    buf[20] = 0;
    buf[21] = 64;
    buf[22] = 32;
    buf[23] = 32;
    buf[27] = 1;
    buf[31] = 1; // dbsize = 1
    buf[100] = 0x0d;
    buf[105] = (PAGE_SIZE >> 8) & 0xff;
    buf[106] = PAGE_SIZE & 0xff;
    return buf;
}

fn openFreshFileWithUserTable(allocator: std.mem.Allocator, path: []const u8) !Database {
    const buf = buildEmptyPage1();
    try test_db_util.writePages(path, &.{&buf});
    var db = try Database.openFile(allocator, path);
    errdefer db.deinit();
    var er = try db.execute("CREATE TABLE u(x);");
    er.deinit();
    return db;
}

fn page1Snapshot(allocator: std.mem.Allocator, db: *Database) ![]u8 {
    const pg = &db.pager.?;
    const slice = try pg.getPage(1);
    return allocator.dupe(u8, slice);
}

fn assertRejectLeavesPage1Unchanged(
    allocator: std.mem.Allocator,
    suffix: []const u8,
    sql: []const u8,
    expected_err: Error,
) !void {
    const path = try test_db_util.makeTempPath(suffix);
    defer allocator.free(path);
    defer test_db_util.unlinkPath(path);

    var db = try openFreshFileWithUserTable(allocator, path);
    defer db.deinit();

    const before = try page1Snapshot(allocator, &db);
    defer allocator.free(before);

    try std.testing.expectError(expected_err, db.execute(sql));

    const after_borrowed = try db.pager.?.getPage(1);
    try std.testing.expectEqualSlices(u8, before, after_borrowed);
}

test "executeCreateTableFile: duplicate table name → page 1 unchanged" {
    try assertRejectLeavesPage1Unchanged(
        std.testing.allocator,
        "ddl-reject-dup-name",
        "CREATE TABLE u(y);",
        Error.TableAlreadyExists,
    );
}

test "executeCreateTableFile: sqlite_schema name conflict → page 1 unchanged" {
    try assertRejectLeavesPage1Unchanged(
        std.testing.allocator,
        "ddl-reject-sqlite-schema",
        "CREATE TABLE sqlite_schema(x);",
        Error.TableAlreadyExists,
    );
}

test "executeCreateTableFile: duplicate column name → page 1 unchanged" {
    try assertRejectLeavesPage1Unchanged(
        std.testing.allocator,
        "ddl-reject-dup-col",
        "CREATE TABLE v(a, A);",
        Error.DuplicateColumnName,
    );
}

test "executeCreateTableFile: multiple INTEGER PRIMARY KEY → page 1 unchanged" {
    try assertRejectLeavesPage1Unchanged(
        std.testing.allocator,
        "ddl-reject-multi-ipk",
        "CREATE TABLE w(a INTEGER PRIMARY KEY, b INTEGER PRIMARY KEY);",
        Error.SyntaxError,
    );
}
