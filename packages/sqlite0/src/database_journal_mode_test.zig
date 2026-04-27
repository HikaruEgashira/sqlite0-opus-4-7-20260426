//! Iter27.0.a unit tests for `Database.openFile` journal-mode detection.
//! Lives in a sibling file because each test builds an in-memory page-1
//! buffer + writes it via `test_db_util.writePages`, taking ~30 lines per
//! case — pushing `database.zig` past the 500-line discipline. The fixture
//! shape is sqlite3 file format §1.6 minimum (header + empty leaf-table
//! sqlite_schema at offset 100); enough for `openFile` to succeed and for
//! the journal-mode bytes to be exercised.

const std = @import("std");
const database = @import("database.zig");
const pager_mod = @import("pager.zig");
const test_db_util = @import("test_db_util.zig");

const Database = database.Database;
const JournalMode = database.JournalMode;
const Error = database.Error;

const PAGE_SIZE = pager_mod.PAGE_SIZE;

/// Build a minimal valid page-1 buffer with caller-chosen journal-mode
/// bytes (write_format / read_format at offsets 18/19). Empty
/// sqlite_schema, dbsize = 1.
fn buildPage1(write_format: u8, read_format: u8) [PAGE_SIZE]u8 {
    var buf: [PAGE_SIZE]u8 = @splat(0);
    @memcpy(buf[0..16], "SQLite format 3\x00");
    buf[16] = 0x10;
    buf[17] = 0x00; // page_size = 4096 (BE)
    buf[18] = write_format;
    buf[19] = read_format;
    buf[20] = 0; // reserved space
    buf[21] = 64;
    buf[22] = 32;
    buf[23] = 32;
    buf[27] = 1; // change counter low byte
    buf[31] = 1; // dbsize = 1 (BE u32 in [28..32])
    // Empty leaf-table sqlite_schema at offset 100.
    buf[100] = 0x0d;
    buf[105] = (PAGE_SIZE >> 8) & 0xff;
    buf[106] = PAGE_SIZE & 0xff;
    return buf;
}

test "Database.openFile: existing DELETE-legacy fixture detected as .delete_legacy" {
    const path = try test_db_util.makeTempPath("jm-delete");
    defer std.testing.allocator.free(path);
    defer test_db_util.unlinkPath(path);

    const buf = buildPage1(1, 1);
    try test_db_util.writePages(path, &.{&buf});

    var db = try Database.openFile(std.testing.allocator, path);
    defer db.deinit();
    try std.testing.expectEqual(JournalMode.delete_legacy, db.journal_mode);
}

test "Database.openFile: WAL-format header detected as .wal" {
    const path = try test_db_util.makeTempPath("jm-wal");
    defer std.testing.allocator.free(path);
    defer test_db_util.unlinkPath(path);

    const buf = buildPage1(2, 2);
    try test_db_util.writePages(path, &.{&buf});

    var db = try Database.openFile(std.testing.allocator, path);
    defer db.deinit();
    try std.testing.expectEqual(JournalMode.wal, db.journal_mode);
}

test "Database.openFile: unknown journal-mode bytes → IoError" {
    const path = try test_db_util.makeTempPath("jm-bad");
    defer std.testing.allocator.free(path);
    defer test_db_util.unlinkPath(path);

    const buf = buildPage1(9, 9);
    try test_db_util.writePages(path, &.{&buf});

    try std.testing.expectError(Error.IoError, Database.openFile(std.testing.allocator, path));
}

test "Database.openFile: write_format != read_format → IoError" {
    // sqlite3 always writes the pair identically; mismatched values are
    // file corruption per §1.6.
    const path = try test_db_util.makeTempPath("jm-mismatch");
    defer std.testing.allocator.free(path);
    defer test_db_util.unlinkPath(path);

    const buf = buildPage1(1, 2);
    try test_db_util.writePages(path, &.{&buf});

    try std.testing.expectError(Error.IoError, Database.openFile(std.testing.allocator, path));
}
