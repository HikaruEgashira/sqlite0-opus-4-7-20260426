//! `sqlite_schema` scanner (Iter25.B.5/C, ADR-0005 §4).
//!
//! Walks page 1 of a Pager-backed sqlite3 .db file (which is the root
//! of `sqlite_schema`), decodes each row's 5 columns
//! `(type, name, tbl_name, rootpage, sql)`, and registers every
//! `type='table'` entry into `Database.tables` with `root_page` set to
//! the value sqlite3 stored. Indexes / views / triggers are skipped.
//!
//! ## File-header sanity check
//!
//! Page 1 starts with the 16-byte ASCII string "SQLite format 3\0".
//! We verify this before attempting any B-tree parse so a non-sqlite3
//! file (or a corrupted header) fails with a clear `IoError` rather
//! than a misleading "invalid page type" error from the parser.
//!
//! ## sqlite_schema column layout
//!
//! Per <https://www.sqlite.org/schematab.html>:
//!   col 0: type     TEXT  ('table'|'index'|'view'|'trigger')
//!   col 1: name     TEXT
//!   col 2: tbl_name TEXT
//!   col 3: rootpage INTEGER (0 for VIEW/TRIGGER)
//!   col 4: sql      TEXT  (the original CREATE statement)

const std = @import("std");
const ops = @import("ops.zig");
const value_mod = @import("value.zig");
const database = @import("database.zig");
const pager_mod = @import("pager.zig");
const btree = @import("btree.zig");
const btree_walk = @import("btree_walk.zig");
const record = @import("record.zig");
const stmt_ddl = @import("stmt_ddl.zig");
const parser_mod = @import("parser.zig");

pub const Error = ops.Error;

pub const SQLITE_FILE_HEADER: []const u8 = "SQLite format 3\x00";

/// Validate the 16-byte magic at offset 0 of page 1.
pub fn validateFileHeader(page1: []const u8) Error!void {
    if (page1.len < SQLITE_FILE_HEADER.len) return Error.IoError;
    if (!std.mem.eql(u8, page1[0..SQLITE_FILE_HEADER.len], SQLITE_FILE_HEADER)) {
        return Error.IoError;
    }
}

/// Walk sqlite_schema (rooted at page 1) and register every CREATE
/// TABLE entry into `db.tables` with `root_page` populated. The Pager
/// remains owned by the caller — we just borrow it for the scan.
///
/// Skips type='index'/'view'/'trigger' silently. Iter25 doesn't support
/// indexes; views/triggers are deferred indefinitely.
pub fn loadFromPager(db: *database.Database, p: *pager_mod.Pager) Error!void {
    // Pre-flight: confirm this is a sqlite3 file at all.
    const page1 = try p.getPage(1);
    try validateFileHeader(page1);

    // Per-statement-style scratch arena for parsing. The duped strings
    // we hand to `registerTable` get re-duped into `db.allocator`, so
    // `arena` can drop everything when this function returns.
    var arena = std.heap.ArenaAllocator.init(db.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var walker = btree_walk.TableLeafWalker.init(a, p, 1);
    defer walker.deinit();

    while (try walker.next()) |leaf| {
        const cells = try btree.parseLeafTablePage(
            a,
            leaf.bytes,
            leaf.header_offset,
            pager_mod.PAGE_SIZE,
        );
        for (cells) |cell| try registerCell(db, a, cell.record_bytes);
    }
}

fn registerCell(
    db: *database.Database,
    arena: std.mem.Allocator,
    record_bytes: []const u8,
) Error!void {
    const cols = try record.decodeRecord(arena, record_bytes);
    if (cols.len < 5) return; // malformed sqlite_schema row — skip silently
    if (cols[0] != .text) return;
    if (!std.mem.eql(u8, cols[0].text, "table")) return; // skip indexes/views/triggers
    if (cols[3] != .integer) return Error.IoError;
    if (cols[4] != .text) return Error.IoError;

    const root_page: u32 = blk: {
        const r = cols[3].integer;
        if (r <= 0) return Error.IoError;
        break :blk @intCast(r);
    };
    const sql = cols[4].text;

    // Some internal tables (sqlite_sequence) carry a CREATE statement
    // sqlite3 understands but our parser doesn't fully — be tolerant
    // and skip ones that fail to parse. A real failure surfaces when
    // a SELECT references the unparseable table.
    var p2 = parser_mod.Parser.init(arena, sql);
    const parsed = stmt_ddl.parseCreateTableStatement(&p2) catch return;

    db.registerTable(parsed) catch |err| switch (err) {
        // Duplicate registration shouldn't happen on a sqlite3 file
        // (each table appears once in sqlite_schema); treat it as IO
        // corruption so we don't silently skip a structural problem.
        Error.TableAlreadyExists => return Error.IoError,
        else => return err,
    };

    // After `registerTable` we look up the just-registered Table and
    // patch root_page. Lookup uses lower-cased name (matches the
    // hashmap key).
    const lower = try database.lowerCaseDupe(arena, parsed.name);
    if (db.tables.getPtr(lower)) |t| {
        t.root_page = root_page;
    } else {
        // Should be unreachable — registerTable just put it there.
        return Error.IoError;
    }
}

// -- tests --

const testing = std.testing;
const test_util = @import("btree_test_util.zig");
const PAGE_SIZE = pager_mod.PAGE_SIZE;

var sch_test_counter: std.atomic.Value(u32) = .init(0);

fn makeTempPath(suffix: []const u8) ![]u8 {
    const tmpdir_raw = std.c.getenv("TMPDIR");
    const tmpdir_slice: []const u8 = if (tmpdir_raw) |x| std.mem.span(@as([*:0]const u8, x)) else "/tmp";
    const trimmed = std.mem.trimEnd(u8, tmpdir_slice, "/");
    const pid = std.c.getpid();
    const seq = sch_test_counter.fetchAdd(1, .seq_cst);
    return std.fmt.allocPrint(testing.allocator, "{s}/sqlite0-schema-test-{d}-{d}-{s}.db", .{ trimmed, pid, seq, suffix });
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

/// Build a hand-rolled record matching sqlite_schema's column layout.
/// All five columns are TEXT/INTEGER for our test purposes.
fn buildSchemaRecord(
    alloc: std.mem.Allocator,
    typ: []const u8,
    name: []const u8,
    tbl_name: []const u8,
    rootpage: i64,
    sql: []const u8,
) ![]u8 {
    // Serial types:
    //   TEXT length L → 13 + 2L (must be odd ≥ 13)
    //   INTEGER 1-byte signed → 1 (covers rootpage values 1..127)
    const t_st: u64 = @intCast(13 + 2 * typ.len);
    const n_st: u64 = @intCast(13 + 2 * name.len);
    const tn_st: u64 = @intCast(13 + 2 * tbl_name.len);
    const rp_st: u64 = 1; // 1-byte int
    const sql_st: u64 = @intCast(13 + 2 * sql.len);

    // Header = header_len varint + 5 serial-type varints.
    // First measure each varint length.
    var tmp: [9]u8 = undefined;
    const t_n = record.encodeVarint(t_st, &tmp);
    const n_n = record.encodeVarint(n_st, &tmp);
    const tn_n = record.encodeVarint(tn_st, &tmp);
    const rp_n = record.encodeVarint(rp_st, &tmp);
    const sql_n = record.encodeVarint(sql_st, &tmp);

    // header_len includes itself; iterate to fixed point (max 9 bytes
    // for header_len varint).
    var hdr_len: usize = 1 + t_n + n_n + tn_n + rp_n + sql_n;
    var hl_n = record.encodeVarint(hdr_len, &tmp);
    while (1 + hl_n + (hdr_len - 1) != hl_n + (hdr_len - 1) + 1) {
        hdr_len = hl_n + t_n + n_n + tn_n + rp_n + sql_n;
        hl_n = record.encodeVarint(hdr_len, &tmp);
    }
    hdr_len = hl_n + t_n + n_n + tn_n + rp_n + sql_n;

    const body_len = typ.len + name.len + tbl_name.len + 1 + sql.len;
    const total = hdr_len + body_len;
    const buf = try alloc.alloc(u8, total);

    var pos: usize = 0;
    pos += record.encodeVarint(hdr_len, buf[pos..]);
    pos += record.encodeVarint(t_st, buf[pos..]);
    pos += record.encodeVarint(n_st, buf[pos..]);
    pos += record.encodeVarint(tn_st, buf[pos..]);
    pos += record.encodeVarint(rp_st, buf[pos..]);
    pos += record.encodeVarint(sql_st, buf[pos..]);
    @memcpy(buf[pos .. pos + typ.len], typ);
    pos += typ.len;
    @memcpy(buf[pos .. pos + name.len], name);
    pos += name.len;
    @memcpy(buf[pos .. pos + tbl_name.len], tbl_name);
    pos += tbl_name.len;
    buf[pos] = @intCast(rootpage & 0xff);
    pos += 1;
    @memcpy(buf[pos .. pos + sql.len], sql);
    pos += sql.len;
    std.debug.assert(pos == total);
    return buf;
}

test "validateFileHeader: rejects garbage" {
    var page1: [16]u8 = undefined;
    @memset(&page1, 0xff);
    try testing.expectError(Error.IoError, validateFileHeader(&page1));
}

test "validateFileHeader: accepts magic" {
    var page1 = [_]u8{0} ** 16;
    @memcpy(&page1, SQLITE_FILE_HEADER);
    try validateFileHeader(&page1);
}

test "loadFromPager: registers a CREATE TABLE entry with root_page" {
    const path = try makeTempPath("loadbasic");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    // Build sqlite_schema row for: CREATE TABLE t(a, b), rootpage=2
    const rec = try buildSchemaRecord(
        testing.allocator,
        "table",
        "t",
        "t",
        2,
        "CREATE TABLE t(a, b)",
    );
    defer testing.allocator.free(rec);

    const inputs = [_]test_util.TestCellInput{.{ .rowid = 1, .record = rec }};
    const page1 = try test_util.buildLeafTablePage(testing.allocator, PAGE_SIZE, 100, &inputs);
    defer testing.allocator.free(page1);

    // Stamp the file header magic.
    @memcpy(page1[0..SQLITE_FILE_HEADER.len], SQLITE_FILE_HEADER);

    // page 2 doesn't matter for this test — we don't query it.
    const blank2 = try testing.allocator.alloc(u8, PAGE_SIZE);
    defer testing.allocator.free(blank2);
    @memset(blank2, 0);

    try writePages(path, &[_][]const u8{ page1, blank2 });

    var p = try pager_mod.Pager.open(testing.allocator, path);
    defer p.close();

    var db = database.Database.init(testing.allocator);
    defer db.deinit();
    try loadFromPager(&db, &p);

    const t = db.tables.getPtr("t").?;
    try testing.expectEqual(@as(usize, 2), t.columns.len);
    try testing.expectEqualStrings("a", t.columns[0]);
    try testing.expectEqualStrings("b", t.columns[1]);
    try testing.expectEqual(@as(u32, 2), t.root_page);
}

test "loadFromPager: skips type='index' rows" {
    const path = try makeTempPath("skipidx");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    const rec_tbl = try buildSchemaRecord(testing.allocator, "table", "u", "u", 2, "CREATE TABLE u(x)");
    defer testing.allocator.free(rec_tbl);
    const rec_idx = try buildSchemaRecord(testing.allocator, "index", "i", "u", 3, "CREATE INDEX i ON u(x)");
    defer testing.allocator.free(rec_idx);

    const inputs = [_]test_util.TestCellInput{
        .{ .rowid = 1, .record = rec_tbl },
        .{ .rowid = 2, .record = rec_idx },
    };
    const page1 = try test_util.buildLeafTablePage(testing.allocator, PAGE_SIZE, 100, &inputs);
    defer testing.allocator.free(page1);
    @memcpy(page1[0..SQLITE_FILE_HEADER.len], SQLITE_FILE_HEADER);

    const blank = try testing.allocator.alloc(u8, PAGE_SIZE);
    defer testing.allocator.free(blank);
    @memset(blank, 0);

    try writePages(path, &[_][]const u8{ page1, blank, blank });

    var p = try pager_mod.Pager.open(testing.allocator, path);
    defer p.close();

    var db = database.Database.init(testing.allocator);
    defer db.deinit();
    try loadFromPager(&db, &p);

    try testing.expect(db.tables.getPtr("u") != null);
    try testing.expect(db.tables.getPtr("i") == null); // index skipped
    try testing.expectEqual(@as(usize, 1), db.tables.count());
}

test "loadFromPager: rejects non-sqlite3 file" {
    const path = try makeTempPath("garbage");
    defer testing.allocator.free(path);
    defer unlinkPath(path);

    const blank = try testing.allocator.alloc(u8, PAGE_SIZE);
    defer testing.allocator.free(blank);
    @memset(blank, 0xff);
    try writePages(path, &[_][]const u8{blank});

    var p = try pager_mod.Pager.open(testing.allocator, path);
    defer p.close();

    var db = database.Database.init(testing.allocator);
    defer db.deinit();
    try testing.expectError(Error.IoError, loadFromPager(&db, &p));
}
