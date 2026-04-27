//! File-mode DDL: CREATE TABLE on a Pager-backed Database (Iter26.A.3).
//!
//! Sequence per call:
//!   1. Allocate a new page (`Pager.allocatePage` bumps the in-header
//!      dbsize and zero-extends the file).
//!   2. Initialise that page as an empty leaf-table B-tree page
//!      (`btree_insert.rebuildLeafTablePage` with no cells).
//!   3. Append a row to `sqlite_schema` describing the new table
//!      (`schema.appendSchemaRow`).
//!   4. `registerTable` into `db.tables` with `root_page` set.
//!
//! All-or-nothing per ADR-0003 §1: a failure between steps 1 and 4 may
//! leave on-disk state inconsistent (e.g. a fresh page exists with no
//! schema entry pointing at it). sqlite3 itself ignores orphaned pages
//! — they're invisible until VACUUM. The in-memory `db.tables` is only
//! mutated AFTER the disk writes succeed, so a failed CREATE TABLE
//! never produces a phantom in-memory table.
//!
//! Restrictions for Iter26.A.3:
//!   - sqlite_schema (page 1) must have room for the new row in its
//!     contiguous gap. `.page_full` returns `Error.UnsupportedFeature`
//!     so the user knows the limit; Iter26.B will lift it.

const std = @import("std");
const ops = @import("ops.zig");
const database = @import("database.zig");
const stmt_mod = @import("stmt.zig");
const pager_mod = @import("pager.zig");
const btree_insert = @import("btree_insert.zig");
const schema = @import("schema.zig");

pub const Error = ops.Error;

pub fn executeCreateTableFile(db: *database.Database, parsed: stmt_mod.ParsedCreateTable) !void {
    const pager = if (db.pager) |*pp| pp else return Error.IoError;

    // Pre-check: reject invalid CREATE TABLE BEFORE any pager mutation.
    // Without this, allocate/writePage/appendSchemaRow run to completion
    // and only step 4 (`registerTable`) catches the conflict — at which
    // point page 1 already has a second sqlite_schema entry, which
    // sqlite3 then refuses to open with "malformed database schema".
    // Covers all three of registerTable's pre-mutation invariants:
    //   - duplicate table name (e.g. `CREATE TABLE sqlite_schema(x);`)
    //   - duplicate column name (e.g. `CREATE TABLE u(a, A);`)
    //   - multiple INTEGER PRIMARY KEY columns
    try db.validateNewTable(parsed);

    // Step 1: allocate the root page for the new table.
    const root_page = try pager.allocatePage();

    // Step 2: initialise as empty leaf table. allocatePage just wrote
    // zeros; we need a valid B-tree page header. Use a stack-bounded
    // local buffer (PAGE_SIZE = 4096) — small enough to avoid heap
    // pressure for a one-shot init.
    var page_buf: [pager_mod.PAGE_SIZE]u8 = undefined;
    @memset(&page_buf, 0);
    const usable_size = try pager.usableSize();
    const empty_cells: []const btree_insert.RebuildCell = &.{};
    try btree_insert.rebuildLeafTablePage(&page_buf, 0, usable_size, empty_cells);
    try pager.writePage(root_page, &page_buf);

    // Step 3: append the sqlite_schema row pointing at root_page.
    try schema.appendSchemaRow(
        pager,
        "table",
        parsed.name,
        parsed.name, // tbl_name = name for CREATE TABLE
        root_page,
        parsed.source_text,
    );

    // Step 4: register in-memory only after disk side committed.
    try db.registerTable(parsed);

    // Patch the freshly-registered Table.root_page so subsequent
    // SELECT/INSERT/UPDATE/DELETE go through the file-mode cursor.
    const lower = try database.lowerCaseDupe(db.allocator, parsed.name);
    defer db.allocator.free(lower);
    if (db.tables.getPtr(lower)) |t| {
        t.root_page = root_page;
    } else {
        return Error.IoError; // unreachable: registerTable just put it there
    }
}
