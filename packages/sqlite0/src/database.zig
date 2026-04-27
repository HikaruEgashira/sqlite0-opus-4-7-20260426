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
const stmt_mod = @import("stmt.zig");
const parser_mod = @import("parser.zig");
const engine = @import("engine.zig");
const pager_mod = @import("pager.zig");

const Value = value_mod.Value;
pub const Error = ops.Error;

/// One executed statement's outcome. Row-producing statements carry their
/// rows; the rest carry no rows (their effect is on `Database` state).
/// `insert` reports the rowcount for symmetry with sqlite3 but the CLI
/// doesn't print it (sqlite3 stays silent too). `.transaction` covers
/// BEGIN / COMMIT / ROLLBACK uniformly — sqlite3 prints nothing for
/// those, and the kind is recoverable from the SQL text if a future
/// caller needs it.
pub const StatementResult = union(enum) {
    select: [][]Value,
    values: [][]Value,
    create_table,
    insert: struct { rowcount: u64 },
    delete: struct { rowcount: u64 },
    update: struct { rowcount: u64 },
    transaction,

    pub fn deinit(self: StatementResult, allocator: std.mem.Allocator) void {
        switch (self) {
            .select, .values => |rows| freeRows(allocator, rows),
            .create_table, .insert, .delete, .update, .transaction => {},
        }
    }
};

/// Table backed by either the in-memory `rows` ArrayList (CREATE TABLE
/// path, in-memory Database) or by a Pager-resident B-tree at
/// `root_page`. Both worlds carry the same column-name slice; the cursor
/// implementation forks on `root_page == 0`.
///
/// All strings (name keys, column names) and `Value` payloads inside
/// `rows` are owned by `Database.allocator`. `root_page` is just a
/// number — the actual page bytes belong to the Pager (Iter25.B/C,
/// ADR-0005 §2). ADR-0003 §2: no type affinity tracking in Phase 2.
pub const Table = struct {
    columns: [][]const u8,
    /// Iter29.B — parallel to `columns`. `true` at index `i` means the
    /// column was declared `NOT NULL`; INSERT/UPDATE must reject NULL
    /// at that column (post-IPK auto-assign — IPK columns whose NULL
    /// is replaced by chooseRowid pass the check). Always the same
    /// length as `columns`; allocated by registerTable / synthetic
    /// registration; freed in `deinit`.
    not_null: []bool,
    rows: std.ArrayListUnmanaged([]Value) = .empty,
    /// Non-zero when this table lives in a Pager-backed sqlite3 .db
    /// file. 0 means in-memory (CREATE TABLE on a memory Database).
    /// `engine_from.resolveSource` forks on this value.
    root_page: u32 = 0,
    /// Index into `columns` of the INTEGER PRIMARY KEY column that
    /// aliases the rowid (Iter28). Null when the table has no IPK.
    /// At most one IPK per table — sqlite3 enforces this at parse time;
    /// `registerTable` mirrors the rule with `Error.SyntaxError`.
    ipk_column: ?usize = null,
    /// Iter29.A — true for engine-managed tables that the SQL surface
    /// must NOT mutate via INSERT/UPDATE/DELETE. Currently set on the
    /// synthetic `sqlite_schema` / `sqlite_master` aliases registered
    /// against root_page=1; without the flag a user `INSERT INTO
    /// sqlite_schema` would silently corrupt page 1 (sqlite3 rejects
    /// with "table sqlite_master may not be modified" — we mirror).
    is_system: bool = false,
    /// Iter29.S — monotonic rowid sequence for in-memory non-IPK tables.
    /// Bumped per row by `executeInsert`; the bumped value becomes
    /// `db.last_insert_rowid` after a successful INSERT. Unused for
    /// IPK tables (the IPK column value is the rowid) and for
    /// file-mode tables (rowid comes from B-tree leaf cell metadata).
    /// Not decremented on DELETE — sqlite3 doesn't reuse rowids by
    /// default (max+1 selection). 0 = no rows ever inserted.
    next_implicit_rowid: i64 = 0,

    pub fn deinit(self: *Table, allocator: std.mem.Allocator) void {
        for (self.rows.items) |row| {
            for (row) |v| ops.freeValue(allocator, v);
            allocator.free(row);
        }
        self.rows.deinit(allocator);
        for (self.columns) |c| allocator.free(c);
        allocator.free(self.columns);
        allocator.free(self.not_null);
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

/// Re-export from `journal.zig` so callers (and the existing
/// `database_journal_mode_test.zig` API) can spell it `database.JournalMode`.
pub const JournalMode = @import("journal.zig").JournalMode;

/// Iter27.F — one entry on the savepoint stack. `name` is owned by
/// `db.allocator` (duped from the parser's source slice). `mark` owns
/// per-page snapshots of `pager.staged_frames` at savepoint time, so
/// ROLLBACK TO can restore the staging buffer byte-for-byte (without
/// a snapshot we'd lose pre-savepoint writes that were overwritten by
/// post-savepoint same-page writes — `pager_wal.writeFrame` mutates
/// the existing snapshot in place when restaging the same page).
/// `is_implicit_tx` is true when this savepoint was the one that
/// flipped `in_transaction` from false to true; RELEASE of an implicit
/// outermost savepoint clears `in_transaction` so the loop hook flushes
/// the batch (matches sqlite3's "RELEASE outermost savepoint commits
/// the implicit tx" behaviour).
pub const Savepoint = struct {
    name: []u8,
    mark: @import("pager_wal.zig").SavepointMark,
    is_implicit_tx: bool,
};

pub const Database = struct {
    allocator: std.mem.Allocator,
    tables: std.StringHashMapUnmanaged(Table) = .{},
    /// Open Pager when this Database was constructed via `openFile`. The
    /// flock travels with Database lifetime: `deinit` releases it. Null
    /// for the in-memory CREATE TABLE path. Tables with non-zero
    /// `root_page` reference pages owned by this pager.
    pager: ?pager_mod.Pager = null,
    /// Journal mode read from the file header at `openFile`. `delete_legacy`
    /// for in-memory Databases (the field is unused there but the default
    /// matches sqlite3's legacy behaviour). Iter27.B will use this to gate
    /// write paths; Iter27.0.a only records it.
    journal_mode: JournalMode = .delete_legacy,
    /// Iter27.E — set by BEGIN, cleared by COMMIT/ROLLBACK. While true,
    /// the per-statement commit hook in `execute` is suppressed so
    /// every staged frame from intervening DML accumulates into one
    /// commit batch (or is dropped as a unit on ROLLBACK). Implicit
    /// rollback on `deinit` discards staged frames via `pager.close`.
    in_transaction: bool = false,
    /// Iter29.S — connection-wide last-INSERT rowid (mirrors sqlite3's
    /// `last_insert_rowid()` API). Updated by `executeInsert` /
    /// `executeInsertFile` after a successful INSERT to the rowid of the
    /// last inserted row. Initial value 0 (sqlite3 quirk: returns 0 on
    /// a fresh connection before any INSERT). Not touched by DELETE /
    /// UPDATE / CREATE TABLE / SELECT.
    last_insert_rowid: i64 = 0,
    /// Iter29.S — rowcount of the most recent DML statement (sqlite3
    /// `changes()`). Set by INSERT/UPDATE/DELETE dispatch in `engine`.
    /// Reset to 0 on connection open. Spec quirk: SELECT/PRAGMA do NOT
    /// touch this — the previous DML's value persists across reads.
    last_changes: i64 = 0,
    /// Iter29.S — cumulative rowcount across all completed DML on this
    /// connection (sqlite3 `total_changes()`). Monotonically increasing.
    total_changes: i64 = 0,
    /// Iter27.F — savepoint stack (LIFO). Entries are pushed by
    /// SAVEPOINT, popped by RELEASE / COMMIT / ROLLBACK / by ROLLBACK
    /// TO (which keeps the named entry but pops everything above it).
    /// Implicit `deinit` cleanup mirrors `pager.close`'s implicit
    /// rollback: free name + mark for every entry. Empty in the common
    /// path (no SAVEPOINT issued).
    savepoints: std.ArrayListUnmanaged(Savepoint) = .empty,

    pub fn init(allocator: std.mem.Allocator) Database {
        return .{ .allocator = allocator };
    }

    /// Open a sqlite3 .db file. Acquires the Pager's exclusive flock
    /// for the lifetime of the Database, reads the journal-mode bits
    /// from the page-1 header (Iter27.0.a / ADR-0007 §1.5), runs
    /// hot-journal recovery if a `<path>-journal` sidecar exists in
    /// delete_legacy mode (Iter27.0.b), then scans `sqlite_schema` to
    /// populate `tables` with `root_page` set. `deinit` releases the
    /// flock.
    pub fn openFile(allocator: std.mem.Allocator, path: []const u8) !Database {
        const p = try pager_mod.Pager.open(allocator, path);
        var db: Database = .{ .allocator = allocator, .pager = p };
        errdefer db.deinit();

        const journal = @import("journal.zig");
        db.journal_mode = try journal.detectJournalMode(&db.pager.?);
        if (db.journal_mode == .delete_legacy) {
            try journal.maybeRunRecovery(&db.pager.?, path);
        } else if (db.journal_mode == .wal) {
            // Iter27.A read-side scan + Iter27.B.2 writer attach. See
            // `pager_wal.openWal` for the create-vs-inherit branch.
            try @import("pager_wal.zig").openWal(&db.pager.?, path);
        }

        const schema = @import("schema.zig");
        try schema.loadFromPager(&db, &db.pager.?);
        return db;
    }

    pub fn deinit(self: *Database) void {
        // Savepoint marks must drop their snapshot pages BEFORE
        // `pager.close()` tears down the allocator-backed staging arena.
        // The stack is empty after a successful COMMIT/ROLLBACK; this
        // path matters only for implicit-rollback-on-close.
        const pager_wal = @import("pager_wal.zig");
        for (self.savepoints.items) |*sp| {
            if (self.pager) |*pg| pager_wal.freeSavepointMark(pg, &sp.mark);
            self.allocator.free(sp.name);
        }
        self.savepoints.deinit(self.allocator);
        var it = self.tables.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.tables.deinit(self.allocator);
        if (self.pager) |*p| p.close();
    }

    /// Register a new empty table. Returns `Error.TableAlreadyExists` if a
    /// table with the (case-insensitive) name is already in `tables`, or
    /// `Error.DuplicateColumnName` if `parsed.columns` contains the same
    /// (case-insensitive) name twice — sqlite3 rejects `CREATE TABLE t(a, A)`
    /// at parse time with "duplicate column name". Both `name` and `cols`
    /// (and each entry of `cols`) are taken into ownership by `self` on
    /// success and freed at `deinit`. On failure ownership of the inputs
    /// returns to the caller (this function frees nothing it didn't allocate
    /// itself).
    pub fn registerTable(self: *Database, parsed: stmt_mod.ParsedCreateTable) !void {
        const key = try lowerCaseDupe(self.allocator, parsed.name);
        errdefer self.allocator.free(key);

        if (self.tables.contains(key)) return Error.TableAlreadyExists;

        const cols = try self.allocator.alloc([]const u8, parsed.columns.len);
        var produced: usize = 0;
        errdefer {
            for (cols[0..produced]) |c| self.allocator.free(c);
            self.allocator.free(cols);
        }
        const not_null = try self.allocator.alloc(bool, parsed.columns.len);
        errdefer self.allocator.free(not_null);
        var ipk: ?usize = null;
        while (produced < parsed.columns.len) : (produced += 1) {
            const src = parsed.columns[produced];
            const lowered = try lowerCaseDupe(self.allocator, src.name);
            // Reject duplicates (case-insensitive). Compare against earlier
            // entries which are already lower-cased; the lowered slice we
            // just created is freed via the errdefer chain on the surrounding
            // allocation if the error escapes.
            for (cols[0..produced]) |existing| {
                if (std.mem.eql(u8, existing, lowered)) {
                    self.allocator.free(lowered);
                    return Error.DuplicateColumnName;
                }
            }
            if (src.is_ipk) {
                if (ipk != null) {
                    self.allocator.free(lowered);
                    return Error.SyntaxError; // sqlite3: "table has more than one primary key"
                }
                ipk = produced;
            }
            cols[produced] = lowered;
            not_null[produced] = src.is_not_null;
        }

        try self.tables.put(self.allocator, key, .{
            .columns = cols,
            .not_null = not_null,
            .ipk_column = ipk,
        });
    }

    /// Run `registerTable`'s pre-mutation invariants (table-name conflict,
    /// duplicate column names, multiple INTEGER PRIMARY KEY) WITHOUT
    /// allocating anything that escapes the function. Used by
    /// `engine_ddl_file.executeCreateTableFile` to reject invalid CREATE
    /// TABLE statements BEFORE any pager mutation runs — the on-disk DDL
    /// path otherwise allocates a root page, writes its header, and
    /// appends a sqlite_schema row before reaching `registerTable`,
    /// leaving page 1 with an orphan schema entry that sqlite3 then
    /// refuses to open ("malformed database schema"). Mirrors the same
    /// errors `registerTable` would return on the same inputs.
    pub fn validateNewTable(self: *Database, parsed: stmt_mod.ParsedCreateTable) Error!void {
        const key = try lowerCaseDupe(self.allocator, parsed.name);
        defer self.allocator.free(key);
        if (self.tables.contains(key)) return Error.TableAlreadyExists;

        // Multi-IPK check first — pure scan, no allocations to unwind on
        // the failure path.
        var ipk_count: usize = 0;
        for (parsed.columns) |c| {
            if (c.is_ipk) {
                ipk_count += 1;
                if (ipk_count > 1) return Error.SyntaxError;
            }
        }

        // Duplicate-column-name check — needs case-insensitive
        // comparison so allocate a lowered copy per column. The defer
        // frees only fully-stored slots (produced exclusive); on
        // mid-iteration reject we free the just-allocated `lcol`
        // explicitly before returning.
        var lowered = try self.allocator.alloc([]u8, parsed.columns.len);
        var produced: usize = 0;
        defer {
            for (lowered[0..produced]) |l| self.allocator.free(l);
            self.allocator.free(lowered);
        }
        while (produced < parsed.columns.len) : (produced += 1) {
            const lcol = try lowerCaseDupe(self.allocator, parsed.columns[produced].name);
            for (lowered[0..produced]) |existing| {
                if (std.mem.eql(u8, existing, lcol)) {
                    self.allocator.free(lcol);
                    return Error.DuplicateColumnName;
                }
            }
            lowered[produced] = lcol;
        }
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
            const sr = try engine.dispatchOne(self, &p);
            try statements.append(self.allocator, sr);
            // Iter27.B.3 + Iter27.E — implicit per-statement commit
            // boundary, gated by `in_transaction`. Inside a BEGIN block
            // the hook is suppressed; the explicit COMMIT statement
            // clears `in_transaction` BEFORE returning, so the same hook
            // call that runs on the COMMIT statement itself flushes the
            // entire accumulated batch. SELECTs queue no frames so this
            // is a no-op for them.
            if (!self.in_transaction) {
                if (self.pager) |*pg| try pg.commit();
            }
            if (p.cur.kind == .semicolon) p.advance();
        }
        return .{
            .statements = try statements.toOwnedSlice(self.allocator),
            .allocator = self.allocator,
        };
    }
};

pub fn lowerCaseDupe(allocator: std.mem.Allocator, src: []const u8) ![]u8 {
    const buf = try allocator.alloc(u8, src.len);
    for (src, buf) |c, *out| out.* = std.ascii.toLower(c);
    return buf;
}

fn freeRows(allocator: std.mem.Allocator, rows: [][]Value) void {
    for (rows) |row| {
        for (row) |v| ops.freeValue(allocator, v);
        allocator.free(row);
    }
    allocator.free(rows);
}
