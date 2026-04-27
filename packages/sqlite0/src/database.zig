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

/// Re-export from `journal.zig` so callers (and the existing
/// `database_journal_mode_test.zig` API) can spell it `database.JournalMode`.
pub const JournalMode = @import("journal.zig").JournalMode;

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
        }

        try self.tables.put(self.allocator, key, .{ .columns = cols, .ipk_column = ipk });
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
