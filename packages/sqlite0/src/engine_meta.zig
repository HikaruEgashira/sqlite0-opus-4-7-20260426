//! "Meta" statement execution — PRAGMA + transaction control. Split
//! out of `engine.zig` (Iter27.E) so adding more PRAGMAs or savepoint
//! semantics doesn't push the dispatch file over the 500-line
//! discipline. The `dispatchOne` switch in `engine.zig` calls in here
//! for `keyword_pragma` / `keyword_begin` / `keyword_commit` /
//! `keyword_rollback`.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const stmt_mod = @import("stmt.zig");
const database = @import("database.zig");
const func_util = @import("func_util.zig");

const Value = value_mod.Value;
const Database = database.Database;
const StatementResult = database.StatementResult;
const Error = ops.Error;

/// Iter27.E + Iter27.F — handle the full transaction-control surface:
/// BEGIN / COMMIT / ROLLBACK + SAVEPOINT / RELEASE / ROLLBACK TO.
///
///   - BEGIN  : require !in_transaction (sqlite3 forbids nested);
///              flip `in_transaction` so the execute() loop suppresses
///              its per-statement commit hook for subsequent statements.
///   - COMMIT : require in_transaction; clear the flag and let the
///              loop's hook flush staged frames (one batch). Discards
///              every savepoint frame — they're subsumed by the COMMIT.
///   - ROLLBACK: require in_transaction; tell the pager to discard
///              staged frames AND evict their cached pages (otherwise
///              subsequent reads would see post-mutation in-memory
///              bytes left by writeFrame's write-through). Drop every
///              savepoint frame, then clear the flag. With staged_frames
///              now empty, `pager.commit` is a no-op.
///   - SAVEPOINT name: snapshot `staged_frames` and push onto the stack.
///              When issued outside a transaction, sqlite3 implicitly
///              opens one; we mirror that by setting `in_transaction`
///              and tagging the savepoint as `is_implicit_tx` so a
///              matching RELEASE can close the implicit tx.
///   - RELEASE name: pop the named savepoint and every more-recently-
///              pushed savepoint above it. If the released savepoint
///              was the implicit-tx one and the stack is now empty,
///              clear `in_transaction` so the loop hook flushes the
///              accumulated batch (matches sqlite3's "RELEASE outermost
///              implicit savepoint commits" rule).
///   - ROLLBACK TO name: pop savepoints above (but KEEP the named one),
///              then restore `staged_frames` from the named frame's
///              snapshot. Leaves `in_transaction` untouched — the
///              transaction continues. Per sqlite3, the named savepoint
///              survives so a subsequent ROLLBACK TO with the same name
///              works.
///
/// In-memory Database (`pager == null`): SAVEPOINT/RELEASE/ROLLBACK TO
/// return UnsupportedFeature. They have no rollback-able layer to
/// snapshot. BEGIN/COMMIT/ROLLBACK on in-memory remain parse-only as
/// documented in the Iter27.E in-memory tx caveat.
pub fn executeTxControl(db: *Database, ctrl: stmt_mod.TxControl) !StatementResult {
    const pager_wal = @import("pager_wal.zig");
    switch (ctrl) {
        .begin => {
            if (db.in_transaction) return Error.SyntaxError;
            db.in_transaction = true;
        },
        .commit => {
            if (!db.in_transaction) return Error.SyntaxError;
            popAllSavepoints(db);
            db.in_transaction = false;
        },
        .rollback => {
            if (!db.in_transaction) return Error.SyntaxError;
            popAllSavepoints(db);
            if (db.pager) |*pg| pager_wal.rollback(pg);
            db.in_transaction = false;
        },
        .savepoint => |name| {
            if (db.pager == null) return Error.UnsupportedFeature;
            const pg = &db.pager.?;
            const is_implicit = !db.in_transaction;
            var mark = try pager_wal.createSavepointMark(pg);
            errdefer pager_wal.freeSavepointMark(pg, &mark);
            const name_copy = try db.allocator.dupe(u8, name);
            errdefer db.allocator.free(name_copy);
            try db.savepoints.append(db.allocator, .{
                .name = name_copy,
                .mark = mark,
                .is_implicit_tx = is_implicit,
            });
            if (is_implicit) db.in_transaction = true;
        },
        .release => |name| {
            const idx = findSavepointIndex(db, name) orelse return Error.SyntaxError;
            const closes_implicit_tx = db.savepoints.items[idx].is_implicit_tx;
            popSavepointsFrom(db, idx);
            if (closes_implicit_tx and db.savepoints.items.len == 0) {
                db.in_transaction = false;
            }
        },
        .rollback_to => |name| {
            const idx = findSavepointIndex(db, name) orelse return Error.SyntaxError;
            // Pop savepoints above the named one (keep the named one
            // itself per sqlite3 semantics: a subsequent ROLLBACK TO
            // with the same name must still find it).
            popSavepointsFrom(db, idx + 1);
            if (db.pager) |*pg| try pager_wal.rollbackToMark(pg, &db.savepoints.items[idx].mark);
        },
    }
    return .transaction;
}

fn findSavepointIndex(db: *Database, name: []const u8) ?usize {
    var i: usize = db.savepoints.items.len;
    while (i > 0) : (i -= 1) {
        if (func_util.eqlIgnoreCase(db.savepoints.items[i - 1].name, name)) return i - 1;
    }
    return null;
}

fn popSavepointsFrom(db: *Database, start: usize) void {
    const pager_wal = @import("pager_wal.zig");
    var i: usize = db.savepoints.items.len;
    while (i > start) : (i -= 1) {
        var sp = &db.savepoints.items[i - 1];
        if (db.pager) |*pg| pager_wal.freeSavepointMark(pg, &sp.mark);
        db.allocator.free(sp.name);
    }
    db.savepoints.shrinkRetainingCapacity(start);
}

fn popAllSavepoints(db: *Database) void {
    popSavepointsFrom(db, 0);
}

/// Iter27.C / Iter27.E / Iter31.W — supported pragmas:
///   - `PRAGMA wal_checkpoint [(MODE)]` (Iter27.C) → single 3-int row
///     `(busy, log, ckpt)` matching sqlite3's shape.
///   - `PRAGMA journal_mode` (Iter27.E, read-only) → single 1-text row
///     reporting the journal mode the file opened in (`wal` or
///     `delete`); in-memory databases report `memory`. Setting the mode
///     (`PRAGMA journal_mode = WAL`) is deferred — the on-disk format
///     conversion belongs in a later iteration alongside vacuum.
///   - Iter31.W read-only constants matching sqlite3 defaults for
///     freshly-created databases:
///       - `encoding` → `UTF-8`
///       - `user_version` / `application_id` / `foreign_keys` /
///         `auto_vacuum` / `temp_store` / `recursive_triggers` /
///         `defer_foreign_keys` → 0 (mutating these is deferred)
///       - `page_size` → 4096 (sqlite0 hard-coded constant)
/// Unknown pragmas return SyntaxError — silently swallowing them would
/// mask compatibility regressions.
pub fn executePragma(db: *Database, parsed: stmt_mod.ParsedPragma) !StatementResult {
    const pager_wal = @import("pager_wal.zig");
    if (func_util.eqlIgnoreCase(parsed.name, "journal_mode")) {
        if (parsed.arg != null) return Error.UnsupportedFeature;
        const mode_text: []const u8 = if (db.pager == null)
            "memory"
        else switch (db.journal_mode) {
            .wal => "wal",
            .delete_legacy => "delete",
        };
        const text_copy = try db.allocator.dupe(u8, mode_text);
        const row = try db.allocator.alloc(Value, 1);
        row[0] = .{ .text = text_copy };
        const rows = try db.allocator.alloc([]Value, 1);
        rows[0] = row;
        return .{ .select = rows };
    }
    if (func_util.eqlIgnoreCase(parsed.name, "wal_checkpoint")) {
        const mode: pager_wal.CheckpointMode = if (parsed.arg) |a|
            (if (func_util.eqlIgnoreCase(a, "truncate") or func_util.eqlIgnoreCase(a, "restart") or func_util.eqlIgnoreCase(a, "full"))
                .truncate
            else if (func_util.eqlIgnoreCase(a, "passive"))
                .passive
            else
                return Error.SyntaxError)
        else
            .passive;
        // No pager → in-memory database. sqlite3 reports `0|-1|-1` for
        // any pragma wal_checkpoint on a non-WAL connection; mirror it.
        const result: pager_wal.CheckpointResult = if (db.pager) |*pg|
            try pager_wal.checkpoint(pg, mode)
        else
            .{ .busy = 0, .log = -1, .ckpt = -1 };
        const row = try db.allocator.alloc(Value, 3);
        row[0] = .{ .integer = result.busy };
        row[1] = .{ .integer = result.log };
        row[2] = .{ .integer = result.ckpt };
        const rows = try db.allocator.alloc([]Value, 1);
        rows[0] = row;
        return .{ .select = rows };
    }
    // Iter31.W single-int read-only PRAGMAs returning sqlite3's
    // freshly-created-database defaults. Setting these values
    // (`PRAGMA name = N`) is rejected — the parser doesn't yet
    // accept the `=` form, so callers reach `parsePragmaStatement`
    // only for the bare read shape.
    if (intReadOnlyPragmaValue(parsed.name)) |v| {
        if (parsed.arg != null) return Error.UnsupportedFeature;
        return singleIntegerSelect(db.allocator, v);
    }
    if (func_util.eqlIgnoreCase(parsed.name, "encoding")) {
        if (parsed.arg != null) return Error.UnsupportedFeature;
        return singleTextSelect(db.allocator, "UTF-8");
    }
    // Iter31.X: unknown PRAGMA silently returns no rows. Matches sqlite3
    // (`PRAGMA totally_made_up_name;` rc=0, no output) — many tools at
    // connection setup time issue PRAGMAs that may not exist on a given
    // build, and erroring on those would block legitimate clients.
    return .{ .select = &.{} };
}

fn intReadOnlyPragmaValue(name: []const u8) ?i64 {
    const eq = func_util.eqlIgnoreCase;
    if (eq(name, "page_size")) return 4096;
    if (eq(name, "user_version")) return 0;
    if (eq(name, "application_id")) return 0;
    if (eq(name, "foreign_keys")) return 0;
    if (eq(name, "auto_vacuum")) return 0;
    if (eq(name, "temp_store")) return 0;
    if (eq(name, "recursive_triggers")) return 0;
    if (eq(name, "defer_foreign_keys")) return 0;
    return null;
}

fn singleIntegerSelect(allocator: std.mem.Allocator, v: i64) !StatementResult {
    const row = try allocator.alloc(Value, 1);
    row[0] = .{ .integer = v };
    const rows = try allocator.alloc([]Value, 1);
    rows[0] = row;
    return .{ .select = rows };
}

fn singleTextSelect(allocator: std.mem.Allocator, text: []const u8) !StatementResult {
    const row = try allocator.alloc(Value, 1);
    row[0] = .{ .text = try allocator.dupe(u8, text) };
    const rows = try allocator.alloc([]Value, 1);
    rows[0] = row;
    return .{ .select = rows };
}
