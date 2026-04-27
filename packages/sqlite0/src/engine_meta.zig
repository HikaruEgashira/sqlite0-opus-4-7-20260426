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

/// Iter27.E — handle BEGIN / COMMIT / ROLLBACK on `db`.
///   - BEGIN  : require !in_transaction (sqlite3 forbids nested);
///              flip `in_transaction` so the execute() loop suppresses
///              its per-statement commit hook for subsequent statements.
///   - COMMIT : require in_transaction; clear the flag and let the
///              loop's hook flush staged frames (one batch).
///   - ROLLBACK: require in_transaction; tell the pager to discard
///              staged frames AND evict their cached pages (otherwise
///              subsequent reads would see post-mutation in-memory
///              bytes left by writeFrame's write-through). Clear the
///              flag last so the loop hook still fires; with
///              staged_frames now empty, `pager.commit` is a no-op.
pub fn executeTransaction(db: *Database, kind: stmt_mod.TransactionKind) !StatementResult {
    switch (kind) {
        .begin => {
            if (db.in_transaction) return Error.SyntaxError;
            db.in_transaction = true;
        },
        .commit => {
            if (!db.in_transaction) return Error.SyntaxError;
            db.in_transaction = false;
        },
        .rollback => {
            if (!db.in_transaction) return Error.SyntaxError;
            if (db.pager) |*pg| @import("pager_wal.zig").rollback(pg);
            db.in_transaction = false;
        },
    }
    return .transaction;
}

/// Iter27.C / Iter27.E — supported pragmas:
///   - `PRAGMA wal_checkpoint [(MODE)]` (Iter27.C) → single 3-int row
///     `(busy, log, ckpt)` matching sqlite3's shape.
///   - `PRAGMA journal_mode` (Iter27.E, read-only) → single 1-text row
///     reporting the journal mode the file opened in (`wal` or
///     `delete`); in-memory databases report `memory`. Setting the mode
///     (`PRAGMA journal_mode = WAL`) is deferred — the on-disk format
///     conversion belongs in a later iteration alongside vacuum.
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
    return Error.SyntaxError;
}
