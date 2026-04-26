//! Per-statement execution engine. Splits out from `database.zig` so that
//! `Database` (state container + dispatch loop) and the actual SELECT/INSERT
//! execution paths can grow independently.
//!
//! `dispatchOne` owns the per-statement `ArenaAllocator` lifecycle and the
//! arena-to-long-lived dupe boundary (ADR-0003 §8). The matching
//! `dupeRowsToLongLived` helper is the single point where TEXT/BLOB bytes
//! cross from arena memory into `db.allocator` ownership.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const stmt_mod = @import("stmt.zig");
const parser_mod = @import("parser.zig");
const select_mod = @import("select.zig");
const database = @import("database.zig");

const Value = value_mod.Value;
const Database = database.Database;
const Table = database.Table;
const StatementResult = database.StatementResult;
const Error = ops.Error;

/// Execute one statement against `db` using a per-statement arena. The arena
/// holds AST nodes and intermediate row buffers (TEXT/BLOB included); the
/// returned `StatementResult` is deep-duped to `db.allocator` before the
/// arena tears down.
pub fn dispatchOne(db: *Database, p: *parser_mod.Parser) !StatementResult {
    var arena = std.heap.ArenaAllocator.init(db.allocator);
    defer arena.deinit();
    const arena_alloc = arena.allocator();

    const saved = p.allocator;
    p.allocator = arena_alloc;
    defer p.allocator = saved;

    switch (p.cur.kind) {
        .keyword_select => {
            const parsed = try stmt_mod.parseSelectStatement(p);
            const arena_rows = try executeSelect(db, p.allocator, parsed);
            const long_rows = try dupeRowsToLongLived(db.allocator, arena_rows);
            return .{ .select = long_rows };
        },
        .keyword_values => {
            const arena_rows = try stmt_mod.parseValuesStatement(p);
            const long_rows = try dupeRowsToLongLived(db.allocator, arena_rows);
            return .{ .values = long_rows };
        },
        .keyword_create => {
            const parsed = try stmt_mod.parseCreateTableStatement(p);
            try db.registerTable(parsed);
            return .create_table;
        },
        .keyword_insert => {
            const parsed = try stmt_mod.parseInsertStatement(p);
            const rowcount = try executeInsert(db, parsed);
            return .{ .insert = .{ .rowcount = rowcount } };
        },
        else => return Error.SyntaxError,
    }
}

/// Run a parsed SELECT against `db` state. Result rows are allocated in
/// `alloc` (the per-statement arena); `dupeRowsToLongLived` later moves them
/// to long-lived memory.
fn executeSelect(db: *Database, alloc: std.mem.Allocator, ps: stmt_mod.ParsedSelect) ![][]Value {
    if (ps.from) |from| switch (from) {
        .inline_values => |iv| return select_mod.executeWithFrom(alloc, ps.items, iv.rows, iv.columns, ps.where),
        .table_ref => |name| {
            const t = try lookupTable(db, alloc, name);
            return select_mod.executeWithFrom(alloc, ps.items, t.rows.items, t.columns, ps.where);
        },
    };
    if (select_mod.containsStar(ps.items)) return Error.SyntaxError;
    return select_mod.executeWithoutFrom(alloc, ps.items, ps.where);
}

/// Append rows from a parsed INSERT into the target table. The Values in
/// `parsed.rows` live in arena memory; this helper deep-dupes each Value to
/// `db.allocator` before storing in the table.
fn executeInsert(db: *Database, parsed: stmt_mod.ParsedInsert) !u64 {
    const t = try lookupTable(db, db.allocator, parsed.table);
    for (parsed.rows) |row| {
        if (row.len != t.columns.len) return Error.ColumnCountMismatch;
    }
    try t.rows.ensureUnusedCapacity(db.allocator, parsed.rows.len);
    var inserted: u64 = 0;
    errdefer {
        // Roll back any rows already appended in this call. (Schema state
        // changes are durable but row-level partial writes shouldn't survive
        // the all-or-nothing error contract — undo the partial append here.)
        var i: u64 = 0;
        while (i < inserted) : (i += 1) {
            const last_idx = t.rows.items.len - 1;
            const undone_row = t.rows.items[last_idx];
            t.rows.items.len = last_idx;
            for (undone_row) |v| ops.freeValue(db.allocator, v);
            db.allocator.free(undone_row);
        }
    }
    for (parsed.rows) |row| {
        const new_row = try db.allocator.alloc(Value, row.len);
        var k: usize = 0;
        errdefer {
            for (new_row[0..k]) |v| ops.freeValue(db.allocator, v);
            db.allocator.free(new_row);
        }
        while (k < row.len) : (k += 1) {
            new_row[k] = try dupeValueDeep(db.allocator, row[k]);
        }
        t.rows.appendAssumeCapacity(new_row);
        inserted += 1;
    }
    return inserted;
}

/// Look up a table by user-supplied (possibly mixed-case) name. `scratch` is
/// used for the temporary lower-cased key buffer.
fn lookupTable(db: *Database, scratch: std.mem.Allocator, name: []const u8) !*Table {
    const lower = try database.lowerCaseDupe(scratch, name);
    defer scratch.free(lower);
    return db.tables.getPtr(lower) orelse Error.NoSuchTable;
}

/// Deep-copy `rows` from arena-backed memory into `long`. Each TEXT/BLOB
/// payload is duped; INTEGER/REAL/NULL copy by value. After this call the
/// arena can be torn down without affecting the returned slices.
fn dupeRowsToLongLived(long: std.mem.Allocator, rows: [][]Value) ![][]Value {
    const out = try long.alloc([]Value, rows.len);
    var produced: usize = 0;
    errdefer {
        for (out[0..produced]) |row| {
            for (row) |v| ops.freeValue(long, v);
            long.free(row);
        }
        long.free(out);
    }
    while (produced < rows.len) : (produced += 1) {
        const src = rows[produced];
        const new_row = try long.alloc(Value, src.len);
        var k: usize = 0;
        errdefer {
            for (new_row[0..k]) |v| ops.freeValue(long, v);
            long.free(new_row);
        }
        while (k < src.len) : (k += 1) {
            new_row[k] = try dupeValueDeep(long, src[k]);
        }
        out[produced] = new_row;
    }
    return out;
}

fn dupeValueDeep(allocator: std.mem.Allocator, v: Value) !Value {
    return switch (v) {
        .text => |t| Value{ .text = try allocator.dupe(u8, t) },
        .blob => |b| Value{ .blob = try allocator.dupe(u8, b) },
        else => v,
    };
}
