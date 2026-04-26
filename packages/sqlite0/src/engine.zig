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
const ast = @import("ast.zig");
const stmt_mod = @import("stmt.zig");
const stmt_dml = @import("stmt_dml.zig");
const parser_mod = @import("parser.zig");
const select_mod = @import("select.zig");
const eval = @import("eval.zig");
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
            const rowcount = try executeInsert(db, p.allocator, parsed);
            return .{ .insert = .{ .rowcount = rowcount } };
        },
        .keyword_delete => {
            const parsed = try stmt_dml.parseDeleteStatement(p);
            const rowcount = try executeDelete(db, p.allocator, parsed);
            return .{ .delete = .{ .rowcount = rowcount } };
        },
        .keyword_update => {
            const parsed = try stmt_dml.parseUpdateStatement(p);
            const rowcount = try executeUpdate(db, p.allocator, parsed);
            return .{ .update = .{ .rowcount = rowcount } };
        },
        else => return Error.SyntaxError,
    }
}

/// Remove rows from `parsed.table` for which the WHERE predicate is truthy
/// (or all rows when WHERE is absent). Returns the count of deleted rows.
/// Mutation is performed by building a survivor list, so partial WHERE
/// failures leave the table unchanged (all-or-nothing — ADR-0003 §1).
fn executeDelete(db: *Database, arena: std.mem.Allocator, parsed: stmt_dml.ParsedDelete) !u64 {
    const t = try lookupTable(db, db.allocator, parsed.table);
    if (parsed.where) |w_ast| {
        var survivors: std.ArrayList([]Value) = .empty;
        errdefer survivors.deinit(arena);
        var to_free: std.ArrayList([]Value) = .empty;
        errdefer to_free.deinit(arena);

        for (t.rows.items) |row| {
            const ctx = eval.EvalContext{
                .allocator = arena,
                .current_row = row,
                .columns = t.columns,
            };
            const cond = try eval.evalExpr(ctx, w_ast);
            defer ops.freeValue(arena, cond);
            if (ops.truthy(cond) orelse false) {
                try to_free.append(arena, row);
            } else {
                try survivors.append(arena, row);
            }
        }

        // Atomic swap: install survivors as the new row list, then free the
        // dropped rows.
        const new_rows = try db.allocator.alloc([]Value, survivors.items.len);
        @memcpy(new_rows, survivors.items);
        const old_storage = t.rows;
        t.rows = .empty;
        try t.rows.ensureTotalCapacity(db.allocator, new_rows.len);
        for (new_rows) |row| t.rows.appendAssumeCapacity(row);
        // Free the storage (capacity buffer); rows themselves are now either
        // referenced by t.rows (survivors) or about to be freed (to_free).
        var os = old_storage;
        os.deinit(db.allocator);
        db.allocator.free(new_rows);

        for (to_free.items) |row| {
            for (row) |v| ops.freeValue(db.allocator, v);
            db.allocator.free(row);
        }
        return to_free.items.len;
    } else {
        // DELETE without WHERE — drop everything.
        const count: u64 = t.rows.items.len;
        for (t.rows.items) |row| {
            for (row) |v| ops.freeValue(db.allocator, v);
            db.allocator.free(row);
        }
        t.rows.clearAndFree(db.allocator);
        return count;
    }
}

/// Apply assignments to rows where WHERE matches. New values are eagerly
/// evaluated into a scratch row, the old values are then freed and the
/// scratch row is moved into place. Errors during evaluation leave the
/// table unchanged for that row (and previously updated rows stay updated —
/// matching sqlite3's per-row UPDATE behavior, not per-statement).
fn executeUpdate(db: *Database, arena: std.mem.Allocator, parsed: stmt_dml.ParsedUpdate) !u64 {
    const t = try lookupTable(db, db.allocator, parsed.table);

    // Resolve column indices once (validation precedes mutation per ADR-0003).
    const indices = try arena.alloc(usize, parsed.assignments.len);
    for (parsed.assignments, indices) |a, *idx| {
        idx.* = findTableColumn(t, a.column) orelse return Error.SyntaxError;
    }

    var changed: u64 = 0;
    for (t.rows.items) |*row_ptr| {
        if (parsed.where) |w_ast| {
            const ctx = eval.EvalContext{
                .allocator = arena,
                .current_row = row_ptr.*,
                .columns = t.columns,
            };
            const cond = try eval.evalExpr(ctx, w_ast);
            defer ops.freeValue(arena, cond);
            if (!(ops.truthy(cond) orelse false)) continue;
        }
        const ctx = eval.EvalContext{
            .allocator = arena,
            .current_row = row_ptr.*,
            .columns = t.columns,
        };
        // Evaluate all RHS values first (in arena), then transfer to long-
        // lived. If any eval fails the row stays untouched.
        const new_values = try arena.alloc(Value, parsed.assignments.len);
        var produced: usize = 0;
        errdefer {
            for (new_values[0..produced]) |v| ops.freeValue(arena, v);
        }
        while (produced < parsed.assignments.len) : (produced += 1) {
            new_values[produced] = try eval.evalExpr(ctx, parsed.assignments[produced].value);
        }
        // All evaluated. Now apply: dupe each new value to db.allocator,
        // free the old value, install the new one. Only commit per assignment
        // pair so an OOM mid-row leaves a consistent (partially updated) row;
        // the cell-level swap still preserves storage-class invariants.
        for (indices, new_values) |col_idx, new_v| {
            const duped = try dupeValueDeep(db.allocator, new_v);
            ops.freeValue(db.allocator, row_ptr.*[col_idx]);
            row_ptr.*[col_idx] = duped;
        }
        for (new_values) |v| ops.freeValue(arena, v);
        changed += 1;
    }
    return changed;
}

/// Run a parsed SELECT against `db` state. Result rows are allocated in
/// `alloc` (the per-statement arena); `dupeRowsToLongLived` later moves them
/// to long-lived memory. Also called by `executeInsert` for `INSERT INTO t
/// SELECT ...`, in which case the rows are deep-duped into the target table
/// rather than the long-lived ExecResult.
pub fn executeSelect(db: *Database, alloc: std.mem.Allocator, ps: stmt_mod.ParsedSelect) ![][]Value {
    const pp = postProcessFromParsed(alloc, ps) catch |err| return err;
    defer alloc.free(pp.order_by);
    if (ps.from) |from| switch (from) {
        .inline_values => |iv| return select_mod.executeWithFrom(alloc, ps.items, iv.rows, iv.columns, ps.where, pp),
        .table_ref => |name| {
            const t = try lookupTable(db, alloc, name);
            return select_mod.executeWithFrom(alloc, ps.items, t.rows.items, t.columns, ps.where, pp);
        },
    };
    if (select_mod.containsStar(ps.items)) return Error.SyntaxError;
    return select_mod.executeWithoutFrom(alloc, ps.items, ps.where, pp);
}

/// Translate stmt-level OrderTerm/limit/offset into the select-module's
/// `PostProcess` shape. The translated `order_by` slice is allocated in
/// `alloc` (per-statement arena) — the original AST nodes are still owned
/// by `ps`.
fn postProcessFromParsed(alloc: std.mem.Allocator, ps: stmt_mod.ParsedSelect) !select_mod.PostProcess {
    const order = try alloc.alloc(select_mod.OrderTerm, ps.order_by.len);
    for (ps.order_by, order) |term, *out| {
        out.* = .{
            .expr = term.expr,
            .position = term.position,
            .descending = term.dir == .desc,
        };
    }
    return .{
        .distinct = ps.distinct,
        .order_by = order,
        .limit = ps.limit,
        .offset = ps.offset,
    };
}

/// Append rows from a parsed INSERT into the target table. Source rows come
/// from either eagerly-evaluated VALUES tuples or a per-row SELECT result;
/// both live in arena memory until `dupeValueDeep` moves them.
///
/// When `parsed.columns` is non-null, source-row columns are projected into
/// the table-schema-shaped row by name (case-insensitive); table columns
/// not mentioned in the column list become NULL. Unknown column names or
/// arity mismatches return `SyntaxError`/`ColumnCountMismatch` before any
/// rows are appended (validation precedes mutation).
fn executeInsert(db: *Database, arena: std.mem.Allocator, parsed: stmt_mod.ParsedInsert) !u64 {
    const t = try lookupTable(db, db.allocator, parsed.table);

    const source_rows: [][]Value = switch (parsed.source) {
        .values => |rows| rows,
        .select => |ps| try executeSelect(db, arena, ps),
    };

    // Map source-row position → table-row position. `null` means "this table
    // column is not supplied by source" and should be NULL-padded.
    const target_indices = try resolveColumnTargets(arena, t, parsed.columns);
    const source_arity: usize = if (parsed.columns) |cs| cs.len else t.columns.len;
    for (source_rows) |row| {
        if (row.len != source_arity) return Error.ColumnCountMismatch;
    }

    try t.rows.ensureUnusedCapacity(db.allocator, source_rows.len);
    var inserted: u64 = 0;
    errdefer {
        // Roll back any rows already appended in this call (ADR-0003 §1
        // all-or-nothing). Schema mutations from registerTable are not
        // rolled back; only same-call row appends are.
        var i: u64 = 0;
        while (i < inserted) : (i += 1) {
            const last_idx = t.rows.items.len - 1;
            const undone_row = t.rows.items[last_idx];
            t.rows.items.len = last_idx;
            for (undone_row) |v| ops.freeValue(db.allocator, v);
            db.allocator.free(undone_row);
        }
    }
    for (source_rows) |row| {
        const new_row = try db.allocator.alloc(Value, t.columns.len);
        var k: usize = 0;
        errdefer {
            for (new_row[0..k]) |v| ops.freeValue(db.allocator, v);
            db.allocator.free(new_row);
        }
        while (k < t.columns.len) : (k += 1) {
            new_row[k] = if (target_indices[k]) |src_idx|
                try dupeValueDeep(db.allocator, row[src_idx])
            else
                Value.null;
        }
        t.rows.appendAssumeCapacity(new_row);
        inserted += 1;
    }
    return inserted;
}

/// Build a per-table-column slice mapping each table column index to either
/// the source row position that provides it (`?usize`) or null (meaning
/// "column omitted; use NULL"). When `cols` is null the mapping is the
/// identity (`[0, 1, 2, ...]`).
fn resolveColumnTargets(
    arena: std.mem.Allocator,
    t: *const Table,
    cols: ?[][]const u8,
) ![]?usize {
    const targets = try arena.alloc(?usize, t.columns.len);
    if (cols) |column_list| {
        @memset(targets, null);
        // Duplicate column names: sqlite3 silently keeps the FIRST mapping
        // and ignores later occurrences. Match that behavior — verified
        // against sqlite3 3.51.0 (`INSERT INTO t (a, a, b) VALUES (1, 2, 3)`
        // → row `(1, 3)`).
        for (column_list, 0..) |name, src_idx| {
            const tcol_idx = findTableColumn(t, name) orelse return Error.SyntaxError;
            if (targets[tcol_idx] == null) targets[tcol_idx] = src_idx;
        }
    } else {
        for (targets, 0..) |*slot, i| slot.* = i;
    }
    return targets;
}

fn findTableColumn(t: *const Table, name: []const u8) ?usize {
    for (t.columns, 0..) |col, i| {
        if (eqlIgnoreCase(name, col)) return i;
    }
    return null;
}

fn eqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| {
        if (std.ascii.toLower(x) != std.ascii.toLower(y)) return false;
    }
    return true;
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
