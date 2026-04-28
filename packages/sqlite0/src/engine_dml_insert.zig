//! In-memory INSERT execution. Split out of `engine_dml.zig` once the
//! function passed 200 lines (Iter31.AG added IGNORE/REPLACE branches),
//! mirroring the existing `engine_dml_insert_file.zig` for the file-mode
//! backend. `engine_dml.zig` keeps `DmlInternal` (shared) plus the
//! UPDATE/DELETE in-memory paths.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const stmt_mod = @import("stmt.zig");
const database = @import("database.zig");
const engine = @import("engine.zig");
const func_util = @import("func_util.zig");
const engine_dml = @import("engine_dml.zig");
const engine_dml_insert_file = @import("engine_dml_insert_file.zig");
const engine_returning = @import("engine_returning.zig");

const Value = value_mod.Value;
const Database = database.Database;
const Table = database.Table;
const Error = ops.Error;
const DmlInternal = engine_dml.DmlInternal;

/// Append rows from a parsed INSERT into the target table. Source rows come
/// from either eagerly-evaluated VALUES tuples or a per-row SELECT result;
/// both live in arena memory until `func_util.dupeValue` moves them.
///
/// `parsed.columns == null` ⇒ identity mapping (all table columns in
/// declaration order). When non-null, columns get projected by name
/// (case-insensitive); table columns absent from the list become NULL.
/// Duplicate column names follow sqlite3's "first occurrence wins" quirk.
pub fn executeInsert(db: *Database, arena: std.mem.Allocator, parsed: stmt_mod.ParsedInsert) !DmlInternal {
    const t = try engine.lookupTable(db, db.allocator, parsed.table);
    // Iter29.A — sqlite_schema / sqlite_master reject direct DML to avoid
    // page-1 silent corruption.
    if (t.is_system) return Error.UnsupportedFeature;

    const source_rows: [][]Value = switch (parsed.source) {
        .values => |rows| rows,
        .select => |ps| try engine.executeSelect(db, arena, ps),
    };

    const target_indices = try resolveColumnTargets(arena, t, parsed.columns);
    const source_arity: usize = if (parsed.columns) |cs| cs.len else t.columns.len;
    for (source_rows) |row| {
        if (row.len != source_arity) return Error.ColumnCountMismatch;
    }

    // File-mode tables route through the Pager + B-tree mutation path.
    if (t.root_page != 0) {
        if (parsed.returning != null) return Error.UnsupportedFeature; // Iter31.AE deferred (a)
        if (parsed.conflict_action != .abort) return Error.UnsupportedFeature; // Iter31.AG deferred (file)
        const rc = try engine_dml_insert_file.executeInsertFile(db, t, target_indices, source_rows);
        return .{ .rowcount = rc };
    }

    try t.rows.ensureUnusedCapacity(db.allocator, source_rows.len);
    if (t.ipk_column == null) {
        try t.rowids.ensureUnusedCapacity(db.allocator, source_rows.len);
    }
    var inserted: u64 = 0;
    // Iter28.fix — in-memory IPK auto-rowid. Mirrors file-mode chooseRowid;
    // explicit values bump max_rowid so subsequent NULLs continue from the
    // higher water-mark. Iter29.T — non-IPK uses parallel rowids list.
    var max_rowid: i64 = if (t.ipk_column) |ipk|
        computeMaxIpkValue(t, ipk)
    else
        currentMaxImplicitRowid(t);
    errdefer {
        // ADR-0003 §1 all-or-nothing: undo same-call appends. REPLACE-removed
        // rows are NOT restored (Iter31.AG known limitation).
        var i: u64 = 0;
        while (i < inserted) : (i += 1) {
            const last_idx = t.rows.items.len - 1;
            const undone_row = t.rows.items[last_idx];
            t.rows.items.len = last_idx;
            for (undone_row) |v| ops.freeValue(db.allocator, v);
            db.allocator.free(undone_row);
            if (t.ipk_column == null) {
                t.rowids.items.len -= 1;
            }
        }
    }
    // Iter29.S — last-inserted rowid commits to db only after success.
    var last_rowid: i64 = db.last_insert_rowid;
    for (source_rows) |row| {
        const new_row = try db.allocator.alloc(Value, t.columns.len);
        var k: usize = 0;
        errdefer {
            for (new_row[0..k]) |v| ops.freeValue(db.allocator, v);
            db.allocator.free(new_row);
        }
        while (k < t.columns.len) : (k += 1) {
            new_row[k] = if (target_indices[k]) |src_idx|
                try func_util.dupeValue(db.allocator, row[src_idx])
            else
                Value.null;
        }
        if (t.ipk_column) |ipk| switch (new_row[ipk]) {
            .null => {
                max_rowid += 1;
                new_row[ipk] = .{ .integer = max_rowid };
                last_rowid = max_rowid;
            },
            .integer => |explicit| {
                if (explicit > max_rowid) max_rowid = explicit;
                last_rowid = explicit;
            },
            else => {},
        } else {
            max_rowid += 1;
            last_rowid = max_rowid;
        }
        // Iter31.AG — `INSERT OR IGNORE` swallows both NOT NULL and UNIQUE.
        // `OR REPLACE` only swallows UNIQUE; NOT NULL still errors here
        // since we have no column DEFAULTs to substitute.
        enforceNotNull(t, new_row) catch |err| {
            if (parsed.conflict_action == .ignore) {
                for (new_row) |v| ops.freeValue(db.allocator, v);
                db.allocator.free(new_row);
                continue;
            }
            return err;
        };
        // Iter31.AF — IPK UNIQUE check. Auto values can't conflict;
        // explicit ones scan t.rows (including same-batch appends).
        var conflict_idx: ?usize = null;
        if (t.ipk_column) |ipk| {
            if (new_row[ipk] == .integer) {
                const new_ipk = new_row[ipk].integer;
                for (t.rows.items, 0..) |existing, i| {
                    if (existing[ipk] == .integer and existing[ipk].integer == new_ipk) {
                        conflict_idx = i;
                        break;
                    }
                }
            }
        }
        if (conflict_idx) |ci| switch (parsed.conflict_action) {
            .ignore => {
                for (new_row) |v| ops.freeValue(db.allocator, v);
                db.allocator.free(new_row);
                continue;
            },
            .replace => {
                // Iter31.AG — drop the offending row, then fall through to
                // append. The replaced row is NOT restored by errdefer.
                const old = t.rows.orderedRemove(ci);
                for (old) |v| ops.freeValue(db.allocator, v);
                db.allocator.free(old);
            },
            .abort, .fail, .rollback => return Error.UniqueConstraint,
        };
        t.rows.appendAssumeCapacity(new_row);
        if (t.ipk_column == null) {
            t.rowids.appendAssumeCapacity(max_rowid);
        }
        inserted += 1;
    }
    if (inserted > 0) {
        db.last_insert_rowid = last_rowid;
    }

    // Iter31.AE — RETURNING projection over newly-inserted rows. Done after
    // the loop so errdefer rollback doesn't strand stale projected rows.
    var returning_rows: ?[][]Value = null;
    if (parsed.returning) |items| {
        const start_idx = t.rows.items.len - inserted;
        const arr = try arena.alloc([]Value, inserted);
        var i: usize = 0;
        while (i < inserted) : (i += 1) {
            const row = t.rows.items[start_idx + i];
            arr[i] = try engine_returning.projectRow(db, arena, items, row, parsed.table, t.columns, t.collations);
        }
        returning_rows = arr;
    }
    return .{ .rowcount = inserted, .returning = returning_rows };
}

fn currentMaxImplicitRowid(t: *const Table) i64 {
    var max_val: i64 = 0;
    for (t.rowids.items) |rid| {
        if (rid > max_val) max_val = rid;
    }
    return max_val;
}

fn computeMaxIpkValue(t: *const Table, ipk: usize) i64 {
    var max_val: i64 = 0;
    for (t.rows.items) |row| {
        if (ipk >= row.len) continue;
        switch (row[ipk]) {
            .integer => |n| if (n > max_val) {
                max_val = n;
            },
            else => {},
        }
    }
    return max_val;
}

/// Map each table column index to the source row position that provides it
/// (`?usize`); null means "column omitted; use NULL". When `cols` is null
/// the mapping is identity. Duplicate names: first occurrence wins (sqlite3
/// 3.51.0 verified — `INSERT INTO t (a, a, b) VALUES (1, 2, 3)` → `(1, 3)`).
fn resolveColumnTargets(
    arena: std.mem.Allocator,
    t: *const Table,
    cols: ?[][]const u8,
) ![]?usize {
    const targets = try arena.alloc(?usize, t.columns.len);
    if (cols) |column_list| {
        @memset(targets, null);
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
        if (func_util.eqlIgnoreCase(name, col)) return i;
    }
    return null;
}

/// Iter29.B — reject if any NOT NULL column carries NULL post-IPK auto-
/// assignment. Mirrors `engine_dml_insert_file.enforceNotNull` so both
/// backends produce identical rejection signals.
pub fn enforceNotNull(t: *const Table, row: []const Value) Error!void {
    for (t.not_null, row) |required, v| {
        if (required and v == .null) return Error.ConstraintNotNull;
    }
}
