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
const select_post = @import("select_post.zig");
const aggregate = @import("aggregate.zig");
const eval = @import("eval.zig");
const database = @import("database.zig");
const engine_from = @import("engine_from.zig");
const engine_setop = @import("engine_setop.zig");
const engine_dml = @import("engine_dml.zig");
const func_util = @import("func_util.zig");

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

    // VALUES tuples evaluate eagerly during parsing; expose `db` so
    // `(SELECT ...)` inside a tuple can resolve. Restored on exit so other
    // entry points (REPL between dispatches) don't see a stale pointer.
    const saved_db = p.db;
    p.db = db;
    defer p.db = saved_db;

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
            try assertWritable(db);
            try db.registerTable(parsed);
            return .create_table;
        },
        .keyword_insert => {
            const parsed = try stmt_mod.parseInsertStatement(p);
            // INSERT is the first DDL/DML to gain a file-mode path
            // (Iter26.A.1) — `executeInsert` itself dispatches on
            // `db.pager`. CREATE TABLE / DELETE / UPDATE still go
            // through the read-only guard.
            const rowcount = try engine_dml.executeInsert(db, p.allocator, parsed);
            return .{ .insert = .{ .rowcount = rowcount } };
        },
        .keyword_delete => {
            const parsed = try stmt_dml.parseDeleteStatement(p);
            // executeDelete dispatches on `t.root_page` — file-mode is
            // handled via Pager + rebuild-page (Iter26.A.2.a). The
            // read-only guard no longer applies here.
            const rowcount = try engine_dml.executeDelete(db, p.allocator, parsed);
            return .{ .delete = .{ .rowcount = rowcount } };
        },
        .keyword_update => {
            const parsed = try stmt_dml.parseUpdateStatement(p);
            // executeUpdate dispatches on `t.root_page` — file-mode is
            // handled via Pager + rebuild-page (Iter26.A.2.b).
            const rowcount = try engine_dml.executeUpdate(db, p.allocator, parsed);
            return .{ .update = .{ .rowcount = rowcount } };
        },
        else => return Error.SyntaxError,
    }
}

/// Reject CREATE TABLE against a Pager-backed Database until Iter26.A.3
/// lands schema-page allocation. INSERT/DELETE/UPDATE now have file-mode
/// paths (Iter26.A.1 / .A.2) and dispatch on `Table.root_page` themselves;
/// only CREATE TABLE would still mutate `db.tables` with no path back to
/// the file, hence the gate stays for it alone.
fn assertWritable(db: *Database) Error!void {
    if (db.pager != null) return Error.ReadOnlyDatabase;
}

/// Result of a SELECT used as a row source. Iter21 added this so subqueries
/// in FROM can advertise their projected column names without a second
/// FROM-resolution pass at the call site. The CLI / direct executeSelect
/// callers (which don't print headers) keep using `executeSelect` and
/// discard the column metadata.
pub const SelectResult = struct {
    rows: [][]Value,
    columns: [][]const u8,
};

/// Run a parsed SELECT and also derive the projected column names. Used by
/// `engine_from.resolveSource` for `(SELECT ...)` subqueries; downstream
/// row-binding (`executeWithFrom`) needs both rows and column names so
/// `outer.col` can resolve into the subquery's projection.
pub fn executeSelectWithColumns(
    db: *Database,
    alloc: std.mem.Allocator,
    ps: stmt_mod.ParsedSelect,
) Error!SelectResult {
    const rows = try executeSelect(db, alloc, ps);
    // For setop chains the leftmost SELECT's columns win (sqlite3 quirk:
    // `SELECT 1 AS a UNION SELECT 2 AS b` projects column "a"). `ps` IS
    // the leftmost SELECT — branches dangle off it — so just derive from
    // ps.items.
    const columns = try projectedColumnNames(db, alloc, ps);
    return .{ .rows = rows, .columns = columns };
}

/// Compute output column names for a parsed SELECT. Per-item rules match
/// sqlite3 closely:
/// - `expr AS alias` → alias
/// - bare column ref (qualified or not) → the column name (qualifier dropped)
/// - any other expression → synthesized `columnN` (sqlite3 uses the source
///   text; we don't reify it, so the synthesized name is a divergence —
///   bare column refs / aliases cover the cases that real queries care about)
/// - `*` / `t.*` → expand against the FROM cartesian's columns/qualifiers
///
/// Star expansion lazily resolves the FROM clause (a second pass for
/// subqueries-of-subqueries) — only paid when at least one star item is
/// present. Pure-expression projections skip the cost.
fn projectedColumnNames(
    db: *Database,
    alloc: std.mem.Allocator,
    ps: stmt_mod.ParsedSelect,
) ![][]const u8 {
    var names: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (names.items) |n| alloc.free(n);
        names.deinit(alloc);
    }
    var cart_opt: ?engine_from.Cartesian = null;
    var expr_idx: usize = 0;
    for (ps.items) |item| {
        switch (item) {
            .star => |q| {
                if (cart_opt == null) {
                    if (ps.from.len == 0) return Error.SyntaxError;
                    cart_opt = try engine_from.cartesianFromSources(db, alloc, ps.from, &.{});
                }
                const cart = cart_opt.?;
                for (cart.columns, cart.qualifiers) |col, qual| {
                    if (q == null or func_util.eqlIgnoreCase(q.?, qual)) {
                        try names.append(alloc, try alloc.dupe(u8, col));
                    }
                }
            },
            .expr => |e| {
                expr_idx += 1;
                try names.append(alloc, try deriveExprColumnName(alloc, e, expr_idx));
            },
        }
    }
    return names.toOwnedSlice(alloc);
}

fn deriveExprColumnName(
    alloc: std.mem.Allocator,
    item: select_mod.SelectItem.ExprItem,
    one_based_idx: usize,
) ![]const u8 {
    if (item.alias) |a| return try alloc.dupe(u8, a);
    if (bareColumnRefName(item.expr)) |name| return try alloc.dupe(u8, name);
    // Prefer the verbatim source text (sqlite3 quirk: `SELECT 1+2` projects
    // column "1+2"). Falls back to `columnN` only when the source span
    // wasn't captured — currently that only happens for synthesized items
    // (no parsing site), but the path stays as a defensive last resort.
    if (item.source_text) |src| return try alloc.dupe(u8, src);
    return try std.fmt.allocPrint(alloc, "column{d}", .{one_based_idx});
}

fn bareColumnRefName(e: *const ast.Expr) ?[]const u8 {
    return switch (e.*) {
        .column_ref => |c| c.name,
        else => null,
    };
}

/// Run a parsed SELECT against `db` state. Result rows are allocated in
/// `alloc` (the per-statement arena); `dupeRowsToLongLived` later moves them
/// to long-lived memory. Also called by `executeInsert` for `INSERT INTO t
/// SELECT ...`, in which case the rows are deep-duped into the target table
/// rather than the long-lived ExecResult.
///
/// Path selection: when GROUP BY is present or any aggregate call is found
/// in the SELECT items / HAVING / ORDER BY, dispatch to `aggregate.executeAggregated`.
/// Otherwise the per-row path runs (`select.executeWithFrom` /
/// `executeWithoutFrom`). HAVING without GROUP BY and without aggregates is
/// rejected here — sqlite3 reports "HAVING clause on a non-aggregate query"
/// (verified against sqlite3 3.51.0); without this check the non-aggregate
/// path would silently drop the predicate.
pub fn executeSelect(db: *Database, alloc: std.mem.Allocator, ps: stmt_mod.ParsedSelect) Error![][]Value {
    return executeSelectWithOuter(db, alloc, ps, &.{});
}

/// Iter22.D entry point: run a parsed SELECT with an outer-frame stack so
/// correlated subqueries can resolve column refs against enclosing rows.
/// Top-level callers (CLI, dispatchOne) use `executeSelect` (empty stack);
/// subquery callers in `eval_subquery` extend `outer_frames` by one (the
/// caller's current frame) and forward here.
pub fn executeSelectWithOuter(
    db: *Database,
    alloc: std.mem.Allocator,
    ps: stmt_mod.ParsedSelect,
    outer_frames: []const eval.OuterFrame,
) Error![][]Value {
    if (ps.branches.len == 0) {
        const pp = try postProcessFromParsed(alloc, ps);
        return executeOneSelect(db, alloc, ps, pp, outer_frames);
    }

    // Setop chain: every branch (and the leftmost SELECT) executes WITHOUT
    // chain-level ORDER BY/LIMIT/OFFSET — those bind to the combined result
    // and are applied last. Per-branch DISTINCT inside a SELECT still goes
    // through (`SELECT DISTINCT x ... UNION ...`).
    var current = try executeOneSelect(db, alloc, ps, postProcessForBranch(ps), outer_frames);
    var left_arity = arityOf(ps, current);
    for (ps.branches) |branch| {
        const right = try executeOneSelect(db, alloc, branch.select, postProcessForBranch(branch.select), outer_frames);
        const right_arity = arityOf(branch.select, right);
        if (left_arity != null and right_arity != null and left_arity.? != right_arity.?) {
            return Error.ColumnCountMismatch;
        }
        current = try engine_setop.combine(alloc, branch.kind, current, right);
        if (left_arity == null) left_arity = right_arity;
    }
    // ORDER BY <name> in a setop chain resolves against the leftmost branch's
    // projection (sqlite3 quirk: `SELECT 1 AS a UNION SELECT 2 ORDER BY a` is
    // valid; the alias from branch 1 wins). Computing names lazily — only
    // when the chain actually has an ORDER BY — keeps the no-ORDER-BY path
    // free of a second FROM-resolution pass.
    const leftmost_columns: []const []const u8 = if (ps.order_by.len > 0)
        try projectedColumnNames(db, alloc, ps)
    else
        &.{};
    return engine_setop.applySetopPostProcess(alloc, db, current, ps.order_by, ps.limit, ps.offset, leftmost_columns, outer_frames);
}

/// Execute one ParsedSelect with the given PostProcess. Stripped out of
/// `executeSelect` so the setop path (Iter20) can run each branch with the
/// per-branch DISTINCT but no chain-level ORDER BY / LIMIT / OFFSET.
fn executeOneSelect(
    db: *Database,
    alloc: std.mem.Allocator,
    ps: stmt_mod.ParsedSelect,
    pp: select_mod.PostProcess,
    outer_frames: []const eval.OuterFrame,
) ![][]Value {
    const has_aggregates = aggregate.selectHasAggregates(ps.items, ps.having, pp.order_by);
    if (ps.having != null and ps.group_by.len == 0 and !has_aggregates) return Error.SyntaxError;
    const wants_grouping = ps.group_by.len > 0 or has_aggregates;

    if (ps.from.len == 0) {
        if (select_mod.containsStar(ps.items)) return Error.SyntaxError;
        if (wants_grouping) {
            const empty_row: []const Value = &.{};
            var synthetic = [_][]const Value{empty_row};
            return aggregate.executeAggregated(alloc, db, ps.items, synthetic[0..], &.{}, &.{}, ps.where, ps.group_by, ps.having, pp, outer_frames);
        }
        return select_mod.executeWithoutFrom(alloc, db, ps.items, ps.where, pp, outer_frames);
    }

    const cart = try engine_from.cartesianFromSources(db, alloc, ps.from, outer_frames);
    try validateStarExpansion(ps.items, cart.columns, cart.qualifiers);
    if (wants_grouping) {
        const inputs = try alloc.alloc([]const Value, cart.rows.len);
        for (cart.rows, inputs) |src, *slot| slot.* = src;
        return aggregate.executeAggregated(alloc, db, ps.items, inputs, cart.columns, cart.qualifiers, ps.where, ps.group_by, ps.having, pp, outer_frames);
    }
    const rows_const = try alloc.alloc([]const Value, cart.rows.len);
    for (cart.rows, rows_const) |src, *slot| slot.* = src;
    return select_mod.executeWithFrom(alloc, db, ps.items, rows_const, cart.columns, cart.qualifiers, ps.where, pp, outer_frames);
}

/// Detect ambiguous star expansion across the FROM cartesian. For each `*`
/// or `t.*` item in the SELECT list, walk the columns the star would expand
/// to and reject if any (qualifier, column-name) pair appears twice
/// (case-insensitive on both axes). Mirrors sqlite3's prepare-time check —
/// `SELECT * FROM a t, a t` errors with "ambiguous column name: t.x", but
/// `SELECT *, *` does not (multi-star duplicates are allowed; only
/// within-star duplicates are flagged).
///
/// Pure-expression projections skip the cost (no allocation, just an early
/// return on the items-have-no-star fast path).
fn validateStarExpansion(
    items: []const select_mod.SelectItem,
    columns: []const []const u8,
    qualifiers: []const []const u8,
) Error!void {
    if (!select_mod.containsStar(items)) return;
    std.debug.assert(columns.len == qualifiers.len);
    for (items) |item| switch (item) {
        .expr => continue,
        .star => |q| {
            for (columns, qualifiers, 0..) |col_a, qual_a, i| {
                if (q) |want| {
                    if (!func_util.eqlIgnoreCase(want, qual_a)) continue;
                }
                var j: usize = i + 1;
                while (j < columns.len) : (j += 1) {
                    const qual_b = qualifiers[j];
                    if (q) |want| {
                        if (!func_util.eqlIgnoreCase(want, qual_b)) continue;
                    }
                    if (!func_util.eqlIgnoreCase(qual_a, qual_b)) continue;
                    if (!func_util.eqlIgnoreCase(col_a, columns[j])) continue;
                    // sqlite3: "ambiguous column name: <qualifier>.<col>".
                    // We surface the same condition as `SyntaxError` to match
                    // the convention `eval_column.evalColumnRef` already uses
                    // for ambiguous frame-local refs.
                    return Error.SyntaxError;
                }
            }
        },
    };
}

/// PostProcess for one branch of a setop chain: keep the per-branch DISTINCT
/// but drop ORDER BY/LIMIT/OFFSET (those attach at chain level). The branch
/// shouldn't have any of those anyway — the parser rejects them via the
/// `allow_post = false` recursion guard — but we strip them defensively.
fn postProcessForBranch(ps: stmt_mod.ParsedSelect) select_mod.PostProcess {
    return .{ .distinct = ps.distinct, .order_by = &.{}, .limit = null, .offset = null };
}

/// Determine projected-row arity for the column-count-mismatch check.
/// Prefers an actual row's length when rows exist; otherwise falls back to
/// counting non-`*` items (returns null when only `*` items are present and
/// no rows are available — in that case we skip the check, matching what
/// sqlite3 catches at parse time but we can't determine without resolving
/// FROM-side columns).
fn arityOf(ps: stmt_mod.ParsedSelect, rows: [][]Value) ?usize {
    if (rows.len > 0) return rows[0].len;
    var n: usize = 0;
    for (ps.items) |item| {
        switch (item) {
            .star => return null,
            .expr => n += 1,
        }
    }
    return n;
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

/// Look up a table by user-supplied (possibly mixed-case) name. `scratch` is
/// used for the temporary lower-cased key buffer.
pub fn lookupTable(db: *Database, scratch: std.mem.Allocator, name: []const u8) !*Table {
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
            new_row[k] = try func_util.dupeValue(long, src[k]);
        }
        out[produced] = new_row;
    }
    return out;
}
