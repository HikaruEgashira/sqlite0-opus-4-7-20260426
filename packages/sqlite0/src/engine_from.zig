//! FROM-list resolution and iterative join execution (Iter19.A–C).
//!
//! Split out of `engine.zig` to keep that file under the 500-line discipline
//! (CLAUDE.md "Module Splitting Rules"). The split point is "before the row
//! loop": this module turns a `[]FromTerm` into a single combined row set +
//! flat column/qualifier metadata. The grouping / aggregate / per-row paths
//! in `engine.zig` then operate uniformly on that output.
//!
//! Iter19.C iterative-join model: rather than building a full Cartesian
//! upfront and applying ONs as global filters, the join chain is folded
//! left-to-right. For each new term we materialise the merged rows
//! (left × right) with ON applied at the join boundary, plus — for `left`
//! kind — NULL-padded rows for left tuples with no matching right.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const ast = @import("ast.zig");
const stmt_mod = @import("stmt.zig");
const database = @import("database.zig");
const engine = @import("engine.zig");
const eval = @import("eval.zig");
const cursor_mod = @import("cursor.zig");
const btree_cursor_mod = @import("btree_cursor.zig");
const collation_mod = @import("collation.zig");

const Value = value_mod.Value;
const Database = database.Database;
const FromTerm = stmt_mod.FromTerm;
const ParsedFromSource = stmt_mod.ParsedFromSource;

/// One FROM source resolved to in-memory state. `qualifier` is the
/// effective alias (explicit `AS x` if given, else the bare table name; ""
/// for inline VALUES with no alias — qualified refs cannot match an empty
/// qualifier). `collations` (Iter31.R) is parallel to `columns`; empty for
/// inline_values / subquery (which carry no schema), populated from
/// `Table.collations` for table_ref.
pub const ResolvedSource = struct {
    rows: []const []const Value,
    columns: []const []const u8,
    qualifier: []const u8,
    collations: []const ast.CollationKind = &.{},
};

/// Output of `buildJoinedRows` — combined rows and the flat metadata (one
/// entry per combined column) that `EvalContext` needs for column
/// resolution. `collations` (Iter31.R) carries each merged column's
/// schema-default collation, parallel to `columns`/`qualifiers`.
pub const Cartesian = struct {
    rows: [][]Value,
    columns: []const []const u8,
    qualifiers: []const []const u8,
    collations: []const ast.CollationKind,
};

/// Resolve the FROM list and fold every term into a single combined row
/// set. ON predicates (Iter19.B) and LEFT-kind NULL padding (Iter19.C) are
/// both applied at the join boundary. All allocations live in `alloc`
/// (the per-statement arena), so the caller releases everything by tearing
/// the arena down.
pub fn cartesianFromSources(
    db: *Database,
    alloc: std.mem.Allocator,
    terms: []FromTerm,
    outer_frames: []const eval.OuterFrame,
) !Cartesian {
    std.debug.assert(terms.len >= 1);
    const first = try resolveSource(db, alloc, terms[0].source);

    var current_rows: [][]Value = try dupRowsToOwned(alloc, first.rows);
    var current_columns: []const []const u8 = first.columns;
    var current_qualifiers: []const []const u8 = blk: {
        const qs = try alloc.alloc([]const u8, first.columns.len);
        for (qs) |*q| q.* = first.qualifier;
        break :blk qs;
    };
    var current_collations: []const ast.CollationKind = try padCollations(alloc, first.collations, first.columns.len);

    for (terms[1..]) |term| {
        const right = try resolveSource(db, alloc, term.source);
        const merged_columns = try concatColumns(alloc, current_columns, right.columns);
        const merged_qualifiers = try concatQualifiers(alloc, current_qualifiers, right.qualifier, right.columns.len);
        const merged_collations = try concatCollations(alloc, current_collations, right.collations, right.columns.len);

        // For each left row, walk the right rows and apply ON. Inner / cross
        // / comma keep only matching pairs (or all pairs when ON is null);
        // left additionally emits a NULL-padded row when a left tuple
        // matches nothing.
        var next: std.ArrayList([]Value) = .empty;
        for (current_rows) |left| {
            var matched = false;
            for (right.rows) |right_row| {
                const combined = try alloc.alloc(Value, left.len + right_row.len);
                for (left, combined[0..left.len]) |v, *s| s.* = v;
                for (right_row, combined[left.len..]) |v, *s| s.* = v;

                if (term.join_on) |on| {
                    const ctx = eval.EvalContext{
                        .allocator = alloc,
                        .current_row = combined,
                        .columns = merged_columns,
                        .column_qualifiers = merged_qualifiers,
                        .column_collations = merged_collations,
                        .db = db,
                        .outer_frames = outer_frames,
                    };
                    const cond = try eval.evalExpr(ctx, on);
                    defer ops.freeValue(alloc, cond);
                    if (!(ops.truthy(cond) orelse false)) {
                        // Discard combined; arena reclaims on teardown.
                        continue;
                    }
                }
                try next.append(alloc, combined);
                matched = true;
            }
            if (term.kind == .left and !matched) {
                // Emit `left ++ NULLs(right.columns.len)`.
                const padded = try alloc.alloc(Value, left.len + right.columns.len);
                for (left, padded[0..left.len]) |v, *s| s.* = v;
                for (padded[left.len..]) |*s| s.* = Value.null;
                try next.append(alloc, padded);
            }
        }
        current_rows = try next.toOwnedSlice(alloc);
        current_columns = merged_columns;
        current_qualifiers = merged_qualifiers;
        current_collations = merged_collations;
    }
    return .{ .rows = current_rows, .columns = current_columns, .qualifiers = current_qualifiers, .collations = current_collations };
}

/// Iter31.R — fill or trim a collation slice to match `width`. Empty
/// input becomes all-binary (sources that don't carry schema info treat
/// every column as default-binary).
fn padCollations(
    alloc: std.mem.Allocator,
    src: []const ast.CollationKind,
    width: usize,
) ![]const ast.CollationKind {
    const out = try alloc.alloc(ast.CollationKind, width);
    for (out, 0..) |*o, i| o.* = if (i < src.len) src[i] else .binary;
    return out;
}

fn concatCollations(
    alloc: std.mem.Allocator,
    left: []const ast.CollationKind,
    right: []const ast.CollationKind,
    right_count: usize,
) ![]const ast.CollationKind {
    const out = try alloc.alloc(ast.CollationKind, left.len + right_count);
    for (left, out[0..left.len]) |k, *s| s.* = k;
    for (out[left.len..], 0..) |*s, i| s.* = if (i < right.len) right[i] else .binary;
    return out;
}

fn dupRowsToOwned(alloc: std.mem.Allocator, rows: []const []const Value) ![][]Value {
    const out = try alloc.alloc([]Value, rows.len);
    for (rows, out) |src, *slot| {
        const dup = try alloc.alloc(Value, src.len);
        for (src, dup) |v, *s| s.* = v;
        slot.* = dup;
    }
    return out;
}

fn concatColumns(
    alloc: std.mem.Allocator,
    left: []const []const u8,
    right: []const []const u8,
) ![]const []const u8 {
    const out = try alloc.alloc([]const u8, left.len + right.len);
    for (left, out[0..left.len]) |c, *s| s.* = c;
    for (right, out[left.len..]) |c, *s| s.* = c;
    return out;
}

fn concatQualifiers(
    alloc: std.mem.Allocator,
    left: []const []const u8,
    right_qualifier: []const u8,
    right_count: usize,
) ![]const []const u8 {
    const out = try alloc.alloc([]const u8, left.len + right_count);
    for (left, out[0..left.len]) |q, *s| s.* = q;
    for (out[left.len..]) |*s| s.* = right_qualifier;
    return out;
}

fn resolveSource(db: *Database, alloc: std.mem.Allocator, src: ParsedFromSource) !ResolvedSource {
    return switch (src) {
        .inline_values => |iv| .{
            .rows = blk: {
                const out = try alloc.alloc([]const Value, iv.rows.len);
                for (iv.rows, out) |row, *slot| slot.* = row;
                break :blk out;
            },
            .columns = iv.columns,
            .qualifier = iv.alias orelse "",
        },
        .table_ref => |tr| tblblk: {
            // Phase 3a (Iter24.A): the row source goes through `Cursor`
            // rather than reading `t.rows.items` directly. Phase 3b
            // (Iter25.B.4/5) added the BtreeCursor backend — `t.root_page
            // != 0` indicates a Pager-resident table on a sqlite3 .db
            // file, otherwise we use the in-memory TableCursor. The fork
            // is the ONLY backend-specific code path; both cursors yield
            // the same `[]Value` shape and the unified arena lifetime
            // contract makes downstream code identical.
            const t = try engine.lookupTable(db, alloc, tr.name);
            const c: cursor_mod.Cursor = if (t.root_page != 0) c_blk: {
                const pager_ptr = if (db.pager) |*pp| pp else return ops.Error.IoError;
                const bc = try alloc.create(btree_cursor_mod.BtreeCursor);
                bc.* = btree_cursor_mod.BtreeCursor.open(alloc, pager_ptr, t.root_page, t.columns, t.ipk_column);
                break :c_blk bc.cursor();
            } else c_blk: {
                const tc = try alloc.create(cursor_mod.TableCursor);
                tc.* = cursor_mod.TableCursor.open(t);
                break :c_blk tc.cursor();
            };
            const materialized = try cursor_mod.materializeRows(alloc, c);
            const out = try alloc.alloc([]const Value, materialized.len);
            for (materialized, out) |row, *slot| slot.* = row;
            break :tblblk .{
                .rows = out,
                .columns = c.columns(),
                .qualifier = tr.alias orelse tr.name,
                .collations = t.collations,
            };
        },
        .subquery => |sq| blk: {
            // Run the inner SELECT and capture its projected column names.
            // The qualifier is the explicit alias (if any); without an alias
            // qualified refs into the subquery can't match — sqlite3 still
            // accepts unqualified refs in that case, which is what an empty
            // qualifier achieves here too. `collations` (Iter31.R) carries
            // each projected column's schema collation across the subquery
            // boundary so column-level COLLATE propagates through
            // `(SELECT x FROM t)` — see `subqueryProjectionCollations`.
            const result = try engine.executeSelectWithColumns(db, alloc, sq.select);
            const out = try alloc.alloc([]const Value, result.rows.len);
            for (result.rows, out) |row, *slot| slot.* = row;
            const cols = try subqueryProjectionCollations(db, alloc, sq.select, result.columns.len);
            break :blk .{
                .rows = out,
                .columns = result.columns,
                .qualifier = sq.alias orelse "",
                .collations = cols,
            };
        },
    };
}

/// Iter31.R — derive per-projection schema collations for a subquery in
/// FROM. For each `.expr` item: peek explicit COLLATE wrapper, else look
/// up bare column-ref's schema collation against the inner FROM
/// cartesian, else BINARY. For each `.star` item: copy matching cart
/// entries by qualifier. Setop chains return all-BINARY (sqlite3 quirk:
/// `SELECT x FROM t UNION SELECT 'b'` projects BINARY, verified). Built
/// length always matches `result.columns.len` from `executeSelectWithColumns`;
/// `expected` is passed in to fall back safely if a future projection
/// path desyncs.
fn subqueryProjectionCollations(
    db: *Database,
    alloc: std.mem.Allocator,
    ps: stmt_mod.ParsedSelect,
    expected: usize,
) ops.Error![]const ast.CollationKind {
    if (ps.branches.len > 0) return padCollations(alloc, &.{}, expected);
    return leftmostProjectionCollations(db, alloc, ps, expected);
}

/// Iter31.T — derive per-projection schema collations from the leftmost
/// branch of a SELECT (or any single SELECT). Same item walk as
/// `subqueryProjectionCollations` but WITHOUT the
/// branches-drop-collation early return — used by setop ORDER BY where
/// sqlite3 carries the leftmost branch's column-default collation
/// through the chain (`SELECT x FROM t COLLATE NOCASE UNION ALL ...
/// ORDER BY x` sorts NOCASE).
pub fn leftmostProjectionCollations(
    db: *Database,
    alloc: std.mem.Allocator,
    ps: stmt_mod.ParsedSelect,
    expected: usize,
) ops.Error![]const ast.CollationKind {
    const opt = try leftmostProjectionCollationsOpt(db, alloc, ps, expected);
    const out = try alloc.alloc(ast.CollationKind, opt.len);
    for (opt, out) |k, *o| o.* = k orelse .binary;
    return out;
}

/// Iter31.U — same as `leftmostProjectionCollations` but preserves
/// "no inherent collation" (literal / expression with no wrapper) as
/// null instead of folding to `.binary`. Setop DEDUP precedence rule
/// "leftmost branch with ANY collation source wins" needs this:
/// a column-ref's column-default (even BINARY) locks the chain in,
/// while a bare literal yields to subsequent branches' collations.
pub fn leftmostProjectionCollationsOpt(
    db: *Database,
    alloc: std.mem.Allocator,
    ps: stmt_mod.ParsedSelect,
    expected: usize,
) ops.Error![]?ast.CollationKind {
    var out: std.ArrayList(?ast.CollationKind) = .empty;
    var cart_opt: ?Cartesian = null;
    for (ps.items) |item| {
        switch (item) {
            .star => |q| {
                if (cart_opt == null and ps.from.len > 0) {
                    cart_opt = try cartesianFromSources(db, alloc, ps.from, &.{});
                }
                if (cart_opt) |cart| {
                    for (cart.qualifiers, cart.collations) |qual, k| {
                        if (q == null or std.ascii.eqlIgnoreCase(q.?, qual)) {
                            try out.append(alloc, k);
                        }
                    }
                }
            },
            .expr => |e| {
                if (collation_mod.peekKind(e.expr)) |k| {
                    try out.append(alloc, k);
                    continue;
                }
                if (e.expr.* == .column_ref and cart_opt == null and ps.from.len > 0) {
                    cart_opt = try cartesianFromSources(db, alloc, ps.from, &.{});
                }
                if (cart_opt) |cart| {
                    if (collation_mod.columnDefault(e.expr, cart.columns, cart.qualifiers, cart.collations)) |k| {
                        try out.append(alloc, k);
                        continue;
                    }
                }
                try out.append(alloc, null);
            },
        }
    }
    // Width-mismatch guard: if items expansion ever falls out of sync with
    // executeSelectWithColumns (e.g. star against an empty FROM), pad/trim.
    if (out.items.len != expected) {
        const padded = try alloc.alloc(?ast.CollationKind, expected);
        for (padded, 0..) |*o, i| o.* = if (i < out.items.len) out.items[i] else null;
        return padded;
    }
    return out.toOwnedSlice(alloc);
}
