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

const Value = value_mod.Value;
const Database = database.Database;
const FromTerm = stmt_mod.FromTerm;
const ParsedFromSource = stmt_mod.ParsedFromSource;

/// One FROM source resolved to in-memory state. `qualifier` is the
/// effective alias (explicit `AS x` if given, else the bare table name; ""
/// for inline VALUES with no alias — qualified refs cannot match an empty
/// qualifier).
pub const ResolvedSource = struct {
    rows: []const []const Value,
    columns: []const []const u8,
    qualifier: []const u8,
};

/// Output of `buildJoinedRows` — combined rows and the flat metadata (one
/// entry per combined column) that `EvalContext` needs for column
/// resolution.
pub const Cartesian = struct {
    rows: [][]Value,
    columns: []const []const u8,
    qualifiers: []const []const u8,
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

    for (terms[1..]) |term| {
        const right = try resolveSource(db, alloc, term.source);
        const merged_columns = try concatColumns(alloc, current_columns, right.columns);
        const merged_qualifiers = try concatQualifiers(alloc, current_qualifiers, right.qualifier, right.columns.len);

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
    }
    return .{ .rows = current_rows, .columns = current_columns, .qualifiers = current_qualifiers };
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
        .table_ref => |tr| blk: {
            const t = try engine.lookupTable(db, alloc, tr.name);
            const out = try alloc.alloc([]const Value, t.rows.items.len);
            for (t.rows.items, out) |row, *slot| slot.* = row;
            break :blk .{
                .rows = out,
                .columns = t.columns,
                .qualifier = tr.alias orelse tr.name,
            };
        },
    };
}
