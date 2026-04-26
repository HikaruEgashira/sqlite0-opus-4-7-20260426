//! FROM-list resolution and Cartesian product (Iter19.A).
//!
//! Split out of `engine.zig` to keep that file under the 500-line discipline
//! (CLAUDE.md "Module Splitting Rules"). The split point is "before the row
//! loop": this module turns a `[]ParsedFromSource` into a single combined
//! row set + flat column/qualifier metadata. The grouping / aggregate /
//! per-row paths in `engine.zig` then operate uniformly on that output.

const std = @import("std");
const value_mod = @import("value.zig");
const stmt_mod = @import("stmt.zig");
const database = @import("database.zig");
const engine = @import("engine.zig");

const Value = value_mod.Value;
const Database = database.Database;

/// One FROM source resolved to in-memory state. `qualifier` is the effective
/// alias for column-ref resolution: explicit `AS x` if given, otherwise the
/// bare table name (matching sqlite3, where `AS` shadows the original name).
/// For inline VALUES with no alias, qualifier is the empty string — qualified
/// refs cannot match an empty qualifier.
pub const ResolvedSource = struct {
    rows: []const []const Value,
    columns: []const []const u8,
    qualifier: []const u8,
};

/// Output of `cartesianFromSources` — combined rows and the flat metadata
/// (one entry per combined column) that `EvalContext` needs for column
/// resolution.
pub const Cartesian = struct {
    rows: [][]Value,
    columns: []const []const u8,
    qualifiers: []const []const u8,
};

/// Resolve every FROM source to its rows + columns + qualifier, then
/// Cartesian-multiply into a single combined row set. Single-source paths
/// short-circuit to avoid the cross-product loop. All allocations live in
/// `alloc` (the per-statement arena), so the caller releases everything by
/// tearing the arena down.
pub fn cartesianFromSources(
    db: *Database,
    alloc: std.mem.Allocator,
    sources: []stmt_mod.ParsedFromSource,
) !Cartesian {
    const resolved = try alloc.alloc(ResolvedSource, sources.len);
    for (sources, resolved) |src, *out| out.* = try resolveSource(db, alloc, src);

    const total_cols = blk: {
        var n: usize = 0;
        for (resolved) |r| n += r.columns.len;
        break :blk n;
    };
    const columns = try alloc.alloc([]const u8, total_cols);
    const qualifiers = try alloc.alloc([]const u8, total_cols);
    var w: usize = 0;
    for (resolved) |r| {
        for (r.columns) |c| {
            columns[w] = c;
            qualifiers[w] = r.qualifier;
            w += 1;
        }
    }

    if (resolved.len == 1) {
        // No Cartesian work — re-shape the source rows into the engine's
        // owned `[][]Value` form so downstream code doesn't have to special-
        // case slice provenance. We borrow the inner Values; the per-
        // statement arena owns them through the source.
        const r = resolved[0];
        const out = try alloc.alloc([]Value, r.rows.len);
        for (r.rows, out) |src_row, *slot| {
            const dup = try alloc.alloc(Value, src_row.len);
            for (src_row, dup) |v, *s| s.* = v;
            slot.* = dup;
        }
        return .{ .rows = out, .columns = columns, .qualifiers = qualifiers };
    }

    // Iterative cross: start with source 0's rows, then fold each subsequent
    // source's rows in by emitting (current × source_k.rows) merged tuples.
    // The arena absorbs the intermediate `current` slice when each iteration
    // overwrites it; cleanup is on full deinit.
    var current: [][]Value = blk: {
        const seed = try alloc.alloc([]Value, resolved[0].rows.len);
        for (resolved[0].rows, seed) |src_row, *slot| {
            const dup = try alloc.alloc(Value, src_row.len);
            for (src_row, dup) |v, *s| s.* = v;
            slot.* = dup;
        }
        break :blk seed;
    };
    for (resolved[1..]) |r| {
        const next_rows = try alloc.alloc([]Value, current.len * r.rows.len);
        var idx: usize = 0;
        for (current) |left| {
            for (r.rows) |right| {
                const merged = try alloc.alloc(Value, left.len + right.len);
                for (left, merged[0..left.len]) |v, *s| s.* = v;
                for (right, merged[left.len..]) |v, *s| s.* = v;
                next_rows[idx] = merged;
                idx += 1;
            }
        }
        current = next_rows;
    }
    return .{ .rows = current, .columns = columns, .qualifiers = qualifiers };
}

fn resolveSource(db: *Database, alloc: std.mem.Allocator, src: stmt_mod.ParsedFromSource) !ResolvedSource {
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
