//! FROM clause parsing: comma-separated source list with optional aliases.
//!
//! Split out of `stmt.zig` to keep that file under the 500-line discipline
//! (CLAUDE.md "Module Splitting Rules") after Iter19.A added per-source
//! aliases and the multi-source list shape. The boundary is "after the
//! `FROM` keyword": this module owns `ParsedFromSource` and produces the
//! list; `stmt.zig` consumes the result and threads it into `ParsedSelect`.
//!
//! `(VALUES ...)` source bodies are parsed via `parseValuesBody` in
//! `stmt.zig` — the call back into `stmt` for that helper is the only
//! cross-module dependency.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const parser_mod = @import("parser.zig");
const stmt = @import("stmt.zig");

const Value = value_mod.Value;
const Error = ops.Error;
const Parser = parser_mod.Parser;

/// One source in the FROM clause. The clause as a whole is a slice of these
/// (`ParsedSelect.from`). With one element, behaviour matches the single-
/// source path; with multiple elements the engine takes the Cartesian
/// product (Iter19.A — comma-separated FROM list as implicit cross join).
pub const ParsedFromSource = union(enum) {
    /// `FROM (VALUES ...)` — rows already evaluated at parse time, columns
    /// auto-named `column1, column2, ...`. All allocations are in
    /// `Parser.allocator` (per-statement arena). `alias` is the source's
    /// effective qualifier for qualified column refs; null when no `AS`
    /// clause was given.
    inline_values: struct {
        rows: [][]Value,
        columns: [][]const u8,
        alias: ?[]const u8 = null,
    },
    /// `FROM <identifier> [AS <alias>]` — both names borrow from `Parser.src`.
    /// Caller resolves `name` against `Database.tables` at execute time.
    /// Effective qualifier is `alias` if present, else `name`.
    table_ref: struct {
        name: []const u8,
        alias: ?[]const u8 = null,
    },
};

/// Free arena-owned content of a single FROM source. Safe to call on either
/// variant — `table_ref` borrows from `Parser.src` and owns nothing.
pub fn freeParsedFrom(allocator: std.mem.Allocator, src: ParsedFromSource) void {
    switch (src) {
        .inline_values => |iv| {
            for (iv.rows) |row| {
                for (row) |v| ops.freeValue(allocator, v);
                allocator.free(row);
            }
            allocator.free(iv.rows);
            for (iv.columns) |c| allocator.free(c);
            allocator.free(iv.columns);
        },
        .table_ref => {},
    }
}

/// Free a whole `ParsedSelect.from` list. Loops over every source and frees
/// the slice itself.
pub fn freeFromList(allocator: std.mem.Allocator, list: []ParsedFromSource) void {
    for (list) |src| freeParsedFrom(allocator, src);
    allocator.free(list);
}

pub fn parseFromClause(p: *Parser) ![]ParsedFromSource {
    try p.expect(.keyword_from);
    var sources: std.ArrayList(ParsedFromSource) = .empty;
    errdefer {
        for (sources.items) |src| freeParsedFrom(p.allocator, src);
        sources.deinit(p.allocator);
    }
    try sources.append(p.allocator, try parseFromSource(p));
    while (p.cur.kind == .comma) {
        p.advance();
        try sources.append(p.allocator, try parseFromSource(p));
    }
    return sources.toOwnedSlice(p.allocator);
}

/// Parse one source: `(VALUES ...)` or an identifier, each optionally
/// followed by `[AS] alias`. Aliases are stored on the source so qualified
/// column refs (`alias.col`) can resolve at execute time.
fn parseFromSource(p: *Parser) !ParsedFromSource {
    if (p.cur.kind == .lparen) {
        p.advance();
        try p.expect(.keyword_values);
        const rows = try stmt.parseValuesBody(p);
        errdefer {
            for (rows) |row| {
                for (row) |v| ops.freeValue(p.allocator, v);
                p.allocator.free(row);
            }
            p.allocator.free(rows);
        }
        try p.expect(.rparen);
        const alias = parseOptionalAlias(p);
        const arity: usize = if (rows.len > 0) rows[0].len else 0;
        const columns = try synthesizeColumnNames(p.allocator, arity);
        return .{ .inline_values = .{ .rows = rows, .columns = columns, .alias = alias } };
    }
    if (p.cur.kind == .identifier) {
        const name = p.cur.slice(p.src);
        p.advance();
        const alias = parseOptionalAlias(p);
        return .{ .table_ref = .{ .name = name, .alias = alias } };
    }
    return Error.SyntaxError;
}

/// Consume `[AS] alias` if present and return the alias slice. The caller's
/// cursor advances over both the optional `AS` keyword and the identifier.
fn parseOptionalAlias(p: *Parser) ?[]const u8 {
    if (p.cur.kind == .keyword_as) {
        p.advance();
        if (p.cur.kind != .identifier) return null;
        const name = p.cur.slice(p.src);
        p.advance();
        return name;
    }
    if (p.cur.kind == .identifier) {
        const name = p.cur.slice(p.src);
        p.advance();
        return name;
    }
    return null;
}

/// Allocate `column1`, `column2`, ... `columnN` matching SQLite's auto-naming
/// for `(VALUES ...)` subqueries.
fn synthesizeColumnNames(allocator: std.mem.Allocator, n: usize) ![][]const u8 {
    var names = try allocator.alloc([]const u8, n);
    var produced: usize = 0;
    errdefer {
        for (names[0..produced]) |name| allocator.free(name);
        allocator.free(names);
    }
    while (produced < n) : (produced += 1) {
        names[produced] = try std.fmt.allocPrint(allocator, "column{d}", .{produced + 1});
    }
    return names;
}
