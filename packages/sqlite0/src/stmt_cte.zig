//! Iter31.Z — non-recursive CTE: `WITH name [(c1, c2, ...)] AS (SELECT ...|VALUES ...)`
//! prefix in front of a top-level SELECT. Iter31.AA extended the grammar
//! to (a) optional column-name list `(c1, c2, ...)` after the CTE name
//! that overrides the body's projected names, and (b) `VALUES` bodies
//! mirroring the inline-VALUES FROM source. Engine materialises each
//! CTE left-to-right against `db.transient_ctes` so a later CTE can
//! reference an earlier one and `engine_from`'s `.table_ref` resolution
//! finds it ahead of `db.tables`.
//!
//! Skipped: `RECURSIVE`, nested WITH inside a CTE body or subquery,
//! WITH-prefixed INSERT/UPDATE/DELETE.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const parser_mod = @import("parser.zig");
const stmt_mod = @import("stmt.zig");
const func_util = @import("func_util.zig");

const Value = value_mod.Value;
const Parser = parser_mod.Parser;
const Error = ops.Error;

pub const ParsedCte = struct {
    /// Borrowed slice into `Parser.src`. Compared via case-insensitive
    /// match against `engine_from.resolveSource`'s `.table_ref` name.
    name: []const u8,
    /// Iter31.AA — optional `(c1, c2, ...)` column rename list. Each
    /// slice borrows from `Parser.src`. When non-null, the engine
    /// overrides the body's projected names with this list (and rejects
    /// width mismatch). `null` = use the body's own column names
    /// (sqlite3 quirk: VALUES bodies default to `column1`...`columnN`).
    column_names: ?[]const []const u8 = null,
    /// SELECT body or VALUES rows. Both shapes share the materialise
    /// path in `engine.materializeCtes`.
    body: Body,
};

pub const Body = union(enum) {
    select: stmt_mod.ParsedSelect,
    /// Iter31.AA — eagerly-evaluated VALUES rows; arity is `rows[0].len`
    /// (or 0 when empty, but `parseValuesBody` enforces ≥1 row).
    values: [][]Value,
};

/// Caller has already verified the cursor is on `keyword_with`. Consumes
/// `WITH name [(c1, ...)] AS ( SELECT ... | VALUES ... ) [, ...]` and
/// returns the CTE list. Leaves the cursor on the next token (the
/// caller-expected `keyword_select`). Inner SELECT bodies go through
/// `parseSelectInner` so a nested `WITH` inside a CTE body is rejected
/// — sqlite3 accepts that, but supporting it would require passing the
/// nested CTE map through executeSelect's signature; deferred per
/// Iter31.Z scope.
pub fn parseWithClause(p: *Parser) Error![]ParsedCte {
    try p.expect(.keyword_with);
    // Iter31.AD — optional `RECURSIVE` keyword. sqlite3 accepts both
    // `WITH RECURSIVE` and `WITH` for recursive CTEs (recursion is
    // detected by shape, not by the keyword); we consume the literal
    // when present so client code that spells it out parses cleanly.
    // `recursive` isn't promoted to a TokenKind because it would
    // shadow legitimate column / table names.
    if (p.cur.kind == .identifier and func_util.eqlIgnoreCase(p.cur.slice(p.src), "recursive")) {
        p.advance();
    }
    var list: std.ArrayList(ParsedCte) = .empty;
    errdefer {
        for (list.items) |c| freeOne(p.allocator, c);
        list.deinit(p.allocator);
    }

    while (true) {
        if (p.cur.kind != .identifier) return Error.SyntaxError;
        const name = p.cur.slice(p.src);
        p.advance();
        var column_names: ?[]const []const u8 = null;
        errdefer if (column_names) |cs| p.allocator.free(cs);
        if (p.cur.kind == .lparen) {
            p.advance();
            column_names = try parseIdentList(p);
            try p.expect(.rparen);
        }
        try p.expect(.keyword_as);
        try p.expect(.lparen);
        const body: Body = if (p.cur.kind == .keyword_values) blk: {
            p.advance();
            const rows = try stmt_mod.parseValuesBody(p);
            break :blk .{ .values = rows };
        } else blk: {
            const inner = try stmt_mod.parseSelectInner(p, true);
            break :blk .{ .select = inner };
        };
        list.append(p.allocator, .{ .name = name, .column_names = column_names, .body = body }) catch |err| {
            freeBody(p.allocator, body);
            if (column_names) |cs| p.allocator.free(cs);
            return err;
        };
        try p.expect(.rparen);
        if (p.cur.kind != .comma) break;
        p.advance();
    }
    return list.toOwnedSlice(p.allocator);
}

fn parseIdentList(p: *Parser) Error![][]const u8 {
    var names: std.ArrayList([]const u8) = .empty;
    errdefer names.deinit(p.allocator);
    if (p.cur.kind != .identifier) return Error.SyntaxError;
    try names.append(p.allocator, p.cur.slice(p.src));
    p.advance();
    while (p.cur.kind == .comma) {
        p.advance();
        if (p.cur.kind != .identifier) return Error.SyntaxError;
        try names.append(p.allocator, p.cur.slice(p.src));
        p.advance();
    }
    return names.toOwnedSlice(p.allocator);
}

pub fn freeCtes(allocator: std.mem.Allocator, ctes: []ParsedCte) void {
    for (ctes) |c| freeOne(allocator, c);
    allocator.free(ctes);
}

fn freeOne(allocator: std.mem.Allocator, c: ParsedCte) void {
    if (c.column_names) |cs| allocator.free(cs);
    freeBody(allocator, c.body);
}

fn freeBody(allocator: std.mem.Allocator, body: Body) void {
    switch (body) {
        .select => |ps| stmt_mod.freeParsedSelectFields(allocator, ps),
        .values => |rows| {
            for (rows) |row| {
                for (row) |v| ops.freeValue(allocator, v);
                allocator.free(row);
            }
            allocator.free(rows);
        },
    }
}
