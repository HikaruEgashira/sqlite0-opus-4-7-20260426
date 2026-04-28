//! Iter31.Z — non-recursive CTE: `WITH name AS (SELECT ...) [, ...]`
//! prefix in front of a top-level SELECT. Only supports the simple
//! shape: comma-separated `IDENT AS (SELECT ...)` items. Skipped:
//! `RECURSIVE`, `t(a, b)` column rename, nested WITH inside a CTE
//! body or subquery, and WITH-prefixed INSERT/UPDATE/DELETE. Engine
//! materialises each CTE left-to-right against `db.transient_ctes` so
//! a later CTE can reference an earlier one and `engine_from`'s
//! `.table_ref` resolution finds it ahead of `db.tables`.

const std = @import("std");
const ops = @import("ops.zig");
const parser_mod = @import("parser.zig");
const stmt_mod = @import("stmt.zig");

const Parser = parser_mod.Parser;
const Error = ops.Error;

pub const ParsedCte = struct {
    /// Borrowed slice into `Parser.src`. Compared via case-insensitive
    /// match against `engine_from.resolveSource`'s `.table_ref` name.
    name: []const u8,
    /// The SELECT body inside the parentheses. Owns its AST nodes —
    /// `freeCtes` calls `freeParsedSelectFields` on each entry.
    select: stmt_mod.ParsedSelect,
};

/// Caller has already verified the cursor is on `keyword_with`. Consumes
/// `WITH name AS ( SELECT ... ) [, ...]` and returns the CTE list.
/// Leaves the cursor on the next token (the caller-expected `keyword_select`).
/// Inner SELECT bodies go through `parseSelectInner` so a nested `WITH`
/// inside a CTE body is rejected — sqlite3 accepts that, but supporting
/// it would require passing the nested CTE map through executeSelect's
/// signature; deferred per Iter31.Z scope.
pub fn parseWithClause(p: *Parser) Error![]ParsedCte {
    try p.expect(.keyword_with);
    var list: std.ArrayList(ParsedCte) = .empty;
    errdefer {
        for (list.items) |c| stmt_mod.freeParsedSelectFields(p.allocator, c.select);
        list.deinit(p.allocator);
    }

    while (true) {
        if (p.cur.kind != .identifier) return Error.SyntaxError;
        const name = p.cur.slice(p.src);
        p.advance();
        try p.expect(.keyword_as);
        try p.expect(.lparen);
        const select = try stmt_mod.parseSelectInner(p, true);
        list.append(p.allocator, .{ .name = name, .select = select }) catch |err| {
            stmt_mod.freeParsedSelectFields(p.allocator, select);
            return err;
        };
        try p.expect(.rparen);
        if (p.cur.kind != .comma) break;
        p.advance();
    }
    return list.toOwnedSlice(p.allocator);
}

pub fn freeCtes(allocator: std.mem.Allocator, ctes: []ParsedCte) void {
    for (ctes) |c| stmt_mod.freeParsedSelectFields(allocator, c.select);
    allocator.free(ctes);
}
