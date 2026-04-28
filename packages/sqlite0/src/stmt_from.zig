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
const ast = @import("ast.zig");
const parser_mod = @import("parser.zig");
const stmt = @import("stmt.zig");

const Value = value_mod.Value;
const Error = ops.Error;
const Parser = parser_mod.Parser;

/// How the term joins with the accumulated left side of the FROM list.
/// `comma` and `cross` produce a Cartesian; `inner` adds the ON predicate
/// at the join boundary (observationally equivalent to a Cartesian + WHERE
/// for INNER); `left` keeps every left row and NULL-pads the right when no
/// row of the right source satisfies ON.
pub const JoinKind = enum { comma, cross, inner, left };

/// One entry in the FROM list: a source, the join kind that introduced
/// it, and the optional ON predicate. The first term's `kind` is `comma`
/// (a no-op marker; nothing precedes it). Iter19.C is the first iteration
/// to use `kind` non-trivially — for INNER/CROSS we still get the same
/// rows as a Cartesian + WHERE, but the engine evaluates ON at the join
/// boundary so LEFT can share the same code path.
pub const FromTerm = struct {
    source: ParsedFromSource,
    kind: JoinKind = .comma,
    join_on: ?*ast.Expr = null,
};

/// One source in the FROM clause. Wrapped in `FromTerm` for the join-on
/// metadata; carried through `ParsedSelect.from` as a slice. With one
/// element, behaviour matches the single-source path; with multiple
/// elements the engine takes the Cartesian product.
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
    /// `FROM (SELECT ...) [AS <alias>]` — subquery materialised at execute
    /// time by `engine_from.resolveSource`. Output column names derive from
    /// the inner SELECT items: explicit alias > bare column ref > `columnN`
    /// for arbitrary expressions; `*` expands to the inner FROM's columns.
    /// `alias` (when present) becomes the qualifier for qualified column
    /// refs (`alias.col`); when absent, qualified refs cannot match this
    /// source (sqlite3 lets unqualified refs still resolve).
    subquery: struct {
        select: stmt.ParsedSelect,
        alias: ?[]const u8 = null,
        /// Iter31.AC — optional `WITH name AS (...)` prefix inside the
        /// subquery. Each CTE is materialised in a nested scope by
        /// `engine_from.resolveSource` (saving / restoring
        /// `db.transient_ctes`) right before the subquery's SELECT runs.
        /// Empty slice = no inner WITH (the common path).
        with_ctes: []stmt.ParsedCte = &.{},
    },
};

/// Free arena-owned content of a single FROM source. Safe to call on any
/// variant — `table_ref` borrows from `Parser.src` and owns nothing;
/// `subquery` recurses through `freeParsedSelectFields` for the inner AST.
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
        .subquery => |sq| {
            stmt.freeParsedSelectFields(allocator, sq.select);
            if (sq.with_ctes.len > 0) stmt.freeCtes(allocator, sq.with_ctes);
        },
    }
}

/// Free a whole `ParsedSelect.from` list. Loops over every term, frees its
/// source content + ON AST, then the slice itself.
pub fn freeFromList(allocator: std.mem.Allocator, list: []FromTerm) void {
    for (list) |term| {
        freeParsedFrom(allocator, term.source);
        if (term.join_on) |on| on.deinit(allocator);
    }
    allocator.free(list);
}

pub fn parseFromClause(p: *Parser) Error![]FromTerm {
    try p.expect(.keyword_from);
    var terms: std.ArrayList(FromTerm) = .empty;
    errdefer {
        for (terms.items) |term| {
            freeParsedFrom(p.allocator, term.source);
            if (term.join_on) |on| on.deinit(p.allocator);
        }
        terms.deinit(p.allocator);
    }
    try terms.append(p.allocator, .{ .source = try parseFromSource(p) });
    while (true) {
        const sep = matchJoinSeparator(p) orelse break;
        const next_source = try parseFromSource(p);
        errdefer freeParsedFrom(p.allocator, next_source);
        var join_on: ?*ast.Expr = null;
        if (sep.allows_on and p.cur.kind == .keyword_on) {
            p.advance();
            join_on = try p.parseExpr();
        }
        try terms.append(p.allocator, .{ .source = next_source, .kind = sep.kind, .join_on = join_on });
    }
    return terms.toOwnedSlice(p.allocator);
}

/// Recognise a join-list separator after the previous source. Returns null
/// when the cursor sits on something else (FROM clause complete). `comma`
/// rejects a following ON; the JOIN keywords accept it (sqlite3 even
/// allows `CROSS JOIN ... ON` — verified against 3.51.0).
const JoinSep = struct { kind: JoinKind, allows_on: bool };

fn matchJoinSeparator(p: *Parser) ?JoinSep {
    if (p.cur.kind == .comma) {
        p.advance();
        return .{ .kind = .comma, .allows_on = false };
    }
    if (p.cur.kind == .keyword_join) {
        p.advance();
        return .{ .kind = .inner, .allows_on = true };
    }
    if (p.cur.kind == .keyword_inner) {
        p.advance();
        if (p.cur.kind != .keyword_join) return null;
        p.advance();
        return .{ .kind = .inner, .allows_on = true };
    }
    if (p.cur.kind == .keyword_cross) {
        p.advance();
        if (p.cur.kind != .keyword_join) return null;
        p.advance();
        return .{ .kind = .cross, .allows_on = true };
    }
    if (p.cur.kind == .keyword_left) {
        p.advance();
        // OUTER is optional in `LEFT [OUTER] JOIN`.
        if (p.cur.kind == .keyword_outer) p.advance();
        if (p.cur.kind != .keyword_join) return null;
        p.advance();
        return .{ .kind = .left, .allows_on = true };
    }
    return null;
}

/// Parse one source: `(VALUES ...)`, `(SELECT ...)`, or an identifier — each
/// optionally followed by `[AS] alias`. Aliases are stored on the source so
/// qualified column refs (`alias.col`) can resolve at execute time. The
/// `lparen` dispatch peeks at the next token: `keyword_select` selects the
/// subquery branch, `keyword_values` the inline-VALUES branch, anything else
/// is a syntax error.
fn parseFromSource(p: *Parser) Error!ParsedFromSource {
    if (p.cur.kind == .lparen) {
        p.advance();
        // Iter31.AC — `(WITH cte AS (...) SELECT ...)` inside FROM. We
        // parse the WITH clause locally so its scope is bounded to this
        // subquery; engine_from.resolveSource saves / restores
        // db.transient_ctes around the inner SELECT.
        var inner_ctes: []stmt.ParsedCte = &.{};
        errdefer if (inner_ctes.len > 0) stmt.freeCtes(p.allocator, inner_ctes);
        if (p.cur.kind == .keyword_with) {
            inner_ctes = try @import("stmt_cte.zig").parseWithClause(p);
        }
        if (p.cur.kind == .keyword_select) {
            const ps = try stmt.parseSelectStatement(p);
            errdefer stmt.freeParsedSelectFields(p.allocator, ps);
            try p.expect(.rparen);
            const alias = parseOptionalAlias(p);
            return .{ .subquery = .{ .select = ps, .alias = alias, .with_ctes = inner_ctes } };
        }
        if (inner_ctes.len > 0) return Error.SyntaxError;
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
/// for `(VALUES ...)` subqueries. Public so `engine.materializeCtes` (Iter31.AA)
/// can reuse the same synthesis for VALUES bodies in CTEs.
pub fn synthesizeColumnNames(allocator: std.mem.Allocator, n: usize) ![][]const u8 {
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
