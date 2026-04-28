//! Statement parsing. `parse*Statement` consumes one statement, returns a
//! `Parsed*` struct, leaves the cursor on `.semicolon`/`.eof`. Execution
//! lives in `engine.dispatchOne`. VALUES inside FROM is eagerly evaluated.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const ast = @import("ast.zig");
const parser_mod = @import("parser.zig");
const select = @import("select.zig");
const func_util = @import("func_util.zig");
const collation = @import("collation.zig");
const stmt_cte = @import("stmt_cte.zig");

const Value = value_mod.Value;
const Error = ops.Error;
const Parser = parser_mod.Parser;

// ── SELECT ──────────────────────────────────────────────────────────────────

pub const ParsedSelect = struct {
    items: []select.SelectItem,
    /// FROM terms, in clause order. Empty slice = no FROM. Each term is a
    /// source plus the optional ON predicate from the JOIN keyword that
    /// introduced it. Multiple terms produce the Cartesian product at
    /// execute time, with ON predicates AND-folded into the row filter.
    from: []FromTerm = &.{},
    where: ?*ast.Expr,
    distinct: bool = false,
    /// `GROUP BY e1 [, e2 ...]`. Empty slice = no clause; aggregate path
    /// treats it as "implicit single group" iff an aggregate is present.
    /// `GroupByTerm.position` resolves bare integer literals positionally
    /// against the SELECT list (mirrors ORDER BY).
    group_by: []GroupByTerm = &.{},
    /// `HAVING <expr>` filter applied after grouping. May reference
    /// aggregates not in the SELECT list (sqlite3 allows this). Null when
    /// absent.
    having: ?*ast.Expr = null,
    /// Set-operation chain (Iter20). Empty when this SELECT is standalone.
    /// Each branch holds another ParsedSelect whose `branches`, `order_by`,
    /// `limit`, and `offset` are guaranteed empty — sqlite3 attaches
    /// post-clauses to the whole UNION chain, not per branch.
    branches: []SetopBranch = &.{},
    order_by: []OrderTerm = &.{},
    limit: ?*ast.Expr = null,
    offset: ?*ast.Expr = null,
    /// Iter31.Z — `WITH name AS (SELECT ...) [, ...]` prefix. Empty when
    /// the SELECT had no WITH clause. Only set on the top-level
    /// `parseSelectStatement` result; setop branches and CTE bodies go
    /// through `parseSelectInner` directly so they can never carry CTEs
    /// (sqlite3 rejects nested WITH at most positions; we reject in all).
    with_ctes: []ParsedCte = &.{},
};

// Set-op types live in stmt_setop.zig; re-exported to keep call sites
// reading `stmt.SetopKind` / `stmt.SetopBranch` working.
const stmt_setop = @import("stmt_setop.zig");
pub const SetopKind = stmt_setop.SetopKind;
pub const SetopBranch = stmt_setop.SetopBranch;
pub const freeSetopBranches = stmt_setop.freeSetopBranches;

// CTE types live in stmt_cte.zig; re-exported for parity.
pub const ParsedCte = stmt_cte.ParsedCte;
pub const CteBody = stmt_cte.Body;
pub const freeCtes = stmt_cte.freeCtes;

pub const OrderDirection = enum { asc, desc };

/// `position` non-null = 1-based SELECT-list column ref (sqlite3 quirk).
/// `nulls_first` null = direction default. `collation` is the outermost
/// COLLATE wrapper (Iter31.O); null = no wrapper at all (so the call site
/// can fall back to a bare column-ref's schema collation per Iter31.R).
pub const OrderTerm = struct {
    expr: *ast.Expr,
    position: ?usize = null,
    dir: OrderDirection,
    nulls_first: ?bool = null,
    collation: ?ast.CollationKind = null,
};

/// `collation` is the outermost COLLATE wrapper (Iter31.P); null = no
/// wrapper (Iter31.R fall-back to column-default).
pub const GroupByTerm = struct {
    expr: *ast.Expr,
    position: ?usize = null,
    collation: ?ast.CollationKind = null,
};

// FROM clause — types and parsing live in stmt_from.zig (split out to
// keep this file under the 500-line discipline). Re-exported here so
// existing call sites keep their `stmt.ParsedFromSource` reference working.
const stmt_from = @import("stmt_from.zig");
pub const ParsedFromSource = stmt_from.ParsedFromSource;
pub const FromTerm = stmt_from.FromTerm;
pub const JoinKind = stmt_from.JoinKind;
pub const freeParsedFrom = stmt_from.freeParsedFrom;
pub const freeFromList = stmt_from.freeFromList;

pub fn parseSelectStatement(p: *Parser) Error!ParsedSelect {
    var with_ctes: []ParsedCte = &.{};
    errdefer freeCtes(p.allocator, with_ctes);
    if (p.cur.kind == .keyword_with) {
        with_ctes = try stmt_cte.parseWithClause(p);
    }
    var ps = try parseSelectInner(p, true);
    ps.with_ctes = with_ctes;
    return ps;
}

/// Iter20 set-op support: `allow_post = true` is the outer call (parses
/// branches and the chain-level ORDER BY / LIMIT / OFFSET). `false` is used
/// recursively for each setop branch — those branches must NOT consume
/// post-clauses, since sqlite3 errors on `... ORDER BY ... UNION ...` and on
/// `... LIMIT ... UNION ...`. Public so `stmt_cte.parseWithClause` can use
/// it for CTE bodies (which must NOT recurse into WITH).
pub fn parseSelectInner(p: *Parser, allow_post: bool) Error!ParsedSelect {
    try p.expect(.keyword_select);

    var distinct = false;
    if (p.cur.kind == .keyword_distinct) {
        distinct = true;
        p.advance();
    } else if (p.cur.kind == .keyword_all) {
        // sqlite3 accepts `SELECT ALL ...` as the explicit non-distinct form.
        p.advance();
    }

    const items = try select.parseSelectList(p);
    errdefer select.freeSelectList(p.allocator, items);

    var from: []FromTerm = &.{};
    errdefer freeFromList(p.allocator, from);
    if (p.cur.kind == .keyword_from) {
        from = try stmt_from.parseFromClause(p);
    }

    var where_ast: ?*ast.Expr = null;
    errdefer if (where_ast) |w| w.deinit(p.allocator);
    if (p.cur.kind == .keyword_where) {
        p.advance();
        where_ast = try p.parseExpr();
    }

    var group_by: []GroupByTerm = &.{};
    errdefer freeGroupByTerms(p.allocator, group_by);
    if (p.cur.kind == .keyword_group) {
        p.advance();
        try p.expect(.keyword_by);
        group_by = try parseGroupByList(p);
    }

    var having_ast: ?*ast.Expr = null;
    errdefer if (having_ast) |h| h.deinit(p.allocator);
    if (p.cur.kind == .keyword_having) {
        p.advance();
        having_ast = try p.parseExpr();
    }

    var branches: []SetopBranch = &.{};
    errdefer freeSetopBranches(p.allocator, branches);
    var order_by: []OrderTerm = &.{};
    errdefer freeOrderTerms(p.allocator, order_by);
    var limit_ast: ?*ast.Expr = null;
    errdefer if (limit_ast) |e| e.deinit(p.allocator);
    var offset_ast: ?*ast.Expr = null;
    errdefer if (offset_ast) |e| e.deinit(p.allocator);

    if (allow_post) {
        branches = try parseSetopBranches(p);

        if (p.cur.kind == .keyword_order) {
            order_by = try parseOrderBy(p);
        }

        if (p.cur.kind == .keyword_limit) {
            p.advance();
            limit_ast = try p.parseExpr();
            // sqlite3 quirk: `LIMIT a, b` means "skip a, take b" — the comma
            // form flips arg order. The lhs (already parsed into limit_ast) is
            // actually the offset; the rhs is the row count.
            if (p.cur.kind == .comma) {
                p.advance();
                offset_ast = limit_ast;
                limit_ast = try p.parseExpr();
            } else if (p.cur.kind == .keyword_offset) {
                p.advance();
                offset_ast = try p.parseExpr();
            }
        }
    }
    // sqlite3 rejects ORDER BY / LIMIT before a setop keyword
    // ("ORDER BY clause should come after UNION not before"). With
    // allow_post = false we never advance past these, so they remain on
    // the cursor; the outer caller will see them as misplaced and the
    // surrounding statement-level dispatch will surface a SyntaxError.

    return .{
        .items = items,
        .from = from,
        .where = where_ast,
        .distinct = distinct,
        .group_by = group_by,
        .having = having_ast,
        .branches = branches,
        .order_by = order_by,
        .limit = limit_ast,
        .offset = offset_ast,
    };
}

/// Loop on UNION / UNION ALL / INTERSECT / EXCEPT and parse the trailing
/// SELECT for each. Returns an empty slice when no setop keyword follows.
/// Lives here (not in stmt_setop.zig) because each iteration recurses into
/// the private `parseSelectInner`.
fn parseSetopBranches(p: *Parser) Error![]SetopBranch {
    var list: std.ArrayList(SetopBranch) = .empty;
    // Same single-errdefer pattern as `parseOrderBy` — separate errdefers
    // for `freeSetopBranches(... list.items)` and `list.deinit()` would
    // double-free on any inner-select parse failure.
    errdefer {
        for (list.items) |b| freeParsedSelectFields(p.allocator, b.select);
        list.deinit(p.allocator);
    }

    while (stmt_setop.matchSetopKind(p)) |kind| {
        const inner = try parseSelectInner(p, false);
        list.append(p.allocator, .{ .kind = kind, .select = inner }) catch |err| {
            freeParsedSelectFields(p.allocator, inner);
            return err;
        };
    }
    return list.toOwnedSlice(p.allocator);
}

/// Free the AST nodes inside a ParsedSelect. Used both by errdefer paths in
/// the parser (when constructing a setop branch fails partway) and by the
/// engine if a downstream step rejects the query. Mirrors the field-by-field
/// cleanup that errdefer would do inside `parseSelectInner`.
pub fn freeParsedSelectFields(allocator: std.mem.Allocator, ps: ParsedSelect) void {
    select.freeSelectList(allocator, ps.items);
    freeFromList(allocator, ps.from);
    if (ps.where) |w| w.deinit(allocator);
    freeGroupByTerms(allocator, ps.group_by);
    if (ps.having) |h| h.deinit(allocator);
    freeSetopBranches(allocator, ps.branches);
    freeOrderTerms(allocator, ps.order_by);
    if (ps.limit) |e| e.deinit(allocator);
    if (ps.offset) |e| e.deinit(allocator);
    if (ps.with_ctes.len > 0) freeCtes(allocator, ps.with_ctes);
}

/// Parse `GROUP BY e1 [, e2 ...]`. Bare positive integer literal is a
/// 1-based SELECT-list column reference (sqlite3 quirk; `GROUP BY 1+0`
/// keeps expression semantics). Mirrors parseOrderTerm: peel COLLATE
/// wrapper(s) so the inner literal drives the position quirk and the
/// outermost collation wins for group-key equality (Iter31.P).
fn parseGroupByList(p: *Parser) ![]GroupByTerm {
    var list: std.ArrayList(GroupByTerm) = .empty;
    errdefer freeGroupByTerms(p.allocator, list.items);
    while (true) {
        const expr = try p.parseExpr();
        const peeled = collation.peel(expr);
        const position: ?usize = if (peeled.inner.* == .literal) switch (peeled.inner.literal) {
            .integer => |n| if (n > 0) @as(usize, @intCast(n)) else null,
            else => null,
        } else null;
        try list.append(p.allocator, .{ .expr = expr, .position = position, .collation = collation.peekKind(expr) });
        if (p.cur.kind != .comma) break;
        p.advance();
    }
    return list.toOwnedSlice(p.allocator);
}

pub fn freeGroupByTerms(allocator: std.mem.Allocator, list: []GroupByTerm) void {
    for (list) |t| t.expr.deinit(allocator);
    allocator.free(list);
}

fn parseOrderBy(p: *Parser) ![]OrderTerm {
    try p.expect(.keyword_order);
    try p.expect(.keyword_by);
    var terms: std.ArrayList(OrderTerm) = .empty;
    // Single combined errdefer (separate per-item + container errdefers
    // double-freed on `ORDER BY @` parse errors).
    errdefer {
        for (terms.items) |t| t.expr.deinit(p.allocator);
        terms.deinit(p.allocator);
    }
    try parseOrderTerm(p, &terms);
    while (p.cur.kind == .comma) {
        p.advance();
        try parseOrderTerm(p, &terms);
    }
    return terms.toOwnedSlice(p.allocator);
}

fn parseOrderTerm(p: *Parser, terms: *std.ArrayList(OrderTerm)) !void {
    const expr = try p.parseExpr();
    // Peel any Collate wrapper(s) to find the inner literal/column-ref.
    // Outermost wins for collation (sqlite3 chained-COLLATE rule); inner
    // is what drives the bare-integer-position quirk.
    const peeled = collation.peel(expr);
    var position: ?usize = null;
    if (peeled.inner.* == .literal) switch (peeled.inner.literal) {
        .integer => |n| if (n > 0) { position = @intCast(n); },
        else => {},
    };
    var dir: OrderDirection = .asc;
    if (p.cur.kind == .keyword_asc) {
        p.advance();
    } else if (p.cur.kind == .keyword_desc) {
        dir = .desc;
        p.advance();
    }
    // `NULLS FIRST` / `NULLS LAST` postfix (SQL standard). None reserved.
    var nulls_first: ?bool = null;
    if (p.cur.kind == .identifier and func_util.eqlIgnoreCase(p.cur.slice(p.src), "nulls")) {
        p.advance();
        if (p.cur.kind != .identifier) return Error.SyntaxError;
        const w = p.cur.slice(p.src);
        if (func_util.eqlIgnoreCase(w, "first")) nulls_first = true
        else if (func_util.eqlIgnoreCase(w, "last")) nulls_first = false
        else return Error.SyntaxError;
        p.advance();
    }
    try terms.append(p.allocator, .{ .expr = expr, .position = position, .dir = dir, .nulls_first = nulls_first, .collation = collation.peekKind(expr) });
}

pub fn freeOrderTerms(allocator: std.mem.Allocator, terms: []OrderTerm) void {
    for (terms) |t| t.expr.deinit(allocator);
    allocator.free(terms);
}

// ── VALUES ──────────────────────────────────────────────────────────────────
// Implementation lives in stmt_values.zig (split out of stmt.zig to keep
// it under 500 lines per CLAUDE.md before adding CTE support). Both
// `parseValuesStatement` (used by engine.dispatchOne for top-level
// VALUES) and `parseValuesBody` (used here by parseInsertStatement, and
// by stmt_from for `FROM (VALUES ...)`) are re-exported.

const stmt_values = @import("stmt_values.zig");
pub const parseValuesStatement = stmt_values.parseValuesStatement;
pub const parseValuesBody = stmt_values.parseValuesBody;

// ── CREATE TABLE ────────────────────────────────────────────────────────────
// Implementation lives in stmt_ddl.zig; re-exported here so existing call
// sites keep their `stmt.parseCreateTableStatement` reference working.

const stmt_ddl = @import("stmt_ddl.zig");
pub const ParsedCreateTable = stmt_ddl.ParsedCreateTable;
pub const parseCreateTableStatement = stmt_ddl.parseCreateTableStatement;

// ── INSERT ──────────────────────────────────────────────────────────────────

/// `INSERT INTO <name> [(c1, c2, ...)] (VALUES (...) [, (...)] | SELECT ...)`
///
/// `columns == null` means "all table columns in declaration order" (the
/// default). When non-null, each name borrows from `Parser.src` and the
/// outer slice lives in the per-statement arena.
///
/// `source` holds either eagerly-evaluated VALUES rows or an unevaluated
/// `ParsedSelect`; the engine resolves the SELECT path against `Database`
/// state at execute time.
pub const ParsedInsert = struct {
    table: []const u8,
    columns: ?[][]const u8,
    source: Source,

    pub const Source = union(enum) {
        values: [][]Value,
        select: ParsedSelect,
    };
};

pub fn parseInsertStatement(p: *Parser) !ParsedInsert {
    try p.expect(.keyword_insert);
    try p.expect(.keyword_into);
    if (p.cur.kind != .identifier) return Error.SyntaxError;
    const table = p.cur.slice(p.src);
    p.advance();

    var columns: ?[][]const u8 = null;
    errdefer if (columns) |cs| p.allocator.free(cs);
    if (p.cur.kind == .lparen) {
        p.advance();
        columns = try parseInsertColumnList(p);
        try p.expect(.rparen);
    }

    switch (p.cur.kind) {
        .keyword_values => {
            p.advance();
            const rows = try parseValuesBody(p);
            return .{ .table = table, .columns = columns, .source = .{ .values = rows } };
        },
        .keyword_select => {
            const ps = try parseSelectStatement(p);
            return .{ .table = table, .columns = columns, .source = .{ .select = ps } };
        },
        else => return Error.SyntaxError,
    }
}

fn parseInsertColumnList(p: *Parser) ![][]const u8 {
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

// ── PRAGMA ──────────────────────────────────────────────────────────────────
// Implementation lives in stmt_pragma.zig; re-exported here for parity
// with the CREATE TABLE / INSERT split-out pattern.

const stmt_pragma = @import("stmt_pragma.zig");
pub const ParsedPragma = stmt_pragma.ParsedPragma;
pub const parsePragmaStatement = stmt_pragma.parsePragmaStatement;

// ── BEGIN / COMMIT / ROLLBACK + SAVEPOINT / RELEASE / ROLLBACK TO ──────────
const stmt_tx = @import("stmt_tx.zig");
pub const TxControl = stmt_tx.TxControl;
pub const parseTxStatement = stmt_tx.parseTxStatement;
