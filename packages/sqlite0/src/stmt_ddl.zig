//! DDL statement parsing — currently just CREATE TABLE. Split from `stmt.zig`
//! when that file crossed the 500-line trigger (CLAUDE.md "Module Splitting
//! Rules"). Future ALTER TABLE / DROP TABLE / CREATE INDEX go here too.

const std = @import("std");
const ops = @import("ops.zig");
const parser_mod = @import("parser.zig");
const func_util = @import("func_util.zig");
const ast = @import("ast.zig");
const collation_mod = @import("collation.zig");

const Error = ops.Error;
const Parser = parser_mod.Parser;

/// One column in a parsed CREATE TABLE. `is_ipk` is set only when the
/// column-def's type+constraint stream matches the literal sequence
/// `INTEGER PRIMARY KEY` (case-insensitive, depth-0). `is_not_null`
/// captures depth-0 `NOT NULL`. `collation` (Iter31.R) captures the
/// last depth-0 `COLLATE <name>` clause and becomes the column's default
/// collating sequence — comparisons / DISTINCT / ORDER BY / GROUP BY on
/// a bare ref to this column use it when no explicit `expr COLLATE name`
/// wrapper is present. Default `.binary` matches sqlite3's column
/// default. Multiple COLLATE clauses on one column let the LAST one
/// win (sqlite3 honors the rightmost too — verified
/// `CREATE TABLE t(x COLLATE BINARY COLLATE NOCASE)` then `x = 'A'`
/// matches 'a').
///
/// Iter31.AJ — `check_text` carries the source slice between the parens
/// of a depth-0 `CHECK (<expr>)` column-constraint. The slice borrows
/// from `Parser.src`; `registerTable` dupes it into long-lived memory
/// and re-parses it into an AST stored on `Table`. Multiple CHECK
/// clauses on one column: LAST one wins (parser captures the most
/// recent span; matches a single-column constraint shape, table-level
/// CHECK with multi-column refs is deferred to Iter31.AL).
pub const ParsedColumn = struct {
    name: []const u8,
    is_ipk: bool = false,
    is_not_null: bool = false,
    collation: ast.CollationKind = .binary,
    check_text: ?[]const u8 = null,
};

/// All slices borrow from `Parser.src`, which the caller (`Database.execute`)
/// holds for the entire `execute` call; the outer slice is in
/// `Parser.allocator` (per-statement arena). The caller dupes `name` and
/// `columns[*]` to long-lived memory before storing them.
///
/// `source_text` is the verbatim CREATE TABLE statement bytes from `p.src`
/// (`CREATE` keyword through the closing `)`). Iter26.A.3 needs this to
/// populate the `sql` column of `sqlite_schema` so that on a subsequent
/// open the schema scanner can re-parse the original definition. Trailing
/// `;` is NOT included — sqlite3 stores the statement without it.
pub const ParsedCreateTable = struct {
    name: []const u8,
    columns: []ParsedColumn,
    source_text: []const u8,
};

/// `CREATE TABLE <name> ( <col-def> [, <col-def> ...] )`
///
/// Each col-def is a column name optionally followed by a type-name and any
/// column-constraints; the type and constraints are consumed and discarded
/// (Iter14.B), with the single exception of the IPK signal captured into
/// `ParsedColumn.is_ipk` (Iter28).
pub fn parseCreateTableStatement(p: *Parser) !ParsedCreateTable {
    const stmt_start: u32 = p.cur.start;
    try p.expect(.keyword_create);
    try p.expect(.keyword_table);

    if (p.cur.kind != .identifier) return Error.SyntaxError;
    const name = p.cur.slice(p.src);
    p.advance();

    try p.expect(.lparen);

    var columns: std.ArrayList(ParsedColumn) = .empty;
    errdefer columns.deinit(p.allocator);

    while (true) {
        const col = try parseColumnDef(p);
        try columns.append(p.allocator, col);
        if (p.cur.kind == .comma) {
            p.advance();
            continue;
        }
        break;
    }
    // Capture the rparen token's end BEFORE `expect` advances past it
    // — once consumed, the lexer cursor moves to the next token and we
    // can't recover the source-text endpoint.
    if (p.cur.kind != .rparen) return Error.SyntaxError;
    const stmt_end: u32 = p.cur.end;
    p.advance();

    return .{
        .name = name,
        .columns = try columns.toOwnedSlice(p.allocator),
        .source_text = p.src[stmt_start..stmt_end],
    };
}

test "parseColumnDef: NOT NULL detected on simple column" {
    const allocator = std.testing.allocator;
    const sql = "CREATE TABLE t(a NOT NULL, b)";
    var p = parser_mod.Parser.init(allocator, sql);
    const parsed = try parseCreateTableStatement(&p);
    defer allocator.free(parsed.columns);
    try std.testing.expectEqual(@as(usize, 2), parsed.columns.len);
    try std.testing.expect(parsed.columns[0].is_not_null);
    try std.testing.expect(!parsed.columns[1].is_not_null);
}

test "parseColumnDef: NOT NULL with type prefix" {
    const allocator = std.testing.allocator;
    const sql = "CREATE TABLE t(a INTEGER NOT NULL, b TEXT NOT NULL)";
    var p = parser_mod.Parser.init(allocator, sql);
    const parsed = try parseCreateTableStatement(&p);
    defer allocator.free(parsed.columns);
    try std.testing.expect(parsed.columns[0].is_not_null);
    try std.testing.expect(parsed.columns[1].is_not_null);
}

test "parseColumnDef: NOT NULL coexists with INTEGER PRIMARY KEY" {
    const allocator = std.testing.allocator;
    const sql = "CREATE TABLE t(a INTEGER PRIMARY KEY NOT NULL, b)";
    var p = parser_mod.Parser.init(allocator, sql);
    const parsed = try parseCreateTableStatement(&p);
    defer allocator.free(parsed.columns);
    try std.testing.expect(parsed.columns[0].is_ipk);
    try std.testing.expect(parsed.columns[0].is_not_null);
}

test "parseColumnDef: missing NOT NULL leaves flag false" {
    const allocator = std.testing.allocator;
    const sql = "CREATE TABLE t(a, b INTEGER)";
    var p = parser_mod.Parser.init(allocator, sql);
    const parsed = try parseCreateTableStatement(&p);
    defer allocator.free(parsed.columns);
    try std.testing.expect(!parsed.columns[0].is_not_null);
    try std.testing.expect(!parsed.columns[1].is_not_null);
}

/// Column-def: `<name> [<type-name> ...] [<column-constraint> ...]`.
/// Captures four depth-0 signals:
///   - `INTEGER PRIMARY KEY` triple → `is_ipk` (Iter28).
///   - `NOT NULL` pair → `is_not_null` (Iter29.B).
///   - `COLLATE <name>` clause → `collation` (Iter31.R). Last wins;
///     unknown name → `Error.SyntaxError` ("no such collation sequence").
///   - `CHECK (<expr>)` clause → `check_text` (Iter31.AJ). Last wins;
///     the source span between the parens is recorded for re-parse at
///     `registerTable` time. Empty body → `SyntaxError`.
/// Other tokens are scanned for paren depth tracking and otherwise
/// discarded — type names, DEFAULT expressions, etc.
fn parseColumnDef(p: *Parser) !ParsedColumn {
    if (p.cur.kind != .identifier) return Error.SyntaxError;
    const name = p.cur.slice(p.src);
    p.advance();
    var depth: u32 = 0;
    // Sliding 3-token window over depth-0 identifiers for IPK detection.
    // `INTEGER` → `PRIMARY` → `KEY` in direct succession at depth 0.
    var saw_integer: bool = false;
    var saw_primary: bool = false;
    var is_ipk: bool = false;
    // 2-token window for NOT NULL — both are keyword tokens, not
    // identifiers, so a separate flag tracks the depth-0 `keyword_not`.
    var saw_not: bool = false;
    var is_not_null: bool = false;
    var collation: ast.CollationKind = .binary;
    var check_text: ?[]const u8 = null;
    while (true) {
        switch (p.cur.kind) {
            .comma, .rparen => if (depth == 0) return .{
                .name = name,
                .is_ipk = is_ipk,
                .is_not_null = is_not_null,
                .collation = collation,
                .check_text = check_text,
            },
            else => {},
        }
        if (depth == 0 and p.cur.kind == .keyword_check) {
            // Consume `CHECK ( <expr> )` as a unit. The body is captured
            // as a raw source slice so registerTable can re-parse it
            // against `db.allocator`-backed memory (the AST has to outlive
            // the per-statement arena that produced this column-def).
            p.advance();
            if (p.cur.kind != .lparen) return Error.SyntaxError;
            p.advance();
            const expr_start: u32 = p.cur.start;
            var nested: u32 = 0;
            while (true) {
                switch (p.cur.kind) {
                    .lparen => nested += 1,
                    .rparen => {
                        if (nested == 0) break;
                        nested -= 1;
                    },
                    .eof => return Error.SyntaxError,
                    else => {},
                }
                p.advance();
            }
            const expr_end: u32 = p.cur.start;
            if (expr_end == expr_start) return Error.SyntaxError;
            check_text = p.src[expr_start..expr_end];
            p.advance(); // consume the closing `)`
            saw_integer = false;
            saw_primary = false;
            saw_not = false;
            continue;
        }
        if (depth == 0 and p.cur.kind == .keyword_collate) {
            // Consume `COLLATE <name>` as a unit. The trailing
            // identifier is the collation name; unknown names match
            // sqlite3's "no such collation sequence" error.
            p.advance();
            if (p.cur.kind != .identifier) return Error.SyntaxError;
            const cname = p.cur.slice(p.src);
            collation = collation_mod.kindFromName(cname) orelse return Error.SyntaxError;
            saw_integer = false;
            saw_primary = false;
            saw_not = false;
        } else if (depth == 0 and p.cur.kind == .identifier) {
            const tok = p.cur.slice(p.src);
            if (saw_primary and func_util.eqlIgnoreCase(tok, "key")) {
                is_ipk = true;
                saw_integer = false;
                saw_primary = false;
            } else if (saw_integer and func_util.eqlIgnoreCase(tok, "primary")) {
                saw_primary = true;
            } else if (func_util.eqlIgnoreCase(tok, "integer")) {
                saw_integer = true;
                saw_primary = false;
            } else {
                saw_integer = false;
                saw_primary = false;
            }
            saw_not = false;
        } else if (depth == 0 and p.cur.kind == .keyword_not) {
            saw_not = true;
            saw_integer = false;
            saw_primary = false;
        } else if (depth == 0 and p.cur.kind == .keyword_null and saw_not) {
            is_not_null = true;
            saw_not = false;
            saw_integer = false;
            saw_primary = false;
        } else {
            saw_integer = false;
            saw_primary = false;
            saw_not = false;
        }
        switch (p.cur.kind) {
            .lparen => depth += 1,
            .rparen => depth -= 1,
            .eof => return Error.SyntaxError,
            else => {},
        }
        p.advance();
    }
}
