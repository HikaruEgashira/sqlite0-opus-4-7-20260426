//! DDL statement parsing — currently just CREATE TABLE. Split from `stmt.zig`
//! when that file crossed the 500-line trigger (CLAUDE.md "Module Splitting
//! Rules"). Future ALTER TABLE / DROP TABLE / CREATE INDEX go here too.

const std = @import("std");
const ops = @import("ops.zig");
const parser_mod = @import("parser.zig");
const func_util = @import("func_util.zig");

const Error = ops.Error;
const Parser = parser_mod.Parser;

/// One column in a parsed CREATE TABLE. `is_ipk` is set only when the
/// column-def's type+constraint stream matches the literal sequence
/// `INTEGER PRIMARY KEY` (case-insensitive, depth-0). sqlite3 reserves
/// IPK aliasing for that exact spelling — `INT PRIMARY KEY`,
/// `BIGINT PRIMARY KEY`, `INTEGER PRIMARY KEY DESC` are NOT aliases
/// (the last creates a regular index instead). `is_not_null` (Iter29.B)
/// is set when the column-constraint stream contains `NOT NULL` at
/// depth 0; sqlite3 enforces this at INSERT/UPDATE time. PRIMARY KEY
/// does NOT imply NOT NULL in sqlite3 (legacy quirk: even non-IPK PK
/// columns accept NULL), so the two flags are independent.
pub const ParsedColumn = struct {
    name: []const u8,
    is_ipk: bool = false,
    is_not_null: bool = false,
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
/// Captures the column name and scans the trailing token stream for two
/// signals at depth 0: the literal `INTEGER PRIMARY KEY` triple (IPK
/// aliasing — Iter28) and the `NOT NULL` pair (constraint enforcement
/// — Iter29.B). Everything else is consumed and discarded —
/// `x INT DEFAULT (1+1)`, `x VARCHAR(255)`, `x TEXT COLLATE NOCASE`,
/// etc. all parse without their semantics being captured.
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
    while (true) {
        switch (p.cur.kind) {
            .comma, .rparen => if (depth == 0) return .{
                .name = name,
                .is_ipk = is_ipk,
                .is_not_null = is_not_null,
            },
            else => {},
        }
        if (depth == 0 and p.cur.kind == .identifier) {
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
