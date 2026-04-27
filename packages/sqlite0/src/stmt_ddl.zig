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
/// (the last creates a regular index instead). Other constraints
/// (`NOT NULL`, `DEFAULT (...)`, etc.) on an IPK column are tolerated
/// — only the IPK signal itself is captured here.
pub const ParsedColumn = struct {
    name: []const u8,
    is_ipk: bool = false,
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

/// Column-def: `<name> [<type-name> ...] [<column-constraint> ...]`.
/// Captures the column name and scans the trailing token stream for the
/// IPK signal (literal `INTEGER PRIMARY KEY` at depth 0). Everything
/// else is consumed and discarded — `x INTEGER NOT NULL`,
/// `x INT DEFAULT (1+1)`, `x VARCHAR(255)`, etc. all parse, but only
/// the literal three-token sequence triggers IPK aliasing.
fn parseColumnDef(p: *Parser) !ParsedColumn {
    if (p.cur.kind != .identifier) return Error.SyntaxError;
    const name = p.cur.slice(p.src);
    p.advance();
    var depth: u32 = 0;
    // Sliding 3-token window over depth-0 identifiers. `is_ipk` flips
    // true once we observe `INTEGER`, then `PRIMARY`, then `KEY` in
    // direct succession at depth 0. Any non-identifier or depth change
    // resets the window.
    var saw_integer: bool = false;
    var saw_primary: bool = false;
    var is_ipk: bool = false;
    while (true) {
        switch (p.cur.kind) {
            .comma, .rparen => if (depth == 0) return .{ .name = name, .is_ipk = is_ipk },
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
        } else {
            saw_integer = false;
            saw_primary = false;
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
