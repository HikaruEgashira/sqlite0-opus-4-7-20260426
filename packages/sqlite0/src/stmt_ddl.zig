//! DDL statement parsing — currently just CREATE TABLE. Split from `stmt.zig`
//! when that file crossed the 500-line trigger (CLAUDE.md "Module Splitting
//! Rules"). Future ALTER TABLE / DROP TABLE / CREATE INDEX go here too.

const std = @import("std");
const ops = @import("ops.zig");
const parser_mod = @import("parser.zig");

const Error = ops.Error;
const Parser = parser_mod.Parser;

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
    columns: [][]const u8,
    source_text: []const u8,
};

/// `CREATE TABLE <name> ( <col-def> [, <col-def> ...] )`
///
/// Each col-def is a column name optionally followed by a type-name and any
/// column-constraints; the type and constraints are consumed and discarded
/// (Iter14.B). SQLite3 dynamic typing means the absence of constraint
/// enforcement is observationally equivalent for Phase 2.
pub fn parseCreateTableStatement(p: *Parser) !ParsedCreateTable {
    const stmt_start: u32 = p.cur.start;
    try p.expect(.keyword_create);
    try p.expect(.keyword_table);

    if (p.cur.kind != .identifier) return Error.SyntaxError;
    const name = p.cur.slice(p.src);
    p.advance();

    try p.expect(.lparen);

    var columns: std.ArrayList([]const u8) = .empty;
    errdefer columns.deinit(p.allocator);

    while (true) {
        const col_name = try parseColumnDef(p);
        try columns.append(p.allocator, col_name);
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

/// Column-def: `<name> [<type-name> ...] [<column-constraint> ...]`. We only
/// care about the column name; everything until the next `,` or `)` at
/// paren-depth 0 is consumed and ignored. This permits `x INTEGER NOT NULL`,
/// `x INT DEFAULT (1+1)`, `x VARCHAR(255)`, etc.
fn parseColumnDef(p: *Parser) ![]const u8 {
    if (p.cur.kind != .identifier) return Error.SyntaxError;
    const name = p.cur.slice(p.src);
    p.advance();
    var depth: u32 = 0;
    while (true) {
        switch (p.cur.kind) {
            .comma, .rparen => if (depth == 0) return name,
            else => {},
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
