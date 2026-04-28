//! DELETE / UPDATE statement parsing.
//!
//! Kept out of `stmt.zig` so that file can stay under the 500-line
//! discipline (CLAUDE.md "Module Splitting Rules"). Execution of these
//! statements lives in `engine.zig`; this file only describes the parsed
//! shape and walks the parser.

const std = @import("std");
const ops = @import("ops.zig");
const ast = @import("ast.zig");
const parser_mod = @import("parser.zig");
const stmt_mod = @import("stmt.zig");
const select = @import("select.zig");
const func_util = @import("func_util.zig");

const Error = ops.Error;
const Parser = parser_mod.Parser;

/// `DELETE FROM <name> [WHERE <expr>] [RETURNING ...]`. Without WHERE,
/// every row in the table is removed (sqlite3 behavior). RETURNING
/// captures the pre-deletion row state — see Iter31.AE.
pub const ParsedDelete = struct {
    table: []const u8,
    where: ?*ast.Expr,
    returning: ?[]select.SelectItem = null,
};

/// `UPDATE <name> SET <col> = <expr> [, <col> = <expr>]... [WHERE <expr>]
/// [RETURNING ...]`. `assignments` is a parallel array of column names +
/// value expressions, in the order they appear in the SET clause.
/// Duplicates are flagged at execute time (sqlite3 errors with "duplicate
/// column name in SET" too). RETURNING captures the post-update row
/// state — see Iter31.AE.
pub const ParsedUpdate = struct {
    table: []const u8,
    assignments: []Assignment,
    where: ?*ast.Expr,
    returning: ?[]select.SelectItem = null,
    /// Iter31.AH — `UPDATE OR <action>` conflict resolution. Default `.abort`
    /// preserves prior behavior. Action enum is shared with `ParsedInsert`.
    conflict_action: stmt_mod.ParsedInsert.ConflictAction = .abort,

    pub const Assignment = struct {
        column: []const u8,
        value: *ast.Expr,
    };
};

pub fn parseDeleteStatement(p: *Parser) !ParsedDelete {
    try p.expect(.keyword_delete);
    try p.expect(.keyword_from);
    if (p.cur.kind != .identifier) return Error.SyntaxError;
    const table = p.cur.slice(p.src);
    p.advance();

    var where_ast: ?*ast.Expr = null;
    errdefer if (where_ast) |w| w.deinit(p.allocator);
    if (p.cur.kind == .keyword_where) {
        p.advance();
        where_ast = try p.parseExpr();
    }
    const returning = try stmt_mod.parseOptionalReturning(p);
    return .{ .table = table, .where = where_ast, .returning = returning };
}

pub fn parseUpdateStatement(p: *Parser) !ParsedUpdate {
    try p.expect(.keyword_update);
    // Iter31.AH — optional `OR <action>` between UPDATE and the table name.
    // Mirrors INSERT's parser (Iter31.AG); the action names are not reserved
    // in sqlite3 so we identifier-match case-insensitively.
    var conflict_action: stmt_mod.ParsedInsert.ConflictAction = .abort;
    if (p.cur.kind == .keyword_or) {
        p.advance();
        const action_text = switch (p.cur.kind) {
            .identifier => p.cur.slice(p.src),
            .keyword_rollback => "rollback",
            else => return Error.SyntaxError,
        };
        const eq = func_util.eqlIgnoreCase;
        if (eq(action_text, "ignore")) {
            conflict_action = .ignore;
        } else if (eq(action_text, "replace")) {
            conflict_action = .replace;
        } else if (eq(action_text, "abort")) {
            conflict_action = .abort;
        } else if (eq(action_text, "fail")) {
            conflict_action = .fail;
        } else if (eq(action_text, "rollback")) {
            conflict_action = .rollback;
        } else {
            return Error.SyntaxError;
        }
        p.advance();
    }
    if (p.cur.kind != .identifier) return Error.SyntaxError;
    const table = p.cur.slice(p.src);
    p.advance();
    try p.expect(.keyword_set);

    var assignments: std.ArrayList(ParsedUpdate.Assignment) = .empty;
    errdefer {
        for (assignments.items) |a| a.value.deinit(p.allocator);
        assignments.deinit(p.allocator);
    }

    try parseAssignment(p, &assignments);
    while (p.cur.kind == .comma) {
        p.advance();
        try parseAssignment(p, &assignments);
    }

    var where_ast: ?*ast.Expr = null;
    errdefer if (where_ast) |w| w.deinit(p.allocator);
    if (p.cur.kind == .keyword_where) {
        p.advance();
        where_ast = try p.parseExpr();
    }
    const returning = try stmt_mod.parseOptionalReturning(p);
    return .{
        .table = table,
        .assignments = try assignments.toOwnedSlice(p.allocator),
        .where = where_ast,
        .returning = returning,
        .conflict_action = conflict_action,
    };
}

fn parseAssignment(p: *Parser, list: *std.ArrayList(ParsedUpdate.Assignment)) !void {
    if (p.cur.kind != .identifier) return Error.SyntaxError;
    const col = p.cur.slice(p.src);
    p.advance();
    try p.expect(.eq);
    const value = try p.parseExpr();
    list.append(p.allocator, .{ .column = col, .value = value }) catch |err| {
        value.deinit(p.allocator);
        return err;
    };
}
