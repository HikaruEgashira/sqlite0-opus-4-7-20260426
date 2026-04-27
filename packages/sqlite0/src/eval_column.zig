//! Column-reference resolver extracted from `eval.zig`.
//!
//! Handles both inner-frame lookup (current row + qualifiers) and the
//! correlated-subquery walk through `EvalContext.outer_frames` (Iter22.D).
//! sqlite3's rule: "innermost scope that has the name wins" — once a
//! frame produces a match, that wins; ambiguity is only checked within
//! a single frame, not across frames.
//!
//! Split out of eval.zig to keep that module under the 500-line
//! discipline (CLAUDE.md "Module Splitting Rules") after Iter22.D added
//! the outer-frame fallback path. Pure resolution logic — no eval
//! recursion, no engine dependency.

const std = @import("std");
const ast = @import("ast.zig");
const ops = @import("ops.zig");
const value_mod = @import("value.zig");
const func_util = @import("func_util.zig");
const eval = @import("eval.zig");

const Value = value_mod.Value;
const Error = ops.Error;

/// Resolve a column reference against `ctx`'s current frame, falling back
/// to outer frames (innermost-out) for correlated subqueries. Returns a
/// fresh `Value` owned by `ctx.allocator` (TEXT/BLOB bytes duped).
/// Unknown name → `Error.SyntaxError`. Ambiguous match within a single
/// frame → `Error.SyntaxError`.
pub fn evalColumnRef(ctx: eval.EvalContext, ref: ast.Expr.ColumnRef) Error!Value {
    if (try resolveInFrame(ctx.allocator, ref, ctx.current_row, ctx.columns, ctx.column_qualifiers)) |v| {
        return v;
    }
    var i: usize = ctx.outer_frames.len;
    while (i > 0) {
        i -= 1;
        const f = ctx.outer_frames[i];
        if (try resolveInFrame(ctx.allocator, ref, f.current_row, f.columns, f.column_qualifiers)) |v| {
            return v;
        }
    }
    return Error.SyntaxError;
}

fn resolveInFrame(
    allocator: std.mem.Allocator,
    ref: ast.Expr.ColumnRef,
    current_row: []const Value,
    columns: []const []const u8,
    column_qualifiers: []const []const u8,
) Error!?Value {
    var found: ?usize = null;
    for (columns, 0..) |col, i| {
        if (!func_util.eqlIgnoreCase(ref.name, col)) continue;
        if (ref.qualifier) |q| {
            if (i >= column_qualifiers.len) continue;
            if (!func_util.eqlIgnoreCase(q, column_qualifiers[i])) continue;
        }
        if (found != null) return Error.SyntaxError; // ambiguous within frame
        found = i;
    }
    if (found) |idx| return try func_util.dupeValue(allocator, current_row[idx]);
    return null;
}
