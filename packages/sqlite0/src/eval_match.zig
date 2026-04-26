//! `LIKE` / `GLOB` evaluation extracted from `eval.zig`.
//!
//! Split out so eval.zig can stay under the 500-line discipline (CLAUDE.md
//! "Module Splitting Rules") after Iter22.B added the scalar-subquery
//! evaluator. The pattern-matching path is cohesive: it owns ESCAPE byte
//! resolution and dispatches to `match.applyLike` / `match.applyGlob`.
//! Moving it out also localises the `match` module dependency.
//!
//! Operand sub-expressions still recurse through `eval.evalExpr` — the
//! eval ↔ eval_match cycle is benign at the Zig import level (lazy
//! resolution, no recursive type instantiation).

const std = @import("std");
const ast = @import("ast.zig");
const ops = @import("ops.zig");
const value_mod = @import("value.zig");
const match = @import("match.zig");
const eval = @import("eval.zig");

const Value = value_mod.Value;
const Error = ops.Error;

/// Evaluate `value LIKE pattern [ESCAPE esc]` or `value GLOB pattern`.
/// Returns 1/0/NULL per SQL three-valued logic. ESCAPE NULL collapses
/// the whole result to NULL (sqlite3 quirk).
pub fn evalLike(ctx: eval.EvalContext, l: ast.Expr.Like) Error!Value {
    const value = try eval.evalExpr(ctx, l.value);
    defer ops.freeValue(ctx.allocator, value);
    const pattern = try eval.evalExpr(ctx, l.pattern);
    defer ops.freeValue(ctx.allocator, pattern);

    var escape_byte: ?u8 = null;
    var escape_value: Value = Value.null;
    defer ops.freeValue(ctx.allocator, escape_value);
    if (l.escape) |esc_expr| {
        escape_value = try eval.evalExpr(ctx, esc_expr);
        // sqlite3: ESCAPE NULL → entire LIKE result is NULL.
        if (escape_value == .null) return Value.null;
        const bytes = switch (escape_value) {
            .text => |t| t,
            .blob => |b| b,
            else => return Error.InvalidEscape,
        };
        if (bytes.len != 1) return Error.InvalidEscape;
        escape_byte = bytes[0];
    }

    const result = switch (l.op) {
        .like => try match.applyLike(ctx.allocator, value, pattern, escape_byte),
        .glob => try match.applyGlob(ctx.allocator, value, pattern),
    };
    return if (l.negated) ops.logicalNot(result) else result;
}
