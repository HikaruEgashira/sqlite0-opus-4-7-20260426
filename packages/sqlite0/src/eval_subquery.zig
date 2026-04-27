//! Subquery evaluators extracted from `eval.zig`.
//!
//! Iter22.B introduced scalar subqueries; Iter22.C adds `IN (SELECT)`
//! and `EXISTS (SELECT)`. The cluster is cohesive: every function here
//! dispatches to `engine.executeSelect` through `ctx.db`, applies a
//! shape rule (column count, row count) to the result, and reduces it
//! to a `Value`. Splitting it out keeps `eval.zig` under the 500-line
//! discipline (CLAUDE.md "Module Splitting Rules").
//!
//! `ctx.db` is required. The only callers without one are parse-time
//! VALUES tuples (`stmt.parseValuesTuple`); subqueries there surface a
//! `SyntaxError` since recursing into the engine is meaningless before
//! a Database exists.

const std = @import("std");
const ast = @import("ast.zig");
const ops = @import("ops.zig");
const value_mod = @import("value.zig");
const stmt_mod = @import("stmt.zig");
const func_util = @import("func_util.zig");
const eval = @import("eval.zig");

const Value = value_mod.Value;
const Error = ops.Error;

/// Build the outer-frame stack the inner SELECT will see: the caller's
/// existing outer_frames extended by one — the caller's *current* frame
/// (per-row binding plus column metadata). Allocated in `ctx.allocator`
/// (per-statement arena), so freeing is implicit at statement teardown.
fn extendOuter(ctx: eval.EvalContext) Error![]eval.OuterFrame {
    const out = try ctx.allocator.alloc(eval.OuterFrame, ctx.outer_frames.len + 1);
    @memcpy(out[0..ctx.outer_frames.len], ctx.outer_frames);
    out[ctx.outer_frames.len] = .{
        .current_row = ctx.current_row,
        .columns = ctx.columns,
        .column_qualifiers = ctx.column_qualifiers,
    };
    return out;
}

/// Run a scalar subquery (`(SELECT ...)`) and reduce it to a single Value.
/// Semantics match sqlite3 (verified against 3.51.0 on 2026-04-26):
/// - 0 rows                         → NULL
/// - any rows × exactly 1 column    → first row's value (multi-row does
///                                    NOT error; sqlite3 silently picks
///                                    the first row produced)
/// - rows × ≠1 columns              → `Error.ColumnCountMismatch`
///   (sqlite3 catches this at parse time; our tree-walking impl catches
///   it at execute time with the same observable outcome)
pub fn evalScalarSubquery(ctx: eval.EvalContext, sq: *const stmt_mod.ParsedSelect) Error!Value {
    const db = ctx.db orelse return Error.SyntaxError;
    const engine = @import("engine.zig");
    const outer = try extendOuter(ctx);
    const rows = try engine.executeSelectWithOuter(db, ctx.allocator, sq.*, outer);
    if (rows.len == 0) return Value.null;
    const first = rows[0];
    if (first.len != 1) return Error.ColumnCountMismatch;
    return func_util.dupeValue(ctx.allocator, first[0]);
}

/// `value [NOT] IN (SELECT ...)` — runs the subquery, projects the first
/// column, and delegates the three-valued-logic reduction to
/// `ops.applyIn`. The subquery must produce exactly one column when it
/// returns any rows; an empty result is well-defined (returns 0 / NOT
/// returns 1 regardless of left, including NULL — verified against
/// sqlite3 3.51.0 on 2026-04-26).
pub fn evalInSubquery(ctx: eval.EvalContext, is: ast.Expr.InSubquery) Error!Value {
    const db = ctx.db orelse return Error.SyntaxError;
    const engine = @import("engine.zig");
    const left = try eval.evalExpr(ctx, is.value);
    defer ops.freeValue(ctx.allocator, left);

    const outer = try extendOuter(ctx);
    const rows = try engine.executeSelectWithOuter(db, ctx.allocator, is.subquery.*, outer);
    if (rows.len > 0 and rows[0].len != 1) return Error.ColumnCountMismatch;

    var items: std.ArrayList(Value) = .empty;
    defer items.deinit(ctx.allocator);
    try items.ensureTotalCapacity(ctx.allocator, rows.len);
    for (rows) |row| items.appendAssumeCapacity(row[0]);

    const result = ops.applyIn(left, items.items);
    return if (is.negated) ops.logicalNot(result) else result;
}

/// `EXISTS (SELECT ...)` — truthy iff the subquery produces ≥1 row.
/// Column count is ignored (sqlite3 quirk: `EXISTS (SELECT 1, 2)` is
/// legal). Returns concrete 0/1, never NULL — `NOT EXISTS` is wrapped
/// by the parser as `logical_not(exists)` and is therefore also
/// always-defined.
pub fn evalExists(ctx: eval.EvalContext, sq: *const stmt_mod.ParsedSelect) Error!Value {
    const db = ctx.db orelse return Error.SyntaxError;
    const engine = @import("engine.zig");
    const outer = try extendOuter(ctx);
    const rows = try engine.executeSelectWithOuter(db, ctx.allocator, sq.*, outer);
    return ops.boolValue(rows.len > 0);
}
