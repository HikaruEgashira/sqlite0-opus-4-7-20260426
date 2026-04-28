//! AST evaluator (ADR-0002). `evalExpr` walks an `ast.Expr` and produces a
//! `Value` allocated from `ctx.allocator`. AST is read-only; we dupe
//! TEXT/BLOB bytes so returned Values outlive `expr.deinit`.

const std = @import("std");
const ast = @import("ast.zig");
const ops = @import("ops.zig");
const lex = @import("lex.zig");
const value_mod = @import("value.zig");
const funcs = @import("funcs.zig");
const func_util = @import("func_util.zig");
const database = @import("database.zig");
const eval_match = @import("eval_match.zig");
const eval_subquery = @import("eval_subquery.zig");
const eval_column = @import("eval_column.zig");
const eval_cast = @import("eval_cast.zig");
const collation = @import("collation.zig");

const Value = value_mod.Value;
const Expr = ast.Expr;
const Error = ops.Error;

pub const EvalContext = struct {
    allocator: std.mem.Allocator,
    /// Source-row Values (positional). Bytes owned by the row producer;
    /// `evalExpr` dupes TEXT/BLOB column refs to outlive the row.
    current_row: []const Value = &.{},
    /// Column names parallel to `current_row`. Borrowed from SQL source.
    columns: []const []const u8 = &.{},
    /// Per-column qualifier (table alias / name) parallel to `columns`.
    /// Drives qualified-ref match and ambiguity checks; empty when FROM
    /// is absent or the source path didn't populate qualifiers.
    column_qualifiers: []const []const u8 = &.{},
    /// Iter31.R — per-column default collation parallel to `columns`.
    /// Empty when the source path didn't carry schema collation info
    /// (treat as all-binary). Read by `collation.pickWithSchema` so a
    /// bare ref to a `COLLATE NOCASE`-declared column drives compares
    /// case-insensitively without an explicit wrapper.
    column_collations: []const ast.CollationKind = &.{},
    /// Per-group aggregate-call substitution map. `aggregate.zig`
    /// pre-computes each aggregate's value for the current group then
    /// puts pointer→Value pairs here; `evalFuncCall` returns a duped
    /// value on hit, skipping arg eval entirely (essential — count(x)
    /// must not re-resolve `x` in per-group scope). Null = scalar path.
    agg_values: ?*const AggregateValues = null,
    /// Live Database handle for paths that need state — set by
    /// `engine.dispatchOne` for every SELECT/DML expression eval so
    /// scalar subqueries dispatch back through `engine.executeSelect`
    /// without eval depending on engine. Null only on the parse-time
    /// `VALUES (..)` tuple path; subqueries there surface a runtime error.
    db: ?*database.Database = null,
    /// Enclosing-SELECT frames for correlated subqueries. Innermost outer
    /// is the last entry. `evalColumnRef` falls back here when local
    /// `columns` don't resolve. `eval_subquery.*` appends the current
    /// frame when descending so any-depth correlation sees every scope.
    outer_frames: []const OuterFrame = &.{},
};

/// One enclosing-SELECT frame snapshot. Mirrors the per-row EvalContext
/// fields so `evalColumnRef` applies the same resolution rules; bytes in
/// `current_row` borrow from the outer frame's row producer.
pub const OuterFrame = struct {
    current_row: []const Value = &.{},
    columns: []const []const u8 = &.{},
    column_qualifiers: []const []const u8 = &.{},
    column_collations: []const ast.CollationKind = &.{},
};

/// Pointer-keyed map from func_call AST nodes to their finalised aggregate
/// values for the current group. Owned and populated by `aggregate.zig`.
pub const AggregateValues = std.AutoHashMapUnmanaged(*const ast.Expr, Value);

pub fn evalExpr(ctx: EvalContext, expr: *const Expr) Error!Value {
    return switch (expr.*) {
        .literal => |v| func_util.dupeValue(ctx.allocator, v),
        .column_ref => |cr| try eval_column.evalColumnRef(ctx, cr),
        .binary_arith => |b| try evalBinaryArith(ctx, b),
        .binary_concat => |b| try evalBinaryConcat(ctx, b),
        .unary_negate => |operand| try evalUnaryNegate(ctx, operand),
        .unary_bit_not => |operand| try evalUnaryBitNot(ctx, operand),
        .compare => |c| try evalCompare(ctx, c),
        .eq_check => |e| try evalEqCheck(ctx, e),
        .is_check => |e| try evalIsCheck(ctx, e),
        .is_truthy => |e| try evalIsTruthy(ctx, e),
        .between => |b| try evalBetween(ctx, b),
        .in_list => |il| try evalInList(ctx, il),
        .logical_and => |b| try evalLogicalAnd(ctx, b),
        .logical_or => |b| try evalLogicalOr(ctx, b),
        .logical_not => |operand| try evalLogicalNot(ctx, operand),
        .case_expr => |ce| try evalCaseExpr(ctx, ce),
        .func_call => |fc| try evalFuncCall(ctx, expr, fc),
        .like => |l| try eval_match.evalLike(ctx, l),
        .subquery => |sq| try eval_subquery.evalScalarSubquery(ctx, sq),
        .in_subquery => |is| try eval_subquery.evalInSubquery(ctx, is),
        .exists => |sq| try eval_subquery.evalExists(ctx, sq),
        .cast => |c| try evalCast(ctx, c),
        // COLLATE is a parse-time hint; the value flows through unchanged.
        // Comparison sites peek the wrapper at the AST layer (see
        // `evalCompare` / `evalEqCheck` / `evalIsCheck` / `evalBetween` /
        // `evalInList`).
        .collate => |c| try evalExpr(ctx, c.value),
    };
}

fn evalCast(ctx: EvalContext, c: Expr.Cast) Error!Value {
    const inner = try evalExpr(ctx, c.value);
    defer ops.freeValue(ctx.allocator, inner);
    return eval_cast.castValue(ctx.allocator, inner, c.target);
}

fn evalBinaryArith(ctx: EvalContext, b: Expr.BinaryArith) Error!Value {
    const left = try evalExpr(ctx, b.left);
    errdefer ops.freeValue(ctx.allocator, left);
    const right = try evalExpr(ctx, b.right);
    defer ops.freeValue(ctx.allocator, right);
    // Bitwise operators take a separate ops dispatch — they coerce
    // operands to i64 (REAL truncates) rather than promoting to
    // numeric affinity, so bundling them through `applyArith` would
    // mean re-encoding the same coercion through TokenKind.
    const out = switch (b.op) {
        .bit_and, .bit_or, .shift_left, .shift_right => ops.applyBitwise(bitOp(b.op), left, right),
        else => blk: {
            const tok_op: lex.TokenKind = switch (b.op) {
                .add => .plus,
                .sub => .minus,
                .mul => .star,
                .div => .slash,
                .mod => .percent,
                else => unreachable,
            };
            break :blk try ops.applyArith(tok_op, left, right);
        },
    };
    ops.freeValue(ctx.allocator, left);
    return out;
}

fn bitOp(op: ast.BinaryOp) ops.BitOp {
    return switch (op) {
        .bit_and => .bit_and,
        .bit_or => .bit_or,
        .shift_left => .shift_left,
        .shift_right => .shift_right,
        else => unreachable,
    };
}

fn evalBinaryConcat(ctx: EvalContext, b: Expr.BinaryConcat) Error!Value {
    const left = try evalExpr(ctx, b.left);
    errdefer ops.freeValue(ctx.allocator, left);
    const right = try evalExpr(ctx, b.right);
    defer ops.freeValue(ctx.allocator, right);
    const out = try ops.concatValues(ctx.allocator, left, right);
    ops.freeValue(ctx.allocator, left);
    return out;
}

fn evalUnaryNegate(ctx: EvalContext, operand: *Expr) Error!Value {
    const inner = try evalExpr(ctx, operand);
    defer ops.freeValue(ctx.allocator, inner);
    return ops.negateValue(inner);
}

fn evalUnaryBitNot(ctx: EvalContext, operand: *Expr) Error!Value {
    const inner = try evalExpr(ctx, operand);
    defer ops.freeValue(ctx.allocator, inner);
    return ops.bitNotValue(inner);
}

fn evalCompare(ctx: EvalContext, c: Expr.Compare) Error!Value {
    const kind = collation.pickWithSchema(c.left, c.right, ctx.columns, ctx.column_qualifiers, ctx.column_collations);
    const left = try evalExpr(ctx, c.left);
    errdefer ops.freeValue(ctx.allocator, left);
    const right = try evalExpr(ctx, c.right);
    defer ops.freeValue(ctx.allocator, right);
    const tok_op: lex.TokenKind = switch (c.op) {
        .lt => .lt,
        .le => .le,
        .gt => .gt,
        .ge => .ge,
    };
    const out = collation.applyComparisonCollated(tok_op, left, right, kind);
    ops.freeValue(ctx.allocator, left);
    return out;
}

fn evalEqCheck(ctx: EvalContext, e: Expr.EqCheck) Error!Value {
    const kind = collation.pickWithSchema(e.left, e.right, ctx.columns, ctx.column_qualifiers, ctx.column_collations);
    const left = try evalExpr(ctx, e.left);
    errdefer ops.freeValue(ctx.allocator, left);
    const right = try evalExpr(ctx, e.right);
    defer ops.freeValue(ctx.allocator, right);
    const tok_op: lex.TokenKind = switch (e.op) {
        .eq => .eq,
        .ne => .ne,
    };
    const out = collation.applyEqualityCollated(tok_op, left, right, kind);
    ops.freeValue(ctx.allocator, left);
    return out;
}

fn evalIsCheck(ctx: EvalContext, e: Expr.IsCheck) Error!Value {
    const kind = collation.pickWithSchema(e.left, e.right, ctx.columns, ctx.column_qualifiers, ctx.column_collations);
    const left = try evalExpr(ctx, e.left);
    errdefer ops.freeValue(ctx.allocator, left);
    const right = try evalExpr(ctx, e.right);
    defer ops.freeValue(ctx.allocator, right);
    const eq = collation.identicalValuesCollated(left, right, kind);
    ops.freeValue(ctx.allocator, left);
    return ops.boolValue(if (e.negated) !eq else eq);
}

fn evalIsTruthy(ctx: EvalContext, e: Expr.IsTruthy) Error!Value {
    const v = try evalExpr(ctx, e.value);
    defer ops.freeValue(ctx.allocator, v);
    const t = ops.truthy(v);
    const matches = if (t) |b| b == e.expect_true else false;
    return ops.boolValue(if (e.negated) !matches else matches);
}

fn evalBetween(ctx: EvalContext, b: Expr.Between) Error!Value {
    // BETWEEN inherits collation from the value expression first, falling
    // back to lo (sqlite3 quirk: lo is checked before hi). This matches
    // `'A' COLLATE NOCASE BETWEEN 'a' AND 'z'` → 1 and
    // `'A' BETWEEN 'a' COLLATE NOCASE AND 'z' COLLATE NOCASE` → 1.
    const lo_kind = collation.pickWithSchema(b.value, b.lo, ctx.columns, ctx.column_qualifiers, ctx.column_collations);
    const hi_kind = collation.pickWithSchema(b.value, b.hi, ctx.columns, ctx.column_qualifiers, ctx.column_collations);
    const value = try evalExpr(ctx, b.value);
    defer ops.freeValue(ctx.allocator, value);
    const lo = try evalExpr(ctx, b.lo);
    defer ops.freeValue(ctx.allocator, lo);
    const hi = try evalExpr(ctx, b.hi);
    defer ops.freeValue(ctx.allocator, hi);
    const ge = collation.applyComparisonCollated(.ge, value, lo, lo_kind);
    const le = collation.applyComparisonCollated(.le, value, hi, hi_kind);
    const conj = ops.logicalAnd(ge, le);
    return if (b.negated) ops.logicalNot(conj) else conj;
}

fn evalInList(ctx: EvalContext, il: Expr.InList) Error!Value {
    // IN list: collation comes from the value side only (sqlite3 ignores
    // per-item COLLATE). Iter31.R: bare column-ref falls back to its
    // schema-default collation.
    const kind = collation.peekKind(il.value) orelse
        collation.columnDefault(il.value, ctx.columns, ctx.column_qualifiers, ctx.column_collations) orelse
        .binary;
    const value = try evalExpr(ctx, il.value);
    defer ops.freeValue(ctx.allocator, value);
    var items: std.ArrayList(Value) = .empty;
    defer {
        for (items.items) |v| ops.freeValue(ctx.allocator, v);
        items.deinit(ctx.allocator);
    }
    try items.ensureTotalCapacity(ctx.allocator, il.items.len);
    for (il.items) |item_expr| {
        items.appendAssumeCapacity(try evalExpr(ctx, item_expr));
    }
    const result = collation.applyInCollated(value, items.items, kind);
    return if (il.negated) ops.logicalNot(result) else result;
}

fn evalLogicalAnd(ctx: EvalContext, b: Expr.LogicalBinary) Error!Value {
    const left = try evalExpr(ctx, b.left);
    defer ops.freeValue(ctx.allocator, left);
    const right = try evalExpr(ctx, b.right);
    defer ops.freeValue(ctx.allocator, right);
    return ops.logicalAnd(left, right);
}

fn evalLogicalOr(ctx: EvalContext, b: Expr.LogicalBinary) Error!Value {
    const left = try evalExpr(ctx, b.left);
    defer ops.freeValue(ctx.allocator, left);
    const right = try evalExpr(ctx, b.right);
    defer ops.freeValue(ctx.allocator, right);
    return ops.logicalOr(left, right);
}

fn evalLogicalNot(ctx: EvalContext, operand: *Expr) Error!Value {
    const v = try evalExpr(ctx, operand);
    defer ops.freeValue(ctx.allocator, v);
    return ops.logicalNot(v);
}

fn evalCaseExpr(ctx: EvalContext, ce: Expr.CaseExpr) Error!Value {
    const subject_opt: ?Value = if (ce.scrutinee) |s| try evalExpr(ctx, s) else null;
    defer if (subject_opt) |sv| ops.freeValue(ctx.allocator, sv);

    for (ce.branches) |branch| {
        const cond = try evalExpr(ctx, branch.when);
        const is_match = blk: {
            if (subject_opt) |sv| {
                const eq = ops.applyEquality(.eq, sv, cond);
                ops.freeValue(ctx.allocator, cond);
                break :blk ops.truthy(eq) orelse false;
            }
            const t = ops.truthy(cond) orelse false;
            ops.freeValue(ctx.allocator, cond);
            break :blk t;
        };
        if (is_match) return try evalExpr(ctx, branch.then);
    }
    if (ce.else_branch) |eb| return try evalExpr(ctx, eb);
    return Value.null;
}

fn evalFuncCall(ctx: EvalContext, expr: *const Expr, fc: Expr.FuncCall) Error!Value {
    // Aggregate substitution: when this func_call is one the aggregate
    // driver pre-computed for the current group, return the precomputed
    // value (duped) without ever evaluating the arguments. Skipping the
    // arg eval is essential — `count(x)` should not error if `x` doesn't
    // resolve in the per-group scope (the value was already accumulated
    // in the source-row scope during the group scan).
    if (ctx.agg_values) |map| {
        if (map.get(expr)) |v| return func_util.dupeValue(ctx.allocator, v);
    }
    var arg_values: std.ArrayList(Value) = .empty;
    defer {
        for (arg_values.items) |v| ops.freeValue(ctx.allocator, v);
        arg_values.deinit(ctx.allocator);
    }
    try arg_values.ensureTotalCapacity(ctx.allocator, fc.args.len);
    for (fc.args) |arg_expr| {
        arg_values.appendAssumeCapacity(try evalExpr(ctx, arg_expr));
    }
    return funcs.call(ctx.allocator, ctx.db, fc.name, arg_values.items);
}

test "eval: literal integer" {
    const allocator = std.testing.allocator;
    const node = try ast.makeLiteral(allocator, Value{ .integer = 7 });
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(@as(i64, 7), v.integer);
}

test "eval: binary_arith add integers" {
    const allocator = std.testing.allocator;
    const left = try ast.makeLiteral(allocator, Value{ .integer = 3 });
    const right = try ast.makeLiteral(allocator, Value{ .integer = 4 });
    const node = try ast.makeBinaryArith(allocator, .add, left, right);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(@as(i64, 7), v.integer);
}

test "eval: eq_check returns 1" {
    const allocator = std.testing.allocator;
    const left = try ast.makeLiteral(allocator, Value{ .integer = 5 });
    const right = try ast.makeLiteral(allocator, Value{ .integer = 5 });
    const node = try ast.makeEqCheck(allocator, .eq, left, right);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(@as(i64, 1), v.integer);
}

test "eval: is_check NULL IS NULL is true" {
    const allocator = std.testing.allocator;
    const left = try ast.makeLiteral(allocator, Value.null);
    const right = try ast.makeLiteral(allocator, Value.null);
    const node = try ast.makeIsCheck(allocator, left, right, false);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(@as(i64, 1), v.integer);
}

test "eval: between inclusive" {
    const allocator = std.testing.allocator;
    const value = try ast.makeLiteral(allocator, Value{ .integer = 5 });
    const lo = try ast.makeLiteral(allocator, Value{ .integer = 1 });
    const hi = try ast.makeLiteral(allocator, Value{ .integer = 10 });
    const node = try ast.makeBetween(allocator, value, lo, hi, false);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(@as(i64, 1), v.integer);
}

test "eval: in_list match" {
    const allocator = std.testing.allocator;
    const value = try ast.makeLiteral(allocator, Value{ .integer = 2 });
    const items = try allocator.alloc(*Expr, 3);
    items[0] = try ast.makeLiteral(allocator, Value{ .integer = 1 });
    items[1] = try ast.makeLiteral(allocator, Value{ .integer = 2 });
    items[2] = try ast.makeLiteral(allocator, Value{ .integer = 3 });
    const node = try ast.makeInList(allocator, value, items, false);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(@as(i64, 1), v.integer);
}

test "eval: logical_and three-valued" {
    const allocator = std.testing.allocator;
    const left = try ast.makeLiteral(allocator, Value.null);
    const right = try ast.makeLiteral(allocator, Value{ .integer = 1 });
    const node = try ast.makeLogicalAnd(allocator, left, right);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(Value.null, v);
}

test "eval: case_expr scrutinee match" {
    const allocator = std.testing.allocator;
    const scrutinee = try ast.makeLiteral(allocator, Value{ .integer = 2 });
    const branches = try allocator.alloc(Expr.CaseBranch, 2);
    branches[0] = .{
        .when = try ast.makeLiteral(allocator, Value{ .integer = 1 }),
        .then = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "a") }),
    };
    branches[1] = .{
        .when = try ast.makeLiteral(allocator, Value{ .integer = 2 }),
        .then = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "b") }),
    };
    const node = try ast.makeCaseExpr(allocator, scrutinee, branches, null);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    defer ops.freeValue(allocator, v);
    try std.testing.expectEqualStrings("b", v.text);
}

test "eval: func_call abs" {
    const allocator = std.testing.allocator;
    const args = try allocator.alloc(*Expr, 1);
    args[0] = try ast.makeLiteral(allocator, Value{ .integer = -7 });
    const node = try ast.makeFuncCall(allocator, "abs", args, false);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(@as(i64, 7), v.integer);
}

test "eval: like matches" {
    const allocator = std.testing.allocator;
    const value = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "hello") });
    const pattern = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "h%o") });
    const node = try ast.makeLike(allocator, .like, value, pattern, null, false);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(@as(i64, 1), v.integer);
}

test "eval: not like" {
    const allocator = std.testing.allocator;
    const value = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "abc") });
    const pattern = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "x%") });
    const node = try ast.makeLike(allocator, .like, value, pattern, null, true);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(@as(i64, 1), v.integer);
}

test "eval: like NULL is NULL" {
    const allocator = std.testing.allocator;
    const value = try ast.makeLiteral(allocator, Value.null);
    const pattern = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "a") });
    const node = try ast.makeLike(allocator, .like, value, pattern, null, false);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(Value.null, v);
}

test "eval: glob case sensitive" {
    const allocator = std.testing.allocator;
    const value = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "abc") });
    const pattern = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "A*") });
    const node = try ast.makeLike(allocator, .glob, value, pattern, null, false);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(@as(i64, 0), v.integer);
}

test "eval: like ESCAPE single byte matches" {
    const allocator = std.testing.allocator;
    const value = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "50%") });
    const pattern = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "50\\%") });
    const escape = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "\\") });
    const node = try ast.makeLike(allocator, .like, value, pattern, escape, false);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(@as(i64, 1), v.integer);
}

test "eval: like ESCAPE multi-byte is InvalidEscape" {
    const allocator = std.testing.allocator;
    const value = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "a") });
    const pattern = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "a") });
    const escape = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "\\\\") });
    const node = try ast.makeLike(allocator, .like, value, pattern, escape, false);
    defer node.deinit(allocator);
    try std.testing.expectError(ops.Error.InvalidEscape, evalExpr(.{ .allocator = allocator }, node));
}

test "eval: like ESCAPE NULL is NULL" {
    const allocator = std.testing.allocator;
    const value = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "a") });
    const pattern = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "a") });
    const escape = try ast.makeLiteral(allocator, Value.null);
    const node = try ast.makeLike(allocator, .like, value, pattern, escape, false);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(Value.null, v);
}
