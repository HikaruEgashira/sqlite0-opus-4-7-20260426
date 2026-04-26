//! AST evaluator (ADR-0002).
//!
//! `evalExpr` walks an `ast.Expr` and produces a `Value` allocated from
//! `ctx.allocator`. The AST itself is read-only; ownership of literal text
//! bytes stays with the AST until the caller invokes `expr.deinit`. We dupe
//! TEXT/BLOB bytes during evaluation so the resulting Value survives AST
//! teardown.
//!
//! `current_row` is the placeholder for Iter8.D's column-reference
//! resolution (`SELECT x FROM (VALUES ...)`). It is unused while the
//! grammar has no `column_ref` node.

const std = @import("std");
const ast = @import("ast.zig");
const ops = @import("ops.zig");
const lex = @import("lex.zig");
const value_mod = @import("value.zig");
const funcs = @import("funcs.zig");
const func_util = @import("func_util.zig");

const Value = value_mod.Value;
const Expr = ast.Expr;
const Error = ops.Error;

pub const EvalContext = struct {
    allocator: std.mem.Allocator,
    /// Values for the row the expression is evaluated against. Empty for
    /// FROM-less SELECT. Bytes here are owned by the row producer (the
    /// FROM-clause materialised rows in `stmt.zig`); `evalExpr` dupes
    /// TEXT/BLOB column refs so the returned `Value` outlives the row.
    current_row: []const Value = &.{},
    /// Column names matching `current_row` positionally (same length).
    /// Borrowed from the SQL source string. Empty for FROM-less SELECT.
    columns: []const []const u8 = &.{},
};

pub fn evalExpr(ctx: EvalContext, expr: *const Expr) Error!Value {
    return switch (expr.*) {
        .literal => |v| dupeLiteral(ctx.allocator, v),
        .column_ref => |name| try evalColumnRef(ctx, name),
        .binary_arith => |b| try evalBinaryArith(ctx, b),
        .binary_concat => |b| try evalBinaryConcat(ctx, b),
        .unary_negate => |operand| try evalUnaryNegate(ctx, operand),
        .compare => |c| try evalCompare(ctx, c),
        .eq_check => |e| try evalEqCheck(ctx, e),
        .is_check => |e| try evalIsCheck(ctx, e),
        .between => |b| try evalBetween(ctx, b),
        .in_list => |il| try evalInList(ctx, il),
        .logical_and => |b| try evalLogicalAnd(ctx, b),
        .logical_or => |b| try evalLogicalOr(ctx, b),
        .logical_not => |operand| try evalLogicalNot(ctx, operand),
        .case_expr => |ce| try evalCaseExpr(ctx, ce),
        .func_call => |fc| try evalFuncCall(ctx, fc),
        .like => |l| try evalLike(ctx, l),
    };
}

fn evalBinaryArith(ctx: EvalContext, b: Expr.BinaryArith) Error!Value {
    const left = try evalExpr(ctx, b.left);
    errdefer ops.freeValue(ctx.allocator, left);
    const right = try evalExpr(ctx, b.right);
    defer ops.freeValue(ctx.allocator, right);
    const tok_op: lex.TokenKind = switch (b.op) {
        .add => .plus,
        .sub => .minus,
        .mul => .star,
        .div => .slash,
        .mod => .percent,
    };
    const out = try ops.applyArith(tok_op, left, right);
    ops.freeValue(ctx.allocator, left);
    return out;
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

fn evalCompare(ctx: EvalContext, c: Expr.Compare) Error!Value {
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
    const out = ops.applyComparison(tok_op, left, right);
    ops.freeValue(ctx.allocator, left);
    return out;
}

fn evalEqCheck(ctx: EvalContext, e: Expr.EqCheck) Error!Value {
    const left = try evalExpr(ctx, e.left);
    errdefer ops.freeValue(ctx.allocator, left);
    const right = try evalExpr(ctx, e.right);
    defer ops.freeValue(ctx.allocator, right);
    const tok_op: lex.TokenKind = switch (e.op) {
        .eq => .eq,
        .ne => .ne,
    };
    const out = ops.applyEquality(tok_op, left, right);
    ops.freeValue(ctx.allocator, left);
    return out;
}

fn evalIsCheck(ctx: EvalContext, e: Expr.IsCheck) Error!Value {
    const left = try evalExpr(ctx, e.left);
    errdefer ops.freeValue(ctx.allocator, left);
    const right = try evalExpr(ctx, e.right);
    defer ops.freeValue(ctx.allocator, right);
    const eq = ops.identicalValues(left, right);
    ops.freeValue(ctx.allocator, left);
    return ops.boolValue(if (e.negated) !eq else eq);
}

fn evalBetween(ctx: EvalContext, b: Expr.Between) Error!Value {
    const value = try evalExpr(ctx, b.value);
    defer ops.freeValue(ctx.allocator, value);
    const lo = try evalExpr(ctx, b.lo);
    defer ops.freeValue(ctx.allocator, lo);
    const hi = try evalExpr(ctx, b.hi);
    defer ops.freeValue(ctx.allocator, hi);
    const ge = ops.applyComparison(.ge, value, lo);
    const le = ops.applyComparison(.le, value, hi);
    const conj = ops.logicalAnd(ge, le);
    return if (b.negated) ops.logicalNot(conj) else conj;
}

fn evalInList(ctx: EvalContext, il: Expr.InList) Error!Value {
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
    const result = ops.applyIn(value, items.items);
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

fn evalFuncCall(ctx: EvalContext, fc: Expr.FuncCall) Error!Value {
    var arg_values: std.ArrayList(Value) = .empty;
    defer {
        for (arg_values.items) |v| ops.freeValue(ctx.allocator, v);
        arg_values.deinit(ctx.allocator);
    }
    try arg_values.ensureTotalCapacity(ctx.allocator, fc.args.len);
    for (fc.args) |arg_expr| {
        arg_values.appendAssumeCapacity(try evalExpr(ctx, arg_expr));
    }
    return funcs.call(ctx.allocator, fc.name, arg_values.items);
}

fn evalLike(ctx: EvalContext, l: ast.Expr.Like) Error!Value {
    const value = try evalExpr(ctx, l.value);
    defer ops.freeValue(ctx.allocator, value);
    const pattern = try evalExpr(ctx, l.pattern);
    defer ops.freeValue(ctx.allocator, pattern);
    const result = try ops.applyLike(ctx.allocator, value, pattern, null);
    return if (l.negated) ops.logicalNot(result) else result;
}

/// Resolve a column reference against the current row. Case-insensitive
/// match per SQL's identifier rules. Returns a fresh `Value` owned by
/// `ctx.allocator` (TEXT/BLOB bytes duped). Unknown name → SyntaxError.
fn evalColumnRef(ctx: EvalContext, name: []const u8) Error!Value {
    for (ctx.columns, 0..) |col, i| {
        if (func_util.eqlIgnoreCase(name, col)) {
            return dupeLiteral(ctx.allocator, ctx.current_row[i]);
        }
    }
    return Error.SyntaxError;
}

/// Dupe TEXT/BLOB bytes so the returned Value outlives the AST node that
/// produced it. INTEGER/REAL/NULL are by-value and copy implicitly.
fn dupeLiteral(allocator: std.mem.Allocator, v: Value) Error!Value {
    return switch (v) {
        .text => |t| Value{ .text = try allocator.dupe(u8, t) },
        .blob => |b| Value{ .blob = try allocator.dupe(u8, b) },
        else => v,
    };
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
    const node = try ast.makeFuncCall(allocator, "abs", args);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(@as(i64, 7), v.integer);
}

test "eval: like matches" {
    const allocator = std.testing.allocator;
    const value = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "hello") });
    const pattern = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "h%o") });
    const node = try ast.makeLike(allocator, value, pattern, false);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(@as(i64, 1), v.integer);
}

test "eval: not like" {
    const allocator = std.testing.allocator;
    const value = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "abc") });
    const pattern = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "x%") });
    const node = try ast.makeLike(allocator, value, pattern, true);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(@as(i64, 1), v.integer);
}

test "eval: like NULL is NULL" {
    const allocator = std.testing.allocator;
    const value = try ast.makeLiteral(allocator, Value.null);
    const pattern = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "a") });
    const node = try ast.makeLike(allocator, value, pattern, false);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(Value.null, v);
}
