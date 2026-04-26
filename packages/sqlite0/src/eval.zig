//! AST evaluator (ADR-0002).
//!
//! `evalExpr` walks an `ast.Expr` and produces a `Value` allocated from
//! `ctx.allocator`. The AST itself is read-only; ownership of literal text
//! bytes stays with the AST until the caller invokes `expr.deinit`. We dupe
//! TEXT/BLOB bytes during evaluation so the resulting Value survives AST
//! teardown.
//!
//! `current_row` is the placeholder for Iter8.D's column-reference resolution
//! (`SELECT x FROM (VALUES ...)`). It is unused while only literal/arithmetic/
//! concat/unary/compare nodes exist.

const std = @import("std");
const ast = @import("ast.zig");
const ops = @import("ops.zig");
const lex = @import("lex.zig");
const value_mod = @import("value.zig");

const Value = value_mod.Value;
const Expr = ast.Expr;
const Error = ops.Error;

pub const EvalContext = struct {
    allocator: std.mem.Allocator,
    current_row: []const Value = &.{},
};

pub fn evalExpr(ctx: EvalContext, expr: *const Expr) Error!Value {
    return switch (expr.*) {
        .literal => |v| dupeLiteral(ctx.allocator, v),
        .binary_arith => |b| try evalBinaryArith(ctx, b),
        .binary_concat => |b| try evalBinaryConcat(ctx, b),
        .unary_negate => |operand| try evalUnaryNegate(ctx, operand),
        .compare => |c| try evalCompare(ctx, c),
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

test "eval: literal text dupes bytes" {
    const allocator = std.testing.allocator;
    const text = try allocator.dupe(u8, "hi");
    const node = try ast.makeLiteral(allocator, Value{ .text = text });
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    defer ops.freeValue(allocator, v);
    try std.testing.expectEqualStrings("hi", v.text);
    try std.testing.expect(v.text.ptr != text.ptr);
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

test "eval: binary_arith mul real propagates" {
    const allocator = std.testing.allocator;
    const left = try ast.makeLiteral(allocator, Value{ .integer = 3 });
    const right = try ast.makeLiteral(allocator, Value{ .real = 1.5 });
    const node = try ast.makeBinaryArith(allocator, .mul, left, right);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(@as(f64, 4.5), v.real);
}

test "eval: binary_arith sub propagates NULL" {
    const allocator = std.testing.allocator;
    const left = try ast.makeLiteral(allocator, Value.null);
    const right = try ast.makeLiteral(allocator, Value{ .integer = 1 });
    const node = try ast.makeBinaryArith(allocator, .sub, left, right);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(Value.null, v);
}

test "eval: unary_negate" {
    const allocator = std.testing.allocator;
    const inner = try ast.makeLiteral(allocator, Value{ .integer = 5 });
    const node = try ast.makeUnaryNegate(allocator, inner);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(@as(i64, -5), v.integer);
}

test "eval: compare lt true" {
    const allocator = std.testing.allocator;
    const left = try ast.makeLiteral(allocator, Value{ .integer = 1 });
    const right = try ast.makeLiteral(allocator, Value{ .integer = 2 });
    const node = try ast.makeCompare(allocator, .lt, left, right);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    try std.testing.expectEqual(@as(i64, 1), v.integer);
}

test "eval: binary_concat allocates result" {
    const allocator = std.testing.allocator;
    const left = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "ab") });
    const right = try ast.makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "cd") });
    const node = try ast.makeBinaryConcat(allocator, left, right);
    defer node.deinit(allocator);
    const v = try evalExpr(.{ .allocator = allocator }, node);
    defer ops.freeValue(allocator, v);
    try std.testing.expectEqualStrings("abcd", v.text);
}
