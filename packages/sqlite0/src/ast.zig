//! Expression AST nodes (ADR-0002).
//!
//! Iter8.A introduced `literal` + `binary_arith`. Iter8.B extends with
//! `binary_concat`, `unary_negate`, and `compare` so the bottom of the
//! precedence stack (mul/div/mod, ||, unary -, lt/le/gt/ge) is fully
//! AST-driven. Iter8.C will add the boolean / IS / BETWEEN / IN / CASE /
//! function-call layers; Iter8.D adds `column_ref`.
//!
//! Each node owns its children and any heap bytes inside `literal` values;
//! `Expr.deinit` recursively releases everything.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");

const Value = value_mod.Value;

pub const BinaryOp = enum { add, sub, mul, div, mod };
pub const CompareOp = enum { lt, le, gt, ge };

pub const Expr = union(enum) {
    literal: Value,
    binary_arith: BinaryArith,
    binary_concat: BinaryConcat,
    unary_negate: *Expr,
    compare: Compare,

    pub const BinaryArith = struct {
        op: BinaryOp,
        left: *Expr,
        right: *Expr,
    };

    pub const BinaryConcat = struct {
        left: *Expr,
        right: *Expr,
    };

    pub const Compare = struct {
        op: CompareOp,
        left: *Expr,
        right: *Expr,
    };

    pub fn deinit(self: *Expr, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .literal => |v| ops.freeValue(allocator, v),
            .binary_arith => |b| {
                b.left.deinit(allocator);
                b.right.deinit(allocator);
            },
            .binary_concat => |b| {
                b.left.deinit(allocator);
                b.right.deinit(allocator);
            },
            .unary_negate => |inner| inner.deinit(allocator),
            .compare => |c| {
                c.left.deinit(allocator);
                c.right.deinit(allocator);
            },
        }
        allocator.destroy(self);
    }
};

/// Allocate a new literal node taking ownership of `v` (TEXT/BLOB bytes
/// must be owned by `allocator`). On allocation failure the caller is
/// responsible for releasing `v` — the function does not free it.
pub fn makeLiteral(allocator: std.mem.Allocator, v: Value) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .literal = v };
    return node;
}

/// Allocate a new binary-arithmetic node taking ownership of `left` / `right`.
/// On allocation failure the caller is responsible for releasing both
/// children — the function does not free them.
pub fn makeBinaryArith(allocator: std.mem.Allocator, op: BinaryOp, left: *Expr, right: *Expr) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .binary_arith = .{ .op = op, .left = left, .right = right } };
    return node;
}

/// Allocate a new `||` concat node. Same ownership rules as `makeBinaryArith`.
pub fn makeBinaryConcat(allocator: std.mem.Allocator, left: *Expr, right: *Expr) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .binary_concat = .{ .left = left, .right = right } };
    return node;
}

/// Allocate a unary-negate node taking ownership of `operand`. On allocation
/// failure the caller is responsible for releasing `operand`.
pub fn makeUnaryNegate(allocator: std.mem.Allocator, operand: *Expr) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .unary_negate = operand };
    return node;
}

/// Allocate a comparison node (lt/le/gt/ge). Same ownership rules as
/// `makeBinaryArith`.
pub fn makeCompare(allocator: std.mem.Allocator, op: CompareOp, left: *Expr, right: *Expr) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .compare = .{ .op = op, .left = left, .right = right } };
    return node;
}

test "ast: literal node round-trips a value" {
    const allocator = std.testing.allocator;
    const node = try makeLiteral(allocator, Value{ .integer = 42 });
    defer node.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 42), node.literal.integer);
}

test "ast: binary_arith deinit frees children" {
    const allocator = std.testing.allocator;
    const left = try makeLiteral(allocator, Value{ .integer = 1 });
    const right = try makeLiteral(allocator, Value{ .integer = 2 });
    const node = try makeBinaryArith(allocator, .add, left, right);
    defer node.deinit(allocator);
    try std.testing.expectEqual(BinaryOp.add, node.binary_arith.op);
    try std.testing.expectEqual(@as(i64, 1), node.binary_arith.left.literal.integer);
}

test "ast: literal text is freed" {
    const allocator = std.testing.allocator;
    const text = try allocator.dupe(u8, "hello");
    const node = try makeLiteral(allocator, Value{ .text = text });
    node.deinit(allocator);
}

test "ast: unary_negate frees operand" {
    const allocator = std.testing.allocator;
    const inner = try makeLiteral(allocator, Value{ .integer = 5 });
    const node = try makeUnaryNegate(allocator, inner);
    defer node.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 5), node.unary_negate.literal.integer);
}

test "ast: compare and concat deinit frees children" {
    const allocator = std.testing.allocator;
    const a = try makeLiteral(allocator, Value{ .integer = 1 });
    const b = try makeLiteral(allocator, Value{ .integer = 2 });
    const cmp = try makeCompare(allocator, .lt, a, b);
    defer cmp.deinit(allocator);

    const x = try makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "x") });
    const y = try makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "y") });
    const cat = try makeBinaryConcat(allocator, x, y);
    defer cat.deinit(allocator);

    try std.testing.expectEqual(CompareOp.lt, cmp.compare.op);
}
