//! Expression AST nodes (ADR-0002 Iter8.A scope: literal + binary_arith).
//!
//! Iter8.B/C will extend this union to cover the rest of the expression
//! grammar (concat, unary, equality, comparison, IS, BETWEEN, IN, CASE,
//! function calls). Iter8.D adds `column_ref` for FROM-clause row binding.
//!
//! Each node owns its children and any heap bytes inside `literal` values;
//! `Expr.deinit` recursively releases everything.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");

const Value = value_mod.Value;

pub const BinaryOp = enum { add, sub };

pub const Expr = union(enum) {
    literal: Value,
    binary_arith: BinaryArith,

    pub const BinaryArith = struct {
        op: BinaryOp,
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
    // No leak — std.testing.allocator panics on leaked bytes.
}
