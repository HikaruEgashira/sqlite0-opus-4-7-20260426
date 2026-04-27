//! Tests for `ops.zig`, split out to bring that file under the 500-line
//! discipline (CLAUDE.md "Module Splitting Rules"). Production code lives
//! exclusively in `ops.zig`; this file is test-only.

const std = @import("std");
const ops = @import("ops.zig");
const value_mod = @import("value.zig");

const Value = value_mod.Value;
const Order = ops.Order;

const applyArith = ops.applyArith;
const truthy = ops.truthy;
const logicalAnd = ops.logicalAnd;
const logicalOr = ops.logicalOr;
const identicalValues = ops.identicalValues;
const compareValues = ops.compareValues;
const applyIn = ops.applyIn;

test "ops: applyArith integer division truncates" {
    const r = try applyArith(.slash, .{ .integer = 100 }, .{ .integer = 4 });
    try std.testing.expectEqual(@as(i64, 25), r.integer);
}

test "ops: applyArith mixed int+real returns real" {
    const r = try applyArith(.plus, .{ .integer = 1 }, .{ .real = 0.5 });
    try std.testing.expectEqual(@as(f64, 1.5), r.real);
}

test "ops: applyArith division by zero returns NULL" {
    const r = try applyArith(.slash, .{ .integer = 1 }, .{ .integer = 0 });
    try std.testing.expectEqual(Value.null, r);
}

test "ops: truthy text coercion" {
    try std.testing.expectEqual(@as(?bool, false), truthy(.{ .text = "foo" }));
    try std.testing.expectEqual(@as(?bool, true), truthy(.{ .text = "1" }));
    try std.testing.expectEqual(@as(?bool, false), truthy(.{ .text = "0" }));
    try std.testing.expectEqual(@as(?bool, null), truthy(.null));
}

test "ops: logicalAnd three-valued" {
    try std.testing.expectEqual(@as(i64, 0), logicalAnd(.null, .{ .integer = 0 }).integer);
    try std.testing.expectEqual(Value.null, logicalAnd(.null, .{ .integer = 1 }));
    try std.testing.expectEqual(Value.null, logicalAnd(.null, .null));
}

test "ops: logicalOr three-valued" {
    try std.testing.expectEqual(@as(i64, 1), logicalOr(.null, .{ .integer = 1 }).integer);
    try std.testing.expectEqual(Value.null, logicalOr(.null, .{ .integer = 0 }));
    try std.testing.expectEqual(Value.null, logicalOr(.null, .null));
}

test "ops: identicalValues" {
    try std.testing.expect(identicalValues(.null, .null));
    try std.testing.expect(!identicalValues(.null, .{ .integer = 1 }));
    try std.testing.expect(identicalValues(.{ .integer = 1 }, .{ .real = 1.0 }));
}

test "ops: compareValues by storage class" {
    try std.testing.expectEqual(Order.lt, compareValues(.{ .integer = 1 }, .{ .text = "a" }));
    try std.testing.expectEqual(Order.gt, compareValues(.{ .blob = "x" }, .{ .text = "x" }));
    try std.testing.expectEqual(Order.eq, compareValues(.{ .integer = 1 }, .{ .real = 1.0 }));
}

test "ops: applyIn matches" {
    const list = [_]Value{ .{ .integer = 1 }, .{ .integer = 2 }, .{ .integer = 3 } };
    try std.testing.expectEqual(@as(i64, 1), applyIn(.{ .integer = 2 }, &list).integer);
    try std.testing.expectEqual(@as(i64, 0), applyIn(.{ .integer = 4 }, &list).integer);
}

test "ops: applyIn with NULL in left is NULL" {
    const list = [_]Value{ .{ .integer = 1 } };
    try std.testing.expectEqual(Value.null, applyIn(.null, &list));
}

test "ops: applyIn no match but contains NULL is NULL" {
    const list = [_]Value{ .{ .integer = 1 }, .null, .{ .integer = 3 } };
    try std.testing.expectEqual(Value.null, applyIn(.{ .integer = 2 }, &list));
}

test "ops: applyIn match preempts NULL" {
    const list = [_]Value{ .null, .{ .integer = 1 } };
    try std.testing.expectEqual(@as(i64, 1), applyIn(.{ .integer = 1 }, &list).integer);
}

test "ops: applyIn empty list is 0" {
    try std.testing.expectEqual(@as(i64, 0), applyIn(.{ .integer = 1 }, &.{}).integer);
}

test "ops: coerceBytesToNumeric — '1.5xyz' → REAL 1.5" {
    const r = try applyArith(.plus, .{ .integer = 1 }, .{ .text = "1.5xyz" });
    try std.testing.expectEqual(@as(f64, 2.5), r.real);
}

test "ops: coerceBytesToNumeric — 'NaN' → INT 0 (not f64.nan)" {
    const r = try applyArith(.plus, .{ .integer = 1 }, .{ .text = "NaN" });
    try std.testing.expectEqual(@as(i64, 1), r.integer);
}

test "ops: coerceBytesToNumeric — '1abc' integer prefix stays INT" {
    const r = try applyArith(.plus, .{ .integer = 0 }, .{ .text = "1abc" });
    try std.testing.expectEqual(@as(i64, 1), r.integer);
}

test "ops: coerceBytesToNumeric — '1e3xyz' real prefix → REAL" {
    const r = try applyArith(.plus, .{ .integer = 0 }, .{ .text = "1e3xyz" });
    try std.testing.expectEqual(@as(f64, 1000.0), r.real);
}
