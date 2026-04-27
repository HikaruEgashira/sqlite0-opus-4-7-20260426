//! Scalar-function dispatcher tests, split out of `funcs.zig` to keep
//! that file under the 500-line discipline (CLAUDE.md "Module Splitting
//! Rules"). Production code lives exclusively in `funcs.zig`; this file
//! is test-only.

const std = @import("std");
const funcs = @import("funcs.zig");
const value = @import("value.zig");

const Value = value.Value;
const call = funcs.call;

test "funcs: length(text) byte count" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "hello" }};
    const r = try call(allocator, null, "length", &args);
    try std.testing.expectEqual(@as(i64, 5), r.integer);
}

test "funcs: length(NULL) is NULL" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.null};
    const r = try call(allocator, null, "length", &args);
    try std.testing.expectEqual(Value.null, r);
}

test "funcs: lower" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "Hello World" }};
    const r = try call(allocator, null, "LOWER", &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("hello world", r.text);
}

test "funcs: substr basic" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "hello" }, .{ .integer = 2 }, .{ .integer = 3 } };
    const r = try call(allocator, null, "substr", &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("ell", r.text);
}

test "funcs: substr negative start" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "hello" }, .{ .integer = -3 } };
    const r = try call(allocator, null, "substr", &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("llo", r.text);
}

test "funcs: substr negative length" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "hello" }, .{ .integer = 2 }, .{ .integer = -1 } };
    const r = try call(allocator, null, "substr", &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("h", r.text);
}

test "funcs: substr counts UTF-8 chars not bytes" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "Hあhi" }, .{ .integer = 2 }, .{ .integer = 1 } };
    const r = try call(allocator, null, "substr", &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("あ", r.text);
}

test "funcs: substr BLOB returns BLOB with byte indices" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .blob = "AB" }, .{ .integer = 1 }, .{ .integer = 1 } };
    const r = try call(allocator, null, "substr", &args);
    defer allocator.free(r.blob);
    try std.testing.expectEqualSlices(u8, "A", r.blob);
}

test "funcs: substr TEXT NUL-truncates length when start is negative" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "A\x00B" }, .{ .integer = -2 }, .{ .integer = 5 } };
    const r = try call(allocator, null, "substr", &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("A", r.text);
}

test "funcs: abs integer" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .integer = -7 }};
    const r = try call(allocator, null, "abs", &args);
    try std.testing.expectEqual(@as(i64, 7), r.integer);
}

test "funcs: abs(text 'foo') is real 0.0" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "foo" }};
    const r = try call(allocator, null, "abs", &args);
    try std.testing.expectEqual(@as(f64, 0.0), r.real);
}

test "funcs: coalesce picks first non-null" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .null, .null, .{ .integer = 42 }, .{ .integer = 99 } };
    const r = try call(allocator, null, "coalesce", &args);
    try std.testing.expectEqual(@as(i64, 42), r.integer);
}

test "funcs: typeof returns lowercase tag" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .real = 1.5 }};
    const r = try call(allocator, null, "typeof", &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("real", r.text);
}

test "funcs: round to integer always returns real" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .real = 3.5 }};
    const r = try call(allocator, null, "round", &args);
    try std.testing.expectEqual(@as(f64, 4.0), r.real);
}

test "funcs: round half-away-from-zero for negative" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .real = -2.5 }};
    const r = try call(allocator, null, "round", &args);
    try std.testing.expectEqual(@as(f64, -3.0), r.real);
}

test "funcs: min(NULL, ...) is NULL" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .null, .{ .integer = 1 }, .{ .integer = 2 } };
    const r = try call(allocator, null, "min", &args);
    try std.testing.expectEqual(Value.null, r);
}

test "funcs: max picks largest" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .integer = 1 }, .{ .integer = 3 }, .{ .integer = 2 } };
    const r = try call(allocator, null, "max", &args);
    try std.testing.expectEqual(@as(i64, 3), r.integer);
}

test "fnRound: tied IEEE 2.55 rounds DOWN to 2.5 (sqlite3 rounder+trunc)" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .real = 2.55 }, .{ .integer = 1 } };
    const r = try call(allocator, null, "round", &args);
    try std.testing.expectEqual(@as(f64, 2.5), r.real);
}

test "fnRound: -2.55 rounds toward zero side to -2.5" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .real = -2.55 }, .{ .integer = 1 } };
    const r = try call(allocator, null, "round", &args);
    try std.testing.expectEqual(@as(f64, -2.5), r.real);
}

test "sqlite_version: returns target compat version string" {
    const allocator = std.testing.allocator;
    const r = try call(allocator, null, "sqlite_version", &.{});
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("3.51.0", r.text);
}

test "sqlite_compileoption_used: unknown name → 0; NULL → NULL" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "FOO" }};
    const r1 = try call(allocator, null, "sqlite_compileoption_used", &args);
    try std.testing.expectEqual(@as(i64, 0), r1.integer);

    var args2 = [_]Value{Value.null};
    const r2 = try call(allocator, null, "sqlite_compileoption_used", &args2);
    try std.testing.expectEqual(Value.null, r2);
}

test "fnRound: 2.355 rounds DOWN to 2.35 via f128 intermediate" {
    // f64 intermediate `(2.355 + 0.005) * 100` rounds back to 2.36 because
    // f64's 52-bit mantissa loses the sub-ulp tail (2.355 is actually
    // stored as 2.3549999...). f128's 113-bit mantissa preserves it, so
    // @trunc sees the actual digit — matching sqlite3's bigint-decimal
    // round path in func.c roundFunc.
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .real = 2.355 }, .{ .integer = 2 } };
    const r = try call(allocator, null, "round", &args);
    try std.testing.expectEqual(@as(f64, 2.35), r.real);
}

test "fnRound: 0.145 rounds DOWN to 0.14 (IEEE bits below 0.145)" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .real = 0.145 }, .{ .integer = 2 } };
    const r = try call(allocator, null, "round", &args);
    try std.testing.expectEqual(@as(f64, 0.14), r.real);
}

test "fnRound: 2.45 rounds UP to 2.5 (IEEE 2.45 is slightly above)" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .real = 2.45 }, .{ .integer = 1 } };
    const r = try call(allocator, null, "round", &args);
    try std.testing.expectEqual(@as(f64, 2.5), r.real);
}

test "fnRound: integer-precision tie rounds half-away-from-zero" {
    const allocator = std.testing.allocator;
    var args1 = [_]Value{.{ .real = 2.5 }};
    const r1 = try call(allocator, null, "round", &args1);
    try std.testing.expectEqual(@as(f64, 3.0), r1.real);

    var args2 = [_]Value{.{ .real = -2.5 }};
    const r2 = try call(allocator, null, "round", &args2);
    try std.testing.expectEqual(@as(f64, -3.0), r2.real);
}
