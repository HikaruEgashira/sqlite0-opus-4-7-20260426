const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");

const Value = value_mod.Value;
const Error = ops.Error;

/// Built-in scalar function dispatch. `name` is matched case-insensitively.
/// `args` are owned by the caller — implementations must not free them, but
/// may dupe data into the returned Value, which the caller takes ownership of.
/// Returns `Error.UnknownFunction` for names that aren't registered yet.
pub fn call(allocator: std.mem.Allocator, name: []const u8, args: []const Value) Error!Value {
    if (eqlIgnoreCase(name, "length")) return fnLength(allocator, args);
    if (eqlIgnoreCase(name, "lower")) return fnLower(allocator, args);
    if (eqlIgnoreCase(name, "upper")) return fnUpper(allocator, args);
    if (eqlIgnoreCase(name, "substr") or eqlIgnoreCase(name, "substring")) return fnSubstr(allocator, args);
    if (eqlIgnoreCase(name, "abs")) return fnAbs(allocator, args);
    if (eqlIgnoreCase(name, "coalesce") or eqlIgnoreCase(name, "ifnull")) return fnCoalesce(allocator, args);
    if (eqlIgnoreCase(name, "nullif")) return fnNullif(allocator, args);
    if (eqlIgnoreCase(name, "typeof")) return fnTypeof(allocator, args);
    return Error.UnknownFunction;
}

fn eqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| {
        if (std.ascii.toLower(x) != std.ascii.toLower(y)) return false;
    }
    return true;
}

fn fnLength(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const v = args[0];
    return switch (v) {
        .null => Value.null,
        .text => |t| Value{ .integer = @intCast(t.len) },
        .blob => |b| Value{ .integer = @intCast(b.len) },
        .integer, .real => blk: {
            const text = ops.valueToOwnedText(allocator, v) catch |err| switch (err) {
                error.OutOfMemory => return Error.OutOfMemory,
                error.NotConvertible => return Error.UnsupportedFeature,
            };
            defer allocator.free(text);
            break :blk Value{ .integer = @intCast(text.len) };
        },
    };
}

fn fnLower(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    return mapAscii(allocator, args[0], std.ascii.toLower);
}

fn fnUpper(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    return mapAscii(allocator, args[0], std.ascii.toUpper);
}

fn mapAscii(allocator: std.mem.Allocator, v: Value, comptime mapFn: fn (u8) u8) Error!Value {
    if (v == .null) return Value.null;
    const source: []const u8 = switch (v) {
        .text => |t| t,
        .blob => |b| b,
        else => return mapAsciiFromNumeric(allocator, v, mapFn),
    };
    const out = try allocator.alloc(u8, source.len);
    for (source, 0..) |c, i| out[i] = mapFn(c);
    return Value{ .text = out };
}

fn mapAsciiFromNumeric(allocator: std.mem.Allocator, v: Value, comptime mapFn: fn (u8) u8) Error!Value {
    const text = ops.valueToOwnedText(allocator, v) catch |err| switch (err) {
                error.OutOfMemory => return Error.OutOfMemory,
                error.NotConvertible => return Error.UnsupportedFeature,
            };
    defer allocator.free(text);
    const out = try allocator.alloc(u8, text.len);
    for (text, 0..) |c, i| out[i] = mapFn(c);
    return Value{ .text = out };
}

/// SQLite `substr(X, P, N)` / `substr(X, P)`.
///
/// 1. Convert X to text. Length L bytes.
/// 2. If P < 0, P := L + 1 + P (counting from end). May still be ≤ 0.
/// 3. If N is negative, |N| chars precede position P:
///       end := P - 1, start := end - |N|; clamp start to 1.
/// 4. If P < 1, shrink the requested length by (1 - P) and clamp P to 1.
/// 5. If N is missing, take the rest of the string.
/// 6. If P > L or final N ≤ 0, return ''.
fn fnSubstr(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 2 and args.len != 3) return Error.WrongArgumentCount;
    if (args[0] == .null or args[1] == .null) return Value.null;
    if (args.len == 3 and args[2] == .null) return Value.null;

    const subject_text = ops.valueToOwnedText(allocator, args[0]) catch |err| switch (err) {
        error.OutOfMemory => return Error.OutOfMemory,
        error.NotConvertible => return Error.UnsupportedFeature,
    };
    defer allocator.free(subject_text);

    const p_raw = try toIntCoerce(args[1]);
    const has_n = args.len == 3;
    var n: i64 = if (has_n) try toIntCoerce(args[2]) else 0;

    const len_i: i64 = @intCast(subject_text.len);
    var p: i64 = p_raw;
    if (p < 0) p = len_i + 1 + p;

    if (has_n and n < 0) {
        const abs_n = -n;
        var end = p - 1;
        var start = end - abs_n;
        if (start < 0) start = 0;
        if (end < 0) end = 0;
        const out = try allocator.dupe(u8, subject_text[@intCast(start)..@intCast(end)]);
        return Value{ .text = out };
    }

    if (p < 1) {
        if (has_n) n -= (1 - p);
        p = 1;
    }
    if (!has_n) n = len_i - p + 1;
    if (p > len_i or n <= 0) {
        return Value{ .text = try allocator.dupe(u8, "") };
    }
    var end = p + n - 1;
    if (end > len_i) end = len_i;
    const start_idx: usize = @intCast(p - 1);
    const end_idx: usize = @intCast(end);
    const out = try allocator.dupe(u8, subject_text[start_idx..end_idx]);
    return Value{ .text = out };
}

fn fnAbs(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    _ = allocator;
    if (args.len != 1) return Error.WrongArgumentCount;
    return switch (args[0]) {
        .null => Value.null,
        .integer => |i| blk: {
            if (i == std.math.minInt(i64)) return Error.IntegerOverflow;
            break :blk Value{ .integer = if (i < 0) -i else i };
        },
        .real => |f| Value{ .real = @abs(f) },
        .text, .blob => |bytes| Value{ .real = @abs(parseFloatLoose(bytes)) },
    };
}

fn fnCoalesce(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len < 2) return Error.WrongArgumentCount;
    for (args) |a| {
        if (a != .null) return try dupeValue(allocator, a);
    }
    return Value.null;
}

fn fnNullif(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 2) return Error.WrongArgumentCount;
    if (args[0] == .null) return Value.null;
    const eq = ops.applyEquality(.eq, args[0], args[1]);
    if (eq == .integer and eq.integer == 1) return Value.null;
    return try dupeValue(allocator, args[0]);
}

fn fnTypeof(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const tag: []const u8 = switch (args[0]) {
        .null => "null",
        .integer => "integer",
        .real => "real",
        .text => "text",
        .blob => "blob",
    };
    return Value{ .text = try allocator.dupe(u8, tag) };
}

fn dupeValue(allocator: std.mem.Allocator, v: Value) Error!Value {
    return switch (v) {
        .text => |t| Value{ .text = try allocator.dupe(u8, t) },
        .blob => |b| Value{ .blob = try allocator.dupe(u8, b) },
        else => v,
    };
}

fn toIntCoerce(v: Value) Error!i64 {
    return switch (v) {
        .integer => |i| i,
        .real => |f| @intFromFloat(f),
        .text, .blob => |bytes| @intFromFloat(parseFloatLoose(bytes)),
        .null => Error.UnsupportedFeature,
    };
}

/// Parse the longest valid numeric prefix of `bytes`. Anything that doesn't
/// parse cleanly returns 0 (matching SQLite's `CAST(...AS REAL)` semantics
/// for non-numeric text).
fn parseFloatLoose(bytes: []const u8) f64 {
    if (std.fmt.parseFloat(f64, bytes)) |f| return f else |_| {}
    var end = bytes.len;
    while (end > 0) : (end -= 1) {
        if (std.fmt.parseFloat(f64, bytes[0..end])) |f| return f else |_| {}
    }
    return 0;
}

test "funcs: length(text) byte count" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "hello" }};
    const r = try call(allocator, "length", &args);
    try std.testing.expectEqual(@as(i64, 5), r.integer);
}

test "funcs: length(NULL) is NULL" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.null};
    const r = try call(allocator, "length", &args);
    try std.testing.expectEqual(Value.null, r);
}

test "funcs: lower" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "Hello World" }};
    const r = try call(allocator, "LOWER", &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("hello world", r.text);
}

test "funcs: substr basic" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "hello" }, .{ .integer = 2 }, .{ .integer = 3 } };
    const r = try call(allocator, "substr", &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("ell", r.text);
}

test "funcs: substr negative start" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "hello" }, .{ .integer = -3 } };
    const r = try call(allocator, "substr", &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("llo", r.text);
}

test "funcs: substr negative length" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "hello" }, .{ .integer = 2 }, .{ .integer = -1 } };
    const r = try call(allocator, "substr", &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("h", r.text);
}

test "funcs: abs integer" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .integer = -7 }};
    const r = try call(allocator, "abs", &args);
    try std.testing.expectEqual(@as(i64, 7), r.integer);
}

test "funcs: abs(text 'foo') is real 0.0" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "foo" }};
    const r = try call(allocator, "abs", &args);
    try std.testing.expectEqual(@as(f64, 0.0), r.real);
}

test "funcs: coalesce picks first non-null" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .null, .null, .{ .integer = 42 }, .{ .integer = 99 } };
    const r = try call(allocator, "coalesce", &args);
    try std.testing.expectEqual(@as(i64, 42), r.integer);
}

test "funcs: typeof returns lowercase tag" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .real = 1.5 }};
    const r = try call(allocator, "typeof", &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("real", r.text);
}
