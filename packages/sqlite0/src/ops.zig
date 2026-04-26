const std = @import("std");
const lex = @import("lex.zig");
const value_mod = @import("value.zig");

const Value = value_mod.Value;
const TokenKind = lex.TokenKind;

pub const Error = error{
    SyntaxError,
    InvalidNumber,
    InvalidString,
    UnsupportedFeature,
    UnknownFunction,
    WrongArgumentCount,
    IntegerOverflow,
    OutOfMemory,
    TableAlreadyExists,
    NoSuchTable,
    ColumnCountMismatch,
    InvalidEscape,
};

pub fn unescapeStringLiteral(allocator: std.mem.Allocator, raw: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);
    var i: usize = 0;
    while (i < raw.len) {
        if (raw[i] == '\'' and i + 1 < raw.len and raw[i + 1] == '\'') {
            try out.append(allocator, '\'');
            i += 2;
        } else {
            try out.append(allocator, raw[i]);
            i += 1;
        }
    }
    return out.toOwnedSlice(allocator);
}

pub fn freeValue(allocator: std.mem.Allocator, v: Value) void {
    switch (v) {
        .text, .blob => |bytes| allocator.free(bytes),
        else => {},
    }
}

pub fn negateValue(v: Value) Error!Value {
    return switch (v) {
        .integer => |i| Value{ .integer = -%i },
        .real => |f| Value{ .real = -f },
        .null => Value.null,
        else => Error.UnsupportedFeature,
    };
}

pub fn applyArith(op: TokenKind, lhs_raw: Value, rhs_raw: Value) Error!Value {
    if (lhs_raw == .null or rhs_raw == .null) return Value.null;
    const lhs = coerceToNumericValue(lhs_raw);
    const rhs = coerceToNumericValue(rhs_raw);
    const both_int = (lhs == .integer and rhs == .integer);
    if (both_int) {
        const a = lhs.integer;
        const b = rhs.integer;
        switch (op) {
            .plus => return Value{ .integer = a +% b },
            .minus => return Value{ .integer = a -% b },
            .star => return Value{ .integer = a *% b },
            .slash => {
                if (b == 0) return Value.null;
                if (a == std.math.minInt(i64) and b == -1) return Value.null;
                return Value{ .integer = @divTrunc(a, b) };
            },
            .percent => {
                if (b == 0) return Value.null;
                return Value{ .integer = @rem(a, b) };
            },
            else => unreachable,
        }
    }
    const a = toReal(lhs) orelse return Error.UnsupportedFeature;
    const b = toReal(rhs) orelse return Error.UnsupportedFeature;
    switch (op) {
        .plus => return Value{ .real = a + b },
        .minus => return Value{ .real = a - b },
        .star => return Value{ .real = a * b },
        .slash => {
            if (b == 0) return Value.null;
            return Value{ .real = a / b };
        },
        .percent => {
            if (b == 0) return Value.null;
            return Value{ .real = @rem(a, b) };
        },
        else => unreachable,
    }
}

fn toReal(v: Value) ?f64 {
    return switch (v) {
        .integer => |i| @as(f64, @floatFromInt(i)),
        .real => |f| f,
        else => null,
    };
}

/// SQLite "applyNumericAffinity": if the value already is INT/REAL, leave it.
/// If TEXT/BLOB, parse the bytes — INTEGER if the result is a whole number
/// without `.` or `e`, otherwise REAL. Bytes that don't parse become INTEGER 0.
/// (This matches `SELECT typeof('foo' + 0)` returning `integer` in sqlite3.)
fn coerceToNumericValue(v: Value) Value {
    return switch (v) {
        .integer, .real, .null => v,
        .text, .blob => |bytes| coerceBytesToNumeric(bytes),
    };
}

fn coerceBytesToNumeric(bytes: []const u8) Value {
    if (std.fmt.parseInt(i64, bytes, 10)) |i| return Value{ .integer = i } else |_| {}
    if (std.fmt.parseFloat(f64, bytes)) |f| return Value{ .real = f } else |_| {}
    return Value{ .integer = 0 };
}

pub fn boolValue(b: bool) Value {
    return Value{ .integer = if (b) 1 else 0 };
}

/// Coerce a Value to a SQL boolean. Returns null when the value is SQL NULL,
/// in which case three-valued logic propagates NULL upward. TEXT/BLOB are
/// coerced numerically: parseable as a non-zero number → true, otherwise
/// false. Matches sqlite3 (`SELECT 'foo' AND 1` → 0, `SELECT '1' AND 1` → 1).
pub fn truthy(v: Value) ?bool {
    return switch (v) {
        .null => null,
        .integer => |i| i != 0,
        .real => |f| f != 0,
        .text, .blob => |bytes| coerceTextToBool(bytes),
    };
}

fn coerceTextToBool(bytes: []const u8) bool {
    if (std.fmt.parseFloat(f64, bytes)) |f| {
        return f != 0;
    } else |_| {}
    return false;
}

pub fn logicalNot(v: Value) Value {
    const t = truthy(v) orelse return Value.null;
    return boolValue(!t);
}

pub fn logicalAnd(a: Value, b: Value) Value {
    const ta = truthy(a);
    const tb = truthy(b);
    if (ta == false or tb == false) return boolValue(false);
    if (ta == null or tb == null) return Value.null;
    return boolValue(true);
}

pub fn logicalOr(a: Value, b: Value) Value {
    const ta = truthy(a);
    const tb = truthy(b);
    if (ta == true or tb == true) return boolValue(true);
    if (ta == null or tb == null) return Value.null;
    return boolValue(false);
}

/// `IS` never produces NULL: `NULL IS NULL` → true, `NULL IS x` → false.
pub fn identicalValues(a: Value, b: Value) bool {
    const a_kind: std.meta.Tag(Value) = a;
    const b_kind: std.meta.Tag(Value) = b;
    if (a_kind == .null or b_kind == .null) return a_kind == b_kind;
    return compareValues(a, b) == .eq;
}

pub const Order = enum { lt, eq, gt };

/// SQLite comparison ordering between non-NULL values, honoring storage class
/// precedence: NUMERIC (INT/REAL) < TEXT < BLOB. Numeric values compare with
/// each other via real coercion. Text and blob compare by raw bytes.
pub fn compareValues(a: Value, b: Value) Order {
    const a_class = numericClass(a);
    const b_class = numericClass(b);
    if (a_class != b_class) {
        return if (@intFromEnum(a_class) < @intFromEnum(b_class)) .lt else .gt;
    }
    return switch (a_class) {
        .numeric => orderReal(toReal(a).?, toReal(b).?),
        .text => orderBytes(a.text, b.text),
        .blob => orderBytes(a.blob, b.blob),
    };
}

const NumericClass = enum(u8) { numeric = 1, text = 2, blob = 3 };

fn numericClass(v: Value) NumericClass {
    return switch (v) {
        .integer, .real => .numeric,
        .text => .text,
        .blob => .blob,
        .null => unreachable,
    };
}

fn orderReal(a: f64, b: f64) Order {
    if (a < b) return .lt;
    if (a > b) return .gt;
    return .eq;
}

fn orderBytes(a: []const u8, b: []const u8) Order {
    return switch (std.mem.order(u8, a, b)) {
        .lt => .lt,
        .eq => .eq,
        .gt => .gt,
    };
}

pub fn applyComparison(op: TokenKind, lhs: Value, rhs: Value) Value {
    if (lhs == .null or rhs == .null) return Value.null;
    const order = compareValues(lhs, rhs);
    return boolValue(switch (op) {
        .lt => order == .lt,
        .le => order != .gt,
        .gt => order == .gt,
        .ge => order != .lt,
        else => unreachable,
    });
}

pub fn applyEquality(op: TokenKind, lhs: Value, rhs: Value) Value {
    if (lhs == .null or rhs == .null) return Value.null;
    const order = compareValues(lhs, rhs);
    const equal = order == .eq;
    return boolValue(switch (op) {
        .eq => equal,
        .ne => !equal,
        else => unreachable,
    });
}

/// Render a Value into the canonical SQLite text representation, allocating
/// a new buffer that the caller owns. For TEXT/BLOB we dupe the bytes; for
/// INTEGER and REAL we format into a fixed-size scratch buffer (numeric
/// renderings never exceed 32 bytes for f64). NULL has no text representation
/// — concatenation/length/substr/etc. handle NULL by short-circuiting before
/// reaching this function.
pub fn valueToOwnedText(allocator: std.mem.Allocator, v: Value) error{ OutOfMemory, NotConvertible }![]u8 {
    return switch (v) {
        .text => |t| allocator.dupe(u8, t) catch error.OutOfMemory,
        .blob => |b| allocator.dupe(u8, b) catch error.OutOfMemory,
        .integer, .real => blk: {
            var buf: [64]u8 = undefined;
            var w = std.Io.Writer.fixed(&buf);
            v.format(&w) catch return error.NotConvertible;
            break :blk allocator.dupe(u8, w.buffered()) catch error.OutOfMemory;
        },
        .null => error.NotConvertible,
    };
}

/// `||` concatenation. Coerces both sides to text, returning a freshly
/// allocated TEXT Value. Either operand being NULL yields NULL.
pub fn concatValues(allocator: std.mem.Allocator, lhs: Value, rhs: Value) Error!Value {
    if (lhs == .null or rhs == .null) return Value.null;
    if (lhs == .text and rhs == .text) {
        const out = try std.mem.concat(allocator, u8, &.{ lhs.text, rhs.text });
        return Value{ .text = out };
    }
    const lhs_text = valueToOwnedText(allocator, lhs) catch |err| switch (err) {
        error.OutOfMemory => return Error.OutOfMemory,
        error.NotConvertible => return Error.UnsupportedFeature,
    };
    defer allocator.free(lhs_text);
    const rhs_text = valueToOwnedText(allocator, rhs) catch |err| switch (err) {
        error.OutOfMemory => return Error.OutOfMemory,
        error.NotConvertible => return Error.UnsupportedFeature,
    };
    defer allocator.free(rhs_text);
    const out = try std.mem.concat(allocator, u8, &.{ lhs_text, rhs_text });
    return Value{ .text = out };
}

/// `a IN (e1, e2, ...)` is three-valued OR over `a = ei`.
/// - left is NULL → NULL
/// - any e matches → 1 (short-circuit)
/// - no match but some `a = ei` was NULL → NULL
/// - no match and no NULLs → 0
/// - empty list → 0
pub fn applyIn(left: Value, list: []const Value) Value {
    if (left == .null) return Value.null;
    var saw_null = false;
    for (list) |item| {
        const eq = applyEquality(.eq, left, item);
        switch (eq) {
            .integer => |i| if (i == 1) return boolValue(true),
            .null => saw_null = true,
            else => unreachable,
        }
    }
    if (saw_null) return Value.null;
    return boolValue(false);
}

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
