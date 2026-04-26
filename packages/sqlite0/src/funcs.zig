const std = @import("std");
const ops = @import("ops.zig");
const util = @import("func_util.zig");
const text = @import("funcs_text.zig");
const fmt_mod = @import("funcs_format.zig");
const time_mod = @import("funcs_time.zig");

const Value = util.Value;
const Error = util.Error;

/// Built-in scalar function dispatch. `name` is matched case-insensitively.
/// `args` are owned by the caller — implementations must not free them, but
/// may dupe data into the returned Value, which the caller takes ownership of.
/// Returns `Error.UnknownFunction` for names that aren't registered yet.
pub fn call(allocator: std.mem.Allocator, name: []const u8, args: []const Value) Error!Value {
    if (util.eqlIgnoreCase(name, "length")) return fnLength(allocator, args);
    if (util.eqlIgnoreCase(name, "lower")) return fnLower(allocator, args);
    if (util.eqlIgnoreCase(name, "upper")) return fnUpper(allocator, args);
    if (util.eqlIgnoreCase(name, "substr") or util.eqlIgnoreCase(name, "substring")) return fnSubstr(allocator, args);
    if (util.eqlIgnoreCase(name, "abs")) return fnAbs(allocator, args);
    if (util.eqlIgnoreCase(name, "coalesce") or util.eqlIgnoreCase(name, "ifnull")) return fnCoalesce(allocator, args);
    if (util.eqlIgnoreCase(name, "nullif")) return fnNullif(allocator, args);
    if (util.eqlIgnoreCase(name, "typeof")) return fnTypeof(allocator, args);
    if (util.eqlIgnoreCase(name, "round")) return fnRound(allocator, args);
    if (util.eqlIgnoreCase(name, "min")) return fnMinMax(allocator, args, .min);
    if (util.eqlIgnoreCase(name, "max")) return fnMinMax(allocator, args, .max);
    if (util.eqlIgnoreCase(name, "replace")) return text.fnReplace(allocator, args);
    if (util.eqlIgnoreCase(name, "hex")) return text.fnHex(allocator, args);
    if (util.eqlIgnoreCase(name, "quote")) return text.fnQuote(allocator, args);
    if (util.eqlIgnoreCase(name, "trim")) return text.fnTrim(allocator, args, .both);
    if (util.eqlIgnoreCase(name, "ltrim")) return text.fnTrim(allocator, args, .left);
    if (util.eqlIgnoreCase(name, "rtrim")) return text.fnTrim(allocator, args, .right);
    if (util.eqlIgnoreCase(name, "instr")) return text.fnInstr(allocator, args);
    if (util.eqlIgnoreCase(name, "char")) return text.fnChar(allocator, args);
    if (util.eqlIgnoreCase(name, "unicode")) return text.fnUnicode(allocator, args);
    if (util.eqlIgnoreCase(name, "random")) return fnRandom(args);
    if (util.eqlIgnoreCase(name, "printf") or util.eqlIgnoreCase(name, "format")) return fmt_mod.fnPrintf(allocator, args);
    if (util.eqlIgnoreCase(name, "strftime")) return time_mod.fnStrftime(allocator, args);
    return Error.UnknownFunction;
}

/// Process-wide PRNG state for `random()`. Lazily initialized on first call.
/// The seed is derived from `&random_prng`'s ASLR-randomized address — this
/// gives run-to-run variation without depending on `std.Io` (Zig 0.16.0 moved
/// time/random APIs behind an Io vtable that we'd otherwise have to thread
/// through the function-dispatch ABI). Within a single process subsequent
/// calls produce a fresh PRNG sequence.
///
/// Differential tests can't byte-compare `random()` output across engines;
/// they only check `typeof(random()) = 'integer'` and per-call freshness.
var random_prng: ?std.Random.DefaultPrng = null;

/// `random()` — return a uniformly-distributed pseudo-random 64-bit signed
/// integer (sqlite3 compat). Each invocation produces a fresh value, mirroring
/// sqlite3's per-call re-evaluation. sqlite3 errors on `random(1)` (no args).
fn fnRandom(args: []const Value) Error!Value {
    if (args.len != 0) return Error.WrongArgumentCount;
    if (random_prng == null) {
        const seed = @intFromPtr(&random_prng) ^ 0xdeadbeefcafebabe;
        random_prng = std.Random.DefaultPrng.init(seed);
    }
    const bits = random_prng.?.random().int(u64);
    return Value{ .integer = @bitCast(bits) };
}

fn fnLength(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const v = args[0];
    return switch (v) {
        .null => Value.null,
        .text => |t| Value{ .integer = @intCast(t.len) },
        .blob => |b| Value{ .integer = @intCast(b.len) },
        .integer, .real => blk: {
            const t = ops.valueToOwnedText(allocator, v) catch |err| switch (err) {
                error.OutOfMemory => return Error.OutOfMemory,
                error.NotConvertible => return Error.UnsupportedFeature,
            };
            defer allocator.free(t);
            break :blk Value{ .integer = @intCast(t.len) };
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
    const t = ops.valueToOwnedText(allocator, v) catch |err| switch (err) {
        error.OutOfMemory => return Error.OutOfMemory,
        error.NotConvertible => return Error.UnsupportedFeature,
    };
    defer allocator.free(t);
    const out = try allocator.alloc(u8, t.len);
    for (t, 0..) |c, i| out[i] = mapFn(c);
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

    const p_raw = try util.toIntCoerce(args[1]);
    const has_n = args.len == 3;
    var n: i64 = if (has_n) try util.toIntCoerce(args[2]) else 0;

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
        .text, .blob => |bytes| Value{ .real = @abs(util.parseFloatLoose(bytes)) },
    };
}

fn fnCoalesce(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len < 2) return Error.WrongArgumentCount;
    for (args) |a| {
        if (a != .null) return try util.dupeValue(allocator, a);
    }
    return Value.null;
}

fn fnNullif(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 2) return Error.WrongArgumentCount;
    if (args[0] == .null) return Value.null;
    const eq = ops.applyEquality(.eq, args[0], args[1]);
    if (eq == .integer and eq.integer == 1) return Value.null;
    return try util.dupeValue(allocator, args[0]);
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

/// SQLite `round(X[, N])` — half-away-from-zero rounding to N decimals.
/// Always returns REAL even for integer inputs (`typeof(round(3)) = real`).
/// N defaults to 0; N is integer-coerced from any input type.
fn fnRound(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    _ = allocator;
    if (args.len < 1 or args.len > 2) return Error.WrongArgumentCount;
    if (args[0] == .null) return Value.null;
    if (args.len == 2 and args[1] == .null) return Value.null;

    const x = util.numericAsReal(args[0]);
    const d_raw: i64 = if (args.len == 2) try util.toIntCoerce(args[1]) else 0;
    const d: i32 = @intCast(@max(@as(i64, -50), @min(@as(i64, 50), d_raw)));

    if (d <= 0) return Value{ .real = @round(x) };
    const factor = std.math.pow(f64, 10, @floatFromInt(d));
    return Value{ .real = @round(x * factor) / factor };
}

const MinMax = enum { min, max };

/// Scalar `min(a, b, ...)` / `max(a, b, ...)`. ≥2 args required (1-arg is the
/// aggregate form which we don't yet support). Any NULL argument forces NULL.
fn fnMinMax(allocator: std.mem.Allocator, args: []const Value, dir: MinMax) Error!Value {
    if (args.len < 2) return Error.WrongArgumentCount;
    var best: Value = args[0];
    if (best == .null) return Value.null;
    for (args[1..]) |a| {
        if (a == .null) return Value.null;
        const order = ops.compareValues(best, a);
        const replace_best = switch (dir) {
            .min => order == .gt,
            .max => order == .lt,
        };
        if (replace_best) best = a;
    }
    return try util.dupeValue(allocator, best);
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

test "funcs: round to integer always returns real" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .real = 3.5 }};
    const r = try call(allocator, "round", &args);
    try std.testing.expectEqual(@as(f64, 4.0), r.real);
}

test "funcs: round half-away-from-zero for negative" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .real = -2.5 }};
    const r = try call(allocator, "round", &args);
    try std.testing.expectEqual(@as(f64, -3.0), r.real);
}

test "funcs: min(NULL, ...) is NULL" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .null, .{ .integer = 1 }, .{ .integer = 2 } };
    const r = try call(allocator, "min", &args);
    try std.testing.expectEqual(Value.null, r);
}

test "funcs: max picks largest" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .integer = 1 }, .{ .integer = 3 }, .{ .integer = 2 } };
    const r = try call(allocator, "max", &args);
    try std.testing.expectEqual(@as(i64, 3), r.integer);
}
