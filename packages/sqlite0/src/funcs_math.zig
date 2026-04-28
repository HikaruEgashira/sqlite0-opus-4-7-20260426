//! SQLite math extension scalar functions
//! (`SQLITE_ENABLE_MATH_FUNCTIONS`).
//!
//! All functions return REAL on success or NULL when the input is
//! NULL / NaN / out-of-domain. `±Inf` results from finite inputs
//! (e.g. `exp(1000)`) are passed through — sqlite3 only collapses
//! domain errors (sqrt of negative, log of non-positive, etc.) to
//! NULL, not overflow.
//!
//! Notable sqlite3 quirks reproduced here:
//!  - `log(x)` is base-10, not natural log. `ln(x)` is natural.
//!    `log(b, x)` is `log(x)/log(b)` for arbitrary base.
//!  - `mod(a, b)` is C `fmod` — sign tracks the dividend
//!    (`mod(-10, 3) = -1`, `mod(10, -3) = 1`).
//!  - `sign(x)` returns INTEGER -1 / 0 / 1 / NULL — not REAL — even
//!    for REAL input.
//!  - `pow(0, 0) = 1.0` (C99 `pow` convention).
//!  - Aliases: `ceil` ≡ `ceiling`, `pow` ≡ `power`.

const std = @import("std");
const util = @import("func_util.zig");

const Value = util.Value;
const Error = util.Error;

/// Coerce a function argument to f64 with sqlite3 math-extension semantics:
///   * NULL → null (whole call short-circuits to NULL)
///   * INTEGER / REAL → direct cast / forward
///   * TEXT / BLOB → `parseFloatStrictOpt` (sqlite3AtoF strict): the
///     entire string post whitespace-strip must be a valid number, no
///     trailing garbage. `sqrt('foo')` → NULL, `sqrt('1.5xyz')` → NULL
///     (distinct from printf's `parseFloatLooseOpt` prefix parse).
fn argReal(v: Value) ?f64 {
    return switch (v) {
        .null => null,
        .integer => |i| @floatFromInt(i),
        .real => |r| r,
        .text => |t| util.parseFloatStrictOpt(t),
        .blob => |b| util.parseFloatStrictOpt(b),
    };
}

pub fn fnPi(args: []const Value) Error!Value {
    if (args.len != 0) return Error.WrongArgumentCount;
    return Value{ .real = std.math.pi };
}

pub fn fnSqrt(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    if (x < 0.0) return Value.null;
    return Value{ .real = @sqrt(x) };
}

/// `log(x)` is base-10 (sqlite3 quirk — verified against 3.51.0).
/// `log(b, x)` returns `log(x) / log(b)` for arbitrary base.
pub fn fnLog(args: []const Value) Error!Value {
    if (args.len == 1) return fnLog10(args);
    if (args.len != 2) return Error.WrongArgumentCount;
    const base = argReal(args[0]) orelse return Value.null;
    const x = argReal(args[1]) orelse return Value.null;
    if (base <= 0.0 or base == 1.0 or x <= 0.0) return Value.null;
    return Value{ .real = @log(x) / @log(base) };
}

pub fn fnLn(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    if (x <= 0.0) return Value.null;
    return Value{ .real = @log(x) };
}

pub fn fnLog10(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    if (x <= 0.0) return Value.null;
    return Value{ .real = @log10(x) };
}

pub fn fnLog2(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    if (x <= 0.0) return Value.null;
    return Value{ .real = @log2(x) };
}

pub fn fnExp(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    return Value{ .real = @exp(x) }; // overflow → +Inf, allowed by sqlite3
}

pub fn fnPow(args: []const Value) Error!Value {
    if (args.len != 2) return Error.WrongArgumentCount;
    const base = argReal(args[0]) orelse return Value.null;
    const e = argReal(args[1]) orelse return Value.null;
    const r = std.math.pow(f64, base, e);
    if (std.math.isNan(r)) return Value.null;
    return Value{ .real = r };
}

pub fn fnSin(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    return Value{ .real = @sin(x) };
}

pub fn fnCos(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    return Value{ .real = @cos(x) };
}

pub fn fnTan(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    return Value{ .real = @tan(x) };
}

pub fn fnAsin(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    if (x < -1.0 or x > 1.0) return Value.null;
    return Value{ .real = std.math.asin(x) };
}

pub fn fnAcos(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    if (x < -1.0 or x > 1.0) return Value.null;
    return Value{ .real = std.math.acos(x) };
}

pub fn fnAtan(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    return Value{ .real = std.math.atan(x) };
}

pub fn fnAtan2(args: []const Value) Error!Value {
    if (args.len != 2) return Error.WrongArgumentCount;
    const y = argReal(args[0]) orelse return Value.null;
    const x = argReal(args[1]) orelse return Value.null;
    return Value{ .real = std.math.atan2(y, x) };
}

pub fn fnSinh(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    return Value{ .real = std.math.sinh(x) };
}

pub fn fnCosh(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    return Value{ .real = std.math.cosh(x) };
}

pub fn fnTanh(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    return Value{ .real = std.math.tanh(x) };
}

pub fn fnAsinh(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    return Value{ .real = std.math.asinh(x) };
}

pub fn fnAcosh(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    if (x < 1.0) return Value.null;
    return Value{ .real = std.math.acosh(x) };
}

pub fn fnAtanh(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    // sqlite3 atanh: domain endpoints x = ±1 yield ±Inf (libm convention),
    // not NULL. Only |x| > 1 (which would give NaN) is rejected.
    if (x < -1.0 or x > 1.0) return Value.null;
    return Value{ .real = std.math.atanh(x) };
}

/// `ceil` / `floor` / `trunc` preserve the input affinity:
///   * INTEGER arg → INTEGER (no-op on integers)
///   * REAL arg → REAL with the operation applied
///   * TEXT arg → sqlite3 first applies numeric affinity (sqlite3_value_
///     numeric_type): if the whole post-whitespace-trimmed string parses
///     as a pure integer (no `.`, no `e`/`E`), treat as INTEGER; if it
///     parses as a real (has `.` or `e`), treat as REAL. Trailing
///     garbage / non-numeric TEXT → NULL.
pub fn fnCeil(args: []const Value) Error!Value {
    return floorCeilTruncDispatch(args, .ceil);
}

pub fn fnFloor(args: []const Value) Error!Value {
    return floorCeilTruncDispatch(args, .floor);
}

pub fn fnTrunc(args: []const Value) Error!Value {
    return floorCeilTruncDispatch(args, .trunc);
}

const FloorCeilTrunc = enum { floor, ceil, trunc };

fn floorCeilTruncDispatch(args: []const Value, op: FloorCeilTrunc) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    switch (args[0]) {
        .null => return Value.null,
        .integer => return args[0],
        .real => |r| return Value{ .real = applyFloorCeilTrunc(r, op) },
        .text, .blob => |bytes| {
            const inferred = inferTextNumericType(bytes) orelse return Value.null;
            return switch (inferred) {
                .integer => |i| Value{ .integer = i },
                .real => |r| Value{ .real = applyFloorCeilTrunc(r, op) },
                else => unreachable,
            };
        },
    }
}

fn applyFloorCeilTrunc(x: f64, op: FloorCeilTrunc) f64 {
    return switch (op) {
        .floor => @floor(x),
        .ceil => @ceil(x),
        .trunc => @trunc(x),
    };
}

/// sqlite3 numeric-affinity rule for TEXT: if the trimmed string looks
/// like a pure integer (sign + digits, no `.`/`e`/`E`) AND fits in i64,
/// it's an INTEGER value. Otherwise, if the string is a valid full
/// number (with `.` or `e`), it's a REAL. Anything else is null.
fn inferTextNumericType(bytes: []const u8) ?Value {
    const s = std.mem.trim(u8, bytes, " \t\n\r");
    if (s.len == 0) return null;
    var has_real_marker = false;
    for (s) |c| {
        if (c == '.' or c == 'e' or c == 'E') {
            has_real_marker = true;
            break;
        }
    }
    if (!has_real_marker) {
        // Pure-integer shape: sign? + digits.
        if (std.fmt.parseInt(i64, s, 10)) |i| return Value{ .integer = i } else |_| {
            // i64 overflow — sqlite3 falls back to REAL.
            const f = util.parseFloatStrictOpt(s) orelse return null;
            return Value{ .real = f };
        }
    }
    const f = util.parseFloatStrictOpt(s) orelse return null;
    return Value{ .real = f };
}

/// `sign(x) ∈ {-1, 0, 1, NULL}`. Always INTEGER (not REAL) per
/// sqlite3 docs — distinct from the rest of this module. Non-numeric
/// TEXT (`'abc'`) and NaN both collapse to NULL.
pub fn fnSign(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    if (std.math.isNan(x)) return Value.null;
    if (x > 0.0) return Value{ .integer = 1 };
    if (x < 0.0) return Value{ .integer = -1 };
    return Value{ .integer = 0 };
}

pub fn fnDegrees(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    return Value{ .real = x * 180.0 / std.math.pi };
}

pub fn fnRadians(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    return Value{ .real = x * std.math.pi / 180.0 };
}

/// `mod(a, b)` is C99 `fmod` — sign tracks the dividend, which means
/// `mod(-10, 3) = -1` and `mod(10, -3) = 1` (matches sqlite3 3.51.0).
/// Division by zero collapses to NULL.
pub fn fnMod(args: []const Value) Error!Value {
    if (args.len != 2) return Error.WrongArgumentCount;
    const a = argReal(args[0]) orelse return Value.null;
    const b = argReal(args[1]) orelse return Value.null;
    if (b == 0.0) return Value.null;
    return Value{ .real = @rem(a, b) };
}
