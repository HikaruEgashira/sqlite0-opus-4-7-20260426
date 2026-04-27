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

/// Coerce a function argument to f64. NULL inputs short-circuit the
/// whole call (sqlite3: any NULL arg → NULL result). TEXT/BLOB are
/// parsed via the lenient prefix parser (`'10' → 10.0`, `'foo' →
/// 0.0`), matching sqlite3's `CAST(... AS REAL)` shape.
fn argReal(v: Value) ?f64 {
    return switch (v) {
        .null => null,
        else => util.numericAsReal(v),
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
    if (x <= -1.0 or x >= 1.0) return Value.null;
    return Value{ .real = std.math.atanh(x) };
}

pub fn fnCeil(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    return Value{ .real = @ceil(x) };
}

pub fn fnFloor(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    return Value{ .real = @floor(x) };
}

pub fn fnTrunc(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const x = argReal(args[0]) orelse return Value.null;
    return Value{ .real = @trunc(x) };
}

/// `sign(x) ∈ {-1, 0, 1, NULL}`. Always INTEGER (not REAL) per
/// sqlite3 docs — distinct from the rest of this module.
pub fn fnSign(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    if (args[0] == .null) return Value.null;
    const x = util.numericAsReal(args[0]);
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
