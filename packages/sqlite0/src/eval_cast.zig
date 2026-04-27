//! `CAST(<expr> AS <type-name>)` value-conversion helpers (Iter23).
//!
//! Split out of `eval.zig` to keep that file under the 500-line discipline
//! (CLAUDE.md "Module Splitting Rules"). The split point is "after the
//! inner expression is evaluated": `eval.evalExpr` recurses into the
//! CAST's child to get a Value, then this module folds that Value into
//! the requested affinity.
//!
//! Conversion rules follow sqlite3's CAST semantics literally:
//!   - INTEGER: skip whitespace, optional sign, accumulate digits, stop at
//!     first non-digit; non-numeric → 0; saturate to i64 min/max on
//!     overflow; REAL truncates toward zero.
//!   - REAL: lenient `parseFloat` (with whitespace trim); non-numeric → 0.0.
//!   - TEXT: numbers stringify via `valueToOwnedText`; bytes copy as-is.
//!   - BLOB: same byte content as the source representation.
//!   - NUMERIC: INTEGER if the value is integer-valued AND the source had
//!     no fractional syntax; else REAL. NULL passes through unchanged for
//!     every target.

const std = @import("std");
const ast = @import("ast.zig");
const ops = @import("ops.zig");
const value_mod = @import("value.zig");

const Value = value_mod.Value;
const Error = ops.Error;

/// Convert `v` to the requested affinity. NULL passes through unchanged
/// for every target — sqlite3 quirk: `CAST(NULL AS X)` is always NULL.
pub fn castValue(allocator: std.mem.Allocator, v: Value, target: ast.Expr.Affinity) Error!Value {
    if (v == .null) return Value.null;
    return switch (target) {
        .integer => Value{ .integer = coerceToIntegerSat(v) },
        .real => Value{ .real = coerceToReal(v) },
        .text => switch (v) {
            .text => |t| Value{ .text = try allocator.dupe(u8, t) },
            .blob => |b| Value{ .text = try allocator.dupe(u8, b) },
            else => Value{ .text = ops.valueToOwnedText(allocator, v) catch |err| switch (err) {
                error.OutOfMemory => return Error.OutOfMemory,
                error.NotConvertible => return Error.UnsupportedFeature,
            } },
        },
        .blob => switch (v) {
            .blob => |b| Value{ .blob = try allocator.dupe(u8, b) },
            .text => |t| Value{ .blob = try allocator.dupe(u8, t) },
            else => Value{ .blob = ops.valueToOwnedText(allocator, v) catch |err| switch (err) {
                error.OutOfMemory => return Error.OutOfMemory,
                error.NotConvertible => return Error.UnsupportedFeature,
            } },
        },
        .numeric => castNumeric(v),
    };
}

fn castNumeric(v: Value) Error!Value {
    return switch (v) {
        .integer => Value{ .integer = v.integer },
        // sqlite3 NUMERIC keeps a direct REAL source as REAL —
        // `CAST(5.0 AS NUMERIC)` returns 5.0 (typeof real), even though
        // 5.0 is integer-representable. The integer-collapse only fires
        // for TEXT/BLOB inputs.
        .real => Value{ .real = v.real },
        .text, .blob => blk: {
            const bytes = if (v == .text) v.text else v.blob;
            // Fast path: leading-integer with no fractional syntax stays
            // INTEGER. Crucially this uses the *strict* (non-saturating)
            // parser — sqlite3 quirk: `cast('99999999999999999999' AS
            // NUMERIC)` returns REAL `1e20`, not saturated LLONG_MAX,
            // because NUMERIC's integer-collapse only fires when the
            // value fits cleanly. The INTEGER cast path keeps saturating
            // semantics through `coerceToIntegerSat`.
            if (parseLeadingI64Strict(bytes)) |i| {
                if (!hasFractionalSyntax(bytes)) break :blk Value{ .integer = i };
            }
            // Otherwise: lenient parseFloat (`'foo'` / `''` / unparseable
            // → 0.0). If the resulting REAL is integer-representable in
            // i64, collapse to INTEGER — this matches sqlite3's
            // sqlite3VdbeMemNumerify path for TEXT-source CAST AS
            // NUMERIC. `'5.0'` → 5, `'1.5e2'` → 150, `'foo'` → 0,
            // `'1e20'` (out of i64 range) → REAL.
            const r = std.fmt.parseFloat(f64, std.mem.trim(u8, bytes, " \t\n\r")) catch 0.0;
            if (realIsExactI64(r)) |i| break :blk Value{ .integer = i };
            break :blk Value{ .real = r };
        },
        .null => unreachable, // handled by caller
    };
}

/// Returns the i64 form of `r` if `r` is finite, in i64 range, and
/// round-trips byte-equal through f64. Used by the TEXT/BLOB → NUMERIC
/// integer-collapse path. The strict round-trip test rejects values
/// that lost fractional precision (`1e20`, `5.5`) — those stay REAL.
fn realIsExactI64(r: f64) ?i64 {
    if (std.math.isNan(r) or std.math.isInf(r)) return null;
    // f64-representable bounds for i64. Comparing against std.math.maxInt(i64)
    // promoted to f64 silently rounds up to 2^63 (out of range), so use
    // the literal nearest-representable bound.
    const i64_max_f: f64 = 9.2233720368547758e18;
    const i64_min_f: f64 = -9.2233720368547758e18;
    if (r >= i64_max_f or r <= i64_min_f) return null;
    const i: i64 = @intFromFloat(@trunc(r));
    if (@as(f64, @floatFromInt(i)) != r) return null;
    return i;
}

fn coerceToIntegerSat(v: Value) i64 {
    return switch (v) {
        .null => 0,
        .integer => |i| i,
        .real => |r| blk: {
            if (std.math.isNan(r)) break :blk 0;
            // i64 max ≈ 9.22e18; the comptime-known maxInt loses precision
            // when promoted to f64, so compare against the f64-representable
            // bound directly.
            const i64_max_f: f64 = 9.2233720368547758e18;
            const i64_min_f: f64 = -9.2233720368547758e18;
            if (r >= i64_max_f) break :blk std.math.maxInt(i64);
            if (r <= i64_min_f) break :blk std.math.minInt(i64);
            break :blk @intFromFloat(@trunc(r));
        },
        .text => |t| parseLeadingI64(t) orelse 0,
        .blob => |b| parseLeadingI64(b) orelse 0,
    };
}

fn coerceToReal(v: Value) f64 {
    return switch (v) {
        .null => 0.0,
        .integer => |i| @floatFromInt(i),
        .real => |r| r,
        .text => |t| std.fmt.parseFloat(f64, std.mem.trim(u8, t, " \t\n\r")) catch 0.0,
        .blob => |b| std.fmt.parseFloat(f64, std.mem.trim(u8, b, " \t\n\r")) catch 0.0,
    };
}

/// sqlite3-style integer extraction: skip ASCII whitespace, read optional
/// `+`/`-`, accumulate digits, stop at first non-digit. Returns null when
/// no digits were seen (caller substitutes 0). Saturates on i64 overflow
/// — used by `CAST AS INTEGER` which mirrors sqlite3 atoi64 semantics.
fn parseLeadingI64(bytes: []const u8) ?i64 {
    return parseLeadingI64Inner(bytes, .saturate);
}

/// Same as `parseLeadingI64` but returns null on i64 overflow instead of
/// saturating. Used by the NUMERIC cast fast path: sqlite3 reserves
/// integer collapse for values that fit cleanly; `'99...'` runs that
/// overflow fall back to REAL via parseFloat.
fn parseLeadingI64Strict(bytes: []const u8) ?i64 {
    return parseLeadingI64Inner(bytes, .strict);
}

const OverflowMode = enum { saturate, strict };

fn parseLeadingI64Inner(bytes: []const u8, mode: OverflowMode) ?i64 {
    var i: usize = 0;
    while (i < bytes.len and (bytes[i] == ' ' or bytes[i] == '\t' or bytes[i] == '\n' or bytes[i] == '\r')) i += 1;
    var neg = false;
    if (i < bytes.len and (bytes[i] == '+' or bytes[i] == '-')) {
        neg = bytes[i] == '-';
        i += 1;
    }
    if (i >= bytes.len or bytes[i] < '0' or bytes[i] > '9') return null;
    var n: i64 = 0;
    while (i < bytes.len and bytes[i] >= '0' and bytes[i] <= '9') : (i += 1) {
        const d: i64 = bytes[i] - '0';
        if (n > @divTrunc(std.math.maxInt(i64) - d, 10)) {
            return switch (mode) {
                .saturate => if (neg) std.math.minInt(i64) else std.math.maxInt(i64),
                .strict => null,
            };
        }
        n = n * 10 + d;
    }
    return if (neg) -n else n;
}

fn hasFractionalSyntax(bytes: []const u8) bool {
    for (bytes) |c| {
        if (c == '.' or c == 'e' or c == 'E') return true;
    }
    return false;
}
