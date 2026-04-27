const std = @import("std");
const ops = @import("ops.zig");
const util = @import("func_util.zig");
const text = @import("funcs_text.zig");
const fmt_mod = @import("funcs_format.zig");
const time_mod = @import("funcs_time.zig");
const time_diff_mod = @import("funcs_time_diff.zig");
const math_mod = @import("funcs_math.zig");
const json_mod = @import("funcs_json.zig");
const match = @import("match.zig");
const database = @import("database.zig");

const Value = util.Value;
const Error = util.Error;
const Database = database.Database;

/// Built-in scalar function dispatch. `name` is matched case-insensitively.
/// `args` are owned by the caller — implementations must not free them, but
/// may dupe data into the returned Value, which the caller takes ownership of.
/// Returns `Error.UnknownFunction` for names that aren't registered yet.
/// `db` is non-null whenever evaluation runs through `engine.dispatchOne`;
/// state-aware functions (`changes()` / `last_insert_rowid()` / etc.) read
/// from it. Functions that only need allocator-and-args ignore the param.
pub fn call(allocator: std.mem.Allocator, db: ?*Database, name: []const u8, args: []const Value) Error!Value {
    if (util.eqlIgnoreCase(name, "length")) return text.fnLength(allocator, args);
    if (util.eqlIgnoreCase(name, "octet_length")) return text.fnOctetLength(allocator, args);
    if (util.eqlIgnoreCase(name, "unhex")) return text.fnUnhex(allocator, args);
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
    if (util.eqlIgnoreCase(name, "concat")) return text.fnConcat(allocator, args);
    if (util.eqlIgnoreCase(name, "concat_ws")) return text.fnConcatWs(allocator, args);
    if (util.eqlIgnoreCase(name, "replace")) return text.fnReplace(allocator, args);
    if (util.eqlIgnoreCase(name, "hex")) return text.fnHex(allocator, args);
    if (util.eqlIgnoreCase(name, "quote")) return text.fnQuote(allocator, args);
    if (util.eqlIgnoreCase(name, "trim")) return text.fnTrim(allocator, args, .both);
    if (util.eqlIgnoreCase(name, "ltrim")) return text.fnTrim(allocator, args, .left);
    if (util.eqlIgnoreCase(name, "rtrim")) return text.fnTrim(allocator, args, .right);
    if (util.eqlIgnoreCase(name, "instr")) return text.fnInstr(allocator, args);
    if (util.eqlIgnoreCase(name, "like")) return match.fnLike(allocator, args);
    if (util.eqlIgnoreCase(name, "glob")) return match.fnGlob(allocator, args);
    if (util.eqlIgnoreCase(name, "char")) return text.fnChar(allocator, args);
    if (util.eqlIgnoreCase(name, "unicode")) return text.fnUnicode(allocator, args);
    if (util.eqlIgnoreCase(name, "random")) return fnRandom(args);
    if (util.eqlIgnoreCase(name, "zeroblob")) return fnZeroblob(allocator, args);
    if (util.eqlIgnoreCase(name, "randomblob")) return fnRandomblob(allocator, args);
    if (util.eqlIgnoreCase(name, "printf") or util.eqlIgnoreCase(name, "format")) return fmt_mod.fnPrintf(allocator, args);
    if (util.eqlIgnoreCase(name, "strftime")) return time_mod.fnStrftime(allocator, args);
    if (util.eqlIgnoreCase(name, "date")) return time_mod.fnDate(allocator, args);
    if (util.eqlIgnoreCase(name, "time")) return time_mod.fnTime(allocator, args);
    if (util.eqlIgnoreCase(name, "datetime")) return time_mod.fnDatetime(allocator, args);
    if (util.eqlIgnoreCase(name, "julianday")) return time_mod.fnJulianday(allocator, args);
    if (util.eqlIgnoreCase(name, "unixepoch")) return time_mod.fnUnixepoch(allocator, args);
    if (util.eqlIgnoreCase(name, "timediff")) return time_diff_mod.fnTimediff(allocator, args);
    if (util.eqlIgnoreCase(name, "iif")) return fnIif(allocator, args);
    // sqlite3 math extension (SQLITE_ENABLE_MATH_FUNCTIONS).
    if (util.eqlIgnoreCase(name, "pi")) return math_mod.fnPi(args);
    if (util.eqlIgnoreCase(name, "sqrt")) return math_mod.fnSqrt(args);
    if (util.eqlIgnoreCase(name, "log")) return math_mod.fnLog(args);
    if (util.eqlIgnoreCase(name, "ln")) return math_mod.fnLn(args);
    if (util.eqlIgnoreCase(name, "log10")) return math_mod.fnLog10(args);
    if (util.eqlIgnoreCase(name, "log2")) return math_mod.fnLog2(args);
    if (util.eqlIgnoreCase(name, "exp")) return math_mod.fnExp(args);
    if (util.eqlIgnoreCase(name, "pow") or util.eqlIgnoreCase(name, "power")) return math_mod.fnPow(args);
    if (util.eqlIgnoreCase(name, "sin")) return math_mod.fnSin(args);
    if (util.eqlIgnoreCase(name, "cos")) return math_mod.fnCos(args);
    if (util.eqlIgnoreCase(name, "tan")) return math_mod.fnTan(args);
    if (util.eqlIgnoreCase(name, "asin")) return math_mod.fnAsin(args);
    if (util.eqlIgnoreCase(name, "acos")) return math_mod.fnAcos(args);
    if (util.eqlIgnoreCase(name, "atan")) return math_mod.fnAtan(args);
    if (util.eqlIgnoreCase(name, "atan2")) return math_mod.fnAtan2(args);
    if (util.eqlIgnoreCase(name, "sinh")) return math_mod.fnSinh(args);
    if (util.eqlIgnoreCase(name, "cosh")) return math_mod.fnCosh(args);
    if (util.eqlIgnoreCase(name, "tanh")) return math_mod.fnTanh(args);
    if (util.eqlIgnoreCase(name, "asinh")) return math_mod.fnAsinh(args);
    if (util.eqlIgnoreCase(name, "acosh")) return math_mod.fnAcosh(args);
    if (util.eqlIgnoreCase(name, "atanh")) return math_mod.fnAtanh(args);
    if (util.eqlIgnoreCase(name, "ceil") or util.eqlIgnoreCase(name, "ceiling")) return math_mod.fnCeil(args);
    if (util.eqlIgnoreCase(name, "floor")) return math_mod.fnFloor(args);
    if (util.eqlIgnoreCase(name, "trunc")) return math_mod.fnTrunc(args);
    if (util.eqlIgnoreCase(name, "sign")) return math_mod.fnSign(args);
    if (util.eqlIgnoreCase(name, "degrees")) return math_mod.fnDegrees(args);
    if (util.eqlIgnoreCase(name, "radians")) return math_mod.fnRadians(args);
    if (util.eqlIgnoreCase(name, "mod")) return math_mod.fnMod(args);
    // Iter29.U — JSON1 constructors (serializer-only subset).
    if (util.eqlIgnoreCase(name, "json_array")) return json_mod.fnJsonArray(allocator, args);
    if (util.eqlIgnoreCase(name, "json_object")) return json_mod.fnJsonObject(allocator, args);
    if (util.eqlIgnoreCase(name, "json_quote")) return json_mod.fnJsonQuote(allocator, args);
    // Iter29.S — state-aware functions reading connection-wide DML
    // bookkeeping. `db == null` (parser-time VALUES tuple eval) gets
    // back 0 — sqlite3's matching behaviour on a fresh connection.
    if (util.eqlIgnoreCase(name, "changes")) return fnChanges(db, args);
    if (util.eqlIgnoreCase(name, "total_changes")) return fnTotalChanges(db, args);
    if (util.eqlIgnoreCase(name, "last_insert_rowid")) return fnLastInsertRowid(db, args);
    return Error.UnknownFunction;
}

fn fnChanges(db: ?*Database, args: []const Value) Error!Value {
    if (args.len != 0) return Error.WrongArgumentCount;
    return Value{ .integer = if (db) |d| d.last_changes else 0 };
}

fn fnTotalChanges(db: ?*Database, args: []const Value) Error!Value {
    if (args.len != 0) return Error.WrongArgumentCount;
    return Value{ .integer = if (db) |d| d.total_changes else 0 };
}

fn fnLastInsertRowid(db: ?*Database, args: []const Value) Error!Value {
    if (args.len != 0) return Error.WrongArgumentCount;
    return Value{ .integer = if (db) |d| d.last_insert_rowid else 0 };
}

/// `iif(cond, a, b)` — sqlite3's CASE WHEN shorthand. Returns `a` when
/// the condition is truthy, `b` otherwise (NULL → ELSE branch, matching
/// CASE WHEN's "not-true" semantics rather than three-valued logic).
/// Both branches are eagerly evaluated upstream — sqlite0 has no
/// short-circuit pathway for scalar args, and sqlite3's iif
/// documentation calls it "syntactic sugar" without a documented
/// short-circuit guarantee, so the divergence is invisible to
/// byte-comparison tests.
fn fnIif(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 3) return Error.WrongArgumentCount;
    const cond = ops.truthy(args[0]) orelse false;
    const picked = if (cond) args[1] else args[2];
    return util.dupeValue(allocator, picked);
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
    ensurePrng();
    const bits = random_prng.?.random().int(u64);
    return Value{ .integer = @bitCast(bits) };
}

fn ensurePrng() void {
    if (random_prng == null) {
        const seed = @intFromPtr(&random_prng) ^ 0xdeadbeefcafebabe;
        random_prng = std.Random.DefaultPrng.init(seed);
    }
}

/// `zeroblob(N)` — return N zero bytes. sqlite3 clamps `N < 0` and
/// NULL to 0 bytes; TEXT input is parsed (`'5'` → 5 bytes, `'foo'`
/// → 0 bytes via the lenient prefix parser). Always returns BLOB
/// even when length is 0.
fn fnZeroblob(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const n = blobLengthFromArg(args[0], 0);
    const bytes = try allocator.alloc(u8, n);
    @memset(bytes, 0);
    return Value{ .blob = bytes };
}

/// `randomblob(N)` — return N random bytes. sqlite3 clamps `N < 1`
/// (including `0` / `-N` / NULL / non-numeric TEXT) up to 1 byte —
/// quirky but stable across versions. Bytes come from the same
/// process-wide PRNG as `random()`.
fn fnRandomblob(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    const n = blobLengthFromArg(args[0], 1);
    const bytes = try allocator.alloc(u8, n);
    ensurePrng();
    random_prng.?.random().bytes(bytes);
    return Value{ .blob = bytes };
}

/// Common length-coercion shape for `zeroblob` / `randomblob`. NULL
/// and out-of-range numerics fall back to `min`. The `i64` cast goes
/// through `numericAsReal` so TEXT inputs parse the same way they do
/// for `mod()` / `pow()` / etc.
fn blobLengthFromArg(v: Value, min: usize) usize {
    if (v == .null) return min;
    const r = util.numericAsReal(v);
    if (std.math.isNan(r) or r < @as(f64, @floatFromInt(min))) return min;
    // u32 is more than enough — 4GB upper cap matches sqlite3's
    // SQLITE_MAX_LENGTH default and prevents OOM on `zeroblob(1e18)`.
    if (r > 1_000_000_000.0) return 1_000_000_000;
    return @intFromFloat(r);
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

/// SQLite `substr(X, P, N)` / `substr(X, P)` — sqlite3 `substrFunc` port.
///
/// TEXT semantics: P and N count UTF-8 *characters* (not bytes), and the
/// scan honors the C-string convention (stops at the first NUL byte). BLOB
/// semantics: P and N are byte indices, and the result type is BLOB. P=0
/// is a special case that pretends the string starts at index 1 but trims
/// the count by one. Negative N (TEXT or BLOB) takes |N| chars/bytes ending
/// before P. INTEGER/REAL inputs render via `valueToOwnedText` and use TEXT
/// semantics — char-count matches byte-count for the canonical numeric forms.
fn fnSubstr(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 2 and args.len != 3) return Error.WrongArgumentCount;
    if (args[0] == .null or args[1] == .null) return Value.null;
    if (args.len == 3 and args[2] == .null) return Value.null;

    const has_n = args.len == 3;
    const p_raw = try util.toIntCoerce(args[1]);
    const n_raw: i64 = if (has_n) try util.toIntCoerce(args[2]) else 0;

    if (args[0] == .blob) {
        const slice = byteSubstrSlice(args[0].blob, p_raw, n_raw, has_n);
        return Value{ .blob = try allocator.dupe(u8, slice) };
    }

    var owned: ?[]u8 = null;
    defer if (owned) |b| allocator.free(b);
    const subject_text: []const u8 = switch (args[0]) {
        .text => |t| t,
        else => blk: {
            owned = ops.valueToOwnedText(allocator, args[0]) catch |err| switch (err) {
                error.OutOfMemory => return Error.OutOfMemory,
                error.NotConvertible => return Error.UnsupportedFeature,
            };
            break :blk owned.?;
        },
    };
    const slice = utf8CharSubstrSlice(subject_text, p_raw, n_raw, has_n);
    return Value{ .text = try allocator.dupe(u8, slice) };
}

fn byteSubstrSlice(z: []const u8, p_raw: i64, n_raw: i64, has_n: bool) []const u8 {
    const len_i: i64 = @intCast(z.len);
    var p1: i64 = p_raw;
    var p2: i64 = if (has_n) n_raw else len_i;
    var negP2: bool = false;
    if (has_n and p2 < 0) {
        p2 = -p2;
        negP2 = true;
    }
    if (p1 < 0) {
        p1 += len_i;
        if (p1 < 0) {
            p2 += p1;
            if (p2 < 0) p2 = 0;
            p1 = 0;
        }
    } else if (p1 > 0) {
        p1 -= 1;
    } else if (p2 > 0) {
        p2 -= 1;
    }
    if (negP2) {
        p1 -= p2;
        if (p1 < 0) {
            p2 += p1;
            p1 = 0;
        }
    }
    if (p1 + p2 > len_i) p2 = len_i - p1;
    if (p2 < 0) p2 = 0;
    if (p1 > len_i) return z[0..0];
    const start: usize = @intCast(p1);
    const end: usize = @intCast(p1 + p2);
    return z[start..end];
}

fn utf8CharSubstrSlice(z: []const u8, p_raw: i64, n_raw: i64, has_n: bool) []const u8 {
    var p1: i64 = p_raw;
    // Without an explicit count, sqlite3 substitutes SQLITE_LIMIT_LENGTH
    // (~1 billion) — a sentinel for "rest of string". We use i64 max for
    // the same effect; the NUL-stop loop bounds the actual reach.
    var p2: i64 = if (has_n) n_raw else std.math.maxInt(i64);
    var negP2: bool = false;
    if (has_n and p2 < 0) {
        p2 = -p2;
        negP2 = true;
    }
    if (p1 < 0) {
        // sqlite3: char count up to first NUL (C-string convention).
        var z_idx: usize = 0;
        var len: i64 = 0;
        while (z_idx < z.len and z[z_idx] != 0) : (len += 1) {
            z_idx = skipUtf8Char(z, z_idx);
        }
        p1 += len;
        if (p1 < 0) {
            if (has_n) p2 += p1;
            if (p2 < 0) p2 = 0;
            p1 = 0;
        }
    } else if (p1 > 0) {
        p1 -= 1;
    } else if (p2 > 0) {
        p2 -= 1;
    }
    if (negP2) {
        p1 -= p2;
        if (p1 < 0) {
            p2 += p1;
            p1 = 0;
        }
    }
    var i: usize = 0;
    while (i < z.len and z[i] != 0 and p1 > 0) : (p1 -= 1) {
        i = skipUtf8Char(z, i);
    }
    const start = i;
    while (i < z.len and z[i] != 0 and p2 > 0) : (p2 -= 1) {
        i = skipUtf8Char(z, i);
    }
    return z[start..i];
}

fn skipUtf8Char(z: []const u8, start: usize) usize {
    var j = start + 1;
    if (z[start] >= 0xC0) {
        while (j < z.len and (z[j] & 0xC0) == 0x80) j += 1;
    }
    return j;
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
    // sqlite3's printf-based round (etFLOAT path in printf.c): add a
    // half-step `rounder = 0.5 * 10^-d` to the magnitude, then truncate.
    // f64 intermediate is not enough — for inputs like `2.355` whose
    // IEEE 754 storage is `2.354999...`, `(x + 0.005)` rounds back UP to
    // 2.36 in f64 (52-bit mantissa) before the truncate step, producing
    // 2.36 instead of sqlite3's 2.35. f128 (113-bit mantissa) widens the
    // intermediate so the sub-ulp tail of the original f64 survives the
    // half-step add, and `@trunc` sees the actual digit. The rounder is
    // applied to the magnitude (not the signed value) so negatives round
    // the same way: `round(-2.355, 2)` → `-2.35`.
    var factor: f128 = 1;
    var i: i32 = 0;
    while (i < d) : (i += 1) factor *= 10;
    const x128: f128 = x;
    const half: f128 = 0.5 / factor;
    const adjusted = if (x128 >= 0) x128 + half else x128 - half;
    const result128 = @trunc(adjusted * factor) / factor;
    return Value{ .real = @floatCast(result128) };
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

// Tests live in funcs_test.zig (split out for the 500-line discipline).
