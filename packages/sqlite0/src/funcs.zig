const std = @import("std");
const ops = @import("ops.zig");
const util = @import("func_util.zig");
const text = @import("funcs_text.zig");
const substr_mod = @import("funcs_substr.zig");
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
    if (util.eqlIgnoreCase(name, "substr") or util.eqlIgnoreCase(name, "substring")) return substr_mod.fnSubstr(allocator, args);
    if (util.eqlIgnoreCase(name, "abs")) return fnAbs(allocator, args);
    // sqlite3 splits these: `coalesce` is variadic with min 2 args;
    // `ifnull` is fixed at exactly 2 args (3+ raises "wrong number of
    // arguments to function ifnull()" at prepare time).
    if (util.eqlIgnoreCase(name, "coalesce")) return fnCoalesce(allocator, args);
    if (util.eqlIgnoreCase(name, "ifnull")) return fnIfnull(allocator, args);
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
    if (util.eqlIgnoreCase(name, "sqlite_version")) return fnSqliteVersion(allocator, args);
    if (util.eqlIgnoreCase(name, "sqlite_compileoption_used")) return fnSqliteCompileoptionUsed(args);
    if (util.eqlIgnoreCase(name, "sqlite_compileoption_get")) return fnSqliteCompileoptionGet(args);
    return Error.UnknownFunction;
}

/// `sqlite_version()` — sqlite3 version string. We pin to the version we
/// target for differential testing parity. Update this constant when
/// rolling forward to a new sqlite3 release. The value is observable to
/// applications gating on version (e.g. `iif(sqlite_version() >= '3.42',
/// ...)`), so it must read true to the feature surface we implement.
const sqlite_compat_version: []const u8 = "3.51.0";

fn fnSqliteVersion(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 0) return Error.WrongArgumentCount;
    return Value{ .text = try allocator.dupe(u8, sqlite_compat_version) };
}

/// `sqlite_compileoption_used(name)` — returns 1 if the build was compiled
/// with the named SQLITE_ENABLE_*/SQLITE_OMIT_* option, 0 otherwise. We
/// have no compile-option matrix; return 0 for everything (sqlite3
/// surface contract: an unknown name is always 0, never an error). NULL
/// arg propagates to NULL.
fn fnSqliteCompileoptionUsed(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    if (args[0] == .null) return Value.null;
    return Value{ .integer = 0 };
}

/// `sqlite_compileoption_get(N)` — returns the Nth compile option as TEXT,
/// or NULL when N is out of range. We have no options to enumerate so any
/// index returns NULL; sqlite3 returns NULL past its option list too.
fn fnSqliteCompileoptionGet(args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;
    return Value.null;
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

/// `iif(c1, v1, c2, v2, ..., [default])` — sqlite3 3.46+ variadic
/// CASE/WHEN/THEN chain. Original 3-arg form (`iif(c, v, default)`)
/// still works. Scan args in pairs: if `cN` truthy, return `vN`. After
/// all pairs, if `args.len` is odd the last unpaired arg is the default;
/// even count → fall through to NULL.
///
/// Examples (verified against sqlite3 3.51.0):
///   iif(1, 'a', 'b')               → 'a'
///   iif(0, 'a', 'b')               → 'b'
///   iif(0, 'a', 1, 'b')            → 'b'
///   iif(0, 'a', 0, 'b')            → NULL
///   iif(0, 'a', 0, 'b', 'def')     → 'def'
///   iif(0, 'a', 'b', 'c')          → NULL ('b' as boolean is 0)
fn fnIif(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len < 2) return Error.WrongArgumentCount;
    var i: usize = 0;
    while (i + 1 < args.len) : (i += 2) {
        const cond = ops.truthy(args[i]) orelse false;
        if (cond) return util.dupeValue(allocator, args[i + 1]);
    }
    // Odd count: leftover arg is the default. Even: NULL.
    if (i < args.len) return util.dupeValue(allocator, args[i]);
    return Value.null;
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

fn fnIfnull(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 2) return Error.WrongArgumentCount;
    return fnCoalesce(allocator, args);
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
        // sqlite3 quirk: ties go LEFT for `max` (strict `<`) and RIGHT
        // for `min` (non-strict `≥`). Empirically: `min(1, 1.0)` → `1.0`,
        // `max(1, 1.0)` → `1`. The aggregate-form min/max also has this
        // skew (last-tied for min, first-tied for max).
        const replace_best = switch (dir) {
            .min => order != .lt,
            .max => order == .lt,
        };
        if (replace_best) best = a;
    }
    return try util.dupeValue(allocator, best);
}

// Tests live in funcs_test.zig (split out for the 500-line discipline).
