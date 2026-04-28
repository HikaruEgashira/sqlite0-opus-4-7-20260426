//! Per-aggregate accumulator state and per-row feed/finalize logic.
//!
//! Split out of `aggregate.zig` to keep both files under the 500-line
//! discipline (CLAUDE.md "Module Splitting Rules"). The boundary is clean:
//! this module knows about one row at a time and one aggregate call at a
//! time. The grouping driver (`aggregate.zig`) owns the row loop, the
//! per-group `Aggregator` slice, and HAVING/ORDER BY post-processing — it
//! never reaches into `Aggregator` internals.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const ast = @import("ast.zig");
const func_util = @import("func_util.zig");
const collation = @import("collation.zig");

const Value = value_mod.Value;
const Error = ops.Error;

/// Per-aggregate accumulator. Keeps the running state for one func_call
/// across the rows of one group. Created once per (group × aggregate-call)
/// pair when a group is first seen.
pub const Aggregator = struct {
    kind: Kind,
    /// `count(DISTINCT x)` / `sum(DISTINCT x)` etc. — when true, `feed`
    /// consults `seen` and skips already-observed non-NULL values before
    /// running the underlying accumulation. NULL is always skipped (matches
    /// the non-DISTINCT non-COUNT-star path).
    distinct: bool = false,
    /// COLLATE kind extracted from the DISTINCT argument's outermost
    /// `Collate(...)` wrapper at parse time (Iter31.Q). `.binary` when none
    /// — TEXT pairs route through `collation.identicalValuesCollated` so
    /// `count(DISTINCT x COLLATE NOCASE)` folds 'A'/'a' into one bucket.
    distinct_kind: ast.CollationKind = .binary,
    /// DISTINCT dedup set. Linear scan against `ops.identicalValues` (sqlite3
    /// "IS NOT DISTINCT FROM" semantics: NULL == NULL, INTEGER 1 == REAL 1.0,
    /// but '1' is its own group). Empty when `distinct == false`. TEXT/BLOB
    /// payloads are duped into the per-statement arena when admitted.
    seen: std.ArrayList(Value) = .empty,
    /// Shared 64-bit row counter. For COUNT it's the result; for
    /// SUM/AVG/TOTAL it's the non-NULL contributor count.
    count: u64 = 0,
    /// Integer running sum (i128 to defer overflow until finalize). Active
    /// while every contributor parses as an integer.
    sum_int: i128 = 0,
    /// Real running sum. Active once any contributor is non-integer (REAL,
    /// non-integer text/blob coercion).
    sum_real: f64 = 0,
    /// Promotion latch: once true we accumulate into `sum_real` only.
    is_real: bool = false,
    /// Best non-NULL value seen so far for MIN/MAX. The bytes live in the
    /// per-statement arena passed to `feed` — arena lifetime spans the
    /// whole grouped SELECT, so this pointer stays valid until finalise.
    best: ?Value = null,
    /// Accumulated `group_concat` output (separators + text-coerced values).
    /// Owned by the per-statement arena passed to `feed`.
    text_buf: std.ArrayListUnmanaged(u8) = .empty,
    /// Number of non-NULL contributors fed so far. `0` after finalise means
    /// the group was all-NULL → result is NULL (sqlite3 quirk).
    text_count: u64 = 0,
    /// Per-row separator override for the 2-arg `group_concat(x, sep)` form.
    /// `aggregate.feedRow` evaluates `fc.args[1]` before each `feed` call and
    /// stores the borrowed text bytes here; `feed` consumes the override
    /// (using it BEFORE appending the current value) and the driver clears
    /// it. NULL ⇒ caller leaves `null` and `feed` substitutes "" (sqlite3
    /// treats a NULL separator as empty).
    sep_override: ?[]const u8 = null,
    /// Whether `sep_override` should be honoured even if it is `null`. When
    /// the 2-arg form yields a NULL separator we want "" (no comma); when
    /// the 1-arg form is used we want the default "," — distinguishing
    /// these two cases requires more than `?[]const u8` alone.
    sep_explicit: bool = false,

    pub const Kind = enum { count_star, count, sum, avg, min, max, total, group_concat };

    pub fn init(kind: Kind, distinct: bool, distinct_kind: ast.CollationKind) Aggregator {
        return .{ .kind = kind, .distinct = distinct, .distinct_kind = distinct_kind };
    }

    /// Try to parse `bytes` as an i64. Returns null if the text isn't a
    /// pure integer (matches sqlite3's "sum keeps integer mode while every
    /// contributor is an integer-shaped string" behaviour).
    fn parseIntStrict(bytes: []const u8) ?i64 {
        const trimmed = std.mem.trim(u8, bytes, " \t\r\n");
        if (trimmed.len == 0) return null;
        return std.fmt.parseInt(i64, trimmed, 10) catch null;
    }

    fn promoteToReal(self: *Aggregator) void {
        if (self.is_real) return;
        self.sum_real = @floatFromInt(self.sum_int);
        self.is_real = true;
    }

    /// Feed one row's contribution. `v` is owned by the caller. For MIN/MAX
    /// the kept best-value is duped into `arena` so the pointer remains
    /// valid after the caller frees `v`.
    pub fn feed(self: *Aggregator, arena: std.mem.Allocator, v: Value) Error!void {
        if (self.distinct) {
            // DISTINCT skips NULL (consistent with non-aggregate-NULL handling
            // for sum/avg/min/max/count(col)) and skips already-seen values.
            // TEXT equality routes through `distinct_kind` (Iter31.Q).
            if (v == .null) return;
            for (self.seen.items) |prior| {
                if (collation.identicalValuesCollated(prior, v, self.distinct_kind)) return;
            }
            try self.seen.append(arena, try dupeArena(arena, v));
        }
        switch (self.kind) {
            .count_star => self.count += 1,
            .count => if (v != .null) {
                self.count += 1;
            },
            .sum, .avg, .total => {
                if (v == .null) return;
                self.count += 1;
                switch (v) {
                    .integer => |i| {
                        if (self.is_real) {
                            self.sum_real += @floatFromInt(i);
                        } else {
                            self.sum_int += i;
                        }
                    },
                    .real => |r| {
                        self.promoteToReal();
                        self.sum_real += r;
                    },
                    .text, .blob => |bytes| {
                        if (!self.is_real) {
                            if (parseIntStrict(bytes)) |as_int| {
                                self.sum_int += as_int;
                                return;
                            }
                            self.promoteToReal();
                        }
                        self.sum_real += func_util.parseFloatLoose(bytes);
                    },
                    .null => unreachable,
                }
            },
            .min, .max => {
                if (v == .null) return;
                if (self.best) |current| {
                    const order = ops.compareValues(current, v);
                    const replace = switch (self.kind) {
                        .min => order == .gt,
                        .max => order == .lt,
                        else => unreachable,
                    };
                    if (replace) {
                        ops.freeValue(arena, current);
                        self.best = try dupeArena(arena, v);
                    }
                } else {
                    self.best = try dupeArena(arena, v);
                }
            },
            .group_concat => {
                if (v == .null) return;
                if (self.text_count > 0) {
                    // sqlite3: the separator used between rows N-1 and N is
                    // the separator EVALUATED ON ROW N (the dynamic-sep
                    // case). For 1-arg we fall back to ",".
                    const sep: []const u8 = if (self.sep_explicit)
                        (self.sep_override orelse "")
                    else
                        ",";
                    try self.text_buf.appendSlice(arena, sep);
                }
                switch (v) {
                    .text, .blob => |bytes| try self.text_buf.appendSlice(arena, bytes),
                    .integer, .real => {
                        const txt = try func_util.ensureText(arena, v);
                        defer arena.free(txt);
                        try self.text_buf.appendSlice(arena, txt);
                    },
                    .null => unreachable,
                }
                self.text_count += 1;
            },
        }
    }

    /// Produce the finalised Value for this aggregator. TEXT/BLOB bytes are
    /// duped into `out_alloc` so the result outlives the accumulator's own
    /// `best` storage (callers free both independently).
    pub fn finalize(self: *Aggregator, out_alloc: std.mem.Allocator) Error!Value {
        return switch (self.kind) {
            .count_star, .count => Value{ .integer = @intCast(self.count) },
            .sum => blk: {
                if (self.count == 0) break :blk Value.null;
                if (self.is_real) break :blk Value{ .real = self.sum_real };
                if (self.sum_int < std.math.minInt(i64) or self.sum_int > std.math.maxInt(i64)) {
                    return Error.IntegerOverflow;
                }
                break :blk Value{ .integer = @intCast(self.sum_int) };
            },
            .total => blk: {
                if (self.count == 0) break :blk Value{ .real = 0 };
                if (self.is_real) break :blk Value{ .real = self.sum_real };
                break :blk Value{ .real = @floatFromInt(self.sum_int) };
            },
            .avg => blk: {
                if (self.count == 0) break :blk Value.null;
                const numer: f64 = if (self.is_real) self.sum_real else @floatFromInt(self.sum_int);
                break :blk Value{ .real = numer / @as(f64, @floatFromInt(self.count)) };
            },
            .min, .max => blk: {
                if (self.best) |current| break :blk dupeArena(out_alloc, current) catch |err| return err;
                break :blk Value.null;
            },
            .group_concat => blk: {
                if (self.text_count == 0) break :blk Value.null;
                break :blk Value{ .text = try out_alloc.dupe(u8, self.text_buf.items) };
            },
        };
    }
};

/// Shared TEXT/BLOB byte-dupe used by both `Aggregator.feed`/`finalize` (for
/// keeping MIN/MAX `best` values alive) and the driver's ORDER-BY-key path.
pub fn dupeArena(allocator: std.mem.Allocator, v: Value) !Value {
    return switch (v) {
        .text => |t| Value{ .text = try allocator.dupe(u8, t) },
        .blob => |b| Value{ .blob = try allocator.dupe(u8, b) },
        else => v,
    };
}

/// Map an `ast.Expr.FuncCall` aggregate call to an initialised `Aggregator`.
/// `count` with zero args is `count(*)`; one arg is the column-aware variant.
/// `min`/`max` arity is already validated to 1 by `aggregate_walk.isAggregateCall`.
pub fn aggregatorFromCall(fc: ast.Expr.FuncCall) Aggregator {
    const kind: Aggregator.Kind = if (func_util.eqlIgnoreCase(fc.name, "count"))
        if (fc.args.len == 0) .count_star else .count
    else if (func_util.eqlIgnoreCase(fc.name, "sum")) .sum
    else if (func_util.eqlIgnoreCase(fc.name, "avg")) .avg
    else if (func_util.eqlIgnoreCase(fc.name, "total")) .total
    else if (func_util.eqlIgnoreCase(fc.name, "min")) .min
    else if (func_util.eqlIgnoreCase(fc.name, "max")) .max
    else if (func_util.eqlIgnoreCase(fc.name, "group_concat") or
        func_util.eqlIgnoreCase(fc.name, "string_agg")) .group_concat
    else unreachable;
    const distinct_kind: ast.CollationKind = if (fc.distinct and fc.args.len == 1)
        (collation.peekKind(fc.args[0]) orelse .binary)
    else
        .binary;
    return Aggregator.init(kind, fc.distinct, distinct_kind);
}
