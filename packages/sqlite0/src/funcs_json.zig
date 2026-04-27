//! JSON1 constructor functions (Iter29.U).
//!
//! `json_array(args...)` / `json_object(k, v, ...)` / `json_quote(x)` —
//! the serializer-only subset. A full JSON1 implementation (parsing
//! + path expressions + `json()` canonicalizer) is a follow-up
//! iteration that adds a JSON tokenizer + reader.
//!
//! sqlite3 quirks reproduced here:
//!  - BLOB inputs raise an error ("JSON cannot hold BLOB values")
//!    because no JSON type maps to opaque bytes.
//!  - `json_object` labels (keys) must be TEXT — INTEGER/REAL/NULL
//!    are rejected with `WrongArgumentCount`-shaped error (sqlite3
//!    says "labels must be TEXT", we use UnsupportedFeature for
//!    matching exit code).
//!  - `json_object` requires an even number of arguments.
//!  - String escaping follows JSON spec: `"` `\` and 0x00-0x1F are
//!    escaped; 0x7F (DEL) is passed through; non-ASCII UTF-8 bytes
//!    are emitted raw (no `\uXXXX` for code points ≥ 0x80).
//!  - REAL values are formatted via `Value.format` (the same shortest
//!    round-trip renderer SQLite uses), so `1e-5` emits `1.0e-05`,
//!    `2.5` emits `2.5`, `1.0` emits `1.0`.
//!
//! Known divergence (deferred): nested JSON. sqlite3 tags JSON-
//! producing function results with a "JSON" subtype; passing such a
//! value into another JSON function inlines it raw rather than
//! quoting it as text. sqlite0 has no Value subtype field, so
//! `json_array(json_array(1,2), 3)` emits `["[1,2]",3]` instead of
//! `[[1,2],3]`. Fixing requires adding a subtype channel to Value.

const std = @import("std");
const util = @import("func_util.zig");

const Value = util.Value;
const Error = util.Error;

/// Append a JSON-quoted string to `out`. Caller owns `out`'s allocator.
fn writeJsonString(
    out: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    s: []const u8,
) !void {
    try out.append(allocator, '"');
    for (s) |b| {
        switch (b) {
            '"' => try out.appendSlice(allocator, "\\\""),
            '\\' => try out.appendSlice(allocator, "\\\\"),
            0x08 => try out.appendSlice(allocator, "\\b"),
            0x09 => try out.appendSlice(allocator, "\\t"),
            0x0a => try out.appendSlice(allocator, "\\n"),
            0x0c => try out.appendSlice(allocator, "\\f"),
            0x0d => try out.appendSlice(allocator, "\\r"),
            0x00...0x07, 0x0b, 0x0e...0x1f => {
                var buf: [6]u8 = undefined;
                const escaped = std.fmt.bufPrint(&buf, "\\u{x:0>4}", .{b}) catch unreachable;
                try out.appendSlice(allocator, escaped);
            },
            else => try out.append(allocator, b),
        }
    }
    try out.append(allocator, '"');
}

/// Append a Value as a JSON value to `out`. BLOB inputs are rejected.
fn writeJsonValue(
    out: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    v: Value,
) !void {
    switch (v) {
        .null => try out.appendSlice(allocator, "null"),
        .integer => {
            var buf: [32]u8 = undefined;
            var w = std.Io.Writer.fixed(&buf);
            v.format(&w) catch return Error.UnsupportedFeature;
            try out.appendSlice(allocator, w.buffered());
        },
        .real => |f| {
            // sqlite3 quirk: NaN → JSON `null`; ±Inf → `9.0e+999` /
            // `-9.0e+999` (a SQLite-specific token, not strict JSON).
            // Verified against sqlite3 3.51.0. The bare REAL path emits
            // `Inf` / `NaN` for human-readable output; JSON has its own
            // rendering rules.
            if (std.math.isNan(f)) {
                try out.appendSlice(allocator, "null");
            } else if (std.math.isInf(f)) {
                try out.appendSlice(allocator, if (f > 0) "9.0e+999" else "-9.0e+999");
            } else {
                var buf: [64]u8 = undefined;
                var w = std.Io.Writer.fixed(&buf);
                v.format(&w) catch return Error.UnsupportedFeature;
                try out.appendSlice(allocator, w.buffered());
            }
        },
        .text => |t| try writeJsonString(out, allocator, t),
        .blob => return Error.UnsupportedFeature,
    }
}

pub fn fnJsonArray(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);

    try out.append(allocator, '[');
    for (args, 0..) |a, i| {
        if (i > 0) try out.append(allocator, ',');
        try writeJsonValue(&out, allocator, a);
    }
    try out.append(allocator, ']');

    return Value{ .text = try out.toOwnedSlice(allocator) };
}

pub fn fnJsonObject(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len % 2 != 0) return Error.WrongArgumentCount;

    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);

    try out.append(allocator, '{');
    var i: usize = 0;
    while (i < args.len) : (i += 2) {
        if (i > 0) try out.append(allocator, ',');
        const key = args[i];
        const val = args[i + 1];
        switch (key) {
            .text => |t| try writeJsonString(&out, allocator, t),
            else => return Error.UnsupportedFeature,
        }
        try out.append(allocator, ':');
        try writeJsonValue(&out, allocator, val);
    }
    try out.append(allocator, '}');

    return Value{ .text = try out.toOwnedSlice(allocator) };
}

pub fn fnJsonQuote(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len != 1) return Error.WrongArgumentCount;

    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);

    try writeJsonValue(&out, allocator, args[0]);

    return Value{ .text = try out.toOwnedSlice(allocator) };
}

test "json_array: empty" {
    const allocator = std.testing.allocator;
    const result = try fnJsonArray(allocator, &.{});
    defer allocator.free(result.text);
    try std.testing.expectEqualStrings("[]", result.text);
}

test "json_array: mixed types" {
    const allocator = std.testing.allocator;
    const args = [_]Value{
        .{ .integer = 1 },
        .{ .real = 2.5 },
        .{ .text = "three" },
        .null,
    };
    const result = try fnJsonArray(allocator, &args);
    defer allocator.free(result.text);
    try std.testing.expectEqualStrings("[1,2.5,\"three\",null]", result.text);
}

test "json_array: BLOB rejected" {
    const allocator = std.testing.allocator;
    const args = [_]Value{.{ .blob = "ab" }};
    try std.testing.expectError(Error.UnsupportedFeature, fnJsonArray(allocator, &args));
}

test "json_array: string escaping" {
    const allocator = std.testing.allocator;
    const args = [_]Value{.{ .text = "a\"b\\c\n" }};
    const result = try fnJsonArray(allocator, &args);
    defer allocator.free(result.text);
    try std.testing.expectEqualStrings("[\"a\\\"b\\\\c\\n\"]", result.text);
}

test "json_array: control char escaping" {
    const allocator = std.testing.allocator;
    const args = [_]Value{.{ .text = "\x01" }};
    const result = try fnJsonArray(allocator, &args);
    defer allocator.free(result.text);
    try std.testing.expectEqualStrings("[\"\\u0001\"]", result.text);
}

test "json_object: simple" {
    const allocator = std.testing.allocator;
    const args = [_]Value{
        .{ .text = "a" },
        .{ .integer = 1 },
        .{ .text = "b" },
        .{ .text = "two" },
    };
    const result = try fnJsonObject(allocator, &args);
    defer allocator.free(result.text);
    try std.testing.expectEqualStrings("{\"a\":1,\"b\":\"two\"}", result.text);
}

test "json_object: empty" {
    const allocator = std.testing.allocator;
    const result = try fnJsonObject(allocator, &.{});
    defer allocator.free(result.text);
    try std.testing.expectEqualStrings("{}", result.text);
}

test "json_object: odd args rejected" {
    const allocator = std.testing.allocator;
    const args = [_]Value{.{ .text = "a" }};
    try std.testing.expectError(Error.WrongArgumentCount, fnJsonObject(allocator, &args));
}

test "json_object: non-text key rejected" {
    const allocator = std.testing.allocator;
    const args = [_]Value{ .{ .integer = 1 }, .{ .integer = 2 } };
    try std.testing.expectError(Error.UnsupportedFeature, fnJsonObject(allocator, &args));
}

test "json_quote: scalar" {
    const allocator = std.testing.allocator;
    const r1 = try fnJsonQuote(allocator, &.{.{ .integer = 42 }});
    defer allocator.free(r1.text);
    try std.testing.expectEqualStrings("42", r1.text);

    const r2 = try fnJsonQuote(allocator, &.{.{ .text = "hi" }});
    defer allocator.free(r2.text);
    try std.testing.expectEqualStrings("\"hi\"", r2.text);

    const r3 = try fnJsonQuote(allocator, &.{Value.null});
    defer allocator.free(r3.text);
    try std.testing.expectEqualStrings("null", r3.text);
}
