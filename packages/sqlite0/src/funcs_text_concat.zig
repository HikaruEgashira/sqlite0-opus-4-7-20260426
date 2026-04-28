//! `concat()` and `concat_ws()` — extracted from `funcs_text.zig` to
//! keep that file under the 500-line discipline.

const std = @import("std");
const ops = @import("ops.zig");
const util = @import("func_util.zig");

const Value = util.Value;
const Error = util.Error;

/// `concat(a, b, ...)` — sqlite3's NULL-skipping TEXT concatenator.
/// Each non-NULL argument is rendered as text (INTEGER/REAL via the
/// %g-style renderer, BLOB as raw bytes, TEXT verbatim) and joined
/// with no separator. NULL arguments are silently dropped — even an
/// all-NULL call returns a non-NULL empty TEXT (sqlite3 quirk).
/// Requires `args.len >= 1`.
pub fn fnConcat(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len < 1) return Error.WrongArgumentCount;
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    for (args) |a| {
        if (a == .null) continue;
        try appendValueAsText(allocator, &out, a);
    }
    return Value{ .text = try out.toOwnedSlice(allocator) };
}

/// `concat_ws(sep, a, b, ...)` — sqlite3 separator-join. NULL sep → NULL.
/// NULL value args are skipped (no value, no sep). Empty-string value args
/// are ALSO skipped while no non-empty value has been emitted yet (sqlite3
/// quirk: leading empties act like NULL); after the first non-empty emit,
/// subsequent empties are kept with their separator. So:
///   `concat_ws('-', '', 'a', '')` → `'a-'`
///   `concat_ws('-', 'a', '', 'b')` → `'a--b'`
///   `concat_ws('-', '', '', 'a')`  → `'a'`
pub fn fnConcatWs(allocator: std.mem.Allocator, args: []const Value) Error!Value {
    if (args.len < 2) return Error.WrongArgumentCount;
    if (args[0] == .null) return Value.null;

    const sep = try util.ensureText(allocator, args[0]);
    defer allocator.free(sep);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var seen_nonempty = false;
    for (args[1..]) |a| {
        if (a == .null) continue;
        var owned: ?[]u8 = null;
        defer if (owned) |b| allocator.free(b);
        const bytes: []const u8 = switch (a) {
            .text => |t| t,
            .blob => |b| b,
            .integer, .real => blk: {
                const t = ops.valueToOwnedText(allocator, a) catch |err| switch (err) {
                    error.OutOfMemory => return Error.OutOfMemory,
                    error.NotConvertible => return Error.UnsupportedFeature,
                };
                owned = t;
                break :blk t;
            },
            .null => unreachable,
        };
        if (!seen_nonempty and bytes.len == 0) continue;
        if (seen_nonempty) try out.appendSlice(allocator, sep);
        try out.appendSlice(allocator, bytes);
        seen_nonempty = true;
    }
    return Value{ .text = try out.toOwnedSlice(allocator) };
}

fn appendValueAsText(
    allocator: std.mem.Allocator,
    out: *std.ArrayList(u8),
    v: Value,
) !void {
    switch (v) {
        .text, .blob => |bytes| try out.appendSlice(allocator, bytes),
        .integer, .real => {
            const t = ops.valueToOwnedText(allocator, v) catch |err| switch (err) {
                error.OutOfMemory => return Error.OutOfMemory,
                error.NotConvertible => return Error.UnsupportedFeature,
            };
            defer allocator.free(t);
            try out.appendSlice(allocator, t);
        },
        .null => unreachable,
    }
}
