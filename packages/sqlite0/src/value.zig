const std = @import("std");

pub const Value = union(enum) {
    null,
    integer: i64,
    real: f64,
    text: []const u8,
    blob: []const u8,

    pub fn format(self: Value, w: *std.Io.Writer) !void {
        switch (self) {
            .null => try w.writeAll(""),
            .integer => |i| try w.print("{d}", .{i}),
            .real => |f| try formatReal(w, f),
            .text => |t| try w.writeAll(t),
            .blob => |b| try w.writeAll(b),
        }
    }
};

fn formatReal(w: *std.Io.Writer, f: f64) !void {
    if (std.math.isNan(f)) {
        try w.writeAll("");
        return;
    }
    if (std.math.isInf(f)) {
        try w.writeAll(if (f > 0) "Inf" else "-Inf");
        return;
    }
    if (f == @trunc(f) and @abs(f) <= 1e15) {
        try w.print("{d}.0", .{f});
        return;
    }
    try w.print("{d}", .{f});
}

test "Value: integer formatting" {
    var buf: [64]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    const v: Value = .{ .integer = 42 };
    try v.format(&w);
    try std.testing.expectEqualStrings("42", w.buffered());
}

test "Value: text formatting" {
    var buf: [64]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    const v: Value = .{ .text = "hello" };
    try v.format(&w);
    try std.testing.expectEqualStrings("hello", w.buffered());
}
