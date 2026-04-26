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

/// SQLite-compatible %g formatting for f64.
///
/// SQLite renders REALs with up to 15 significant digits, switching to
/// scientific notation when the decimal exponent falls outside [-4, 14].
/// The decimal form keeps a trailing `.0` for integer-valued reals so that
/// the type is unambiguous (e.g. `1.0` instead of `1`). Scientific output
/// uses the lowercase `e±NN` form with at least 2 digits in the exponent
/// (matching the system sqlite3 CLI default).
fn formatReal(w: *std.Io.Writer, f: f64) !void {
    if (std.math.isNan(f)) {
        try w.writeAll("NaN");
        return;
    }
    if (std.math.isInf(f)) {
        try w.writeAll(if (f > 0) "Inf" else "-Inf");
        return;
    }
    if (f == 0) {
        try w.writeAll("0.0");
        return;
    }

    var raw_buf: [64]u8 = undefined;
    const raw = std.fmt.float.render(&raw_buf, f, .{
        .mode = .scientific,
        .precision = 14,
    }) catch {
        try w.print("{d}", .{f});
        return;
    };

    var rest = raw;
    var negative = false;
    if (rest.len > 0 and rest[0] == '-') {
        negative = true;
        rest = rest[1..];
    }

    const e_pos = std.mem.indexOfScalar(u8, rest, 'e') orelse {
        if (negative) try w.writeByte('-');
        try w.writeAll(rest);
        return;
    };
    const mantissa = rest[0..e_pos];
    const exp_str = rest[e_pos + 1 ..];
    const exp = std.fmt.parseInt(i32, exp_str, 10) catch {
        if (negative) try w.writeByte('-');
        try w.writeAll(rest);
        return;
    };

    const dot_pos = std.mem.indexOfScalar(u8, mantissa, '.');
    const lead = if (dot_pos) |p| mantissa[0..p] else mantissa;
    var frac: []const u8 = if (dot_pos) |p| mantissa[p + 1 ..] else "";
    while (frac.len > 0 and frac[frac.len - 1] == '0') frac = frac[0 .. frac.len - 1];

    if (negative) try w.writeByte('-');

    if (exp >= -4 and exp <= 14) {
        try writeDecimal(w, lead, frac, exp);
    } else {
        try writeScientific(w, lead, frac, exp);
    }
}

fn writeDecimal(w: *std.Io.Writer, lead: []const u8, frac: []const u8, exp: i32) !void {
    if (exp >= 0) {
        const e: usize = @intCast(exp);
        try w.writeAll(lead);
        const move = @min(e, frac.len);
        try w.writeAll(frac[0..move]);
        if (e > frac.len) {
            for (0..e - frac.len) |_| try w.writeByte('0');
        }
        const remaining = frac[move..];
        if (remaining.len > 0) {
            try w.writeByte('.');
            try w.writeAll(remaining);
        } else {
            try w.writeAll(".0");
        }
    } else {
        const zeros: usize = @intCast(-exp - 1);
        try w.writeAll("0.");
        for (0..zeros) |_| try w.writeByte('0');
        try w.writeAll(lead);
        try w.writeAll(frac);
    }
}

fn writeScientific(w: *std.Io.Writer, lead: []const u8, frac: []const u8, exp: i32) !void {
    try w.writeAll(lead);
    if (frac.len > 0) {
        try w.writeByte('.');
        try w.writeAll(frac);
    } else {
        try w.writeAll(".0");
    }
    try w.writeByte('e');
    if (exp >= 0) {
        try w.writeByte('+');
        const abs_exp: u32 = @intCast(exp);
        if (abs_exp < 10) try w.writeByte('0');
        try w.print("{d}", .{abs_exp});
    } else {
        try w.writeByte('-');
        const abs_exp: u32 = @intCast(-exp);
        if (abs_exp < 10) try w.writeByte('0');
        try w.print("{d}", .{abs_exp});
    }
}

fn expectFmt(expected: []const u8, value: Value) !void {
    var buf: [128]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try value.format(&w);
    try std.testing.expectEqualStrings(expected, w.buffered());
}

test "Value: integer formatting" {
    try expectFmt("42", .{ .integer = 42 });
    try expectFmt("-7", .{ .integer = -7 });
    try expectFmt("0", .{ .integer = 0 });
}

test "Value: text formatting" {
    try expectFmt("hello", .{ .text = "hello" });
    try expectFmt("", .{ .text = "" });
}

test "Value: null formatting is empty string" {
    try expectFmt("", .null);
}

test "Value: real — integer-valued has .0 suffix" {
    try expectFmt("3.0", .{ .real = 3.0 });
    try expectFmt("-2.0", .{ .real = -2.0 });
    try expectFmt("0.0", .{ .real = 0.0 });
    try expectFmt("0.0", .{ .real = -0.0 });
}

test "Value: real — 15 sig digits, decimal range" {
    try expectFmt("3.14", .{ .real = 3.14 });
    try expectFmt("0.3", .{ .real = 0.1 + 0.2 });
    try expectFmt("0.333333333333333", .{ .real = 1.0 / 3.0 });
    try expectFmt("33.3333333333333", .{ .real = 100.0 / 3.0 });
    try expectFmt("0.0001", .{ .real = 0.0001 });
}

test "Value: real — scientific outside [-4, 14]" {
    try expectFmt("1.0e-05", .{ .real = 1e-5 });
    try expectFmt("1.0e+15", .{ .real = 1e15 });
    try expectFmt("1.0e+16", .{ .real = 1e16 });
    try expectFmt("1.0e-100", .{ .real = 1e-100 });
    try expectFmt("1.0e+100", .{ .real = 1e100 });
}

test "Value: real — Inf / NaN" {
    try expectFmt("Inf", .{ .real = std.math.inf(f64) });
    try expectFmt("-Inf", .{ .real = -std.math.inf(f64) });
    try expectFmt("NaN", .{ .real = std.math.nan(f64) });
}
