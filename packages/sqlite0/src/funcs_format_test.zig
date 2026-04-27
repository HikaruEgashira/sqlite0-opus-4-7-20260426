//! `printf` / `format` tests, split out of `funcs_format.zig` to keep
//! that file under the 500-line discipline (CLAUDE.md "Module Splitting
//! Rules"). Production code lives exclusively in `funcs_format.zig`;
//! this file is test-only.

const std = @import("std");
const fmt_mod = @import("funcs_format.zig");
const util = @import("func_util.zig");

const Value = util.Value;
const fnPrintf = fmt_mod.fnPrintf;

test "fnPrintf: %q doubles single quotes" {
    const a = std.testing.allocator;
    var args = [_]Value{ .{ .text = "%q" }, .{ .text = "it's" } };
    const r = try fnPrintf(a, &args);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("it''s", r.text);
}

test "fnPrintf: %Q wraps and quotes; NULL → bare NULL" {
    const a = std.testing.allocator;
    var args1 = [_]Value{ .{ .text = "%Q" }, .{ .text = "x" } };
    const r1 = try fnPrintf(a, &args1);
    defer a.free(r1.text);
    try std.testing.expectEqualStrings("'x'", r1.text);

    var args2 = [_]Value{ .{ .text = "%Q" }, .null };
    const r2 = try fnPrintf(a, &args2);
    defer a.free(r2.text);
    try std.testing.expectEqualStrings("NULL", r2.text);
}

test "fnPrintf: %w doubles double quotes" {
    const a = std.testing.allocator;
    var args = [_]Value{ .{ .text = "%w" }, .{ .text = "a\"b" } };
    const r = try fnPrintf(a, &args);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("a\"\"b", r.text);
}

test "fnPrintf: %.3q truncates BEFORE doubling" {
    // sqlite3: precision counts input chars before quote-doubling expands.
    const a = std.testing.allocator;
    var args = [_]Value{ .{ .text = "%.3q" }, .{ .text = "ab'cdef" } };
    const r = try fnPrintf(a, &args);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("ab''", r.text);
}

test "fnPrintf: %q truncates at embedded NUL byte (C-string convention)" {
    const a = std.testing.allocator;
    var args = [_]Value{ .{ .text = "%q" }, .{ .text = "ab\x00cd" } };
    const r = try fnPrintf(a, &args);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("ab", r.text);
}

test "fnPrintf: %q NULL → (NULL); %Q NULL → NULL; precision applies" {
    const a = std.testing.allocator;
    var p1 = [_]Value{ .{ .text = "%q" }, .null };
    const r1 = try fnPrintf(a, &p1);
    defer a.free(r1.text);
    try std.testing.expectEqualStrings("(NULL)", r1.text);

    var p2 = [_]Value{ .{ .text = "%.4q" }, .null };
    const r2 = try fnPrintf(a, &p2);
    defer a.free(r2.text);
    try std.testing.expectEqualStrings("(NUL", r2.text);
}

test "fnPrintf: unknown spec at start → NULL (empty accumulator)" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%j" }, .{ .integer = 1 } };
    const r = try fnPrintf(a, &p);
    try std.testing.expect(r == .null);
}

test "fnPrintf: unknown spec after literal → TEXT of accumulated literal" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "a%j" }, .{ .integer = 1 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("a", r.text);
}

test "fnPrintf: unknown spec after preceding %d preserves width-padded prefix" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%5d %j" }, .{ .integer = 1 }, .{ .integer = 2 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("    1 ", r.text);
}

test "fnPrintf: unknown letter %a (not implemented) aborts at spec" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "hello %a world" }, .{ .integer = 1 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("hello ", r.text);
}

test "fnPrintf: %e (deferred — Ryu vs dtoa) treated as unknown → NULL" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%e" }, .{ .real = 1.5 } };
    const r = try fnPrintf(a, &p);
    try std.testing.expect(r == .null);
}

test "fnPrintf: %s NUL-truncates TEXT before precision" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%.10s" }, .{ .text = "ab\x00cd" } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("ab", r.text);
}

test "fnPrintf: %s width pads truncated length, not raw" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%5s|" }, .{ .text = "ab\x00cd" } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("   ab|", r.text);
}

test "fnPrintf: %s NUL-truncates BLOB" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%s|" }, .{ .blob = "\x00ab" } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("|", r.text);
}

test "fnPrintf: %c on leading-NUL TEXT writes 1 NUL byte (sqlite3 quirk)" {
    // sqlite3 always emits exactly 1 byte for `%c` — the C-string's first
    // byte (the NUL terminator if input is empty / NUL-leading / NULL).
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%c" }, .{ .text = "\x00xyz" } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("\x00", r.text);
}

test "fnPrintf: %c on NULL writes 1 NUL byte (not empty string)" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%c" }, .null };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("\x00", r.text);
}

test "fnPrintf: empty fmt → NULL (sqlite3 sqlite3_str_finish empty rule)" {
    const a = std.testing.allocator;
    var p = [_]Value{.{ .text = "" }};
    const r = try fnPrintf(a, &p);
    try std.testing.expect(r == .null);
}

test "fnPrintf: %s with NULL → NULL (empty accumulator)" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%s" }, .null };
    const r = try fnPrintf(a, &p);
    try std.testing.expect(r == .null);
}

test "fnPrintf: %#o non-zero prepends '0'" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%#o" }, .{ .integer = 8 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("010", r.text);
}

test "fnPrintf: %#o zero gets NO prefix (sqlite3 quirk)" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%#o" }, .{ .integer = 0 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("0", r.text);
}

test "fnPrintf: %#x non-zero prepends '0x'" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%#x" }, .{ .integer = 255 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("0xff", r.text);
}

test "fnPrintf: %#X uses uppercase prefix and digits" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%#X" }, .{ .integer = 255 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("0XFF", r.text);
}

test "fnPrintf: %#5x width includes prefix" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%#5x" }, .{ .integer = 15 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("  0xf", r.text);
}

test "fnPrintf: %#d / %#u alt-form is no-op for decimal" {
    const a = std.testing.allocator;
    var p1 = [_]Value{ .{ .text = "%#d" }, .{ .integer = 42 } };
    const r1 = try fnPrintf(a, &p1);
    defer a.free(r1.text);
    try std.testing.expectEqualStrings("42", r1.text);

    var p2 = [_]Value{ .{ .text = "%#u" }, .{ .integer = 42 } };
    const r2 = try fnPrintf(a, &p2);
    defer a.free(r2.text);
    try std.testing.expectEqualStrings("42", r2.text);
}
