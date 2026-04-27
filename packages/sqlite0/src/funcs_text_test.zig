//! Tests for `funcs_text.zig`, split out to keep that file under the
//! 500-line discipline (CLAUDE.md "Module Splitting Rules"). Production
//! code lives exclusively in `funcs_text.zig`; this file is test-only.

const std = @import("std");
const text = @import("funcs_text.zig");
const value = @import("value.zig");

const Value = value.Value;

const fnReplace = text.fnReplace;
const fnHex = text.fnHex;
const fnQuote = text.fnQuote;
const fnTrim = text.fnTrim;
const fnInstr = text.fnInstr;
const fnChar = text.fnChar;
const fnLength = text.fnLength;
const fnOctetLength = text.fnOctetLength;
const fnUnhex = text.fnUnhex;
const fnConcat = text.fnConcat;
const fnConcatWs = text.fnConcatWs;

test "fnReplace: simple replacement" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "hello world" }, .{ .text = "world" }, .{ .text = "zig" } };
    const r = try fnReplace(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("hello zig", r.text);
}

test "fnReplace: empty find returns subject unchanged" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "abc" }, .{ .text = "" }, .{ .text = "X" } };
    const r = try fnReplace(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("abc", r.text);
}

test "fnHex: ASCII bytes" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "abc" }};
    const r = try fnHex(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("616263", r.text);
}

test "fnQuote: text wraps and doubles apostrophes" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "it's" }};
    const r = try fnQuote(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("'it''s'", r.text);
}

test "fnQuote: NULL is the literal text 'NULL'" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.null};
    const r = try fnQuote(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("NULL", r.text);
}

test "fnTrim: defaults to whitespace, both sides" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "  hi  " }};
    const r = try fnTrim(allocator, &args, .both);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("hi", r.text);
}

test "fnInstr: 1-based offset, 0 when missing" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "hello world" }, .{ .text = "world" } };
    const r1 = try fnInstr(allocator, &args);
    try std.testing.expectEqual(@as(i64, 7), r1.integer);

    var miss = [_]Value{ .{ .text = "hello" }, .{ .text = "world" } };
    const r2 = try fnInstr(allocator, &miss);
    try std.testing.expectEqual(@as(i64, 0), r2.integer);
}

test "fnChar: ASCII code points" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .integer = 72 }, .{ .integer = 105 } };
    const r = try fnChar(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("Hi", r.text);
}

test "fnChar: out-of-range codepoint → U+FFFD" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .integer = 0x110000 }};
    const r = try fnChar(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualSlices(u8, "\xEF\xBF\xBD", r.text);
}

test "fnChar: negative codepoint → U+FFFD" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .integer = -1 }};
    const r = try fnChar(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualSlices(u8, "\xEF\xBF\xBD", r.text);
}

test "fnChar: surrogate codepoint emits WTF-8 (3-byte ED Ax/Bx xx)" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .integer = 0xD800 }};
    const r = try fnChar(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualSlices(u8, "\xED\xA0\x80", r.text);
}

test "fnChar: mixed valid + out-of-range keeps order" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .integer = 65 }, .{ .integer = 0x110000 }, .{ .integer = 66 } };
    const r = try fnChar(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualSlices(u8, "A\xEF\xBF\xBDB", r.text);
}

test "fnLength: UTF-8 char count not byte count" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "aあ" }};
    const r = try fnLength(allocator, &args);
    try std.testing.expectEqual(@as(i64, 2), r.integer);
}

test "fnOctetLength: TEXT byte count" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "aあ" }};
    const r = try fnOctetLength(allocator, &args);
    try std.testing.expectEqual(@as(i64, 4), r.integer);
}

test "fnLength: TEXT NUL-truncates (C-string convention)" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "A\x00B" }};
    const r = try fnLength(allocator, &args);
    try std.testing.expectEqual(@as(i64, 1), r.integer);
}

test "fnLength: leading NUL TEXT → 0" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "\x00xyz" }};
    const r = try fnLength(allocator, &args);
    try std.testing.expectEqual(@as(i64, 0), r.integer);
}

test "fnOctetLength: TEXT with embedded NUL counts raw bytes" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "A\x00B" }};
    const r = try fnOctetLength(allocator, &args);
    try std.testing.expectEqual(@as(i64, 3), r.integer);
}

test "fnUnhex: ignore-set strips spaces, decodes to BLOB" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "41 42" }, .{ .text = " " } };
    const r = try fnUnhex(allocator, &args);
    defer allocator.free(r.blob);
    try std.testing.expectEqualSlices(u8, &.{ 0x41, 0x42 }, r.blob);
}

test "fnUnhex: odd hex length yields NULL" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "414" }};
    const r = try fnUnhex(allocator, &args);
    try std.testing.expectEqual(Value.null, r);
}

test "fnConcat: NULL skipped, all-NULL returns empty TEXT not NULL" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .null, .null };
    const r = try fnConcat(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("", r.text);
}

test "fnConcatWs: NULL sep collapses, NULL value skipped" {
    const allocator = std.testing.allocator;
    var null_sep = [_]Value{ .null, .{ .text = "a" }, .{ .text = "b" } };
    const r1 = try fnConcatWs(allocator, &null_sep);
    try std.testing.expectEqual(Value.null, r1);

    var null_value = [_]Value{ .{ .text = "-" }, .{ .text = "a" }, .null, .{ .text = "b" } };
    const r2 = try fnConcatWs(allocator, &null_value);
    defer allocator.free(r2.text);
    try std.testing.expectEqualStrings("a-b", r2.text);
}
