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

test "fnPrintf: %e renders scientific notation with C-printf exponent form" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%e" }, .{ .real = 1.5 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("1.500000e+00", r.text);
}

test "fnPrintf: %g strips trailing zeros and picks %f form for mid-magnitudes" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%g" }, .{ .real = 100.5 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("100.5", r.text);
}

test "fnPrintf: %g switches to %e form when |exp| crosses precision threshold" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%g" }, .{ .real = 1000000.0 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("1e+06", r.text);
}

test "fnPrintf: %G uppercases the E in exponent" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%G" }, .{ .real = 1.5e100 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("1.5E+100", r.text);
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

test "fnPrintf: %-05d → 0 wins over -, content fills width" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%-05d" }, .{ .integer = 42 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("00042", r.text);
}

test "fnPrintf: %-05d of -42 keeps sign outside zero-pad" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%-05d" }, .{ .integer = -42 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("-0042", r.text);
}

test "fnPrintf: %05.0d of 0 → 0 flag boosts precision past explicit precision" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%05.0d" }, .{ .integer = 0 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("00000", r.text);
}

test "fnPrintf: %-#05x of 255 → alt prefix added AFTER zero-pad, exceeds width" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%-#05x" }, .{ .integer = 255 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("0x000ff", r.text);
}

test "fnPrintf: %05.10d → explicit precision exceeds width-boosted minimum" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%05.10d" }, .{ .integer = 42 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("0000000042", r.text);
}

test "fnPrintf: %*d consumes width from int arg" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%*d" }, .{ .integer = 5 }, .{ .integer = 42 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("   42", r.text);
}

test "fnPrintf: %*d with negative width arg → left-align with abs" {
    // sqlite3 quirk: `*` arg negative flips left-align even without explicit `-` flag.
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%*d|" }, .{ .integer = -5 }, .{ .integer = 42 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("42   |", r.text);
}

test "fnPrintf: %.*f consumes precision from int arg" {
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%.*f" }, .{ .integer = 3 }, .{ .real = 3.14159 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("3.142", r.text);
}

test "fnPrintf: %.*f with negative precision arg → abs (sqlite3 quirk)" {
    // sqlite3 takes abs(value) instead of dropping precision the way
    // C printf does: `printf('%.*f', -2, 3.14)` → "3.14".
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "%.*f" }, .{ .integer = -2 }, .{ .real = 3.14 } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("3.14", r.text);
}

test "fnPrintf: %*.*f consumes both width and precision args" {
    const a = std.testing.allocator;
    var p = [_]Value{
        .{ .text = "%*.*f" },
        .{ .integer = 10 },
        .{ .integer = 3 },
        .{ .real = 3.14159 },
    };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    try std.testing.expectEqualStrings("     3.142", r.text);
}

test "fnPrintf: %z is alias for %s (string with width/precision)" {
    const a = std.testing.allocator;
    var p1 = [_]Value{ .{ .text = "%5z|" }, .{ .text = "hi" } };
    const r1 = try fnPrintf(a, &p1);
    defer a.free(r1.text);
    try std.testing.expectEqualStrings("   hi|", r1.text);

    var p2 = [_]Value{ .{ .text = "%.3z" }, .{ .text = "hello" } };
    const r2 = try fnPrintf(a, &p2);
    defer a.free(r2.text);
    try std.testing.expectEqualStrings("hel", r2.text);
}

test "fnPrintf: %p uppercase hex digits, lowercase 0x prefix" {
    const a = std.testing.allocator;
    var p1 = [_]Value{ .{ .text = "%p" }, .{ .integer = 255 } };
    const r1 = try fnPrintf(a, &p1);
    defer a.free(r1.text);
    try std.testing.expectEqualStrings("FF", r1.text);

    var p2 = [_]Value{ .{ .text = "%#p" }, .{ .integer = 255 } };
    const r2 = try fnPrintf(a, &p2);
    defer a.free(r2.text);
    try std.testing.expectEqualStrings("0xFF", r2.text);

    // -1 bit-cast to u64 → 0xFFFFFFFFFFFFFFFF
    var p3 = [_]Value{ .{ .text = "%p" }, .{ .integer = -1 } };
    const r3 = try fnPrintf(a, &p3);
    defer a.free(r3.text);
    try std.testing.expectEqualStrings("FFFFFFFFFFFFFFFF", r3.text);
}

test "fnPrintf: %r adds English ordinal suffix with teen exception" {
    const a = std.testing.allocator;
    const cases = .{
        .{ 1, "1st" },     .{ 2, "2nd" },     .{ 3, "3rd" }, .{ 4, "4th" },
        .{ 11, "11th" },   .{ 12, "12th" },   .{ 13, "13th" }, // teens always "th"
        .{ 21, "21st" },   .{ 111, "111th" }, // last-2 = 11 → "th"
        .{ 0, "0th" },     .{ -1, "-1st" },   .{ -11, "-11th" },
    };
    inline for (cases) |c| {
        var p = [_]Value{ .{ .text = "%r" }, .{ .integer = c[0] } };
        const r = try fnPrintf(a, &p);
        defer a.free(r.text);
        try std.testing.expectEqualStrings(c[1], r.text);
    }
}

test "fnPrintf: %.Nr precision targets total digits+suffix length" {
    // sqlite3 quirk: precision is the total `digits+suffix` length budget,
    // not the digit count alone (which is what `%d` precision means).
    const a = std.testing.allocator;
    var p1 = [_]Value{ .{ .text = "[%.5r]" }, .{ .integer = 1 } };
    const r1 = try fnPrintf(a, &p1);
    defer a.free(r1.text);
    try std.testing.expectEqualStrings("[001st]", r1.text);

    var p2 = [_]Value{ .{ .text = "[%.5r]" }, .{ .integer = 22 } };
    const r2 = try fnPrintf(a, &p2);
    defer a.free(r2.text);
    try std.testing.expectEqualStrings("[022nd]", r2.text);
}

test "fnPrintf: %n is no-op (no output, no arg consumed)" {
    // sqlite3 disables C printf's `%n` byte-count-write for safety; the
    // spec is silently skipped without consuming any arg.
    const a = std.testing.allocator;
    var p = [_]Value{ .{ .text = "[%n %s]" }, .{ .integer = 5 }, .{ .text = "hi" } };
    const r = try fnPrintf(a, &p);
    defer a.free(r.text);
    // %n consumes nothing → first arg (5) goes to %s, "hi" is ignored.
    try std.testing.expectEqualStrings("[ 5]", r.text);
}

test "fnPrintf: %5c width-pads single byte; %05c uses spaces (no zero-pad)" {
    const a = std.testing.allocator;
    var p1 = [_]Value{ .{ .text = "%5c" }, .{ .integer = 65 } };
    const r1 = try fnPrintf(a, &p1);
    defer a.free(r1.text);
    // INTEGER 65 → "65" → first byte '6'; width 5 prepends 4 spaces.
    try std.testing.expectEqualStrings("    6", r1.text);

    var p2 = [_]Value{ .{ .text = "%05c" }, .{ .integer = 65 } };
    const r2 = try fnPrintf(a, &p2);
    defer a.free(r2.text);
    try std.testing.expectEqualStrings("    6", r2.text);
}

test "printf alt-form: %#.0f keeps trailing dot" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "%#.0f" }, .{ .real = 1.0 } };
    const r = try fnPrintf(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("1.", r.text);
}

test "printf alt-form: %#.0e keeps trailing dot before exponent" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "%#.0e" }, .{ .real = 1.0 } };
    const r = try fnPrintf(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("1.e+00", r.text);
}

test "printf alt-form: %#.0g forces decimal point on rounded value" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "%#.0g" }, .{ .real = 1.5 } };
    const r = try fnPrintf(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("2.", r.text);
}

test "printf %% width: %5%% pads with spaces" {
    const allocator = std.testing.allocator;
    var args = [_]Value{.{ .text = "%5%" }};
    const r = try fnPrintf(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("    %", r.text);
}

test "printf %c precision = repeat count" {
    const allocator = std.testing.allocator;
    var args = [_]Value{ .{ .text = "%.3c" }, .{ .text = "A" } };
    const r = try fnPrintf(allocator, &args);
    defer allocator.free(r.text);
    try std.testing.expectEqualStrings("AAA", r.text);
}

test "printf length modifier: %lld / %llu / %lf accepted as alias" {
    const a = std.testing.allocator;

    var p1 = [_]Value{ .{ .text = "%lld" }, .{ .integer = 5 } };
    const r1 = try fnPrintf(a, &p1);
    defer a.free(r1.text);
    try std.testing.expectEqualStrings("5", r1.text);

    var p2 = [_]Value{ .{ .text = "%5lld" }, .{ .integer = 1 } };
    const r2 = try fnPrintf(a, &p2);
    defer a.free(r2.text);
    try std.testing.expectEqualStrings("    1", r2.text);

    var p3 = [_]Value{ .{ .text = "%lf" }, .{ .real = 1.5 } };
    const r3 = try fnPrintf(a, &p3);
    defer a.free(r3.text);
    try std.testing.expectEqualStrings("1.500000", r3.text);
}

test "printf sign flag: + and space last-wins" {
    const a = std.testing.allocator;

    // `%+ d` order: + then space → space wins → ' 1'
    var p1 = [_]Value{ .{ .text = "%+ d" }, .{ .integer = 1 } };
    const r1 = try fnPrintf(a, &p1);
    defer a.free(r1.text);
    try std.testing.expectEqualStrings(" 1", r1.text);

    // `% +d` order: space then + → + wins → '+1'
    var p2 = [_]Value{ .{ .text = "% +d" }, .{ .integer = 1 } };
    const r2 = try fnPrintf(a, &p2);
    defer a.free(r2.text);
    try std.testing.expectEqualStrings("+1", r2.text);
}
