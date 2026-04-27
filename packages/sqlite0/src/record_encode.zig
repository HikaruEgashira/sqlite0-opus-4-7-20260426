//! Record encoder — `[]Value` → SQLite3 record bytes (Iter26.A.1,
//! ADR-0005 §2). The decoder side lives in `record.zig`; encode/decode
//! must round-trip for every Value type our schema supports.
//!
//! ## Serial-type choice for INTEGER
//!
//! Pick the smallest serial type that can hold the value. SQLite3 uses
//! the dedicated literal types 8 and 9 for the integer values 0 and 1
//! (zero-byte body) — we mirror that to keep encoded sizes byte-equal
//! to what sqlite3 itself would write, which makes
//! `PRAGMA integrity_check` happy and reduces test-fixture diff noise.
//!
//! ## NULL / REAL / TEXT / BLOB
//!
//! NULL → type 0 (no body). REAL → type 7 (8-byte IEEE 754 BE). TEXT
//! length L → 13 + 2L (must be odd ≥ 13). BLOB length L → 12 + 2L
//! (must be even ≥ 12). All match `record.serialTypeBodyLen` exactly —
//! decode/encode round-trip is a tested invariant.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const record = @import("record.zig");

const Value = value_mod.Value;
pub const Error = ops.Error;

/// Serial-type tag for one Value. Mirrors SQLite3's encoding choices.
pub fn serialTypeForValue(v: Value) u64 {
    return switch (v) {
        .null => 0,
        .integer => |i| integerSerialType(i),
        .real => 7,
        .text => |t| 13 + 2 * @as(u64, t.len),
        .blob => |b| 12 + 2 * @as(u64, b.len),
    };
}

/// Pick the smallest serial type for an i64 value. Types 8/9 carry no
/// body and are reserved for the literals 0 and 1 — they give a 1-byte
/// savings per occurrence in records that contain those values.
pub fn integerSerialType(i: i64) u64 {
    if (i == 0) return 8;
    if (i == 1) return 9;
    if (i >= -128 and i <= 127) return 1;
    if (i >= -32768 and i <= 32767) return 2;
    if (i >= -8388608 and i <= 8388607) return 3;
    if (i >= -2147483648 and i <= 2147483647) return 4;
    // 48-bit signed range: −2^47 .. 2^47 − 1
    if (i >= -140737488355328 and i <= 140737488355327) return 5;
    return 6;
}

/// Write the body bytes for one column into `out` and return the number
/// of bytes written. `out` must have at least
/// `record.serialTypeBodyLen(serial_type)` bytes available.
pub fn encodeColumnBody(v: Value, out: []u8) Error!usize {
    return switch (v) {
        .null => 0,
        .integer => |i| try encodeIntegerBody(i, out),
        .real => |f| blk: {
            if (out.len < 8) return Error.IoError;
            const bits: u64 = @bitCast(f);
            // 8-byte big-endian.
            var k: usize = 0;
            while (k < 8) : (k += 1) {
                out[k] = @intCast((bits >> @intCast(8 * (7 - k))) & 0xff);
            }
            break :blk 8;
        },
        .text => |t| blk: {
            if (out.len < t.len) return Error.IoError;
            @memcpy(out[0..t.len], t);
            break :blk t.len;
        },
        .blob => |b| blk: {
            if (out.len < b.len) return Error.IoError;
            @memcpy(out[0..b.len], b);
            break :blk b.len;
        },
    };
}

fn encodeIntegerBody(i: i64, out: []u8) Error!usize {
    const st = integerSerialType(i);
    return switch (st) {
        8, 9 => 0, // literals — no body
        1 => blk: {
            if (out.len < 1) return Error.IoError;
            out[0] = @bitCast(@as(i8, @intCast(i)));
            break :blk 1;
        },
        2 => blk: {
            if (out.len < 2) return Error.IoError;
            const u: u16 = @bitCast(@as(i16, @intCast(i)));
            out[0] = @intCast((u >> 8) & 0xff);
            out[1] = @intCast(u & 0xff);
            break :blk 2;
        },
        3 => blk: {
            if (out.len < 3) return Error.IoError;
            // Truncate i64 to low 24 bits; preserves sign in 24-bit BE
            // so that decode's sign-extension reproduces the original.
            const u: u32 = @bitCast(@as(i32, @intCast(i)));
            out[0] = @intCast((u >> 16) & 0xff);
            out[1] = @intCast((u >> 8) & 0xff);
            out[2] = @intCast(u & 0xff);
            break :blk 3;
        },
        4 => blk: {
            if (out.len < 4) return Error.IoError;
            const u: u32 = @bitCast(@as(i32, @intCast(i)));
            out[0] = @intCast((u >> 24) & 0xff);
            out[1] = @intCast((u >> 16) & 0xff);
            out[2] = @intCast((u >> 8) & 0xff);
            out[3] = @intCast(u & 0xff);
            break :blk 4;
        },
        5 => blk: {
            if (out.len < 6) return Error.IoError;
            const u: u64 = @bitCast(i);
            // Take the low 6 bytes (BE).
            var k: usize = 0;
            while (k < 6) : (k += 1) {
                out[k] = @intCast((u >> @intCast(8 * (5 - k))) & 0xff);
            }
            break :blk 6;
        },
        6 => blk: {
            if (out.len < 8) return Error.IoError;
            const u: u64 = @bitCast(i);
            var k: usize = 0;
            while (k < 8) : (k += 1) {
                out[k] = @intCast((u >> @intCast(8 * (7 - k))) & 0xff);
            }
            break :blk 8;
        },
        else => Error.IoError,
    };
}

/// Encode a record (header + body) from `cols` into a freshly-allocated
/// `[]u8` owned by `alloc`. The result round-trips through
/// `record.decodeRecord` byte-for-byte.
pub fn encodeRecord(alloc: std.mem.Allocator, cols: []const Value) Error![]u8 {
    // Pass 1: compute serial types and bodies' total length.
    const types = try alloc.alloc(u64, cols.len);
    defer alloc.free(types);
    var body_len: usize = 0;
    for (cols, 0..) |v, i| {
        types[i] = serialTypeForValue(v);
        body_len += try record.serialTypeBodyLen(types[i]);
    }

    // Pass 2: compute header length (header_len varint + serial-type
    // varints). Header_len varint width depends on header_len itself,
    // so iterate once to fixed point. In practice header_len fits in
    // 1 byte for tables up to ~63 columns, so the loop runs once.
    var hl_n: usize = 1;
    var header_len: usize = hl_n + serialTypeVarintsLen(types);
    while (true) {
        const new_hl_n = varintLen(header_len);
        if (new_hl_n == hl_n) break;
        hl_n = new_hl_n;
        header_len = hl_n + serialTypeVarintsLen(types);
    }

    const total = header_len + body_len;
    const buf = try alloc.alloc(u8, total);
    errdefer alloc.free(buf);

    // Write header.
    var pos: usize = 0;
    pos += record.encodeVarint(header_len, buf[pos..]);
    for (types) |t| pos += record.encodeVarint(t, buf[pos..]);
    std.debug.assert(pos == header_len);

    // Write bodies.
    for (cols) |v| {
        const n = try encodeColumnBody(v, buf[pos..]);
        pos += n;
    }
    std.debug.assert(pos == total);
    return buf;
}

fn serialTypeVarintsLen(types: []const u64) usize {
    var sum: usize = 0;
    for (types) |t| sum += varintLen(t);
    return sum;
}

/// Number of bytes a varint encoding consumes — 1..9.
pub fn varintLen(v: u64) usize {
    if (v < (1 << 7)) return 1;
    if (v < (1 << 14)) return 2;
    if (v < (1 << 21)) return 3;
    if (v < (1 << 28)) return 4;
    if (v < (1 << 35)) return 5;
    if (v < (1 << 42)) return 6;
    if (v < (1 << 49)) return 7;
    if (v < (1 << 56)) return 8;
    return 9;
}

// -- tests --

const testing = std.testing;

fn roundTrip(cols: []const Value) !void {
    const enc = try encodeRecord(testing.allocator, cols);
    defer testing.allocator.free(enc);
    const dec = try record.decodeRecord(testing.allocator, enc);
    defer testing.allocator.free(dec);
    try testing.expectEqual(cols.len, dec.len);
    for (cols, dec) |a, b| {
        try testing.expectEqual(@as(std.meta.Tag(Value), a), @as(std.meta.Tag(Value), b));
        switch (a) {
            .null => {},
            .integer => |x| try testing.expectEqual(x, b.integer),
            .real => |x| try testing.expectEqual(x, b.real),
            .text => |x| try testing.expectEqualStrings(x, b.text),
            .blob => |x| try testing.expectEqualSlices(u8, x, b.blob),
        }
    }
}

test "encodeRecord: round-trips small ints (types 1-6 + literals 8/9)" {
    try roundTrip(&[_]Value{.{ .integer = 0 }}); // type 8
    try roundTrip(&[_]Value{.{ .integer = 1 }}); // type 9
    try roundTrip(&[_]Value{.{ .integer = -1 }});
    try roundTrip(&[_]Value{.{ .integer = 127 }});
    try roundTrip(&[_]Value{.{ .integer = -128 }});
    try roundTrip(&[_]Value{.{ .integer = 32767 }});
    try roundTrip(&[_]Value{.{ .integer = -32768 }});
    try roundTrip(&[_]Value{.{ .integer = 8388607 }});
    try roundTrip(&[_]Value{.{ .integer = -8388608 }});
    try roundTrip(&[_]Value{.{ .integer = 2147483647 }});
    try roundTrip(&[_]Value{.{ .integer = -2147483648 }});
    try roundTrip(&[_]Value{.{ .integer = 140737488355327 }});
    try roundTrip(&[_]Value{.{ .integer = -140737488355328 }});
    try roundTrip(&[_]Value{.{ .integer = std.math.maxInt(i64) }});
    try roundTrip(&[_]Value{.{ .integer = std.math.minInt(i64) }});
}

test "encodeRecord: round-trips REAL" {
    try roundTrip(&[_]Value{.{ .real = 1.5 }});
    try roundTrip(&[_]Value{.{ .real = -3.14159 }});
    try roundTrip(&[_]Value{.{ .real = 0.0 }});
}

test "encodeRecord: round-trips TEXT and BLOB" {
    try roundTrip(&[_]Value{.{ .text = "" }});
    try roundTrip(&[_]Value{.{ .text = "hello" }});
    try roundTrip(&[_]Value{.{ .blob = &[_]u8{ 0xde, 0xad, 0xbe, 0xef } }});
    try roundTrip(&[_]Value{.{ .blob = &[_]u8{} }});
}

test "encodeRecord: round-trips NULL" {
    try roundTrip(&[_]Value{Value.null});
    try roundTrip(&[_]Value{ Value.null, .{ .integer = 1 }, Value.null });
}

test "encodeRecord: multi-column mixed types matches decode" {
    try roundTrip(&[_]Value{
        .{ .integer = 42 },
        .{ .text = "alice" },
        Value.null,
        .{ .real = 2.71828 },
        .{ .blob = &[_]u8{ 1, 2, 3 } },
    });
}

test "integerSerialType: literal 0/1 vs 1-byte type 1" {
    try testing.expectEqual(@as(u64, 8), integerSerialType(0));
    try testing.expectEqual(@as(u64, 9), integerSerialType(1));
    try testing.expectEqual(@as(u64, 1), integerSerialType(2));
    try testing.expectEqual(@as(u64, 1), integerSerialType(-1));
    try testing.expectEqual(@as(u64, 2), integerSerialType(128));
    try testing.expectEqual(@as(u64, 3), integerSerialType(32768));
    try testing.expectEqual(@as(u64, 6), integerSerialType(std.math.maxInt(i64)));
}

test "varintLen: matches actual encode width" {
    var buf: [9]u8 = undefined;
    const cases = [_]u64{ 0, 1, 127, 128, 16383, 16384, 1 << 35, std.math.maxInt(u64) };
    for (cases) |v| {
        const enc = record.encodeVarint(v, &buf);
        try testing.expectEqual(enc, varintLen(v));
    }
}
