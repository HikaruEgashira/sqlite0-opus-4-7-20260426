//! SQLite3 record format decoder (Iter25.B.1, ADR-0005 §2).
//!
//! Pure-logic module: takes `[]const u8` byte slices and returns `Value`
//! arrays. No Pager / file I/O. The decoder is format-spec-driven so any
//! bug here will surface long before Iter25.B.5 puts a real sqlite3
//! fixture through it.
//!
//! Reference: <https://www.sqlite.org/fileformat.html> §2.1 ("Record
//! Format") and §6 ("Variable-Length Integers (Varints)").
//!
//! ## Varint
//!
//! 1–9 bytes, big-endian. The first 8 bytes encode 7 bits of value each
//! with the high bit acting as a continuation flag. The 9th byte (if
//! the prior 8 all had the high bit set) contributes a full 8 bits.
//! Maximum value is 64 bits unsigned. Decoded as `u64`.
//!
//! ## SerialType → Value
//!
//!   0           → NULL
//!   1           → 1-byte signed integer (sign-extended to i64)
//!   2           → 2-byte BE signed integer
//!   3           → 3-byte BE signed integer (sign-extended)
//!   4           → 4-byte BE signed integer
//!   5           → 6-byte BE signed integer
//!   6           → 8-byte BE signed integer
//!   7           → 8-byte BE IEEE-754 double
//!   8           → integer 0 (no body bytes)
//!   9           → integer 1 (no body bytes)
//!   10, 11      → reserved (decoder error)
//!   ≥12 and even → BLOB of (n-12)/2 bytes
//!   ≥13 and odd  → TEXT of (n-13)/2 bytes (UTF-8)
//!
//! ## Record header
//!
//! Header starts with a varint giving the total header length **including
//! the varint itself**. Then the header continues with one varint per
//! column giving the serial type. After the header, bodies appear in
//! column order — sizes are entirely determined by the serial type.

const std = @import("std");
const ops = @import("ops.zig");
const value_mod = @import("value.zig");

const Value = value_mod.Value;
pub const Error = ops.Error;

/// Decoded varint result: the u64 value plus the number of bytes consumed.
pub const Varint = struct { value: u64, bytes_consumed: usize };

/// Decode one big-endian varint at the start of `bytes`. Returns the value
/// and how many bytes were consumed (1–9). `Error.IoError` if the buffer
/// runs out before the varint terminates.
pub fn decodeVarint(bytes: []const u8) Error!Varint {
    if (bytes.len == 0) return Error.IoError;
    var value: u64 = 0;
    // First 8 bytes contribute 7 bits each.
    var i: usize = 0;
    while (i < 8) : (i += 1) {
        if (i >= bytes.len) return Error.IoError;
        const b = bytes[i];
        value = (value << 7) | @as(u64, b & 0x7f);
        if ((b & 0x80) == 0) {
            return .{ .value = value, .bytes_consumed = i + 1 };
        }
    }
    // The 9th byte contributes 8 bits, and there is no continuation
    // flag check — the high bit IS data.
    if (bytes.len < 9) return Error.IoError;
    value = (value << 8) | @as(u64, bytes[8]);
    return .{ .value = value, .bytes_consumed = 9 };
}

/// Encode `value` as a varint into `out`, returning the byte length.
/// `out` must have capacity ≥ 9. Used in tests; production write path
/// (Iter26.A) will reuse this.
pub fn encodeVarint(value: u64, out: []u8) usize {
    if (value <= 0x7f) {
        out[0] = @intCast(value);
        return 1;
    }
    // Determine how many 7-bit groups are needed. Up to 8 groups; if
    // the value needs 64 bits we use the 9-byte form (8 bits + 7×8 bits).
    const bits_needed: usize = 64 - @clz(value);
    var n_bytes: usize = (bits_needed + 6) / 7;
    if (n_bytes > 8) n_bytes = 9;

    if (n_bytes == 9) {
        // 9-byte form: low byte is full 8 bits; previous 8 bytes are
        // 7-bit groups with continuation set on all but ... actually, all
        // 8 of them have continuation set (that's how the decoder knows
        // to read the 9th byte).
        out[8] = @intCast(value & 0xff);
        var v = value >> 8;
        var i: usize = 8;
        while (i > 0) {
            i -= 1;
            out[i] = @as(u8, @intCast(v & 0x7f)) | 0x80;
            v >>= 7;
        }
        return 9;
    }

    var v = value;
    var i: usize = n_bytes;
    while (i > 0) {
        i -= 1;
        const continuation: u8 = if (i + 1 == n_bytes) 0 else 0x80;
        out[i] = @as(u8, @intCast(v & 0x7f)) | continuation;
        v >>= 7;
    }
    return n_bytes;
}

/// How many body bytes a serial type consumes after the header. Returns
/// `Error.IoError` for the reserved types 10 / 11 (sqlite3 raises a
/// "malformed database" error in that case).
pub fn serialTypeBodyLen(t: u64) Error!usize {
    return switch (t) {
        0, 8, 9 => 0,
        1 => 1,
        2 => 2,
        3 => 3,
        4 => 4,
        5 => 6,
        6, 7 => 8,
        10, 11 => Error.IoError,
        else => if (t & 1 == 0) (t - 12) / 2 else (t - 13) / 2,
    };
}

/// Decode the body bytes of one column according to its serial type into a
/// `Value`. TEXT/BLOB Values borrow from `body` — caller must dupe before
/// the source bytes are invalidated (page eviction). Integer/REAL/NULL are
/// inline and need no further lifetime management.
pub fn decodeColumn(serial_type: u64, body: []const u8) Error!Value {
    return switch (serial_type) {
        0 => Value.null,
        8 => Value{ .integer = 0 },
        9 => Value{ .integer = 1 },
        1 => blk: {
            if (body.len < 1) return Error.IoError;
            // sign-extend i8 → i64
            break :blk Value{ .integer = @as(i64, @as(i8, @bitCast(body[0]))) };
        },
        2 => blk: {
            if (body.len < 2) return Error.IoError;
            const u: u16 = (@as(u16, body[0]) << 8) | body[1];
            break :blk Value{ .integer = @as(i64, @as(i16, @bitCast(u))) };
        },
        3 => blk: {
            if (body.len < 3) return Error.IoError;
            // 24-bit BE → sign-extend
            const u: u32 = (@as(u32, body[0]) << 16) | (@as(u32, body[1]) << 8) | body[2];
            const sign_extended: i32 = if (u & 0x80_0000 != 0)
                @bitCast(u | 0xff00_0000)
            else
                @intCast(u);
            break :blk Value{ .integer = @as(i64, sign_extended) };
        },
        4 => blk: {
            if (body.len < 4) return Error.IoError;
            const u: u32 = (@as(u32, body[0]) << 24) |
                (@as(u32, body[1]) << 16) |
                (@as(u32, body[2]) << 8) |
                body[3];
            break :blk Value{ .integer = @as(i64, @as(i32, @bitCast(u))) };
        },
        5 => blk: {
            if (body.len < 6) return Error.IoError;
            // 48-bit BE → sign-extend to i64
            var u: u64 = 0;
            for (body[0..6]) |b| u = (u << 8) | b;
            const sign_extended: i64 = if (u & 0x8000_0000_0000 != 0)
                @bitCast(u | 0xffff_0000_0000_0000)
            else
                @intCast(u);
            break :blk Value{ .integer = sign_extended };
        },
        6 => blk: {
            if (body.len < 8) return Error.IoError;
            var u: u64 = 0;
            for (body[0..8]) |b| u = (u << 8) | b;
            break :blk Value{ .integer = @as(i64, @bitCast(u)) };
        },
        7 => blk: {
            if (body.len < 8) return Error.IoError;
            var u: u64 = 0;
            for (body[0..8]) |b| u = (u << 8) | b;
            break :blk Value{ .real = @as(f64, @bitCast(u)) };
        },
        10, 11 => Error.IoError,
        else => blk: {
            const want = try serialTypeBodyLen(serial_type);
            if (body.len < want) return Error.IoError;
            break :blk if (serial_type & 1 == 0)
                Value{ .blob = body[0..want] }
            else
                Value{ .text = body[0..want] };
        },
    };
}

/// Decode a complete record (header + body) into a freshly-allocated
/// `[]Value` slice in `alloc`. TEXT/BLOB Values inside the result borrow
/// from `record_bytes` — caller must dupe before the source is freed.
///
/// Returns the columns in order. The record's column count is implicit in
/// the header length (header is read until exhausted).
pub fn decodeRecord(alloc: std.mem.Allocator, record_bytes: []const u8) Error![]Value {
    const header_var = try decodeVarint(record_bytes);
    const header_len = header_var.value;
    if (header_len == 0 or header_len > record_bytes.len) return Error.IoError;

    var pos: usize = header_var.bytes_consumed;
    var serial_types: std.ArrayList(u64) = .empty;
    defer serial_types.deinit(alloc);

    while (pos < header_len) {
        const sv = try decodeVarint(record_bytes[pos..]);
        try serial_types.append(alloc, sv.value);
        pos += sv.bytes_consumed;
    }
    if (pos != header_len) return Error.IoError;

    var body_pos: usize = @intCast(header_len);
    const result = try alloc.alloc(Value, serial_types.items.len);
    errdefer alloc.free(result);
    for (serial_types.items, 0..) |st, i| {
        const body_len = try serialTypeBodyLen(st);
        if (body_pos + body_len > record_bytes.len) return Error.IoError;
        result[i] = try decodeColumn(st, record_bytes[body_pos..]);
        body_pos += body_len;
    }
    return result;
}

// -- tests --

const testing = std.testing;

test "decodeVarint: single-byte values" {
    const cases = [_]struct { bytes: []const u8, expected: u64, len: usize }{
        .{ .bytes = &.{0x00}, .expected = 0, .len = 1 },
        .{ .bytes = &.{0x01}, .expected = 1, .len = 1 },
        .{ .bytes = &.{0x7f}, .expected = 127, .len = 1 },
    };
    for (cases) |c| {
        const v = try decodeVarint(c.bytes);
        try testing.expectEqual(c.expected, v.value);
        try testing.expectEqual(c.len, v.bytes_consumed);
    }
}

test "decodeVarint: multi-byte values" {
    // 0x81 0x00 → continuation set, then 0x00 → (1 << 7) | 0 = 128
    const a = try decodeVarint(&.{ 0x81, 0x00 });
    try testing.expectEqual(@as(u64, 128), a.value);
    try testing.expectEqual(@as(usize, 2), a.bytes_consumed);

    // 0xff 0x7f → ((127) << 7) | 127 = 16383
    const b = try decodeVarint(&.{ 0xff, 0x7f });
    try testing.expectEqual(@as(u64, 16383), b.value);
    try testing.expectEqual(@as(usize, 2), b.bytes_consumed);
}

test "decodeVarint: 9-byte form" {
    // Eight 0xff bytes followed by 0xff — full 64 bits set.
    const all_ones: [9]u8 = .{ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff };
    const v = try decodeVarint(&all_ones);
    try testing.expectEqual(@as(u64, 0xffff_ffff_ffff_ffff), v.value);
    try testing.expectEqual(@as(usize, 9), v.bytes_consumed);
}

test "decodeVarint: empty input → IoError" {
    try testing.expectError(Error.IoError, decodeVarint(&.{}));
}

test "decodeVarint: continuation without termination → IoError" {
    // 0x80 with no follow-up byte.
    try testing.expectError(Error.IoError, decodeVarint(&.{0x80}));
}

test "encodeVarint: round-trip single-byte" {
    var buf: [9]u8 = undefined;
    inline for (.{ 0, 1, 64, 127 }) |v| {
        const n = encodeVarint(v, &buf);
        try testing.expectEqual(@as(usize, 1), n);
        const decoded = try decodeVarint(buf[0..n]);
        try testing.expectEqual(@as(u64, v), decoded.value);
    }
}

test "encodeVarint: round-trip multi-byte" {
    var buf: [9]u8 = undefined;
    const values = [_]u64{ 128, 16383, 65535, 1_000_000, 0x7fff_ffff_ffff_ffff, 0xffff_ffff_ffff_ffff };
    for (values) |v| {
        const n = encodeVarint(v, &buf);
        const decoded = try decodeVarint(buf[0..n]);
        try testing.expectEqual(v, decoded.value);
        try testing.expectEqual(n, decoded.bytes_consumed);
    }
}

test "serialTypeBodyLen: fixed-size types" {
    try testing.expectEqual(@as(usize, 0), try serialTypeBodyLen(0));
    try testing.expectEqual(@as(usize, 1), try serialTypeBodyLen(1));
    try testing.expectEqual(@as(usize, 2), try serialTypeBodyLen(2));
    try testing.expectEqual(@as(usize, 3), try serialTypeBodyLen(3));
    try testing.expectEqual(@as(usize, 4), try serialTypeBodyLen(4));
    try testing.expectEqual(@as(usize, 6), try serialTypeBodyLen(5));
    try testing.expectEqual(@as(usize, 8), try serialTypeBodyLen(6));
    try testing.expectEqual(@as(usize, 8), try serialTypeBodyLen(7));
    try testing.expectEqual(@as(usize, 0), try serialTypeBodyLen(8));
    try testing.expectEqual(@as(usize, 0), try serialTypeBodyLen(9));
}

test "serialTypeBodyLen: variable-length BLOB/TEXT" {
    // type 12 → BLOB of 0 bytes
    try testing.expectEqual(@as(usize, 0), try serialTypeBodyLen(12));
    // type 13 → TEXT of 0 bytes
    try testing.expectEqual(@as(usize, 0), try serialTypeBodyLen(13));
    // type 14 → BLOB of 1 byte
    try testing.expectEqual(@as(usize, 1), try serialTypeBodyLen(14));
    // type 25 → TEXT of 6 bytes ((25-13)/2 = 6)
    try testing.expectEqual(@as(usize, 6), try serialTypeBodyLen(25));
}

test "serialTypeBodyLen: reserved types 10, 11 → IoError" {
    try testing.expectError(Error.IoError, serialTypeBodyLen(10));
    try testing.expectError(Error.IoError, serialTypeBodyLen(11));
}

test "decodeColumn: NULL / 0 / 1 inline" {
    try testing.expect((try decodeColumn(0, &.{})) == .null);
    try testing.expectEqual(@as(i64, 0), (try decodeColumn(8, &.{})).integer);
    try testing.expectEqual(@as(i64, 1), (try decodeColumn(9, &.{})).integer);
}

test "decodeColumn: 1-byte signed integer" {
    try testing.expectEqual(@as(i64, 42), (try decodeColumn(1, &.{42})).integer);
    try testing.expectEqual(@as(i64, -1), (try decodeColumn(1, &.{0xff})).integer);
    try testing.expectEqual(@as(i64, -128), (try decodeColumn(1, &.{0x80})).integer);
}

test "decodeColumn: 2-byte signed integer (BE)" {
    try testing.expectEqual(@as(i64, 1), (try decodeColumn(2, &.{ 0x00, 0x01 })).integer);
    try testing.expectEqual(@as(i64, -1), (try decodeColumn(2, &.{ 0xff, 0xff })).integer);
    try testing.expectEqual(@as(i64, 256), (try decodeColumn(2, &.{ 0x01, 0x00 })).integer);
}

test "decodeColumn: 3-byte signed integer (BE)" {
    try testing.expectEqual(@as(i64, 0x010203), (try decodeColumn(3, &.{ 0x01, 0x02, 0x03 })).integer);
    try testing.expectEqual(@as(i64, -1), (try decodeColumn(3, &.{ 0xff, 0xff, 0xff })).integer);
}

test "decodeColumn: 4-byte signed integer (BE)" {
    try testing.expectEqual(@as(i64, 0x01020304), (try decodeColumn(4, &.{ 0x01, 0x02, 0x03, 0x04 })).integer);
    try testing.expectEqual(@as(i64, -1), (try decodeColumn(4, &.{ 0xff, 0xff, 0xff, 0xff })).integer);
}

test "decodeColumn: 6-byte signed integer (BE)" {
    try testing.expectEqual(@as(i64, 0x010203040506), (try decodeColumn(5, &.{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06 })).integer);
    try testing.expectEqual(@as(i64, -1), (try decodeColumn(5, &.{ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff })).integer);
}

test "decodeColumn: 8-byte signed integer (BE)" {
    const all_ff: [8]u8 = .{ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff };
    try testing.expectEqual(@as(i64, -1), (try decodeColumn(6, &all_ff)).integer);
    const big_pos: [8]u8 = .{ 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff };
    try testing.expectEqual(std.math.maxInt(i64), (try decodeColumn(6, &big_pos)).integer);
}

test "decodeColumn: REAL (IEEE 754 BE)" {
    // 1.0 as BE bytes: 0x3FF0000000000000
    const one: [8]u8 = .{ 0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
    try testing.expectEqual(@as(f64, 1.0), (try decodeColumn(7, &one)).real);
}

test "decodeColumn: TEXT borrows source bytes" {
    const src: []const u8 = "hello";
    const v = try decodeColumn(13 + 5 * 2, src); // type for 5-byte TEXT = 23
    try testing.expectEqualStrings("hello", v.text);
    // Pointer identity: text should alias src.
    try testing.expect(v.text.ptr == src.ptr);
}

test "decodeColumn: BLOB borrows source bytes" {
    const src: []const u8 = &.{ 0xde, 0xad, 0xbe, 0xef };
    const v = try decodeColumn(12 + 4 * 2, src); // type for 4-byte BLOB = 20
    try testing.expectEqual(@as(usize, 4), v.blob.len);
    try testing.expectEqual(@as(u8, 0xde), v.blob[0]);
}

test "decodeRecord: single column INTEGER 42" {
    // Header: varint(2) "header_len = 2", varint(1) "i8".
    // Body: 0x2a (= 42).
    const bytes = [_]u8{ 0x02, 0x01, 0x2a };
    const result = try decodeRecord(testing.allocator, &bytes);
    defer testing.allocator.free(result);
    try testing.expectEqual(@as(usize, 1), result.len);
    try testing.expectEqual(@as(i64, 42), result[0].integer);
}

test "decodeRecord: three columns INTEGER, NULL, TEXT" {
    // header_len = 4 (1 byte for header_len + 3 byte for types)
    // types: 1 (i8), 0 (NULL), 13+2*2=17 (TEXT len 2)
    // bodies: 0x07, [no body for null], 'a', 'b'
    const bytes = [_]u8{ 0x04, 0x01, 0x00, 0x11, 0x07, 'a', 'b' };
    const result = try decodeRecord(testing.allocator, &bytes);
    defer testing.allocator.free(result);
    try testing.expectEqual(@as(usize, 3), result.len);
    try testing.expectEqual(@as(i64, 7), result[0].integer);
    try testing.expect(result[1] == .null);
    try testing.expectEqualStrings("ab", result[2].text);
}

test "decodeRecord: empty body (all NULLs)" {
    // header_len = 3 (varint(3) + 2 type varints), all NULL.
    const bytes = [_]u8{ 0x03, 0x00, 0x00 };
    const result = try decodeRecord(testing.allocator, &bytes);
    defer testing.allocator.free(result);
    try testing.expectEqual(@as(usize, 2), result.len);
    try testing.expect(result[0] == .null);
    try testing.expect(result[1] == .null);
}

test "decodeRecord: malformed header_len → IoError" {
    // header_len claims 99 but record is short.
    const bytes = [_]u8{ 99, 0x01, 0x2a };
    try testing.expectError(Error.IoError, decodeRecord(testing.allocator, &bytes));
}
