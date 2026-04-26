const std = @import("std");
const lex = @import("lex.zig");
const value = @import("value.zig");

const Token = lex.Token;
const TokenKind = lex.TokenKind;
const Value = value.Value;

pub const Error = error{
    SyntaxError,
    DivisionByZero,
    InvalidNumber,
    InvalidString,
    UnsupportedFeature,
    OutOfMemory,
};

pub const Row = struct {
    values: []Value,

    pub fn deinit(self: *Row, allocator: std.mem.Allocator) void {
        for (self.values) |v| switch (v) {
            .text, .blob => |bytes| allocator.free(bytes),
            else => {},
        };
        allocator.free(self.values);
    }
};

pub const Result = struct {
    rows: []Row,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *Result) void {
        for (self.rows) |*row| row.deinit(self.allocator);
        self.allocator.free(self.rows);
    }
};

const Parser = struct {
    src: []const u8,
    lx: lex.Lexer,
    cur: Token,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator, src: []const u8) Parser {
        var lxr = lex.Lexer.init(src);
        const first = lxr.next();
        return .{ .src = src, .lx = lxr, .cur = first, .allocator = allocator };
    }

    fn advance(self: *Parser) void {
        self.cur = self.lx.next();
    }

    fn expect(self: *Parser, kind: TokenKind) !void {
        if (self.cur.kind != kind) return Error.SyntaxError;
        self.advance();
    }

    fn parseSelect(self: *Parser) ![]Value {
        try self.expect(.keyword_select);
        var values: std.ArrayList(Value) = .empty;
        defer values.deinit(self.allocator);
        errdefer for (values.items) |v| freeValue(self.allocator, v);

        try values.append(self.allocator, try self.parseExpr());
        while (self.cur.kind == .comma) {
            self.advance();
            try values.append(self.allocator, try self.parseExpr());
        }
        if (self.cur.kind == .semicolon) self.advance();
        if (self.cur.kind != .eof) return Error.SyntaxError;

        return values.toOwnedSlice(self.allocator);
    }

    fn parseExpr(self: *Parser) Error!Value {
        return self.parseAddSub();
    }

    fn parseAddSub(self: *Parser) Error!Value {
        var left = try self.parseMulDiv();
        while (self.cur.kind == .plus or self.cur.kind == .minus) {
            const op = self.cur.kind;
            self.advance();
            const right = try self.parseMulDiv();
            defer freeValue(self.allocator, right);
            const new_left = try applyArith(self.allocator, op, left, right);
            freeValue(self.allocator, left);
            left = new_left;
        }
        return left;
    }

    fn parseMulDiv(self: *Parser) Error!Value {
        var left = try self.parseUnary();
        while (self.cur.kind == .star or self.cur.kind == .slash or self.cur.kind == .percent) {
            const op = self.cur.kind;
            self.advance();
            const right = try self.parseUnary();
            defer freeValue(self.allocator, right);
            const new_left = try applyArith(self.allocator, op, left, right);
            freeValue(self.allocator, left);
            left = new_left;
        }
        return left;
    }

    fn parseUnary(self: *Parser) Error!Value {
        if (self.cur.kind == .minus) {
            self.advance();
            const inner = try self.parseUnary();
            defer freeValue(self.allocator, inner);
            return negateValue(inner);
        }
        if (self.cur.kind == .plus) {
            self.advance();
            return self.parseUnary();
        }
        return self.parsePrimary();
    }

    fn parsePrimary(self: *Parser) Error!Value {
        const tok = self.cur;
        switch (tok.kind) {
            .integer => {
                self.advance();
                const text = tok.slice(self.src);
                const n = std.fmt.parseInt(i64, text, 10) catch return Error.InvalidNumber;
                return Value{ .integer = n };
            },
            .real => {
                self.advance();
                const text = tok.slice(self.src);
                const f = std.fmt.parseFloat(f64, text) catch return Error.InvalidNumber;
                return Value{ .real = f };
            },
            .string => {
                self.advance();
                const text = tok.slice(self.src);
                if (text.len < 2 or text[0] != '\'' or text[text.len - 1] != '\'') return Error.InvalidString;
                return Value{ .text = try unescapeStringLiteral(self.allocator, text[1 .. text.len - 1]) };
            },
            .keyword_null => {
                self.advance();
                return Value.null;
            },
            .lparen => {
                self.advance();
                const v = try self.parseExpr();
                if (self.cur.kind != .rparen) {
                    freeValue(self.allocator, v);
                    return Error.SyntaxError;
                }
                self.advance();
                return v;
            },
            else => return Error.SyntaxError,
        }
    }
};

fn unescapeStringLiteral(allocator: std.mem.Allocator, raw: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);
    var i: usize = 0;
    while (i < raw.len) {
        if (raw[i] == '\'' and i + 1 < raw.len and raw[i + 1] == '\'') {
            try out.append(allocator, '\'');
            i += 2;
        } else {
            try out.append(allocator, raw[i]);
            i += 1;
        }
    }
    return out.toOwnedSlice(allocator);
}

fn freeValue(allocator: std.mem.Allocator, v: Value) void {
    switch (v) {
        .text, .blob => |bytes| allocator.free(bytes),
        else => {},
    }
}

fn negateValue(v: Value) Error!Value {
    return switch (v) {
        .integer => |i| Value{ .integer = -%i },
        .real => |f| Value{ .real = -f },
        .null => Value.null,
        else => Error.UnsupportedFeature,
    };
}

fn applyArith(allocator: std.mem.Allocator, op: TokenKind, lhs: Value, rhs: Value) Error!Value {
    _ = allocator;
    if (lhs == .null or rhs == .null) return Value.null;
    const both_int = (lhs == .integer and rhs == .integer);
    if (both_int) {
        const a = lhs.integer;
        const b = rhs.integer;
        switch (op) {
            .plus => return Value{ .integer = a +% b },
            .minus => return Value{ .integer = a -% b },
            .star => return Value{ .integer = a *% b },
            .slash => {
                if (b == 0) return Value.null;
                if (a == std.math.minInt(i64) and b == -1) return Value.null;
                return Value{ .integer = @divTrunc(a, b) };
            },
            .percent => {
                if (b == 0) return Value.null;
                return Value{ .integer = @rem(a, b) };
            },
            else => unreachable,
        }
    }
    const a = toReal(lhs) orelse return Error.UnsupportedFeature;
    const b = toReal(rhs) orelse return Error.UnsupportedFeature;
    switch (op) {
        .plus => return Value{ .real = a + b },
        .minus => return Value{ .real = a - b },
        .star => return Value{ .real = a * b },
        .slash => {
            if (b == 0) return Value.null;
            return Value{ .real = a / b };
        },
        .percent => {
            if (b == 0) return Value.null;
            return Value{ .real = @rem(a, b) };
        },
        else => unreachable,
    }
}

fn toReal(v: Value) ?f64 {
    return switch (v) {
        .integer => |i| @as(f64, @floatFromInt(i)),
        .real => |f| f,
        else => null,
    };
}

pub fn execute(allocator: std.mem.Allocator, sql: []const u8) !Result {
    var parser = Parser.init(allocator, sql);
    const cols = try parser.parseSelect();
    errdefer {
        for (cols) |v| freeValue(allocator, v);
        allocator.free(cols);
    }
    var rows = try allocator.alloc(Row, 1);
    rows[0] = .{ .values = cols };
    return .{ .rows = rows, .allocator = allocator };
}

test "execute: SELECT 1" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT 1");
    defer r.deinit();
    try std.testing.expectEqual(@as(usize, 1), r.rows.len);
    try std.testing.expectEqual(@as(usize, 1), r.rows[0].values.len);
    try std.testing.expectEqual(@as(i64, 1), r.rows[0].values[0].integer);
}

test "execute: SELECT 1+2*3" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT 1+2*3");
    defer r.deinit();
    try std.testing.expectEqual(@as(i64, 7), r.rows[0].values[0].integer);
}

test "execute: SELECT (1+2)*3" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT (1+2)*3");
    defer r.deinit();
    try std.testing.expectEqual(@as(i64, 9), r.rows[0].values[0].integer);
}

test "execute: SELECT -5" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT -5");
    defer r.deinit();
    try std.testing.expectEqual(@as(i64, -5), r.rows[0].values[0].integer);
}

test "execute: SELECT 1, 2, 3" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT 1, 2, 3");
    defer r.deinit();
    try std.testing.expectEqual(@as(usize, 3), r.rows[0].values.len);
    try std.testing.expectEqual(@as(i64, 1), r.rows[0].values[0].integer);
    try std.testing.expectEqual(@as(i64, 2), r.rows[0].values[1].integer);
    try std.testing.expectEqual(@as(i64, 3), r.rows[0].values[2].integer);
}

test "execute: SELECT NULL" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT NULL");
    defer r.deinit();
    try std.testing.expectEqual(Value.null, r.rows[0].values[0]);
}

test "execute: SELECT 'hello'" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT 'hello'");
    defer r.deinit();
    try std.testing.expectEqualStrings("hello", r.rows[0].values[0].text);
}

test "execute: SELECT 'it''s'" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT 'it''s'");
    defer r.deinit();
    try std.testing.expectEqualStrings("it's", r.rows[0].values[0].text);
}

test "execute: division by zero is NULL" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT 1/0");
    defer r.deinit();
    try std.testing.expectEqual(Value.null, r.rows[0].values[0]);
}
