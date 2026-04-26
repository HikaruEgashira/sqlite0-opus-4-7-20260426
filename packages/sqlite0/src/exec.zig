const std = @import("std");
const lex = @import("lex.zig");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");

const Token = lex.Token;
const TokenKind = lex.TokenKind;
const Value = value_mod.Value;

pub const Error = ops.Error;

pub const Row = struct {
    values: []Value,

    pub fn deinit(self: *Row, allocator: std.mem.Allocator) void {
        for (self.values) |v| ops.freeValue(allocator, v);
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
        errdefer for (values.items) |v| ops.freeValue(self.allocator, v);

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
        return self.parseOr();
    }

    fn parseOr(self: *Parser) Error!Value {
        var left = try self.parseAnd();
        while (self.cur.kind == .keyword_or) {
            self.advance();
            const right = try self.parseAnd();
            defer ops.freeValue(self.allocator, right);
            const new_left = ops.logicalOr(left, right);
            ops.freeValue(self.allocator, left);
            left = new_left;
        }
        return left;
    }

    fn parseAnd(self: *Parser) Error!Value {
        var left = try self.parseNot();
        while (self.cur.kind == .keyword_and) {
            self.advance();
            const right = try self.parseNot();
            defer ops.freeValue(self.allocator, right);
            const new_left = ops.logicalAnd(left, right);
            ops.freeValue(self.allocator, left);
            left = new_left;
        }
        return left;
    }

    fn parseNot(self: *Parser) Error!Value {
        if (self.cur.kind == .keyword_not) {
            self.advance();
            const inner = try self.parseNot();
            defer ops.freeValue(self.allocator, inner);
            return ops.logicalNot(inner);
        }
        return self.parseEquality();
    }

    fn parseEquality(self: *Parser) Error!Value {
        var left = try self.parseComparison();
        while (true) {
            switch (self.cur.kind) {
                .eq, .ne => {
                    const op = self.cur.kind;
                    self.advance();
                    const right = try self.parseComparison();
                    defer ops.freeValue(self.allocator, right);
                    const new_left = ops.applyEquality(op, left, right);
                    ops.freeValue(self.allocator, left);
                    left = new_left;
                },
                .keyword_is => {
                    self.advance();
                    var negate = false;
                    if (self.cur.kind == .keyword_not) {
                        negate = true;
                        self.advance();
                    }
                    const right = try self.parseComparison();
                    defer ops.freeValue(self.allocator, right);
                    const eq = ops.identicalValues(left, right);
                    ops.freeValue(self.allocator, left);
                    left = ops.boolValue(if (negate) !eq else eq);
                },
                else => break,
            }
        }
        return left;
    }

    fn parseComparison(self: *Parser) Error!Value {
        var left = try self.parseAddSub();
        while (self.cur.kind == .lt or self.cur.kind == .le or self.cur.kind == .gt or self.cur.kind == .ge) {
            const op = self.cur.kind;
            self.advance();
            const right = try self.parseAddSub();
            defer ops.freeValue(self.allocator, right);
            const new_left = ops.applyComparison(op, left, right);
            ops.freeValue(self.allocator, left);
            left = new_left;
        }
        return left;
    }

    fn parseAddSub(self: *Parser) Error!Value {
        var left = try self.parseMulDiv();
        while (self.cur.kind == .plus or self.cur.kind == .minus) {
            const op = self.cur.kind;
            self.advance();
            const right = try self.parseMulDiv();
            defer ops.freeValue(self.allocator, right);
            const new_left = try ops.applyArith(op, left, right);
            ops.freeValue(self.allocator, left);
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
            defer ops.freeValue(self.allocator, right);
            const new_left = try ops.applyArith(op, left, right);
            ops.freeValue(self.allocator, left);
            left = new_left;
        }
        return left;
    }

    fn parseUnary(self: *Parser) Error!Value {
        if (self.cur.kind == .minus) {
            self.advance();
            const inner = try self.parseUnary();
            defer ops.freeValue(self.allocator, inner);
            return ops.negateValue(inner);
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
                return Value{ .text = try ops.unescapeStringLiteral(self.allocator, text[1 .. text.len - 1]) };
            },
            .keyword_null => {
                self.advance();
                return Value.null;
            },
            .lparen => {
                self.advance();
                const v = try self.parseExpr();
                if (self.cur.kind != .rparen) {
                    ops.freeValue(self.allocator, v);
                    return Error.SyntaxError;
                }
                self.advance();
                return v;
            },
            else => return Error.SyntaxError,
        }
    }
};

pub fn execute(allocator: std.mem.Allocator, sql: []const u8) !Result {
    var parser = Parser.init(allocator, sql);
    const cols = try parser.parseSelect();
    errdefer {
        for (cols) |v| ops.freeValue(allocator, v);
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

test "execute: SELECT 1=1" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT 1=1");
    defer r.deinit();
    try std.testing.expectEqual(@as(i64, 1), r.rows[0].values[0].integer);
}

test "execute: SELECT NULL IS NULL" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT NULL IS NULL");
    defer r.deinit();
    try std.testing.expectEqual(@as(i64, 1), r.rows[0].values[0].integer);
}

test "execute: SELECT NOT NULL is NULL" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT NOT NULL");
    defer r.deinit();
    try std.testing.expectEqual(Value.null, r.rows[0].values[0]);
}

test "execute: SELECT 1<2 AND 2<3" {
    const allocator = std.testing.allocator;
    var r = try execute(allocator, "SELECT 1<2 AND 2<3");
    defer r.deinit();
    try std.testing.expectEqual(@as(i64, 1), r.rows[0].values[0].integer);
}
