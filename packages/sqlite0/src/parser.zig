const std = @import("std");
const lex = @import("lex.zig");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");

const Token = lex.Token;
const TokenKind = lex.TokenKind;
const Value = value_mod.Value;
const Error = ops.Error;

/// Parse a single `SELECT <expr-list>` statement and return the evaluated
/// scalar values. The parser uses an eager tree-walking strategy: each
/// `parse*` method both consumes tokens and folds the values it just parsed,
/// so by the end of `parseSelect` we have a `[]Value` ready to ship to the
/// caller. There is no AST.
///
/// Caller owns the returned slice and the heap memory inside each `Value`.
/// Free with `ops.freeValue` per element and `allocator.free` for the slice.
pub fn parseSelect(allocator: std.mem.Allocator, sql: []const u8) ![]Value {
    var p = Parser.init(allocator, sql);
    return p.parseSelectInternal();
}

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

    /// Capture the lexer/cursor state before a speculative advance so we can
    /// restore it if the lookahead fails. Used for resolving the `NOT BETWEEN`
    /// / `NOT IN` ambiguity, where a leading `NOT` may belong to an outer
    /// rule (`parseNot`) rather than the postfix BETWEEN/IN form.
    const Snapshot = struct { cur: Token, pos: u32 };

    fn snapshot(self: *const Parser) Snapshot {
        return .{ .cur = self.cur, .pos = self.lx.pos };
    }

    fn restore(self: *Parser, snap: Snapshot) void {
        self.cur = snap.cur;
        self.lx.pos = snap.pos;
    }

    fn expect(self: *Parser, kind: TokenKind) !void {
        if (self.cur.kind != kind) return Error.SyntaxError;
        self.advance();
    }

    fn parseSelectInternal(self: *Parser) ![]Value {
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
                    var has_not = false;
                    if (self.cur.kind == .keyword_not) {
                        has_not = true;
                        self.advance();
                    }
                    var has_distinct = false;
                    if (self.cur.kind == .keyword_distinct) {
                        self.advance();
                        try self.expect(.keyword_from);
                        has_distinct = true;
                    }
                    const right = try self.parseComparison();
                    defer ops.freeValue(self.allocator, right);
                    const eq = ops.identicalValues(left, right);
                    ops.freeValue(self.allocator, left);
                    const negate = has_not != has_distinct; // XOR
                    left = ops.boolValue(if (negate) !eq else eq);
                },
                .keyword_between => {
                    self.advance();
                    left = try self.parseBetween(left, false);
                },
                .keyword_in => {
                    self.advance();
                    left = try self.parseInList(left, false);
                },
                .keyword_not => {
                    const snap = self.snapshot();
                    self.advance();
                    if (self.cur.kind == .keyword_between) {
                        self.advance();
                        left = try self.parseBetween(left, true);
                    } else if (self.cur.kind == .keyword_in) {
                        self.advance();
                        left = try self.parseInList(left, true);
                    } else {
                        self.restore(snap);
                        break;
                    }
                },
                else => break,
            }
        }
        return left;
    }

    fn parseBetween(self: *Parser, left: Value, negate: bool) Error!Value {
        const lo = try self.parseComparison();
        defer ops.freeValue(self.allocator, lo);
        try self.expect(.keyword_and);
        const hi = try self.parseComparison();
        defer ops.freeValue(self.allocator, hi);

        const ge = ops.applyComparison(.ge, left, lo);
        const le = ops.applyComparison(.le, left, hi);
        ops.freeValue(self.allocator, left);
        const conj = ops.logicalAnd(ge, le);
        if (negate) return ops.logicalNot(conj);
        return conj;
    }

    fn parseInList(self: *Parser, left: Value, negate: bool) Error!Value {
        try self.expect(.lparen);
        var items: std.ArrayList(Value) = .empty;
        defer {
            for (items.items) |v| ops.freeValue(self.allocator, v);
            items.deinit(self.allocator);
        }
        if (self.cur.kind != .rparen) {
            try items.append(self.allocator, try self.parseExpr());
            while (self.cur.kind == .comma) {
                self.advance();
                try items.append(self.allocator, try self.parseExpr());
            }
        }
        try self.expect(.rparen);

        defer ops.freeValue(self.allocator, left);
        const result = ops.applyIn(left, items.items);
        if (negate) return ops.logicalNot(result);
        return result;
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
            .keyword_case => return self.parseCase(),
            else => return Error.SyntaxError,
        }
    }

    /// SQL CASE expression. Two forms:
    ///   simple:   CASE expr WHEN v1 THEN r1 ... [ELSE rd] END
    ///             — first WHEN whose value `=` the subject wins (truthy
    ///             check; NULL = NULL is NULL → false, so NULL never matches)
    ///   searched: CASE WHEN c1 THEN r1 ... [ELSE rd] END
    ///             — first WHEN whose condition is truthy wins
    /// Returns NULL when no WHEN matches and no ELSE is provided.
    fn parseCase(self: *Parser) Error!Value {
        self.advance();
        var subject_opt: ?Value = null;
        defer if (subject_opt) |s| ops.freeValue(self.allocator, s);

        if (self.cur.kind != .keyword_when) {
            subject_opt = try self.parseExpr();
        }

        var result: ?Value = null;
        errdefer if (result) |r| ops.freeValue(self.allocator, r);
        var matched = false;

        if (self.cur.kind != .keyword_when) return Error.SyntaxError;

        while (self.cur.kind == .keyword_when) {
            self.advance();
            const cond = try self.parseExpr();
            try self.expect(.keyword_then);
            const branch_val = try self.parseExpr();

            if (matched) {
                ops.freeValue(self.allocator, cond);
                ops.freeValue(self.allocator, branch_val);
                continue;
            }

            const is_match = blk: {
                if (subject_opt) |s| {
                    const eq = ops.applyEquality(.eq, s, cond);
                    ops.freeValue(self.allocator, cond);
                    break :blk ops.truthy(eq) orelse false;
                }
                const t = ops.truthy(cond) orelse false;
                ops.freeValue(self.allocator, cond);
                break :blk t;
            };

            if (is_match) {
                result = branch_val;
                matched = true;
            } else {
                ops.freeValue(self.allocator, branch_val);
            }
        }

        if (self.cur.kind == .keyword_else) {
            self.advance();
            const else_val = try self.parseExpr();
            if (!matched) {
                result = else_val;
                matched = true;
            } else {
                ops.freeValue(self.allocator, else_val);
            }
        }

        try self.expect(.keyword_end);

        return result orelse Value.null;
    }
};

test "parser: SELECT 1 returns one Value" {
    const allocator = std.testing.allocator;
    const cols = try parseSelect(allocator, "SELECT 1");
    defer {
        for (cols) |v| ops.freeValue(allocator, v);
        allocator.free(cols);
    }
    try std.testing.expectEqual(@as(usize, 1), cols.len);
    try std.testing.expectEqual(@as(i64, 1), cols[0].integer);
}

test "parser: trailing token after SELECT is a syntax error" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(Error.SyntaxError, parseSelect(allocator, "SELECT 1 garbage"));
}
