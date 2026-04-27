const std = @import("std");
const lex = @import("lex.zig");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const ast = @import("ast.zig");
const eval = @import("eval.zig");
const parser_predicate = @import("parser_predicate.zig");
const database = @import("database.zig");

const Token = lex.Token;
const TokenKind = lex.TokenKind;
const Value = value_mod.Value;
const Error = ops.Error;
const Database = database.Database;

/// Recursive-descent SQL expression parser. Per ADR-0002 (Iter8.A → C) the
/// entire expression grammar is now AST-driven: every `parse*` method
/// returns `*ast.Expr`. The previous eager-evaluation paths are gone, and
/// `eval.evalExpr` is the sole consumer that lowers an AST to a `Value`.
/// Statement-level dispatch lives in `stmt.zig`.
///
/// `db` is the (optional) live `Database` for parser-time evaluation paths
/// that need state access — currently only `VALUES` tuples, which sqlite3
/// accepts subqueries inside (`INSERT INTO t VALUES ((SELECT ...))`). It's
/// set in `engine.dispatchOne` for the duration of one statement; the
/// REPL/CLI `Parser.init` leaves it null (those callers don't reach VALUES).
pub const Parser = struct {
    src: []const u8,
    lx: lex.Lexer,
    cur: Token,
    allocator: std.mem.Allocator,
    db: ?*Database = null,

    pub fn init(allocator: std.mem.Allocator, src: []const u8) Parser {
        var lxr = lex.Lexer.init(src);
        const first = lxr.next();
        return .{ .src = src, .lx = lxr, .cur = first, .allocator = allocator };
    }

    pub fn advance(self: *Parser) void {
        self.cur = self.lx.next();
    }

    /// Capture the lexer/cursor state before a speculative advance so we can
    /// restore it if the lookahead fails. Used for resolving the `NOT BETWEEN`
    /// / `NOT IN` ambiguity, where a leading `NOT` may belong to an outer
    /// rule (`parseNot`) rather than the postfix BETWEEN/IN form.
    pub const Snapshot = struct { cur: Token, pos: u32 };

    pub fn snapshot(self: *const Parser) Snapshot {
        return .{ .cur = self.cur, .pos = self.lx.pos };
    }

    pub fn restore(self: *Parser, snap: Snapshot) void {
        self.cur = snap.cur;
        self.lx.pos = snap.pos;
    }

    pub fn expect(self: *Parser, kind: TokenKind) !void {
        if (self.cur.kind != kind) return Error.SyntaxError;
        self.advance();
    }

    pub fn parseExpr(self: *Parser) Error!*ast.Expr {
        return self.parseOr();
    }

    fn parseOr(self: *Parser) Error!*ast.Expr {
        var left = try self.parseAnd();
        errdefer left.deinit(self.allocator);
        while (self.cur.kind == .keyword_or) {
            self.advance();
            const right = try self.parseAnd();
            errdefer right.deinit(self.allocator);
            left = try ast.makeLogicalOr(self.allocator, left, right);
        }
        return left;
    }

    fn parseAnd(self: *Parser) Error!*ast.Expr {
        var left = try self.parseNot();
        errdefer left.deinit(self.allocator);
        while (self.cur.kind == .keyword_and) {
            self.advance();
            const right = try self.parseNot();
            errdefer right.deinit(self.allocator);
            left = try ast.makeLogicalAnd(self.allocator, left, right);
        }
        return left;
    }

    fn parseNot(self: *Parser) Error!*ast.Expr {
        if (self.cur.kind == .keyword_not) {
            self.advance();
            const inner = try self.parseNot();
            errdefer inner.deinit(self.allocator);
            return ast.makeLogicalNot(self.allocator, inner);
        }
        return self.parseEquality();
    }

    fn parseEquality(self: *Parser) Error!*ast.Expr {
        var left = try self.parseComparison();
        errdefer left.deinit(self.allocator);
        while (true) {
            switch (self.cur.kind) {
                .eq, .ne => {
                    const op: ast.EqOp = if (self.cur.kind == .eq) .eq else .ne;
                    self.advance();
                    const right = try self.parseComparison();
                    errdefer right.deinit(self.allocator);
                    left = try ast.makeEqCheck(self.allocator, op, left, right);
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
                    errdefer right.deinit(self.allocator);
                    const negated = has_not != has_distinct;
                    left = try ast.makeIsCheck(self.allocator, left, right, negated);
                },
                .keyword_between => {
                    self.advance();
                    left = try parser_predicate.parseBetween(self, left, false);
                },
                .keyword_in => {
                    self.advance();
                    left = try parser_predicate.parseInList(self, left, false);
                },
                .keyword_like => {
                    self.advance();
                    left = try parser_predicate.parseLike(self, left, .like, false);
                },
                .keyword_glob => {
                    self.advance();
                    left = try parser_predicate.parseLike(self, left, .glob, false);
                },
                .keyword_not => {
                    const snap = self.snapshot();
                    self.advance();
                    if (self.cur.kind == .keyword_between) {
                        self.advance();
                        left = try parser_predicate.parseBetween(self, left, true);
                    } else if (self.cur.kind == .keyword_in) {
                        self.advance();
                        left = try parser_predicate.parseInList(self, left, true);
                    } else if (self.cur.kind == .keyword_like) {
                        self.advance();
                        left = try parser_predicate.parseLike(self, left, .like, true);
                    } else if (self.cur.kind == .keyword_glob) {
                        self.advance();
                        left = try parser_predicate.parseLike(self, left, .glob, true);
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

    pub fn parseComparison(self: *Parser) Error!*ast.Expr {
        var left = try self.parseAddSub();
        errdefer left.deinit(self.allocator);
        while (self.cur.kind == .lt or self.cur.kind == .le or self.cur.kind == .gt or self.cur.kind == .ge) {
            const op: ast.CompareOp = switch (self.cur.kind) {
                .lt => .lt,
                .le => .le,
                .gt => .gt,
                .ge => .ge,
                else => unreachable,
            };
            self.advance();
            const right = try self.parseAddSub();
            errdefer right.deinit(self.allocator);
            left = try ast.makeCompare(self.allocator, op, left, right);
        }
        return left;
    }

    pub fn parseAddSub(self: *Parser) Error!*ast.Expr {
        var left = try self.parseMulDiv();
        errdefer left.deinit(self.allocator);
        while (self.cur.kind == .plus or self.cur.kind == .minus) {
            const op: ast.BinaryOp = if (self.cur.kind == .plus) .add else .sub;
            self.advance();
            const right = try self.parseMulDiv();
            errdefer right.deinit(self.allocator);
            left = try ast.makeBinaryArith(self.allocator, op, left, right);
        }
        return left;
    }

    fn parseMulDiv(self: *Parser) Error!*ast.Expr {
        var left = try self.parseConcat();
        errdefer left.deinit(self.allocator);
        while (self.cur.kind == .star or self.cur.kind == .slash or self.cur.kind == .percent) {
            const op: ast.BinaryOp = switch (self.cur.kind) {
                .star => .mul,
                .slash => .div,
                .percent => .mod,
                else => unreachable,
            };
            self.advance();
            const right = try self.parseConcat();
            errdefer right.deinit(self.allocator);
            left = try ast.makeBinaryArith(self.allocator, op, left, right);
        }
        return left;
    }

    /// `||` string concatenation. Sits between *,/,% and unary +,- in the
    /// precedence chain (see SQLite "Operators, expressions, and parsed
    /// elements" docs § 3 Order of Operations). Left-associative.
    fn parseConcat(self: *Parser) Error!*ast.Expr {
        var left = try self.parseUnary();
        errdefer left.deinit(self.allocator);
        while (self.cur.kind == .concat) {
            self.advance();
            const right = try self.parseUnary();
            errdefer right.deinit(self.allocator);
            left = try ast.makeBinaryConcat(self.allocator, left, right);
        }
        return left;
    }

    fn parseUnary(self: *Parser) Error!*ast.Expr {
        if (self.cur.kind == .minus) {
            self.advance();
            const inner = try self.parseUnary();
            errdefer inner.deinit(self.allocator);
            return ast.makeUnaryNegate(self.allocator, inner);
        }
        if (self.cur.kind == .plus) {
            self.advance();
            return self.parseUnary();
        }
        return self.parsePrimary();
    }

    fn parsePrimary(self: *Parser) Error!*ast.Expr {
        const tok = self.cur;
        switch (tok.kind) {
            .integer => {
                self.advance();
                const text = tok.slice(self.src);
                const n = std.fmt.parseInt(i64, text, 10) catch return Error.InvalidNumber;
                return ast.makeLiteral(self.allocator, Value{ .integer = n });
            },
            .real => {
                self.advance();
                const text = tok.slice(self.src);
                const f = std.fmt.parseFloat(f64, text) catch return Error.InvalidNumber;
                return ast.makeLiteral(self.allocator, Value{ .real = f });
            },
            .string => {
                self.advance();
                const text = tok.slice(self.src);
                if (text.len < 2 or text[0] != '\'' or text[text.len - 1] != '\'') return Error.InvalidString;
                const unesc = try ops.unescapeStringLiteral(self.allocator, text[1 .. text.len - 1]);
                return ast.makeLiteral(self.allocator, Value{ .text = unesc }) catch |err| {
                    self.allocator.free(unesc);
                    return err;
                };
            },
            .keyword_null => {
                self.advance();
                return ast.makeLiteral(self.allocator, Value.null);
            },
            .lparen => {
                self.advance();
                if (self.cur.kind == .keyword_select) {
                    // Scalar subquery: `(SELECT ...)` in expression position.
                    // The inner SELECT supports the full grammar (ORDER BY /
                    // LIMIT / setop chain) — we just terminate at `rparen`
                    // and box the ParsedSelect into an ast.Expr.subquery.
                    const stmt_mod = @import("stmt.zig");
                    const ps = try stmt_mod.parseSelectStatement(self);
                    errdefer stmt_mod.freeParsedSelectFields(self.allocator, ps);
                    try self.expect(.rparen);
                    return ast.makeSubquery(self.allocator, ps);
                }
                const inner = try self.parseExpr();
                errdefer inner.deinit(self.allocator);
                try self.expect(.rparen);
                return inner;
            },
            .identifier => return self.parseIdentifierExpr(),
            .keyword_case => return self.parseCase(),
            .keyword_exists => {
                self.advance();
                return parser_predicate.parseExists(self);
            },
            else => return Error.SyntaxError,
        }
    }

    /// Identifier disambiguation:
    ///   `name(args...)`  → function call
    ///   `qual.name`      → qualified column reference (Iter19.A)
    ///   `name`           → bare column reference resolved at eval time
    ///
    /// SQLite has no other use of bare identifiers in expressions (no
    /// enums, no bare constants like `current_date`). Multi-level
    /// qualifiers (`schema.table.column`) are not supported.
    fn parseIdentifierExpr(self: *Parser) Error!*ast.Expr {
        const name = self.cur.slice(self.src);
        self.advance();
        if (self.cur.kind == .lparen) {
            return self.parseFunctionCallTail(name);
        }
        if (self.cur.kind == .dot) {
            self.advance();
            if (self.cur.kind != .identifier) return Error.SyntaxError;
            const col = self.cur.slice(self.src);
            self.advance();
            return ast.makeColumnRef(self.allocator, name, col);
        }
        return ast.makeColumnRef(self.allocator, null, name);
    }

    /// Function-call body parser, called after the name and `(` have been
    /// consumed by `parseIdentifierExpr`.
    ///
    /// `name(*)` is the aggregate-`*` form (`count(*)`). It's modeled as a
    /// 0-arg call because `count()`/`count(*)`/`count(1)` all behave
    /// identically as "count rows" once aggregate dispatch sees a 0-arg or
    /// always-truthy arg. The `*` is consumed silently — distinguishing it
    /// from a true 0-arg call would require an AST shape we don't need.
    fn parseFunctionCallTail(self: *Parser, name: []const u8) Error!*ast.Expr {
        self.advance(); // consume lparen
        var args: std.ArrayList(*ast.Expr) = .empty;
        errdefer {
            for (args.items) |a| a.deinit(self.allocator);
            args.deinit(self.allocator);
        }
        // Optional DISTINCT modifier (sqlite3 allows this on any function call;
        // aggregate dispatch enforces the "exactly one argument" rule). `ALL`
        // is accepted as the explicit non-distinct counterpart to keep parity
        // with sqlite3 (`count(ALL x)`).
        var distinct = false;
        if (self.cur.kind == .keyword_distinct) {
            distinct = true;
            self.advance();
        } else if (self.cur.kind == .keyword_all) {
            self.advance();
        }
        if (self.cur.kind == .star) {
            self.advance();
        } else if (self.cur.kind != .rparen) {
            try args.ensureUnusedCapacity(self.allocator, 1);
            args.appendAssumeCapacity(try self.parseExpr());
            while (self.cur.kind == .comma) {
                self.advance();
                try args.ensureUnusedCapacity(self.allocator, 1);
                args.appendAssumeCapacity(try self.parseExpr());
            }
        }
        try self.expect(.rparen);
        const args_slice = try args.toOwnedSlice(self.allocator);
        return ast.makeFuncCall(self.allocator, name, args_slice, distinct) catch |err| {
            for (args_slice) |a| a.deinit(self.allocator);
            self.allocator.free(args_slice);
            return err;
        };
    }

    /// SQL CASE expression. Two forms:
    ///   simple:   CASE expr WHEN v1 THEN r1 ... [ELSE rd] END
    ///             — first WHEN whose value `=` the subject wins (truthy
    ///             check; NULL = NULL is NULL → false, so NULL never matches)
    ///   searched: CASE WHEN c1 THEN r1 ... [ELSE rd] END
    ///             — first WHEN whose condition is truthy wins
    /// Returns NULL when no WHEN matches and no ELSE is provided.
    fn parseCase(self: *Parser) Error!*ast.Expr {
        self.advance(); // consume CASE

        var scrutinee: ?*ast.Expr = null;
        errdefer if (scrutinee) |s| s.deinit(self.allocator);

        if (self.cur.kind != .keyword_when) {
            scrutinee = try self.parseExpr();
        }

        var branches: std.ArrayList(ast.Expr.CaseBranch) = .empty;
        errdefer {
            for (branches.items) |b| {
                b.when.deinit(self.allocator);
                b.then.deinit(self.allocator);
            }
            branches.deinit(self.allocator);
        }

        if (self.cur.kind != .keyword_when) return Error.SyntaxError;

        while (self.cur.kind == .keyword_when) {
            self.advance();
            const when = try self.parseExpr();
            errdefer when.deinit(self.allocator);
            try self.expect(.keyword_then);
            const then_expr = try self.parseExpr();
            errdefer then_expr.deinit(self.allocator);
            try branches.ensureUnusedCapacity(self.allocator, 1);
            branches.appendAssumeCapacity(.{ .when = when, .then = then_expr });
        }

        var else_branch: ?*ast.Expr = null;
        errdefer if (else_branch) |eb| eb.deinit(self.allocator);
        if (self.cur.kind == .keyword_else) {
            self.advance();
            else_branch = try self.parseExpr();
        }
        try self.expect(.keyword_end);

        const branches_slice = try branches.toOwnedSlice(self.allocator);
        return ast.makeCaseExpr(self.allocator, scrutinee, branches_slice, else_branch) catch |err| {
            for (branches_slice) |b| {
                b.when.deinit(self.allocator);
                b.then.deinit(self.allocator);
            }
            self.allocator.free(branches_slice);
            return err;
        };
    }
};

test "Parser: parseExpr literal" {
    const allocator = std.testing.allocator;
    var p = Parser.init(allocator, "42");
    const expr = try p.parseExpr();
    defer expr.deinit(allocator);
    const v = try eval.evalExpr(.{ .allocator = allocator }, expr);
    try std.testing.expectEqual(@as(i64, 42), v.integer);
}

test "Parser: parseExpr arithmetic" {
    const allocator = std.testing.allocator;
    var p = Parser.init(allocator, "1+2*3");
    const expr = try p.parseExpr();
    defer expr.deinit(allocator);
    const v = try eval.evalExpr(.{ .allocator = allocator }, expr);
    try std.testing.expectEqual(@as(i64, 7), v.integer);
}
