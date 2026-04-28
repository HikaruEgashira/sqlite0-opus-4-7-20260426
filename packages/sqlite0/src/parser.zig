const std = @import("std");
const lex = @import("lex.zig");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const ast = @import("ast.zig");
const eval = @import("eval.zig");
const parser_predicate = @import("parser_predicate.zig");
const parser_cast = @import("parser_cast.zig");
const parser_call = @import("parser_call.zig");
const parser_literal = @import("parser_literal.zig");
const database = @import("database.zig");
const collation = @import("collation.zig");

const hexDigitValue = parser_literal.hexDigitValue;
const parseIntegerLiteralAsValue = parser_literal.parseIntegerLiteralAsValue;
const parseNegatedDecimalI64 = parser_literal.parseNegatedDecimalI64;
const parseRealLiteral = parser_literal.parseRealLiteral;

const Token = lex.Token;
const TokenKind = lex.TokenKind;
const Value = value_mod.Value;
const Error = ops.Error;
const Database = database.Database;

/// Recursive-descent SQL expression parser (ADR-0002). Each `parse*` returns
/// `*ast.Expr`; `eval.evalExpr` is the sole AST→Value lowering. Statement
/// dispatch lives in `stmt.zig`. `db` is set by `engine.dispatchOne` so
/// `VALUES (..)` tuples (eagerly evaluated at parse time) can resolve
/// subqueries; left null on REPL `Parser.init` (no VALUES path there).
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
        var expr = try self.parseOr();
        errdefer expr.deinit(self.allocator);
        // Trailing-COLLATE rescue: sqlite3 accepts `'A' IN ('a','b') COLLATE
        // NOCASE` and `1 < 2 COLLATE NOCASE` (parseEquality consumes the
        // RHS without re-entering parseUnary, so the in-operand path
        // missed it). COLLATE on a non-TEXT result is a no-op, but the
        // parse must succeed.
        while (self.cur.kind == .keyword_collate) expr = try self.consumeCollatePostfix(expr);
        return expr;
    }

    fn consumeCollatePostfix(self: *Parser, expr: *ast.Expr) Error!*ast.Expr {
        self.advance();
        if (self.cur.kind != .identifier) return Error.SyntaxError;
        const kind = collation.kindFromName(self.cur.slice(self.src)) orelse return Error.SyntaxError;
        self.advance();
        return ast.makeCollate(self.allocator, expr, kind);
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
                    const negated = has_not != has_distinct;
                    // sqlite3 quirk: bare TRUE/FALSE on RHS uses truthiness
                    // coercion (not strict identicalValues). Catch before
                    // parseComparison so the keyword stays as a keyword.
                    if (self.cur.kind == .keyword_true or self.cur.kind == .keyword_false) {
                        const expect_true = self.cur.kind == .keyword_true;
                        self.advance();
                        left = try ast.makeIsTruthy(self.allocator, left, expect_true, negated);
                    } else {
                        const right = try self.parseComparison();
                        errdefer right.deinit(self.allocator);
                        left = try ast.makeIsCheck(self.allocator, left, right, negated);
                    }
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
        var left = try self.parseBitwise();
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
            const right = try self.parseBitwise();
            errdefer right.deinit(self.allocator);
            left = try ast.makeCompare(self.allocator, op, left, right);
        }
        return left;
    }

    /// Bitwise operators `&`, `|`, `<<`, `>>` — left-associative, all at
    /// one precedence level (sqlite3 docs § Operator Precedence).
    /// Higher-binding than `<` etc., lower-binding than `+` / `-`.
    pub fn parseBitwise(self: *Parser) Error!*ast.Expr {
        var left = try self.parseAddSub();
        errdefer left.deinit(self.allocator);
        while (true) {
            const op: ast.BinaryOp = switch (self.cur.kind) {
                .bit_and => .bit_and,
                .bit_or => .bit_or,
                .shift_left => .shift_left,
                .shift_right => .shift_right,
                else => break,
            };
            self.advance();
            const right = try self.parseAddSub();
            errdefer right.deinit(self.allocator);
            left = try ast.makeBinaryArith(self.allocator, op, left, right);
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

    /// `||` left-assoc, between */% and unary +,- in precedence chain.
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
            // Sign-fold `-<int-literal>`: `-9223372036854775808` is exactly
            // LLONG_MIN but the positive form overflows i64; fold here so
            // the literal stays INTEGER. `-0x8000000000000000` is the one
            // u64 whose negation overflows i64 — sqlite3 errors there
            // ("hex literal too big") rather than promoting to REAL.
            if (self.cur.kind == .integer) {
                const itext = self.cur.slice(self.src);
                const is_hex = itext.len > 2 and itext[0] == '0' and (itext[1] == 'x' or itext[1] == 'X');
                if (is_hex) {
                    const u = std.fmt.parseInt(u64, itext[2..], 16) catch return Error.InvalidNumber;
                    const llong_min_mag: u64 = @as(u64, 1) << 63;
                    if (u == llong_min_mag) return Error.SyntaxError;
                    self.advance();
                    const lit = try ast.makeLiteral(self.allocator, Value{ .integer = -@as(i64, @bitCast(u)) });
                    return self.collateTail(lit);
                }
                if (parseNegatedDecimalI64(itext)) |neg_i| {
                    self.advance();
                    const lit = try ast.makeLiteral(self.allocator, Value{ .integer = neg_i });
                    return self.collateTail(lit);
                } else |_| {
                    // Overflows LLONG_MIN — fall through. Positive literal
                    // parses as REAL, then eval-time negate keeps REAL.
                }
            }
            const inner = try self.parseUnary();
            errdefer inner.deinit(self.allocator);
            return ast.makeUnaryNegate(self.allocator, inner);
        }
        if (self.cur.kind == .plus) {
            self.advance();
            return self.parseUnary();
        }
        if (self.cur.kind == .bit_not) {
            self.advance();
            const inner = try self.parseUnary();
            errdefer inner.deinit(self.allocator);
            return ast.makeUnaryBitNot(self.allocator, inner);
        }
        return self.parseCollatePostfix();
    }

    /// `expr COLLATE <name>` postfix. Sits below parseUnary so COLLATE
    /// binds tighter than `-`/`+`/`~`. Left-associative — chained
    /// `COLLATE A COLLATE B` outermost (B) wins at compare time per
    /// sqlite3. Unknown name → SyntaxError ("no such collation sequence").
    fn parseCollatePostfix(self: *Parser) Error!*ast.Expr {
        return self.collateTail(try self.parsePrimary());
    }

    /// Attach any postfix `COLLATE <name>` chain to an already-parsed expr.
    /// Used by parseCollatePostfix and the `-<int-literal>` sign-fold path.
    fn collateTail(self: *Parser, base: *ast.Expr) Error!*ast.Expr {
        var expr = base;
        errdefer expr.deinit(self.allocator);
        while (self.cur.kind == .keyword_collate) expr = try self.consumeCollatePostfix(expr);
        return expr;
    }

    fn parsePrimary(self: *Parser) Error!*ast.Expr {
        const tok = self.cur;
        switch (tok.kind) {
            .integer => {
                self.advance();
                const text = tok.slice(self.src);
                const v = parseIntegerLiteralAsValue(self.allocator, text) catch return Error.InvalidNumber;
                return ast.makeLiteral(self.allocator, v);
            },
            .real => {
                self.advance();
                const text = tok.slice(self.src);
                const f = parseRealLiteral(self.allocator, text) catch return Error.InvalidNumber;
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
            .blob_lit => {
                // Lexer guarantees the shape `x'<even-length hex>'` —
                // strip the `x'`/`'` envelope and decode pairs of hex
                // digits into bytes. Empty hex span yields a 0-byte BLOB
                // (sqlite3: `x''` → empty blob).
                self.advance();
                const text = tok.slice(self.src);
                const hex = text[2 .. text.len - 1];
                // No errdefer: `hexDigitValue` is infallible, so `bytes`
                // only needs cleanup if `makeLiteral` itself errors —
                // the catch handles that. Same shape as the .string arm
                // (mixing both fires double-free on OOM).
                const bytes = try self.allocator.alloc(u8, hex.len / 2);
                var i: usize = 0;
                while (i < hex.len) : (i += 2) {
                    bytes[i / 2] = (hexDigitValue(hex[i]) << 4) | hexDigitValue(hex[i + 1]);
                }
                return ast.makeLiteral(self.allocator, Value{ .blob = bytes }) catch |err| {
                    self.allocator.free(bytes);
                    return err;
                };
            },
            .keyword_null => {
                self.advance();
                return ast.makeLiteral(self.allocator, Value.null);
            },
            .keyword_true => {
                // sqlite3 collapses TRUE/FALSE to INTEGER 1/0 (typeof =
                // 'integer'). Lexer always emits keyword_true/false so a
                // same-named column can't shadow the literal — sqlite3
                // resolves to the column when one is in scope; deferred.
                self.advance();
                return ast.makeLiteral(self.allocator, Value{ .integer = 1 });
            },
            .keyword_false => {
                self.advance();
                return ast.makeLiteral(self.allocator, Value{ .integer = 0 });
            },
            .keyword_like, .keyword_glob => {
                // sqlite3 fallback-keyword: in expression-start followed
                // by `(`, LIKE/GLOB act as scalar function calls. Bare
                // `SELECT like` still errors (we synthesize call only).
                const fname_tok = self.cur;
                self.advance();
                if (self.cur.kind != .lparen) return Error.SyntaxError;
                return parser_call.parseFunctionCallTail(self, fname_tok.slice(self.src));
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
            .keyword_case => return parser_call.parseCase(self),
            .keyword_exists => {
                self.advance();
                return parser_predicate.parseExists(self);
            },
            .keyword_cast => {
                self.advance();
                return parser_cast.parseCastBody(self);
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
            return parser_call.parseFunctionCallTail(self, name);
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
