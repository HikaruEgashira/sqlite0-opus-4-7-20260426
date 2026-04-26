const std = @import("std");

pub const TokenKind = enum {
    eof,
    invalid,
    integer,
    real,
    string,
    blob_lit,
    identifier,
    keyword_select,
    keyword_from,
    keyword_where,
    keyword_null,
    keyword_true,
    keyword_false,
    keyword_and,
    keyword_or,
    keyword_not,
    keyword_is,
    keyword_between,
    keyword_in,
    keyword_distinct,
    keyword_case,
    keyword_when,
    keyword_then,
    keyword_else,
    keyword_end,
    keyword_values,
    keyword_as,
    keyword_create,
    keyword_table,
    keyword_insert,
    keyword_into,
    plus,
    minus,
    star,
    slash,
    percent,
    lparen,
    rparen,
    comma,
    semicolon,
    eq,
    ne,
    lt,
    le,
    gt,
    ge,
    concat,
};

pub const Token = struct {
    kind: TokenKind,
    start: u32,
    end: u32,

    pub fn slice(self: Token, src: []const u8) []const u8 {
        return src[self.start..self.end];
    }
};

pub const Lexer = struct {
    src: []const u8,
    pos: u32 = 0,

    pub fn init(src: []const u8) Lexer {
        return .{ .src = src };
    }

    pub fn next(self: *Lexer) Token {
        self.skipTrivia();
        const start: u32 = self.pos;
        if (self.pos >= self.src.len) {
            return .{ .kind = .eof, .start = start, .end = start };
        }
        const c = self.src[self.pos];
        switch (c) {
            '+' => return self.single(.plus),
            '-' => return self.single(.minus),
            '*' => return self.single(.star),
            '/' => return self.single(.slash),
            '%' => return self.single(.percent),
            '(' => return self.single(.lparen),
            ')' => return self.single(.rparen),
            ',' => return self.single(.comma),
            ';' => return self.single(.semicolon),
            '=' => return self.single(.eq),
            '<' => {
                self.pos += 1;
                if (self.peek()) |next_c| {
                    if (next_c == '=') {
                        self.pos += 1;
                        return .{ .kind = .le, .start = start, .end = self.pos };
                    }
                    if (next_c == '>') {
                        self.pos += 1;
                        return .{ .kind = .ne, .start = start, .end = self.pos };
                    }
                }
                return .{ .kind = .lt, .start = start, .end = self.pos };
            },
            '>' => {
                self.pos += 1;
                if (self.peek()) |next_c| {
                    if (next_c == '=') {
                        self.pos += 1;
                        return .{ .kind = .ge, .start = start, .end = self.pos };
                    }
                }
                return .{ .kind = .gt, .start = start, .end = self.pos };
            },
            '!' => {
                self.pos += 1;
                if (self.peek()) |next_c| {
                    if (next_c == '=') {
                        self.pos += 1;
                        return .{ .kind = .ne, .start = start, .end = self.pos };
                    }
                }
                return .{ .kind = .invalid, .start = start, .end = self.pos };
            },
            '|' => {
                self.pos += 1;
                if (self.peek()) |next_c| {
                    if (next_c == '|') {
                        self.pos += 1;
                        return .{ .kind = .concat, .start = start, .end = self.pos };
                    }
                }
                return .{ .kind = .invalid, .start = start, .end = self.pos };
            },
            '\'' => return self.string(start),
            '0'...'9' => return self.number(start),
            'a'...'z', 'A'...'Z', '_' => return self.identifier(start),
            else => {
                self.pos += 1;
                return .{ .kind = .invalid, .start = start, .end = self.pos };
            },
        }
    }

    fn peek(self: *const Lexer) ?u8 {
        if (self.pos >= self.src.len) return null;
        return self.src[self.pos];
    }

    fn single(self: *Lexer, kind: TokenKind) Token {
        const start = self.pos;
        self.pos += 1;
        return .{ .kind = kind, .start = start, .end = self.pos };
    }

    fn skipTrivia(self: *Lexer) void {
        while (self.pos < self.src.len) {
            const c = self.src[self.pos];
            switch (c) {
                ' ', '\t', '\n', '\r' => self.pos += 1,
                '-' => {
                    if (self.pos + 1 < self.src.len and self.src[self.pos + 1] == '-') {
                        while (self.pos < self.src.len and self.src[self.pos] != '\n') self.pos += 1;
                    } else return;
                },
                else => return,
            }
        }
    }

    fn number(self: *Lexer, start: u32) Token {
        var saw_dot = false;
        var saw_exp = false;
        while (self.pos < self.src.len) {
            const c = self.src[self.pos];
            switch (c) {
                '0'...'9' => self.pos += 1,
                '.' => {
                    if (saw_dot or saw_exp) break;
                    saw_dot = true;
                    self.pos += 1;
                },
                'e', 'E' => {
                    if (saw_exp) break;
                    saw_exp = true;
                    self.pos += 1;
                    if (self.pos < self.src.len and (self.src[self.pos] == '+' or self.src[self.pos] == '-')) {
                        self.pos += 1;
                    }
                },
                else => break,
            }
        }
        const kind: TokenKind = if (saw_dot or saw_exp) .real else .integer;
        return .{ .kind = kind, .start = start, .end = self.pos };
    }

    fn string(self: *Lexer, start: u32) Token {
        self.pos += 1;
        while (self.pos < self.src.len) {
            if (self.src[self.pos] == '\'') {
                if (self.pos + 1 < self.src.len and self.src[self.pos + 1] == '\'') {
                    self.pos += 2;
                    continue;
                }
                self.pos += 1;
                return .{ .kind = .string, .start = start, .end = self.pos };
            }
            self.pos += 1;
        }
        return .{ .kind = .invalid, .start = start, .end = self.pos };
    }

    fn identifier(self: *Lexer, start: u32) Token {
        while (self.pos < self.src.len) {
            const c = self.src[self.pos];
            switch (c) {
                'a'...'z', 'A'...'Z', '0'...'9', '_' => self.pos += 1,
                else => break,
            }
        }
        const text = self.src[start..self.pos];
        const kind = keywordKind(text) orelse TokenKind.identifier;
        return .{ .kind = kind, .start = start, .end = self.pos };
    }
};

fn eqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| {
        if (std.ascii.toLower(x) != std.ascii.toLower(y)) return false;
    }
    return true;
}

fn keywordKind(text: []const u8) ?TokenKind {
    if (eqlIgnoreCase(text, "select")) return .keyword_select;
    if (eqlIgnoreCase(text, "from")) return .keyword_from;
    if (eqlIgnoreCase(text, "where")) return .keyword_where;
    if (eqlIgnoreCase(text, "null")) return .keyword_null;
    if (eqlIgnoreCase(text, "true")) return .keyword_true;
    if (eqlIgnoreCase(text, "false")) return .keyword_false;
    if (eqlIgnoreCase(text, "and")) return .keyword_and;
    if (eqlIgnoreCase(text, "or")) return .keyword_or;
    if (eqlIgnoreCase(text, "not")) return .keyword_not;
    if (eqlIgnoreCase(text, "is")) return .keyword_is;
    if (eqlIgnoreCase(text, "between")) return .keyword_between;
    if (eqlIgnoreCase(text, "in")) return .keyword_in;
    if (eqlIgnoreCase(text, "distinct")) return .keyword_distinct;
    if (eqlIgnoreCase(text, "case")) return .keyword_case;
    if (eqlIgnoreCase(text, "when")) return .keyword_when;
    if (eqlIgnoreCase(text, "then")) return .keyword_then;
    if (eqlIgnoreCase(text, "else")) return .keyword_else;
    if (eqlIgnoreCase(text, "end")) return .keyword_end;
    if (eqlIgnoreCase(text, "values")) return .keyword_values;
    if (eqlIgnoreCase(text, "as")) return .keyword_as;
    if (eqlIgnoreCase(text, "create")) return .keyword_create;
    if (eqlIgnoreCase(text, "table")) return .keyword_table;
    if (eqlIgnoreCase(text, "insert")) return .keyword_insert;
    if (eqlIgnoreCase(text, "into")) return .keyword_into;
    return null;
}

test "Lexer: SELECT integer" {
    var lx = Lexer.init("SELECT 42;");
    try std.testing.expectEqual(TokenKind.keyword_select, lx.next().kind);
    const num = lx.next();
    try std.testing.expectEqual(TokenKind.integer, num.kind);
    try std.testing.expectEqualStrings("42", num.slice("SELECT 42;"));
    try std.testing.expectEqual(TokenKind.semicolon, lx.next().kind);
    try std.testing.expectEqual(TokenKind.eof, lx.next().kind);
}

test "Lexer: arithmetic" {
    var lx = Lexer.init("1 + 2 * 3");
    try std.testing.expectEqual(TokenKind.integer, lx.next().kind);
    try std.testing.expectEqual(TokenKind.plus, lx.next().kind);
    try std.testing.expectEqual(TokenKind.integer, lx.next().kind);
    try std.testing.expectEqual(TokenKind.star, lx.next().kind);
    try std.testing.expectEqual(TokenKind.integer, lx.next().kind);
    try std.testing.expectEqual(TokenKind.eof, lx.next().kind);
}

test "Lexer: real number" {
    var lx = Lexer.init("3.14 1e10 2.5e-3");
    try std.testing.expectEqual(TokenKind.real, lx.next().kind);
    try std.testing.expectEqual(TokenKind.real, lx.next().kind);
    try std.testing.expectEqual(TokenKind.real, lx.next().kind);
}

test "Lexer: string literal with escaped quote" {
    var lx = Lexer.init("'it''s' 'plain'");
    const a = lx.next();
    try std.testing.expectEqual(TokenKind.string, a.kind);
    try std.testing.expectEqualStrings("'it''s'", a.slice("'it''s' 'plain'"));
    const b = lx.next();
    try std.testing.expectEqual(TokenKind.string, b.kind);
}

test "Lexer: comparison operators" {
    var lx = Lexer.init("= != <> <= >= < >");
    try std.testing.expectEqual(TokenKind.eq, lx.next().kind);
    try std.testing.expectEqual(TokenKind.ne, lx.next().kind);
    try std.testing.expectEqual(TokenKind.ne, lx.next().kind);
    try std.testing.expectEqual(TokenKind.le, lx.next().kind);
    try std.testing.expectEqual(TokenKind.ge, lx.next().kind);
    try std.testing.expectEqual(TokenKind.lt, lx.next().kind);
    try std.testing.expectEqual(TokenKind.gt, lx.next().kind);
}

test "Lexer: concat operator ||" {
    var lx = Lexer.init("'a' || 'b'");
    try std.testing.expectEqual(TokenKind.string, lx.next().kind);
    try std.testing.expectEqual(TokenKind.concat, lx.next().kind);
    try std.testing.expectEqual(TokenKind.string, lx.next().kind);
}

test "Lexer: line comment" {
    var lx = Lexer.init("SELECT -- comment here\n 1");
    try std.testing.expectEqual(TokenKind.keyword_select, lx.next().kind);
    try std.testing.expectEqual(TokenKind.integer, lx.next().kind);
}
