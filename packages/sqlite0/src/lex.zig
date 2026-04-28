const std = @import("std");
const lex_keyword = @import("lex_keyword.zig");

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
    keyword_like,
    keyword_glob,
    keyword_escape,
    keyword_order,
    keyword_by,
    keyword_asc,
    keyword_desc,
    keyword_limit,
    keyword_offset,
    keyword_delete,
    keyword_update,
    keyword_set,
    keyword_group,
    keyword_having,
    keyword_join,
    keyword_inner,
    keyword_cross,
    keyword_left,
    keyword_outer,
    keyword_on,
    keyword_union,
    keyword_all,
    keyword_intersect,
    keyword_except,
    keyword_exists,
    keyword_cast,
    keyword_pragma,
    keyword_begin,
    keyword_commit,
    keyword_rollback,
    keyword_savepoint,
    keyword_release,
    keyword_to,
    keyword_collate,
    keyword_with,
    keyword_returning,
    plus,
    minus,
    star,
    slash,
    percent,
    lparen,
    rparen,
    comma,
    dot,
    semicolon,
    eq,
    ne,
    lt,
    le,
    gt,
    ge,
    concat,
    bit_and,
    bit_or,
    bit_not,
    shift_left,
    shift_right,
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
            '.' => {
                // `.` is a number prefix when followed by a digit (`.5` →
                // 0.5); otherwise it's the qualified-name separator.
                if (self.pos + 1 < self.src.len and std.ascii.isDigit(self.src[self.pos + 1])) {
                    return self.number(start);
                }
                return self.single(.dot);
            },
            ';' => return self.single(.semicolon),
            '=' => {
                // sqlite3 accepts both `=` and `==` as the equality operator.
                self.pos += 1;
                if (self.peek()) |next_c| {
                    if (next_c == '=') self.pos += 1;
                }
                return .{ .kind = .eq, .start = start, .end = self.pos };
            },
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
                    if (next_c == '<') {
                        self.pos += 1;
                        return .{ .kind = .shift_left, .start = start, .end = self.pos };
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
                    if (next_c == '>') {
                        self.pos += 1;
                        return .{ .kind = .shift_right, .start = start, .end = self.pos };
                    }
                }
                return .{ .kind = .gt, .start = start, .end = self.pos };
            },
            '&' => return self.single(.bit_and),
            '~' => return self.single(.bit_not),
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
                return .{ .kind = .bit_or, .start = start, .end = self.pos };
            },
            '\'' => return self.string(start),
            '0'...'9' => return self.number(start),
            'x', 'X' => {
                // sqlite3 blob literal: `x'AABB'` or `X'AABB'` — only when
                // the `x`/`X` is immediately followed by `'`. Otherwise
                // fall through to the identifier path so `x` (single-char
                // column name) still parses.
                if (self.pos + 1 < self.src.len and self.src[self.pos + 1] == '\'') {
                    return self.blobLit(start);
                }
                return self.identifier(start);
            },
            'a'...'w', 'y', 'z', 'A'...'W', 'Y', 'Z', '_' => return self.identifier(start),
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
        // Hex prefix: `0x` or `0X` followed by `[0-9a-fA-F]+`. Once the
        // prefix is consumed sqlite3 commits to hex mode — missing
        // digits or a trailing alphanumeric char surface as `.invalid`,
        // not as `0` + identifier (matches sqlite3's "unrecognized
        // token: 0x..." parse error).
        if (self.src[self.pos] == '0' and self.pos + 1 < self.src.len) {
            const next_c = self.src[self.pos + 1];
            if (next_c == 'x' or next_c == 'X') return self.hexNumber(start);
        }
        var saw_dot = false;
        var saw_exp = false;
        // `prev_digit` gates two things: (1) the `_` digit-separator —
        // sqlite3 accepts `_` only between two decimal digits, never
        // adjacent to `.`/`e`/sign or at start/end; (2) the `e`
        // introducer — `1e10` is real but `e10` alone is an identifier
        // and `.e5` is rejected.
        var prev_digit = false;
        while (self.pos < self.src.len) {
            const c = self.src[self.pos];
            switch (c) {
                '0'...'9' => {
                    self.pos += 1;
                    prev_digit = true;
                },
                '_' => {
                    if (!prev_digit) break;
                    if (self.pos + 1 >= self.src.len) break;
                    const n = self.src[self.pos + 1];
                    if (n < '0' or n > '9') break;
                    self.pos += 1;
                    prev_digit = false;
                },
                '.' => {
                    if (saw_dot or saw_exp) break;
                    saw_dot = true;
                    self.pos += 1;
                    prev_digit = false;
                },
                'e', 'E' => {
                    if (saw_exp) break;
                    saw_exp = true;
                    self.pos += 1;
                    if (self.pos < self.src.len and (self.src[self.pos] == '+' or self.src[self.pos] == '-')) {
                        self.pos += 1;
                    }
                    prev_digit = false;
                },
                else => break,
            }
        }
        // Trailing identifier-like char (incl. `_`) on a numeric token
        // means the user wrote a malformed literal (`1_`, `1__0`,
        // `1e10g`, `1.5x`); sqlite3 emits one `.invalid` token spanning
        // the whole bad span rather than `1` + `_000_000` (which would
        // alias-parse silently in `SELECT 1_000_000`).
        if (self.pos < self.src.len) {
            const c = self.src[self.pos];
            if (isIdentTail(c)) {
                while (self.pos < self.src.len and isIdentTail(self.src[self.pos])) self.pos += 1;
                return .{ .kind = .invalid, .start = start, .end = self.pos };
            }
        }
        // Exponent introduced but no exponent digit followed (`1e`,
        // `1e+`). sqlite3 rejects.
        if (saw_exp and !prev_digit) {
            return .{ .kind = .invalid, .start = start, .end = self.pos };
        }
        const kind: TokenKind = if (saw_dot or saw_exp) .real else .integer;
        return .{ .kind = kind, .start = start, .end = self.pos };
    }

    /// Hex integer literal `0x<hex>` / `0X<hex>`. Caller has verified
    /// `src[pos] == '0'` and `src[pos+1]` is `x`/`X`. The hex span
    /// accepts a single `_` as a digit separator (`0xff_ff`), but only
    /// between two hex digits — never directly after `0x` or trailing.
    /// Empty digits or any leftover identifier-like tail surface as one
    /// `.invalid` token (matches sqlite3 3.51.0 "unrecognized token").
    fn hexNumber(self: *Lexer, start: u32) Token {
        self.pos += 2; // skip `0x` / `0X`
        const digits_start = self.pos;
        var prev_digit = false;
        while (self.pos < self.src.len) {
            const c = self.src[self.pos];
            if (isHexDigit(c)) {
                self.pos += 1;
                prev_digit = true;
                continue;
            }
            if (c == '_' and prev_digit) {
                if (self.pos + 1 >= self.src.len) break;
                if (!isHexDigit(self.src[self.pos + 1])) break;
                self.pos += 1;
                prev_digit = false;
                continue;
            }
            break;
        }
        if (self.pos == digits_start) {
            // `0x` with no hex digits. Consume the identifier-like tail
            // so re-lex emits a single `.invalid` span.
            while (self.pos < self.src.len and isIdentTail(self.src[self.pos])) self.pos += 1;
            return .{ .kind = .invalid, .start = start, .end = self.pos };
        }
        // Trailing identifier-like char on a valid hex span: malformed
        // (`0x10g`, `0xff_`, `0xff__ff`). Sqlite3 emits one bad token.
        if (self.pos < self.src.len and isIdentTail(self.src[self.pos])) {
            while (self.pos < self.src.len and isIdentTail(self.src[self.pos])) self.pos += 1;
            return .{ .kind = .invalid, .start = start, .end = self.pos };
        }
        return .{ .kind = .integer, .start = start, .end = self.pos };
    }

    /// `x'<hex>'` blob literal. Caller has already verified that
    /// `src[pos]` is `x` or `X` and `src[pos+1]` is `'`. The hex span
    /// must be even-length and contain only `0-9 a-f A-F`; otherwise
    /// emit `.invalid` so the parser can surface a SyntaxError instead
    /// of silently lexing as identifier+string. sqlite3 rejects the
    /// same shapes (`x'A'`, `x'GG'`, unterminated `x'AB`) at prepare
    /// time.
    fn blobLit(self: *Lexer, start: u32) Token {
        // Skip the `x` and the opening `'`.
        self.pos += 2;
        const hex_start = self.pos;
        while (self.pos < self.src.len and self.src[self.pos] != '\'') : (self.pos += 1) {
            const c = self.src[self.pos];
            const valid = (c >= '0' and c <= '9') or (c >= 'a' and c <= 'f') or (c >= 'A' and c <= 'F');
            if (!valid) return .{ .kind = .invalid, .start = start, .end = self.pos };
        }
        // Reached end without `'` → unterminated.
        if (self.pos >= self.src.len) return .{ .kind = .invalid, .start = start, .end = self.pos };
        const hex_len = self.pos - hex_start;
        // Hex span must be even (each pair → one byte). sqlite3 rejects
        // odd-length blob literals.
        if (hex_len % 2 != 0) {
            self.pos += 1; // consume the closing `'` so re-lex doesn't loop
            return .{ .kind = .invalid, .start = start, .end = self.pos };
        }
        self.pos += 1; // consume closing `'`
        return .{ .kind = .blob_lit, .start = start, .end = self.pos };
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
        const kind = lex_keyword.keywordKind(text) orelse TokenKind.identifier;
        return .{ .kind = kind, .start = start, .end = self.pos };
    }
};

fn isHexDigit(c: u8) bool {
    return (c >= '0' and c <= '9') or (c >= 'a' and c <= 'f') or (c >= 'A' and c <= 'F');
}

/// Identifier-tail char class — alphanumeric or `_`. Used by the
/// numeric lexers to detect malformed-literal trailing tails like
/// `1_`, `1e10g`, `0xff_`.
fn isIdentTail(c: u8) bool {
    return (c >= '0' and c <= '9') or (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or c == '_';
}

