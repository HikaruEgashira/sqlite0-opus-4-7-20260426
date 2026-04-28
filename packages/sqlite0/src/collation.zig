//! Collation comparators (Iter31.O).
//!
//! sqlite3 ships three built-in collating sequences:
//!   - BINARY: bytewise (the default)
//!   - NOCASE: ASCII case-fold both sides, then bytewise (only A-Z↔a-z;
//!     non-ASCII bytes pass through unchanged — sqlite3 does NOT
//!     Unicode-fold by default)
//!   - RTRIM: strip trailing 0x20 SPACE bytes from both sides, then
//!     bytewise. Other whitespace (tab/newline) is NOT stripped.
//!
//! These functions only fire when both operands are TEXT — non-TEXT
//! comparisons keep the storage-class precedence (NULL < numeric <
//! TEXT < BLOB) defined in `ops.compareValues`. Lives in its own
//! module to keep `ops.zig` under the 500-line discipline (it's at
//! 498 and can't grow).
//!
//! `applyComparisonCollated` / `applyEqualityCollated` / `applyInCollated`
//! mirror the BINARY-only ops in `ops.zig` but route TEXT pairs through
//! `compareTextCollated`. Other paths (eval call sites that pass
//! `.binary`) get back the same answer as the original ops, so existing
//! tests stay green.

const std = @import("std");
const lex = @import("lex.zig");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");
const ast = @import("ast.zig");

const Value = value_mod.Value;
const TokenKind = lex.TokenKind;
const CollationKind = ast.CollationKind;
const Order = ops.Order;

/// Map a parsed identifier (e.g. `NOCASE`) to its CollationKind. Case-
/// insensitive — sqlite3 accepts `nocase`, `NoCase`, etc. Returns null
/// for anything not in the built-in set; the parser surfaces this as
/// SyntaxError ("no such collation sequence: BOGUS" in sqlite3).
pub fn kindFromName(name: []const u8) ?CollationKind {
    const eq = std.ascii.eqlIgnoreCase;
    if (eq(name, "binary")) return .binary;
    if (eq(name, "nocase")) return .nocase;
    if (eq(name, "rtrim")) return .rtrim;
    return null;
}

/// Peek the outermost COLLATE wrapper on `expr`. Returns null if the
/// top node is not `.collate`. Does NOT recurse — chained
/// `a COLLATE X COLLATE Y` produces a nested wrapper whose outermost
/// kind (Y) is what sqlite3 honors per the "outer wins" probe:
/// `('A' COLLATE BINARY) COLLATE NOCASE = 'a'` → 1.
pub fn peekKind(expr: *const ast.Expr) ?CollationKind {
    return switch (expr.*) {
        .collate => |c| c.kind,
        else => null,
    };
}

/// Peel any `Collate(...)` wrapper(s) off `expr`, returning the outermost
/// kind plus the innermost non-collate node. Outermost wins per sqlite3's
/// chained-COLLATE rule (`('A' COLLATE BINARY) COLLATE NOCASE = 'a'` → 1).
/// Returns `.binary` and `expr` itself when there is no wrapper.
pub const Peeled = struct { kind: CollationKind, inner: *const ast.Expr };

pub fn peel(expr: *const ast.Expr) Peeled {
    var inner = expr;
    var kind: CollationKind = .binary;
    var saw = false;
    while (inner.* == .collate) {
        if (!saw) {
            kind = inner.collate.kind;
            saw = true;
        }
        inner = inner.collate.value;
    }
    return .{ .kind = kind, .inner = inner };
}

/// Pick the collation that drives a binary comparison. sqlite3's rule:
/// LHS-collated wins; if LHS has none, RHS-collated wins; otherwise
/// BINARY (verified `'a' COLLATE BINARY = 'A' COLLATE NOCASE` → 0,
/// `'A' COLLATE NOCASE = 'a' COLLATE BINARY` → 1).
pub fn pick(left: *const ast.Expr, right: *const ast.Expr) CollationKind {
    if (peekKind(left)) |k| return k;
    if (peekKind(right)) |k| return k;
    return .binary;
}

/// 3-way text compare under `kind`. NOCASE folds ASCII letters only,
/// then byte-compares; RTRIM strips trailing spaces, then byte-compares.
/// Empty / equal-after-fold / equal-after-trim collapses to `.eq`.
pub fn compareTextCollated(a: []const u8, b: []const u8, kind: CollationKind) Order {
    return switch (kind) {
        .binary => orderBytes(a, b),
        .nocase => orderNoCase(a, b),
        .rtrim => orderBytes(rtrim(a), rtrim(b)),
    };
}

fn orderBytes(a: []const u8, b: []const u8) Order {
    return switch (std.mem.order(u8, a, b)) {
        .lt => .lt,
        .eq => .eq,
        .gt => .gt,
    };
}

fn orderNoCase(a: []const u8, b: []const u8) Order {
    const n = @min(a.len, b.len);
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const al = std.ascii.toLower(a[i]);
        const bl = std.ascii.toLower(b[i]);
        if (al < bl) return .lt;
        if (al > bl) return .gt;
    }
    if (a.len < b.len) return .lt;
    if (a.len > b.len) return .gt;
    return .eq;
}

fn rtrim(s: []const u8) []const u8 {
    var n = s.len;
    while (n > 0 and s[n - 1] == ' ') : (n -= 1) {}
    return s[0..n];
}

/// 3-way value compare under `kind`. Mirrors `ops.compareValues` but
/// routes the TEXT-vs-TEXT branch through `compareTextCollated`. Other
/// class pairings (numeric/blob/cross-class) ignore collation —
/// `1 COLLATE NOCASE = 1` returns 1 regardless because both sides are
/// numeric. Caller must guarantee neither value is NULL (NULL handling
/// stays at the `applyComparison` / `applyEquality` layer).
pub fn compareValuesCollated(a: Value, b: Value, kind: CollationKind) Order {
    if (a == .text and b == .text) {
        return compareTextCollated(a.text, b.text, kind);
    }
    return ops.compareValues(a, b);
}

/// Collated `<`/`<=`/`>`/`>=`. NULL on either side propagates NULL,
/// matching the BINARY form in `ops.applyComparison`.
pub fn applyComparisonCollated(op: TokenKind, lhs: Value, rhs: Value, kind: CollationKind) Value {
    if (lhs == .null or rhs == .null) return Value.null;
    const order = compareValuesCollated(lhs, rhs, kind);
    return ops.boolValue(switch (op) {
        .lt => order == .lt,
        .le => order != .gt,
        .gt => order == .gt,
        .ge => order != .lt,
        else => unreachable,
    });
}

/// Collated `=` / `<>`. Same NULL propagation as `ops.applyEquality`.
pub fn applyEqualityCollated(op: TokenKind, lhs: Value, rhs: Value, kind: CollationKind) Value {
    if (lhs == .null or rhs == .null) return Value.null;
    const order = compareValuesCollated(lhs, rhs, kind);
    const equal = order == .eq;
    return ops.boolValue(switch (op) {
        .eq => equal,
        .ne => !equal,
        else => unreachable,
    });
}

/// Collated `IS` (sqlite3 `'A' COLLATE NOCASE IS 'a'` → 1). NULL
/// matching follows `ops.identicalValues`: `NULL IS NULL` true,
/// `NULL IS x` false. Collation only affects the non-NULL TEXT branch.
pub fn identicalValuesCollated(a: Value, b: Value, kind: CollationKind) bool {
    const a_kind: std.meta.Tag(Value) = a;
    const b_kind: std.meta.Tag(Value) = b;
    if (a_kind == .null or b_kind == .null) return a_kind == b_kind;
    return compareValuesCollated(a, b, kind) == .eq;
}

/// Collated `IN (...)` — same three-valued logic as `ops.applyIn` but
/// member equality goes through `applyEqualityCollated`.
pub fn applyInCollated(left: Value, list: []const Value, kind: CollationKind) Value {
    if (list.len == 0) return ops.boolValue(false);
    if (left == .null) return Value.null;
    var saw_null = false;
    for (list) |item| {
        const eq = applyEqualityCollated(.eq, left, item, kind);
        switch (eq) {
            .integer => |i| if (i == 1) return ops.boolValue(true),
            .null => saw_null = true,
            else => unreachable,
        }
    }
    if (saw_null) return Value.null;
    return ops.boolValue(false);
}

test "collation: NOCASE folds ASCII letters" {
    try std.testing.expectEqual(Order.eq, compareTextCollated("Abc", "abc", .nocase));
    try std.testing.expectEqual(Order.lt, compareTextCollated("abc", "abd", .nocase));
    try std.testing.expectEqual(Order.gt, compareTextCollated("ABC", "abb", .nocase));
}

test "collation: RTRIM strips trailing spaces only" {
    try std.testing.expectEqual(Order.eq, compareTextCollated("a ", "a", .rtrim));
    try std.testing.expectEqual(Order.eq, compareTextCollated("a   ", "a   ", .rtrim));
    try std.testing.expectEqual(Order.lt, compareTextCollated("a", "a b", .rtrim));
    // Non-space whitespace is NOT trimmed.
    try std.testing.expectEqual(Order.gt, compareTextCollated("a\t", "a", .rtrim));
}

test "collation: numeric pair ignores collation" {
    const a = Value{ .integer = 1 };
    const b = Value{ .integer = 1 };
    try std.testing.expectEqual(Order.eq, compareValuesCollated(a, b, .nocase));
}

test "collation: kindFromName accepts case variants" {
    try std.testing.expectEqual(CollationKind.nocase, kindFromName("NOCASE").?);
    try std.testing.expectEqual(CollationKind.nocase, kindFromName("NoCase").?);
    try std.testing.expectEqual(CollationKind.binary, kindFromName("binary").?);
    try std.testing.expectEqual(CollationKind.rtrim, kindFromName("RTRIM").?);
    try std.testing.expectEqual(@as(?CollationKind, null), kindFromName("BOGUS"));
}
