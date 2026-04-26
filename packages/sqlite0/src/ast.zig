//! Expression AST nodes (ADR-0002).
//!
//! Iter8.A introduced `literal` + `binary_arith`. Iter8.B added concat,
//! unary, compare. Iter8.C completed the expression grammar: eq_check,
//! is_check, between, in_list, logical_and/or/not, case_expr, func_call.
//! Iter8.D adds `column_ref` for FROM-clause row binding.
//!
//! Each node owns its children and any heap bytes inside `literal` values;
//! `Expr.deinit` recursively releases everything. `func_call.name` is
//! borrowed from the SQL source string (the parser's `src`), which the
//! caller of `parser.parseExpr` is responsible for keeping alive while the
//! AST exists. That's trivially true today: stmt.zig parses, evaluates,
//! and tears down within a single call.

const std = @import("std");
const value_mod = @import("value.zig");
const ops = @import("ops.zig");

const Value = value_mod.Value;

pub const BinaryOp = enum { add, sub, mul, div, mod };
pub const CompareOp = enum { lt, le, gt, ge };
pub const EqOp = enum { eq, ne };
pub const LikeOp = enum { like, glob };

pub const Expr = union(enum) {
    literal: Value,
    /// Column name borrowed from the SQL source string. Resolved at
    /// `eval` time against `EvalContext.columns` (case-insensitive). See
    /// ADR-0002 §"Iter8.D column_ref": eval-time resolution sidesteps the
    /// SELECT-before-FROM ordering problem since the parser doesn't know
    /// the binding scope when it consumes a SELECT-list identifier.
    column_ref: []const u8,
    binary_arith: BinaryArith,
    binary_concat: BinaryConcat,
    unary_negate: *Expr,
    compare: Compare,
    eq_check: EqCheck,
    is_check: IsCheck,
    between: Between,
    in_list: InList,
    logical_and: LogicalBinary,
    logical_or: LogicalBinary,
    logical_not: *Expr,
    case_expr: CaseExpr,
    func_call: FuncCall,
    /// `value LIKE pattern` / `value GLOB pattern`. The `op` field selects
    /// between SQLite's two pattern-match operators which differ in
    /// case-sensitivity and wildcard syntax. ESCAPE is deferred to Iter13.C.
    like: Like,

    pub const BinaryArith = struct { op: BinaryOp, left: *Expr, right: *Expr };
    pub const BinaryConcat = struct { left: *Expr, right: *Expr };
    pub const Compare = struct { op: CompareOp, left: *Expr, right: *Expr };
    pub const EqCheck = struct { op: EqOp, left: *Expr, right: *Expr };
    /// `IS [NOT] [DISTINCT FROM]` collapsed to one bit. The parse-time
    /// transformation is `negated = has_not XOR has_distinct`:
    ///   IS                         → negated=false (return identical)
    ///   IS NOT                     → negated=true
    ///   IS DISTINCT FROM           → negated=true
    ///   IS NOT DISTINCT FROM       → negated=false
    pub const IsCheck = struct { left: *Expr, right: *Expr, negated: bool };
    pub const Between = struct { value: *Expr, lo: *Expr, hi: *Expr, negated: bool };
    pub const InList = struct { value: *Expr, items: []*Expr, negated: bool };
    pub const LogicalBinary = struct { left: *Expr, right: *Expr };
    pub const CaseBranch = struct { when: *Expr, then: *Expr };
    pub const CaseExpr = struct {
        scrutinee: ?*Expr,
        branches: []CaseBranch,
        else_branch: ?*Expr,
    };
    pub const FuncCall = struct { name: []const u8, args: []*Expr };
    pub const Like = struct { op: LikeOp, value: *Expr, pattern: *Expr, negated: bool };

    pub fn deinit(self: *Expr, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .literal => |v| ops.freeValue(allocator, v),
            .column_ref => {}, // name borrowed from src; nothing to free
            .binary_arith => |b| {
                b.left.deinit(allocator);
                b.right.deinit(allocator);
            },
            .binary_concat => |b| {
                b.left.deinit(allocator);
                b.right.deinit(allocator);
            },
            .unary_negate => |inner| inner.deinit(allocator),
            .compare => |c| {
                c.left.deinit(allocator);
                c.right.deinit(allocator);
            },
            .eq_check => |e| {
                e.left.deinit(allocator);
                e.right.deinit(allocator);
            },
            .is_check => |e| {
                e.left.deinit(allocator);
                e.right.deinit(allocator);
            },
            .between => |b| {
                b.value.deinit(allocator);
                b.lo.deinit(allocator);
                b.hi.deinit(allocator);
            },
            .in_list => |il| {
                il.value.deinit(allocator);
                for (il.items) |item| item.deinit(allocator);
                allocator.free(il.items);
            },
            .logical_and, .logical_or => |b| {
                b.left.deinit(allocator);
                b.right.deinit(allocator);
            },
            .logical_not => |inner| inner.deinit(allocator),
            .case_expr => |ce| {
                if (ce.scrutinee) |s| s.deinit(allocator);
                for (ce.branches) |b| {
                    b.when.deinit(allocator);
                    b.then.deinit(allocator);
                }
                allocator.free(ce.branches);
                if (ce.else_branch) |eb| eb.deinit(allocator);
            },
            .func_call => |fc| {
                for (fc.args) |arg| arg.deinit(allocator);
                allocator.free(fc.args);
                // fc.name is borrowed from src; nothing to free.
            },
            .like => |l| {
                l.value.deinit(allocator);
                l.pattern.deinit(allocator);
            },
        }
        allocator.destroy(self);
    }
};

pub fn makeLiteral(allocator: std.mem.Allocator, v: Value) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .literal = v };
    return node;
}

pub fn makeColumnRef(allocator: std.mem.Allocator, name: []const u8) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .column_ref = name };
    return node;
}

pub fn makeBinaryArith(allocator: std.mem.Allocator, op: BinaryOp, left: *Expr, right: *Expr) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .binary_arith = .{ .op = op, .left = left, .right = right } };
    return node;
}

pub fn makeBinaryConcat(allocator: std.mem.Allocator, left: *Expr, right: *Expr) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .binary_concat = .{ .left = left, .right = right } };
    return node;
}

pub fn makeUnaryNegate(allocator: std.mem.Allocator, operand: *Expr) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .unary_negate = operand };
    return node;
}

pub fn makeCompare(allocator: std.mem.Allocator, op: CompareOp, left: *Expr, right: *Expr) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .compare = .{ .op = op, .left = left, .right = right } };
    return node;
}

pub fn makeEqCheck(allocator: std.mem.Allocator, op: EqOp, left: *Expr, right: *Expr) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .eq_check = .{ .op = op, .left = left, .right = right } };
    return node;
}

pub fn makeIsCheck(allocator: std.mem.Allocator, left: *Expr, right: *Expr, negated: bool) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .is_check = .{ .left = left, .right = right, .negated = negated } };
    return node;
}

pub fn makeBetween(allocator: std.mem.Allocator, value: *Expr, lo: *Expr, hi: *Expr, negated: bool) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .between = .{ .value = value, .lo = lo, .hi = hi, .negated = negated } };
    return node;
}

pub fn makeInList(allocator: std.mem.Allocator, value: *Expr, items: []*Expr, negated: bool) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .in_list = .{ .value = value, .items = items, .negated = negated } };
    return node;
}

pub fn makeLogicalAnd(allocator: std.mem.Allocator, left: *Expr, right: *Expr) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .logical_and = .{ .left = left, .right = right } };
    return node;
}

pub fn makeLogicalOr(allocator: std.mem.Allocator, left: *Expr, right: *Expr) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .logical_or = .{ .left = left, .right = right } };
    return node;
}

pub fn makeLogicalNot(allocator: std.mem.Allocator, inner: *Expr) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .logical_not = inner };
    return node;
}

pub fn makeCaseExpr(
    allocator: std.mem.Allocator,
    scrutinee: ?*Expr,
    branches: []Expr.CaseBranch,
    else_branch: ?*Expr,
) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .case_expr = .{ .scrutinee = scrutinee, .branches = branches, .else_branch = else_branch } };
    return node;
}

pub fn makeFuncCall(allocator: std.mem.Allocator, name: []const u8, args: []*Expr) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .func_call = .{ .name = name, .args = args } };
    return node;
}

pub fn makeLike(allocator: std.mem.Allocator, op: LikeOp, value: *Expr, pattern: *Expr, negated: bool) !*Expr {
    const node = try allocator.create(Expr);
    node.* = .{ .like = .{ .op = op, .value = value, .pattern = pattern, .negated = negated } };
    return node;
}

test "ast: literal node round-trips a value" {
    const allocator = std.testing.allocator;
    const node = try makeLiteral(allocator, Value{ .integer = 42 });
    defer node.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 42), node.literal.integer);
}

test "ast: binary_arith deinit frees children" {
    const allocator = std.testing.allocator;
    const left = try makeLiteral(allocator, Value{ .integer = 1 });
    const right = try makeLiteral(allocator, Value{ .integer = 2 });
    const node = try makeBinaryArith(allocator, .add, left, right);
    defer node.deinit(allocator);
    try std.testing.expectEqual(BinaryOp.add, node.binary_arith.op);
}

test "ast: in_list owns item slice and frees on deinit" {
    const allocator = std.testing.allocator;
    const value = try makeLiteral(allocator, Value{ .integer = 1 });
    const items = try allocator.alloc(*Expr, 2);
    items[0] = try makeLiteral(allocator, Value{ .integer = 2 });
    items[1] = try makeLiteral(allocator, Value{ .integer = 3 });
    const node = try makeInList(allocator, value, items, false);
    node.deinit(allocator); // no leak — testing allocator panics if items leak
}

test "ast: case_expr with branches and else" {
    const allocator = std.testing.allocator;
    const scrutinee = try makeLiteral(allocator, Value{ .integer = 1 });
    const branches = try allocator.alloc(Expr.CaseBranch, 1);
    branches[0] = .{
        .when = try makeLiteral(allocator, Value{ .integer = 1 }),
        .then = try makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "hit") }),
    };
    const else_branch = try makeLiteral(allocator, Value{ .text = try allocator.dupe(u8, "miss") });
    const node = try makeCaseExpr(allocator, scrutinee, branches, else_branch);
    node.deinit(allocator);
}

test "ast: func_call owns args slice" {
    const allocator = std.testing.allocator;
    const args = try allocator.alloc(*Expr, 1);
    args[0] = try makeLiteral(allocator, Value{ .integer = 7 });
    const node = try makeFuncCall(allocator, "abs", args);
    node.deinit(allocator);
}
