//! Transaction-control statement parsing (Iter27.E): BEGIN, COMMIT,
//! ROLLBACK. Tiny dedicated module — same shape as `stmt_pragma.zig`,
//! kept separate so `stmt.zig` doesn't grow another small parser.
//!
//! Iter27.E only supports flat single-level transactions. SAVEPOINT /
//! RELEASE / nested ROLLBACK TO are deferred — they need a savepoint
//! frame stack that the engine doesn't yet have.
//!
//! ## Optional suffixes (silently consumed)
//!
//! sqlite3 allows:
//!   - `BEGIN [DEFERRED|IMMEDIATE|EXCLUSIVE] [TRANSACTION]`
//!   - `COMMIT [TRANSACTION]`
//!   - `ROLLBACK [TRANSACTION]`
//!
//! The DEFERRED/IMMEDIATE/EXCLUSIVE modifiers control when the
//! database lock is acquired. We hold an exclusive flock for the
//! Database lifetime regardless (Iter25.A), so all three reduce to
//! the same behaviour. Consume them silently rather than rejecting.

const std = @import("std");
const ops = @import("ops.zig");
const parser_mod = @import("parser.zig");
const func_util = @import("func_util.zig");

const Parser = parser_mod.Parser;
const Error = ops.Error;

pub const TransactionKind = enum { begin, commit, rollback };

pub fn parseTransactionStatement(p: *Parser) Error!TransactionKind {
    const kind: TransactionKind = switch (p.cur.kind) {
        .keyword_begin => .begin,
        .keyword_commit => .commit,
        .keyword_rollback => .rollback,
        else => return Error.SyntaxError,
    };
    p.advance();

    // BEGIN [DEFERRED|IMMEDIATE|EXCLUSIVE]
    if (kind == .begin and p.cur.kind == .identifier) {
        const id = p.cur.slice(p.src);
        if (func_util.eqlIgnoreCase(id, "deferred") or
            func_util.eqlIgnoreCase(id, "immediate") or
            func_util.eqlIgnoreCase(id, "exclusive"))
        {
            p.advance();
        }
    }

    // [TRANSACTION] suffix common to all three.
    if (p.cur.kind == .identifier and func_util.eqlIgnoreCase(p.cur.slice(p.src), "transaction")) {
        p.advance();
    }
    return kind;
}
