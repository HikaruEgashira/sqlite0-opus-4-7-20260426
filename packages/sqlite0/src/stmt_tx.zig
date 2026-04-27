//! Transaction-control statement parsing (Iter27.E + Iter27.F):
//! BEGIN, COMMIT, ROLLBACK [TO], SAVEPOINT, RELEASE.
//!
//! Returns a `TxControl` union so the engine dispatches a single
//! function for all six forms — savepoint stack manipulation lives
//! alongside flat tx control because the implicit-tx semantics
//! (SAVEPOINT outside BEGIN opens a tx, RELEASE of the outermost
//! implicit savepoint commits it) are inseparable from BEGIN/COMMIT.
//!
//! ## Optional suffixes silently consumed
//!
//!   - `BEGIN [DEFERRED|IMMEDIATE|EXCLUSIVE] [TRANSACTION]`
//!   - `COMMIT [TRANSACTION]`
//!   - `ROLLBACK [TRANSACTION] [TO [SAVEPOINT] name]`
//!   - `SAVEPOINT name`
//!   - `RELEASE [SAVEPOINT] name`
//!
//! The DEFERRED/IMMEDIATE/EXCLUSIVE modifiers control when the
//! database lock is acquired. We hold an exclusive flock for the
//! Database lifetime regardless (Iter25.A), so all three reduce to
//! the same behaviour. Consume them silently.
//!
//! ## Name lifetime
//!
//! The `[]const u8` slice in `.savepoint` / `.release` / `.rollback_to`
//! borrows from `Parser.src` — the raw SQL text. Caller must dupe into
//! `db.allocator` before storing in the savepoint stack.

const std = @import("std");
const ops = @import("ops.zig");
const parser_mod = @import("parser.zig");
const func_util = @import("func_util.zig");

const Parser = parser_mod.Parser;
const Error = ops.Error;

pub const TxControl = union(enum) {
    begin,
    commit,
    rollback,
    savepoint: []const u8,
    release: []const u8,
    rollback_to: []const u8,
};

pub fn parseTxStatement(p: *Parser) Error!TxControl {
    switch (p.cur.kind) {
        .keyword_begin => {
            p.advance();
            // Optional DEFERRED|IMMEDIATE|EXCLUSIVE — single connection
            // model, so they're all the same to us.
            if (p.cur.kind == .identifier) {
                const id = p.cur.slice(p.src);
                if (func_util.eqlIgnoreCase(id, "deferred") or
                    func_util.eqlIgnoreCase(id, "immediate") or
                    func_util.eqlIgnoreCase(id, "exclusive"))
                {
                    p.advance();
                }
            }
            consumeOptionalTransaction(p);
            return .begin;
        },
        .keyword_commit => {
            p.advance();
            consumeOptionalTransaction(p);
            return .commit;
        },
        .keyword_rollback => {
            p.advance();
            consumeOptionalTransaction(p);
            // ROLLBACK [TRANSACTION] [TO [SAVEPOINT] name] — the TO
            // clause turns this into a savepoint operation that retains
            // both the named frame and the surrounding transaction.
            if (p.cur.kind == .keyword_to) {
                p.advance();
                if (p.cur.kind == .keyword_savepoint) p.advance();
                if (p.cur.kind != .identifier) return Error.SyntaxError;
                const name = p.cur.slice(p.src);
                p.advance();
                return .{ .rollback_to = name };
            }
            return .rollback;
        },
        .keyword_savepoint => {
            p.advance();
            if (p.cur.kind != .identifier) return Error.SyntaxError;
            const name = p.cur.slice(p.src);
            p.advance();
            return .{ .savepoint = name };
        },
        .keyword_release => {
            p.advance();
            if (p.cur.kind == .keyword_savepoint) p.advance();
            if (p.cur.kind != .identifier) return Error.SyntaxError;
            const name = p.cur.slice(p.src);
            p.advance();
            return .{ .release = name };
        },
        else => return Error.SyntaxError,
    }
}

fn consumeOptionalTransaction(p: *Parser) void {
    if (p.cur.kind == .identifier and func_util.eqlIgnoreCase(p.cur.slice(p.src), "transaction")) {
        p.advance();
    }
}
