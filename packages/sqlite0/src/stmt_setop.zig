//! Set-operator chain parsing for SELECT (Iter20). The actual row-combining
//! logic lives in `engine_setop.zig`; this module only owns the parser-level
//! types (`SetopKind`, `SetopBranch`) and the small recogniser that turns
//! the next few tokens into a kind enum.
//!
//! Split out of `stmt.zig` proactively to keep that file under the 500-line
//! discipline (CLAUDE.md "Module Splitting Rules") before subsequent
//! iterations grow the SELECT parser further.
//!
//! The branch-loop driver itself (`parseSetopBranches`) stays in `stmt.zig`
//! because it has to recurse into `parseSelectInner` — a private helper of
//! the SELECT parser. Moving it would either require exporting that helper
//! or threading a function pointer; neither pays for itself for ~15 lines.

const std = @import("std");
const lex = @import("lex.zig");
const stmt_mod = @import("stmt.zig");
const parser_mod = @import("parser.zig");

const Parser = parser_mod.Parser;

/// Set-operator chained against the preceding SELECT. `union_distinct`
/// applies dedup-replace-last; `union_all` is plain concatenation;
/// `intersect` keeps left rows whose key appears in right (then dedup);
/// `except` keeps left rows whose key does NOT appear in right (then dedup).
pub const SetopKind = enum { union_all, union_distinct, intersect, except };

pub const SetopBranch = struct {
    kind: SetopKind,
    /// Inner select. Always parsed via the same parseSelectStatement
    /// machinery, but with a recursion guard (`allow_post = false`) that
    /// rejects ORDER BY/LIMIT/OFFSET inside the branch — those bind to the
    /// outer chain in sqlite3. As a result, `branches`, `order_by`, `limit`,
    /// and `offset` on this struct are always empty.
    select: stmt_mod.ParsedSelect,
};

/// Recognise the next setop keyword and advance past it. Returns null when
/// the cursor sits on something else, signalling end of chain. `UNION ALL`
/// is two tokens; the others are single-keyword.
pub fn matchSetopKind(p: *Parser) ?SetopKind {
    switch (p.cur.kind) {
        .keyword_union => {
            p.advance();
            if (p.cur.kind == .keyword_all) {
                p.advance();
                return .union_all;
            }
            return .union_distinct;
        },
        .keyword_intersect => {
            p.advance();
            return .intersect;
        },
        .keyword_except => {
            p.advance();
            return .except;
        },
        else => return null,
    }
}

/// Free a branch list and the ParsedSelect inside each branch. Calls back
/// through `stmt.freeParsedSelectFields` for per-branch teardown.
pub fn freeSetopBranches(allocator: std.mem.Allocator, list: []SetopBranch) void {
    for (list) |b| stmt_mod.freeParsedSelectFields(allocator, b.select);
    allocator.free(list);
}
