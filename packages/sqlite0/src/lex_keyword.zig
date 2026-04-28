//! SQL keyword → TokenKind mapping.
//!
//! Split out of `lex.zig` to keep that file under the 500-line discipline
//! before the next lexer-touching iteration (hex integer literals). The
//! table is pure data: a sequence of case-insensitive equality probes.
//! `lex.identifier()` calls `keywordKind(text)` after slicing the
//! identifier span; if the result is `null` the token stays `.identifier`.

const lex = @import("lex.zig");
const func_util = @import("func_util.zig");

const TokenKind = lex.TokenKind;

pub fn keywordKind(text: []const u8) ?TokenKind {
    const eq = func_util.eqlIgnoreCase;
    if (eq(text, "select")) return .keyword_select;
    if (eq(text, "from")) return .keyword_from;
    if (eq(text, "where")) return .keyword_where;
    if (eq(text, "null")) return .keyword_null;
    if (eq(text, "true")) return .keyword_true;
    if (eq(text, "false")) return .keyword_false;
    if (eq(text, "and")) return .keyword_and;
    if (eq(text, "or")) return .keyword_or;
    if (eq(text, "not")) return .keyword_not;
    if (eq(text, "is")) return .keyword_is;
    if (eq(text, "between")) return .keyword_between;
    if (eq(text, "in")) return .keyword_in;
    if (eq(text, "distinct")) return .keyword_distinct;
    if (eq(text, "case")) return .keyword_case;
    if (eq(text, "when")) return .keyword_when;
    if (eq(text, "then")) return .keyword_then;
    if (eq(text, "else")) return .keyword_else;
    if (eq(text, "end")) return .keyword_end;
    if (eq(text, "values")) return .keyword_values;
    if (eq(text, "as")) return .keyword_as;
    if (eq(text, "create")) return .keyword_create;
    if (eq(text, "table")) return .keyword_table;
    if (eq(text, "insert")) return .keyword_insert;
    if (eq(text, "into")) return .keyword_into;
    if (eq(text, "like")) return .keyword_like;
    if (eq(text, "glob")) return .keyword_glob;
    if (eq(text, "escape")) return .keyword_escape;
    if (eq(text, "order")) return .keyword_order;
    if (eq(text, "by")) return .keyword_by;
    if (eq(text, "asc")) return .keyword_asc;
    if (eq(text, "desc")) return .keyword_desc;
    if (eq(text, "limit")) return .keyword_limit;
    if (eq(text, "offset")) return .keyword_offset;
    if (eq(text, "delete")) return .keyword_delete;
    if (eq(text, "update")) return .keyword_update;
    if (eq(text, "set")) return .keyword_set;
    if (eq(text, "group")) return .keyword_group;
    if (eq(text, "having")) return .keyword_having;
    if (eq(text, "join")) return .keyword_join;
    if (eq(text, "inner")) return .keyword_inner;
    if (eq(text, "cross")) return .keyword_cross;
    if (eq(text, "left")) return .keyword_left;
    if (eq(text, "outer")) return .keyword_outer;
    if (eq(text, "on")) return .keyword_on;
    if (eq(text, "union")) return .keyword_union;
    if (eq(text, "all")) return .keyword_all;
    if (eq(text, "intersect")) return .keyword_intersect;
    if (eq(text, "except")) return .keyword_except;
    if (eq(text, "exists")) return .keyword_exists;
    if (eq(text, "cast")) return .keyword_cast;
    if (eq(text, "pragma")) return .keyword_pragma;
    if (eq(text, "begin")) return .keyword_begin;
    if (eq(text, "commit")) return .keyword_commit;
    if (eq(text, "rollback")) return .keyword_rollback;
    if (eq(text, "savepoint")) return .keyword_savepoint;
    if (eq(text, "release")) return .keyword_release;
    if (eq(text, "to")) return .keyword_to;
    if (eq(text, "collate")) return .keyword_collate;
    if (eq(text, "with")) return .keyword_with;
    if (eq(text, "returning")) return .keyword_returning;
    return null;
}
