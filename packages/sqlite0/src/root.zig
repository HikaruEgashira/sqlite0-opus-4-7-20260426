const std = @import("std");

pub const value = @import("value.zig");
pub const lex = @import("lex.zig");
pub const ops = @import("ops.zig");
pub const func_util = @import("func_util.zig");
pub const funcs = @import("funcs.zig");
pub const funcs_text = @import("funcs_text.zig");
pub const parser = @import("parser.zig");
pub const stmt = @import("stmt.zig");
pub const exec = @import("exec.zig");

pub const Value = value.Value;
pub const Result = exec.Result;
pub const execute = exec.execute;

pub const version = "0.0.0";

test {
    std.testing.refAllDecls(@This());
}
