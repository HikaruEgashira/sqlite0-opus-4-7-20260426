const std = @import("std");

pub const value = @import("value.zig");
pub const lex = @import("lex.zig");
pub const exec = @import("exec.zig");

pub const Value = value.Value;
pub const Result = exec.Result;
pub const execute = exec.execute;

pub const version = "0.0.0";

test {
    std.testing.refAllDecls(@This());
}
