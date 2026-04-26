const std = @import("std");
const builtin = @import("builtin");

pub fn build(b: *std.Build) void {
    const default_query: std.Target.Query = if (builtin.target.os.tag == .macos)
        .{ .os_version_min = .{ .semver = .{ .major = 14, .minor = 0, .patch = 0 } } }
    else
        .{};
    const target = b.standardTargetOptions(.{ .default_target = default_query });
    const optimize = b.standardOptimizeOption(.{});

    const sqlite0_mod = b.addModule("sqlite0", .{
        .root_source_file = b.path("packages/sqlite0/src/root.zig"),
        .target = target,
    });

    const exe = b.addExecutable(.{
        .name = "sqlite0",
        .root_module = b.createModule(.{
            .root_source_file = b.path("packages/sqlite0/src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "sqlite0", .module = sqlite0_mod },
            },
        }),
    });

    b.installArtifact(exe);

    const run_step = b.step("run", "Run sqlite0 REPL");
    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);

    const lib_tests = b.addTest(.{ .root_module = sqlite0_mod });
    const run_lib_tests = b.addRunArtifact(lib_tests);

    const exe_tests = b.addTest(.{ .root_module = exe.root_module });
    const run_exe_tests = b.addRunArtifact(exe_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_tests.step);
    test_step.dependOn(&run_exe_tests.step);
}
