const std = @import("std");
const sqlite0 = @import("sqlite0");

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.gpa;

    var stdout_buf: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buf);
    const stdout = &stdout_writer.interface;
    defer stdout.flush() catch {};

    var stderr_buf: [4096]u8 = undefined;
    var stderr_writer = std.Io.File.stderr().writer(io, &stderr_buf);
    const stderr = &stderr_writer.interface;
    defer stderr.flush() catch {};

    var args_iter = try std.process.Args.Iterator.initAllocator(init.minimal.args, gpa);
    defer args_iter.deinit();
    _ = args_iter.next();

    var sql_inline: ?[]const u8 = null;
    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "-c") or std.mem.eql(u8, arg, "--command")) {
            sql_inline = args_iter.next() orelse {
                try stderr.writeAll("error: -c requires an argument\n");
                return;
            };
        } else if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) {
            try printHelp(stdout);
            return;
        } else if (std.mem.eql(u8, arg, "-v") or std.mem.eql(u8, arg, "--version")) {
            try stdout.print("sqlite0 {s}\n", .{sqlite0.version});
            return;
        }
    }

    if (sql_inline) |sql| {
        try runSql(gpa, sql, stdout, stderr);
        return;
    }

    try repl(gpa, io, stdout, stderr);
}

fn printHelp(w: *std.Io.Writer) !void {
    try w.writeAll(
        \\sqlite0 — SQLite3-compatible database (work in progress)
        \\
        \\Usage:
        \\  sqlite0                    Start REPL
        \\  sqlite0 -c "SQL"           Execute one statement and exit
        \\  sqlite0 --version          Show version
        \\  sqlite0 --help             Show this help
        \\
    );
}

fn repl(gpa: std.mem.Allocator, io: std.Io, stdout: *std.Io.Writer, stderr: *std.Io.Writer) !void {
    var stdin_buf: [4096]u8 = undefined;
    var stdin_reader = std.Io.File.stdin().reader(io, &stdin_buf);
    const stdin = &stdin_reader.interface;

    try stdout.print("sqlite0 {s}\n", .{sqlite0.version});
    try stdout.writeAll("Enter \".help\" for usage hints. \".exit\" or Ctrl-D to exit.\n");
    try stdout.flush();

    while (true) {
        try stdout.writeAll("sqlite0> ");
        try stdout.flush();
        const line = stdin.takeDelimiterExclusive('\n') catch |err| switch (err) {
            error.EndOfStream => return,
            error.StreamTooLong => {
                try stderr.writeAll("error: input too long\n");
                continue;
            },
            else => return err,
        };
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (trimmed.len == 0) continue;
        if (std.mem.eql(u8, trimmed, ".exit") or std.mem.eql(u8, trimmed, ".quit")) return;
        if (std.mem.eql(u8, trimmed, ".help")) {
            try printHelp(stdout);
            try stdout.flush();
            continue;
        }
        runSql(gpa, trimmed, stdout, stderr) catch |err| {
            try stderr.print("error: {s}\n", .{@errorName(err)});
        };
        try stdout.flush();
        try stderr.flush();
    }
}

fn runSql(gpa: std.mem.Allocator, sql: []const u8, stdout: *std.Io.Writer, stderr: *std.Io.Writer) !void {
    var result = sqlite0.execute(gpa, sql) catch |err| {
        try stderr.print("error: {s}\n", .{@errorName(err)});
        return;
    };
    defer result.deinit();

    for (result.rows) |row| {
        for (row.values, 0..) |v, i| {
            if (i > 0) try stdout.writeByte('|');
            try v.format(stdout);
        }
        try stdout.writeByte('\n');
    }
}
