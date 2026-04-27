//! Test-only filesystem helpers shared across pager / btree_walk /
//! btree_cursor / schema test suites. Unifies the four near-identical
//! `makeTempPath` / `unlinkPath` / `writePages` copies that grew up
//! alongside each module.
//!
//! Always uses `testing.allocator` — these are test-only and the leak
//! checker enforces correctness. Production code must NOT depend on
//! this module.

const std = @import("std");
const pager_mod = @import("pager.zig");

const testing = std.testing;

const PAGE_SIZE = pager_mod.PAGE_SIZE;

/// Module-scope monotonic counter so concurrent tests don't collide on
/// generated paths. Combined with pid for cross-process safety.
var counter: std.atomic.Value(u32) = .init(0);

/// Construct a unique temp-file path under `$TMPDIR` (or `/tmp`).
/// Caller frees with `testing.allocator.free`.
pub fn makeTempPath(suffix: []const u8) ![]u8 {
    const tmpdir_raw = std.c.getenv("TMPDIR");
    const tmpdir_slice: []const u8 = if (tmpdir_raw) |p|
        std.mem.span(@as([*:0]const u8, p))
    else
        "/tmp";
    const trimmed = std.mem.trimEnd(u8, tmpdir_slice, "/");
    const pid = std.c.getpid();
    const seq = counter.fetchAdd(1, .seq_cst);
    return std.fmt.allocPrint(testing.allocator, "{s}/sqlite0-test-{d}-{d}-{s}.db", .{ trimmed, pid, seq, suffix });
}

/// Best-effort unlink. Failures (file already gone, permission) are
/// swallowed since this lives in `defer` on the cleanup path.
pub fn unlinkPath(path: []const u8) void {
    const path_z = testing.allocator.dupeZ(u8, path) catch return;
    defer testing.allocator.free(path_z);
    _ = std.c.unlink(path_z.ptr);
}

/// Write `content` to a fresh file at `path`, padding with zeros to
/// the next `PAGE_SIZE` boundary so the file is exactly
/// `ceil(content.len / PAGE_SIZE)` pages long.
pub fn writeFixture(path: []const u8, content: []const u8) !void {
    const path_z = try testing.allocator.dupeZ(u8, path);
    defer testing.allocator.free(path_z);
    const flags: std.c.O = .{
        .ACCMODE = .RDWR,
        .CREAT = true,
        .TRUNC = true,
    };
    const fd = std.c.open(path_z.ptr, flags, @as(std.c.mode_t, 0o644));
    if (fd < 0) return error.OpenFailed;
    defer _ = std.c.close(fd);

    const n_pages = (content.len + PAGE_SIZE - 1) / PAGE_SIZE;
    const total = n_pages * PAGE_SIZE;
    const padded = try testing.allocator.alloc(u8, total);
    defer testing.allocator.free(padded);
    @memset(padded, 0);
    @memcpy(padded[0..content.len], content);

    const w = std.c.write(fd, padded.ptr, total);
    if (w != @as(isize, @intCast(total))) return error.WriteFailed;
}

/// Write `pages` consecutively to `path`, one PAGE_SIZE chunk per
/// entry. Each entry must be exactly PAGE_SIZE bytes — the caller
/// owns the layout (e.g. page 1 has the file header prefix baked
/// into its bytes).
pub fn writePages(path: []const u8, pages: []const []const u8) !void {
    const path_z = try testing.allocator.dupeZ(u8, path);
    defer testing.allocator.free(path_z);
    const flags: std.c.O = .{
        .ACCMODE = .RDWR,
        .CREAT = true,
        .TRUNC = true,
    };
    const fd = std.c.open(path_z.ptr, flags, @as(std.c.mode_t, 0o644));
    if (fd < 0) return error.OpenFailed;
    defer _ = std.c.close(fd);

    const total = pages.len * PAGE_SIZE;
    const buf = try testing.allocator.alloc(u8, total);
    defer testing.allocator.free(buf);
    @memset(buf, 0);
    for (pages, 0..) |p, i| {
        @memcpy(buf[i * PAGE_SIZE .. i * PAGE_SIZE + p.len], p);
    }
    const w = std.c.write(fd, buf.ptr, total);
    if (w != @as(isize, @intCast(total))) return error.WriteFailed;
}
