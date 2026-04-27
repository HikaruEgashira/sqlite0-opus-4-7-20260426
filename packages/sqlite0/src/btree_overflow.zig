//! Overflow page chain support (Iter26.C, ADR-0005 §2). When a table
//! B-tree leaf cell's payload exceeds X = `usable_size − 35`, sqlite3
//! stores an inline prefix on the leaf followed by a 4-byte big-endian
//! pointer to the head of an overflow page chain. Each overflow page
//! carries up to `usable_size − 4` payload bytes plus a 4-byte
//! next-page link (0 sentinel on the tail).
//!
//! ## Spec arithmetic (sqlite.org/fileformat.html §1.6)
//!
//! - X = U − 35
//! - M = ((U − 12) × 32 / 255) − 23   (integer math: multiply, then
//!   floor-divide, then subtract)
//! - If P ≤ X: full P inline, no chain.
//! - Else compute K = M + ((P − M) mod (U − 4)).
//!   - inline_len = K when K ≤ X, otherwise M  (ternary, NOT clamp).
//!
//! Worked example U=4084, P=10000:
//!   X = 4049
//!   M = floor(4072 × 32 / 255) − 23 = 510 − 23 = 487
//!   K = 487 + (9513 mod 4080) = 487 + 1353 = 1840  (≤ X → use K)
//!   inline_len = 1840, spill = 8160, chain pages = 8160 / 4080 = 2
//!
//! ## Crash-safety stance
//!
//! Same parent-last invariant the rest of Iter26.B uses: write the
//! overflow chain pages first (tail → head order so the chain is
//! self-consistent on disk before the leaf cell points at it), then
//! commit the leaf rebuild last. A crash mid-allocation leaves orphan
//! pages that `PRAGMA integrity_check` will flag — same accepted
//! window as B.1/B.2; Phase 4 (WAL) absorbs this end-to-end.
//!
//! ## Module placement
//!
//! Pure logic (`inlineSplitForPayload`) plus pager-touching helpers
//! live here together so `btree_insert.zig` and `btree.zig` can stay
//! under the 500-line discipline once C.2 wires chain emission into
//! `rebuildLeafTablePage` and chain free into the DELETE/UPDATE walker.

const std = @import("std");
const ops = @import("ops.zig");
const btree = @import("btree.zig");
const pager_mod = @import("pager.zig");

pub const Error = ops.Error;

pub const InlineSplit = struct {
    /// Bytes of the payload that live inline on the leaf cell.
    inline_len: usize,
    /// Bytes that spill into the overflow chain (= P − inline_len).
    spill_len: usize,
};

/// Compute the inline / spill split for a payload of length `P` on a
/// page with usable size `U`. Pure function — caller passes `U` rather
/// than a pager so the same helper drives both read-side parsing
/// (`parseLeafTablePage`) and write-side allocation
/// (`rebuildLeafTablePage` / chain emit). See module doc for spec
/// derivation.
pub fn inlineSplitForPayload(p: usize, u: usize) InlineSplit {
    if (u <= 35) {
        // Degenerate fixture: tiny pages aren't supported by sqlite3
        // either (min usable for the format is 480 bytes per the spec).
        // Returning everything inline keeps callers from dereferencing
        // negative arithmetic; the higher layer will reject anyway.
        return .{ .inline_len = p, .spill_len = 0 };
    }
    const x: usize = u - 35;
    if (p <= x) return .{ .inline_len = p, .spill_len = 0 };

    // M = ((U - 12) * 32 / 255) - 23. Guard against tiny U where the
    // subtraction would underflow — we already handled u <= 35 above.
    const m_num: usize = (u - 12) * 32;
    const m_div: usize = m_num / 255;
    const m: usize = if (m_div > 23) m_div - 23 else 0;

    const k: usize = m + ((p - m) % (u - 4));
    const inline_len: usize = if (k <= x) k else m;
    return .{ .inline_len = inline_len, .spill_len = p - inline_len };
}

/// Reassemble the full payload of a leaf cell whose `overflow_head` is
/// non-zero, walking the chain from `pager`. Returns `cell.inline_bytes`
/// directly when there's no overflow (no copy needed; arena unused).
/// On overflow, allocates `cell.payload_len` bytes from `arena` and
/// copies inline + every chain page's `[4..U]` slice. Validates that
/// the sum of bytes copied matches `payload_len` exactly — chain length
/// mismatch surfaces as `Error.IoError`.
///
/// `usable_size` (`U`) determines how many payload bytes each chain
/// page carries (`U − 4`). Caller threads it from `pager.usableSize()`.
pub fn assemblePayload(
    arena: std.mem.Allocator,
    pager: *pager_mod.Pager,
    cell: btree.LeafTableCell,
    usable_size: usize,
) Error![]const u8 {
    if (cell.overflow_head == 0) return cell.inline_bytes;
    if (cell.payload_len < cell.inline_bytes.len) return Error.IoError;
    if (usable_size < 5) return Error.IoError;

    const result = try arena.alloc(u8, cell.payload_len);
    @memcpy(result[0..cell.inline_bytes.len], cell.inline_bytes);
    var pos: usize = cell.inline_bytes.len;
    var next: u32 = cell.overflow_head;
    const per_page: usize = usable_size - 4;

    // Bound the walk so a corrupt cyclic chain can't loop forever.
    // sqlite3's hard limit is `(payload_len / per_page) + 1` pages.
    var hops: usize = 0;
    const max_hops: usize = (cell.payload_len / per_page) + 2;

    while (next != 0) {
        if (hops > max_hops) return Error.IoError;
        hops += 1;
        const page = try pager.getPage(next);
        if (page.len < 4) return Error.IoError;
        const next_next: u32 = (@as(u32, page[0]) << 24) |
            (@as(u32, page[1]) << 16) |
            (@as(u32, page[2]) << 8) |
            @as(u32, page[3]);
        const remaining = cell.payload_len - pos;
        const take = @min(remaining, per_page);
        if (4 + take > page.len) return Error.IoError;
        @memcpy(result[pos .. pos + take], page[4 .. 4 + take]);
        pos += take;
        next = next_next;
    }
    if (pos != cell.payload_len) return Error.IoError;
    return result;
}

/// Allocate an overflow chain for `payload[inline_len..]` and write
/// every chain page. Returns the head page number to embed in the
/// leaf cell. The chain pages each carry a 4-byte u32 BE next-link
/// (0 sentinel on the tail) plus up to `usable_size − 4` payload
/// bytes. Caller commits the leaf cell with the head pointer LAST
/// to land the chain transactionally (parent-last invariant).
///
/// Write order: tail-first. Each `pager.allocatePage` bumps page-1
/// dbsize, so a crash mid-loop leaves orphan pages — same accepted
/// window as B.1/B.2; integrity_check would warn until Phase 4 (WAL).
///
/// `pager.allocatePage` allocates each new page contiguous to the
/// end-of-file when the freelist is empty. With an empty freelist
/// (the differential fixtures stay in this regime) sqlite3 hands
/// out the same page numbers in the same order, which keeps byte-
/// equivalent output realistic to assert.
pub fn allocateOverflowChain(
    pager: *pager_mod.Pager,
    payload: []const u8,
    inline_len: usize,
) Error!u32 {
    if (inline_len > payload.len) return Error.IoError;
    const spill = payload[inline_len..];
    if (spill.len == 0) return Error.IoError; // caller should not chain a fully-inline payload

    const usable = try pager.usableSize();
    if (usable < 5) return Error.IoError;
    const per_page: usize = usable - 4;
    const page_count: usize = (spill.len + per_page - 1) / per_page;

    const allocator = pager.allocator;
    const pages = try allocator.alloc(u32, page_count);
    defer allocator.free(pages);
    for (pages) |*slot| slot.* = try pager.allocatePage();

    // Build & write tail-first so the chain on disk is consistent
    // before any earlier page links to it. A crash between writes
    // leaves orphan pages but no dangling pointers.
    //
    // Layout: page i carries `spill[i*per_page .. min((i+1)*per_page,
    // spill.len)]`. The tail (i == page_count − 1) is the only page
    // that may carry less than `per_page` bytes; every earlier page
    // is fully utilised. Matches sqlite3's chain-writer.
    const buf = try allocator.alloc(u8, pager_mod.PAGE_SIZE);
    defer allocator.free(buf);

    var i: usize = page_count;
    while (i > 0) {
        i -= 1;
        @memset(buf, 0);
        const next: u32 = if (i + 1 < page_count) pages[i + 1] else 0;
        buf[0] = @intCast((next >> 24) & 0xff);
        buf[1] = @intCast((next >> 16) & 0xff);
        buf[2] = @intCast((next >> 8) & 0xff);
        buf[3] = @intCast(next & 0xff);
        const start = i * per_page;
        const end = @min(start + per_page, spill.len);
        @memcpy(buf[4 .. 4 + (end - start)], spill[start..end]);
        try pager.writePage(pages[i], buf);
    }
    return pages[0];
}

/// Walk the chain rooted at `head`, returning every page number to a
/// freshly-allocated slice (caller frees), then call `pager.freePage`
/// on each in reverse (tail → head) order so the most-recently-
/// allocated page lands at the head of the freelist's leaf array
/// first. That matches sqlite3's own allocation/free pairing pattern
/// and keeps subsequent INSERT chain reuse deterministic.
///
/// Caller must first remove or rewrite the leaf cell so the chain is
/// no longer reachable BEFORE freeing pages — otherwise a crash
/// mid-loop leaves the leaf pointing at a freed page.
pub fn freeOverflowChain(pager: *pager_mod.Pager, head: u32) Error!void {
    if (head == 0) return;
    const allocator = pager.allocator;

    // First pass: walk the chain, collect every page number.
    var collected: std.ArrayList(u32) = .empty;
    defer collected.deinit(allocator);

    var next: u32 = head;
    var hops: usize = 0;
    while (next != 0) {
        if (hops > 1024 * 1024) return Error.IoError; // cyclic-chain guard
        hops += 1;
        try collected.append(allocator, next);
        const page = try pager.getPage(next);
        if (page.len < 4) return Error.IoError;
        next = (@as(u32, page[0]) << 24) |
            (@as(u32, page[1]) << 16) |
            (@as(u32, page[2]) << 8) |
            @as(u32, page[3]);
    }

    // Second pass: free in reverse (tail-first).
    var i: usize = collected.items.len;
    while (i > 0) {
        i -= 1;
        try pager.freePage(collected.items[i]);
    }
}

// -- tests --

const testing = std.testing;

test "inlineSplitForPayload: payload ≤ X stays fully inline" {
    const s = inlineSplitForPayload(100, 4084);
    try testing.expectEqual(@as(usize, 100), s.inline_len);
    try testing.expectEqual(@as(usize, 0), s.spill_len);
}

test "inlineSplitForPayload: P=10000 U=4084 → 1840 inline, 8160 spill (spec example)" {
    const s = inlineSplitForPayload(10000, 4084);
    try testing.expectEqual(@as(usize, 1840), s.inline_len);
    try testing.expectEqual(@as(usize, 8160), s.spill_len);
    // Spill = exactly 2 pages of (U−4)=4080 bytes each.
    try testing.expectEqual(@as(usize, 2), s.spill_len / (4084 - 4));
}

test "inlineSplitForPayload: P just over X stays at min inline (M)" {
    // X = 4049, M = 487 for U=4084. P = X+1 = 4050.
    // K = 487 + ((4050 - 487) mod 4080) = 487 + 3563 = 4050.
    // 4050 > X=4049 → falls to M=487.
    const s = inlineSplitForPayload(4050, 4084);
    try testing.expectEqual(@as(usize, 487), s.inline_len);
    try testing.expectEqual(@as(usize, 4050 - 487), s.spill_len);
}

test "inlineSplitForPayload: U=512 (tiny page) still computes coherently" {
    // X = 477, M = ((500*32/255) - 23) = 62 - 23 = 39. P = 1000.
    // K = 39 + ((961) mod 508) = 39 + 453 = 492. 492 > 477 → M.
    const s = inlineSplitForPayload(1000, 512);
    try testing.expectEqual(@as(usize, 39), s.inline_len);
    try testing.expectEqual(@as(usize, 961), s.spill_len);
}
