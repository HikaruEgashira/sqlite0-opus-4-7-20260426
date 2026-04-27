//! Recursive interior page split (Iter26.B.3, ADR-0005 §2). The leaf
//! split primitives in `btree_split.zig` only handle the depth ≤ 1
//! shapes (balance-deeper-leaf and split-rightmost-leaf with a
//! depth-1 parent that fits). When the parent itself overflows, the
//! split must propagate up — split the parent interior page, promote
//! a divider cell to the grandparent, and recurse until either an
//! ancestor `.fits` or the root is reached (then balance-deeper the
//! interior root).
//!
//! ## Why interior split is asymmetric vs. leaf split
//!
//! Leaf split COPIES the boundary key into the parent (the cell stays
//! in one of the two halves). Interior split CONSUMES the pivot cell:
//! its `key` becomes the new divider in the grandparent, its
//! `left_child` becomes the new LEFT half's right_child. Worked
//! example for `cells = [c0, c1, c2, c3, c4, c_new]`, `right_child =
//! R_below`, `pivot = 2` (i.e. cells[2] is promoted):
//!
//! ```
//!   L_new (interior):
//!     cells       = [c0, c1]
//!     right_child = c2.left_child   <-- promoted cell's left_child
//!
//!   promoted to grandparent:
//!     new_cell    = { left_child = L_new_page_no, key = c2.key }
//!     new_right_child_of_grandparent = R_new_page_no
//!
//!   R_new (interior):
//!     cells       = [c3, c4, c_new]
//!     right_child = R_below          <-- input right_child stays here
//! ```
//!
//! Mixing the two shapes (e.g. copying instead of consuming the pivot)
//! produces duplicate or missing divider keys that
//! `PRAGMA integrity_check` rejects on the next read.
//!
//! ## Module split rationale
//!
//! `btree_split.zig` was already at 384 lines after B.2.c; adding
//! `splitInteriorCells` + `splitInteriorPage` + `balanceDeeperInterior`
//! would push it past the 500-line discipline (CLAUDE.md "Module
//! Splitting Rules"). Splitting on the leaf vs. interior boundary
//! also matches the natural test partition.

const std = @import("std");
const ops = @import("ops.zig");
const btree_split = @import("btree_split.zig");
const btree_insert = @import("btree_insert.zig");
const pager_mod = @import("pager.zig");
const record_encode = @import("record_encode.zig");

pub const Error = ops.Error;

/// Re-export so callers don't need a parallel `btree_split` import for
/// the cell type alone. Identical to `btree_split.InteriorCell`.
pub const InteriorCell = btree_split.InteriorCell;

/// What an interior split (or balance-deeper) hands back to its
/// grandparent: a divider cell to insert and a new right_child. The
/// grandparent (or top-level orchestrator if this came from the
/// root's child) merges this into its own cell list.
pub const PromotedSplit = struct {
    new_cell: InteriorCell,
    new_right_child: u32,
};

/// Result of `splitInteriorCells`. All three slices borrow from the
/// caller's `cells_in` — no allocation. `pivot` cell (whose `key`
/// became `promoted_key` and whose `left_child` became
/// `left_right_child`) is **consumed**: it appears in neither
/// `left_cells` nor `right_cells`.
pub const InteriorSplit = struct {
    /// Cells for the new left half (= cells_in[0..pivot]). Always
    /// non-empty because `pivot >= 1`.
    left_cells: []const InteriorCell,
    /// right_child pointer of the new left half (= cells_in[pivot].left_child).
    left_right_child: u32,
    /// Key handed up to the grandparent's new divider cell.
    promoted_key: i64,
    /// Cells for the new right half (= cells_in[pivot+1..]). May be
    /// empty when `pivot == cells_in.len - 1` (e.g. the 2-cell minimal
    /// case): an interior page with 0 cells + 1 right_child is well-
    /// formed and `parseInteriorTablePage` accepts it.
    right_cells: []const InteriorCell,
    /// right_child pointer of the new right half (= the input
    /// `right_child_in`, which still anchors the spine's rightmost
    /// descent after the split).
    right_right_child: u32,
};

/// Byte-cumulative pivot for an interior page's `(cells, right_child)`
/// content. Picks the smallest index where left-half byte mass first
/// reaches half of the total cell content; that index is **promoted**.
///
/// Why byte-cumulative (not positional): variable-width key varints
/// can make a positional midpoint produce two halves that differ in
/// byte mass by 8× — one half then refuses to fit `usable_size` even
/// though the other has plenty of slack. Same reasoning as the leaf
/// split (`btree_split.splitLeafCells`).
///
/// Edge cases (`cells_in.len < 2` rejected; pivot clamped to
/// `cells_in.len - 1` when one cell dominates):
/// - `len == 2` ⇒ `pivot = 1`: `left = [cells[0]]`, `right = []`,
///   `promoted = cells[1]`. Right half is the degenerate interior
///   page (0 cells + right_child only).
/// - one cell carries 80% of the total bytes ⇒ pivot lands so that
///   the heavy cell ends up in the right half (or is itself the
///   pivot if the heavy cell is right at the boundary).
pub fn splitInteriorCells(
    cells_in: []const InteriorCell,
    right_child_in: u32,
) Error!InteriorSplit {
    if (cells_in.len < 2) return Error.IoError;

    var total: usize = 0;
    for (cells_in) |c| total += interiorCellByteCost(c);
    const half = total / 2;

    // Min-distance-to-half pivot: when adding cells[i] would push the
    // left half across `half`, decide whether to *include* cells[i]
    // (split_at = i+1, pivot = cells[i+1]) or *stop before* it
    // (split_at = i, pivot = cells[i]) — whichever ends up with the
    // smaller |left_bytes − half|. Ties go to "stop before" so odd-N
    // uniform inputs promote the geometric middle (e.g. 3 cells →
    // pivot = 1, not pivot = 2).
    //
    // Why we don't reuse `splitLeafCells`'s "first index where running
    // >= half" rule: that pure right-bias is fine for leaves (no cell
    // is consumed) but for interior splits it concentrates the pivot
    // toward the right tail and leaves an empty right half on small
    // inputs more often than necessary. The min-distance form keeps
    // both halves non-trivial whenever the input allows.
    var split_at: usize = 1;
    var running: usize = 0;
    for (cells_in, 0..) |c, i| {
        const next_running = running + interiorCellByteCost(c);
        if (next_running >= half) {
            const dist_with: usize = next_running - half;
            const dist_without: usize = half - running;
            split_at = if (dist_with < dist_without) i + 1 else i;
            break;
        }
        running = next_running;
    }
    // Clamp so pivot stays a valid index. Worst case (one cell
    // dominates the byte total) lands pivot = cells_in.len - 1, which
    // gives an empty right half — see InteriorSplit doc.
    if (split_at < 1) split_at = 1;
    if (split_at >= cells_in.len) split_at = cells_in.len - 1;
    const pivot = split_at;

    return .{
        .left_cells = cells_in[0..pivot],
        .left_right_child = cells_in[pivot].left_child,
        .promoted_key = cells_in[pivot].key,
        .right_cells = cells_in[pivot + 1 ..],
        .right_right_child = right_child_in,
    };
}

/// Per-cell byte cost in an interior page: 4-byte left_child + key
/// varint + 2-byte ptr-array slot. The page's right_child lives in
/// the header (no per-cell contribution) and `header_offset` /
/// 12-byte interior header are accounted for by `classifyForInterior`,
/// not here.
fn interiorCellByteCost(c: InteriorCell) usize {
    return 4 +
        record_encode.varintLen(@as(u64, @bitCast(c.key))) +
        2;
}

/// Split an overflowing interior page (Iter26.B.3.b). Allocates two
/// fresh interior pages (L_new, R_new), writes the byte-cumulative
/// halves of `merged_cells`, and returns a `PromotedSplit` for the
/// grandparent to install. Does NOT touch the old interior page —
/// the caller knows its page number and is responsible for
/// `pager.freePage(old)` AFTER its grandparent commits the new
/// child pointer (parent-last invariant; see module doc).
///
/// `merged_cells` is the existing parent's cells plus the cell
/// promoted up from a child split, already merged in rowid order.
/// `merged_right_child` is the parent's new right_child after the
/// child split (= the rightmost subtree's new root page).
///
/// `merged_cells.len < 2` is `Error.IoError` — there's nothing
/// meaningful to split. Each half is also re-checked against
/// `usable_size` via `classifyForInterior`; failure there is also
/// `Error.IoError` (would indicate a bug in the splitter, since
/// byte-cumulative midpoint guarantees both halves ≤ original size).
pub fn splitInteriorPage(
    pager: *pager_mod.Pager,
    merged_cells: []const InteriorCell,
    merged_right_child: u32,
) Error!PromotedSplit {
    if (merged_cells.len < 2) return Error.IoError;

    const usable_size = try pager.usableSize();
    const split = try splitInteriorCells(merged_cells, merged_right_child);

    if (btree_split.classifyForInterior(split.left_cells, 0, usable_size) != .fits) return Error.IoError;
    if (btree_split.classifyForInterior(split.right_cells, 0, usable_size) != .fits) return Error.IoError;

    const allocator = pager.allocator;

    // Allocate L_new + R_new BEFORE writing anything: a failure at
    // alloc leaves the on-disk dbsize alone. Order: L then R so the
    // page numbers ascend with key, matching sqlite3's layout
    // convention. (Same as `balanceDeeperRoot`.)
    const left_page = try pager.allocatePage();
    const right_page = try pager.allocatePage();

    // Build both child bodies in scratch buffers so a partial alloc
    // failure above doesn't leave a half-formatted page on disk.
    const left_buf = try allocator.alloc(u8, pager_mod.PAGE_SIZE);
    defer allocator.free(left_buf);
    @memset(left_buf, 0);
    try btree_split.writeInteriorTablePage(
        left_buf,
        0,
        usable_size,
        split.left_cells,
        split.left_right_child,
    );

    const right_buf = try allocator.alloc(u8, pager_mod.PAGE_SIZE);
    defer allocator.free(right_buf);
    @memset(right_buf, 0);
    try btree_split.writeInteriorTablePage(
        right_buf,
        0,
        usable_size,
        split.right_cells,
        split.right_right_child,
    );

    try pager.writePage(left_page, left_buf);
    try pager.writePage(right_page, right_buf);

    return .{
        .new_cell = .{ .left_child = left_page, .key = split.promoted_key },
        .new_right_child = right_page,
    };
}

/// Balance-deeper for an overflowing INTERIOR root (Iter26.B.3.b).
/// Symmetric with `btree_split.balanceDeeperRoot` (which handles a
/// LEAF root) — `root_page_no` is preserved (same `sqlite_schema.rootpage`
/// invariant), the existing root bytes get rewritten LAST as a 1-cell
/// interior page pointing at the two new interior children.
///
/// Inputs match `splitInteriorPage`: `merged_cells` + `merged_right_child`
/// are what the root would have held had it not overflowed. Caller has
/// already merged the child-promoted cell into the root's cell list.
///
/// Page 1 root is rejected with `UnsupportedFeature` — sqlite_schema
/// growth past one page is deferred (same reason `balanceDeeperRoot`
/// rejects page 1).
///
/// Crash window: same parent-last shape as `balanceDeeperRoot`. New
/// interior children are written first; root rewrite is the LAST
/// pager mutation. A crash before the root write leaves the OLD root
/// untouched at `root_page_no` (queryable, just stale) plus orphan
/// child pages — `PRAGMA integrity_check` warns but data is preserved.
/// Phase 4 (WAL) absorbs the residual window end-to-end.
pub fn balanceDeeperInterior(
    pager: *pager_mod.Pager,
    root_page_no: u32,
    merged_cells: []const InteriorCell,
    merged_right_child: u32,
) Error!void {
    if (root_page_no == 1) return Error.UnsupportedFeature;
    if (merged_cells.len < 2) return Error.IoError;

    const usable_size = try pager.usableSize();
    const split = try splitInteriorCells(merged_cells, merged_right_child);

    if (btree_split.classifyForInterior(split.left_cells, 0, usable_size) != .fits) return Error.IoError;
    if (btree_split.classifyForInterior(split.right_cells, 0, usable_size) != .fits) return Error.IoError;

    const allocator = pager.allocator;

    const left_page = try pager.allocatePage();
    const right_page = try pager.allocatePage();

    const left_buf = try allocator.alloc(u8, pager_mod.PAGE_SIZE);
    defer allocator.free(left_buf);
    @memset(left_buf, 0);
    try btree_split.writeInteriorTablePage(
        left_buf,
        0,
        usable_size,
        split.left_cells,
        split.left_right_child,
    );

    const right_buf = try allocator.alloc(u8, pager_mod.PAGE_SIZE);
    defer allocator.free(right_buf);
    @memset(right_buf, 0);
    try btree_split.writeInteriorTablePage(
        right_buf,
        0,
        usable_size,
        split.right_cells,
        split.right_right_child,
    );

    try pager.writePage(left_page, left_buf);
    try pager.writePage(right_page, right_buf);

    // Root rewrite LAST. The old root bytes at `root_page_no` are
    // overwritten; no freePage needed (page is reused).
    const root_buf = try allocator.alloc(u8, pager_mod.PAGE_SIZE);
    defer allocator.free(root_buf);
    @memset(root_buf, 0);
    const root_cells = [_]InteriorCell{
        .{ .left_child = left_page, .key = split.promoted_key },
    };
    try btree_split.writeInteriorTablePage(
        root_buf,
        0,
        usable_size,
        &root_cells,
        right_page,
    );
    try pager.writePage(root_page_no, root_buf);
}

/// Bottom-of-spine leaf split for the recursive INSERT path
/// (Iter26.B.3.c). Symmetric with `splitInteriorPage` but produces
/// LEAF children. Allocates L_new + R_new, splits `all_combined` by
/// `btree_split.splitLeafCells`, writes both leaves, and returns a
/// `PromotedSplit` whose `new_cell` divider key matches the leaf
/// split's `divider_key` (the largest rowid in the left half — leaf
/// dividers are COPIED, not consumed, unlike interior split).
///
/// Does NOT touch the OLD leaf page — caller (= `insertIntoDeepTree`)
/// tracks it in a deferred-free list and frees it after the topmost
/// commit.
///
/// `all_combined.len < 2` → `Error.IoError` (nothing to split).
pub fn splitLeafProduceChildren(
    pager: *pager_mod.Pager,
    all_combined: []const btree_insert.RebuildCell,
) Error!PromotedSplit {
    if (all_combined.len < 2) return Error.IoError;

    const usable_size = try pager.usableSize();
    const split = try btree_split.splitLeafCells(all_combined);

    const allocator = pager.allocator;

    // Allocate L_new + R_new before any pager writes — same shape as
    // `splitInteriorPage` so `allocatePage` failures don't leave half-
    // formatted pages on disk.
    const left_page = try pager.allocatePage();
    const right_page = try pager.allocatePage();

    const left_buf = try allocator.alloc(u8, pager_mod.PAGE_SIZE);
    defer allocator.free(left_buf);
    @memset(left_buf, 0);
    try btree_insert.rebuildLeafTablePage(left_buf, 0, usable_size, split.left);

    const right_buf = try allocator.alloc(u8, pager_mod.PAGE_SIZE);
    defer allocator.free(right_buf);
    @memset(right_buf, 0);
    try btree_insert.rebuildLeafTablePage(right_buf, 0, usable_size, split.right);

    try pager.writePage(left_page, left_buf);
    try pager.writePage(right_page, right_buf);

    return .{
        .new_cell = .{ .left_child = left_page, .key = split.divider_key },
        .new_right_child = right_page,
    };
}

// Unit tests live in `btree_split_interior_test.zig` to keep this
// file under the 500-line discipline.
