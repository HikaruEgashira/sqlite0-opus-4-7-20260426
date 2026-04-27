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
const record_encode = @import("record_encode.zig");

pub const Error = ops.Error;

/// Re-export so callers don't need a parallel `btree_split` import for
/// the cell type alone. Identical to `btree_split.InteriorCell`.
pub const InteriorCell = btree_split.InteriorCell;

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

// Unit tests live in `btree_split_interior_test.zig` to keep this
// file headed for sub-500-line discipline once B.3.b adds the
// pager-touching `splitInteriorPage` + `balanceDeeperInterior`.
