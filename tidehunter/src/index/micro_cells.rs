//! Micro-cell count of a lookup-header index blob.
//!
//! A `LookupHeaderIndex` blob is `[header: n * 8 bytes][sections ...]`: one
//! `(from, to)` element per micro-cell, and a point lookup reads the key's
//! element, then binary-searches its section. The writer decides `n` per
//! blob from the configured [`MicroCellPolicy`] and records it in the blob's
//! `IndexRef`, which is what the reader uses; nothing is derived from the
//! blob itself.

use serde::{Deserialize, Serialize};

/// Bytes per header element: `(from: u32, to: u32)` section offsets.
pub const HEADER_ELEMENT_SIZE: usize = 8;
/// The micro-cell count every blob had before refs recorded it.
pub const LEGACY_MICRO_CELLS: u32 = 128;
/// Largest count a policy may produce: `2^20` micro-cells, an 8 MiB header.
pub const MAX_MICRO_CELLS_LOG2: u32 = 20;
/// Smallest allowed `section_target_log2` (32-byte sections).
pub const MIN_SECTION_TARGET_LOG2: u8 = 5;
/// Largest allowed `section_target_log2` (1 MiB sections).
pub const MAX_SECTION_TARGET_LOG2: u8 = 20;

/// How the writer picks the micro-cell count of a lookup-header blob.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum MicroCellPolicy {
    /// Every blob gets `count` micro-cells regardless of size. `LEGACY` is
    /// `Fixed { count: 128 }`, the behaviour of databases created before
    /// this setting existed; the variant also pins it for comparison.
    Fixed { count: u32 },
    /// The count follows the blob's data size: the largest power of two
    /// such that a section averages at least `2^section_target_log2` bytes
    /// and the header is no larger than a section. See [`scaled_log2`].
    Scaled { section_target_log2: u8 },
}

impl MicroCellPolicy {
    pub const LEGACY: Self = Self::Fixed {
        count: LEGACY_MICRO_CELLS,
    };
    /// Default: 16 KiB sections.
    pub const DEFAULT: Self = Self::Scaled {
        section_target_log2: 14,
    };

    pub fn validate(&self) -> Result<(), String> {
        match *self {
            Self::Fixed { count } => {
                if count == 0 || count > (1 << MAX_MICRO_CELLS_LOG2) {
                    return Err(format!(
                        "micro_cell_policy Fixed count {count} must be in 1..=2^{MAX_MICRO_CELLS_LOG2}"
                    ));
                }
            }
            Self::Scaled {
                section_target_log2,
            } => {
                if !(MIN_SECTION_TARGET_LOG2..=MAX_SECTION_TARGET_LOG2)
                    .contains(&section_target_log2)
                {
                    return Err(format!(
                        "micro_cell_policy Scaled section_target_log2 {section_target_log2} must be \
                         in {MIN_SECTION_TARGET_LOG2}..={MAX_SECTION_TARGET_LOG2}"
                    ));
                }
            }
        }
        Ok(())
    }

    /// Micro-cell count for a blob whose sections total about `data_len`
    /// bytes. Always at least 1.
    pub fn micro_cells(self, data_len: usize) -> u32 {
        match self {
            Self::Fixed { count } => count,
            Self::Scaled {
                section_target_log2,
            } => 1 << scaled_log2(data_len, section_target_log2 as u32),
        }
    }
}

impl Default for MicroCellPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// log2 of the count [`MicroCellPolicy::Scaled`] picks for `data_len` bytes
/// of sections, `T = 2^section_target_log2`: the largest `k` such that
///
/// 1. `2^k * T <= data_len`: sections average at least `T` bytes;
/// 2. `8 * 4^k <= data_len`: the header (`8 * 2^k` bytes) is no larger than
///    the blob's share per micro-cell (`data_len / 2^k`), i.e. no larger
///    than a section.
///
/// Rule 2 has no parameter and binds only for `data_len >= T^2 / 8`
/// (32 MiB at `T = 16 KiB`); below that sections average `[T, 2T)`. Above,
/// header and sections grow together as about `sqrt(8 * data_len)`: a 1 GiB
/// blob gets 8192 micro-cells, a 64 KiB header and 128 KiB sections.
#[inline]
pub fn scaled_log2(data_len: usize, section_target_log2: u32) -> u32 {
    let by_target = floor_log2(data_len >> section_target_log2);
    let by_header = floor_log2(data_len >> 3) / 2;
    by_target.min(by_header).min(MAX_MICRO_CELLS_LOG2)
}

/// `floor(log2(v))`, with `floor_log2(0) == 0`.
#[inline]
fn floor_log2(v: usize) -> u32 {
    if v == 0 {
        0
    } else {
        usize::BITS - 1 - v.leading_zeros()
    }
}

#[inline]
pub const fn header_len(micro_cells: u32) -> usize {
    micro_cells as usize * HEADER_ELEMENT_SIZE
}

/// Micro-cell of a key in a blob with `micro_cells` micro-cells.
/// `cell_offset` is the key's u32 index prefix minus the start of the
/// cell's prefix range; `cell_width` is that range's width (up to 2^32,
/// hence u64). Monotone in `cell_offset`, so keys in key order fall into
/// non-decreasing micro-cells.
#[inline]
pub fn micro_cell(cell_offset: u32, cell_width: u64, micro_cells: u32) -> usize {
    assert!(
        (cell_offset as u64) < cell_width,
        "cell offset {cell_offset} outside cell width {cell_width}"
    );
    // Both factors are below 2^32: no overflow.
    ((cell_offset as u64) * (micro_cells as u64) / cell_width) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scaled_count_follows_data_size() {
        let p = MicroCellPolicy::DEFAULT;
        assert_eq!(p.micro_cells(0), 1);
        assert_eq!(p.micro_cells(1138), 1);
        assert_eq!(p.micro_cells(16 * 1024 - 1), 1);
        assert_eq!(p.micro_cells(16 * 1024), 1); // header rule: 8 * 4 > 16 KiB? no: 2 cells need 32 B header <= 8 KiB share
        assert_eq!(p.micro_cells(32 * 1024), 2);
        // mainnet-sized blobs
        assert_eq!(p.micro_cells(78 * 1024), 4);
        assert_eq!(p.micro_cells(2_180_000), 128);
        assert_eq!(p.micro_cells(17_550_000), 1024);
        // the header-balance rule takes over at T^2 / 8 = 32 MiB
        assert_eq!(p.micro_cells(1 << 25), 2048);
        assert_eq!(p.micro_cells(1 << 26), 2048);
        assert_eq!(p.micro_cells(1 << 27), 4096);
        assert_eq!(p.micro_cells((1 << 30) - 1), 8192);
        assert_eq!(header_len(8192), 64 * 1024);
        assert_eq!(((1usize << 30) - 1) / 8192, 128 * 1024 - 1);
        // clamp
        assert_eq!(p.micro_cells(usize::MAX), 1 << MAX_MICRO_CELLS_LOG2);

        // monotone in data size, power of two, header never larger than a section
        for &t in &[5u32, 12, 14, 16] {
            let p = MicroCellPolicy::Scaled {
                section_target_log2: t as u8,
            };
            let mut prev = 0;
            let mut len = 0usize;
            while len < (1 << 36) {
                let n = p.micro_cells(len);
                assert!(n.is_power_of_two());
                assert!(n >= prev, "t {t} len {len}");
                if n > 1 {
                    assert!(len / n as usize >= (1 << t), "t {t} len {len} n {n}");
                    assert!(header_len(n) <= len / n as usize, "t {t} len {len} n {n}");
                }
                prev = n;
                len = len * 5 / 4 + 1;
            }
        }
    }

    #[test]
    fn fixed_and_legacy() {
        assert_eq!(MicroCellPolicy::LEGACY.micro_cells(0), 128);
        assert_eq!(MicroCellPolicy::LEGACY.micro_cells(usize::MAX), 128);
        assert_eq!(MicroCellPolicy::Fixed { count: 3 }.micro_cells(5), 3);
        assert_eq!(header_len(128), 1024);
    }

    #[test]
    fn validate_bounds() {
        assert!(MicroCellPolicy::DEFAULT.validate().is_ok());
        assert!(MicroCellPolicy::LEGACY.validate().is_ok());
        assert!(MicroCellPolicy::Fixed { count: 0 }.validate().is_err());
        assert!(MicroCellPolicy::Fixed { count: 1 << 20 }.validate().is_ok());
        assert!(
            MicroCellPolicy::Fixed { count: 1 << 21 }
                .validate()
                .is_err()
        );
        for (t, ok) in [(4u8, false), (5, true), (20, true), (21, false)] {
            assert_eq!(
                MicroCellPolicy::Scaled {
                    section_target_log2: t
                }
                .validate()
                .is_ok(),
                ok
            );
        }
    }

    #[test]
    fn micro_cell_is_the_plain_rescale() {
        // the legacy count reproduces the pre-ref header mapping
        assert_eq!(micro_cell(0, 1 << 32, 128), 0);
        assert_eq!(micro_cell(u32::MAX, 1 << 32, 128), 127);
        assert_eq!(micro_cell(1 << 31, 1 << 32, 128), 64);
        assert_eq!(micro_cell(999, 1000, 1), 0);
        assert_eq!(micro_cell(999, 1000, 1000), 999);
        assert_eq!(micro_cell(0, 3, 1 << 20), 0);
        assert_eq!(micro_cell(2, 3, 1 << 20), (2u64 << 20) as usize / 3);
        // monotone
        let mut prev = 0;
        for v in (0..u32::MAX).step_by(1 << 20) {
            let mc = micro_cell(v, 1 << 32, 4096);
            assert!(mc >= prev);
            prev = mc;
        }
    }
}
