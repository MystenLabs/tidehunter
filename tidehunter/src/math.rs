use std::cmp;

/// Scale down u32 value from the range 0..=u32::MAX to the given range 0..upper_bound_exclusive
pub fn downscale_u32(v: u32, upper_bound_exclusive: u32) -> u32 {
    rescale_u32(v, u32::MAX as u64 + 1, upper_bound_exclusive)
}

/// Scale down u32 value from the range 0..=from_upper_bound_exclusive to the given range 0..upper_bound_exclusive
/// from_upper_bound_exclusive is specified as u64 to allow for value u32::MAX+1 as exclusive upper range
pub fn rescale_u32(v: u32, from_upper_bound_exclusive: u64, to_upper_bound_exclusive: u32) -> u32 {
    assert!(
        (v as u64) < from_upper_bound_exclusive,
        "v: {v}, from_upper_bound_exclusive: {from_upper_bound_exclusive}"
    );
    assert!(to_upper_bound_exclusive > 0);
    let v = v as u64;
    // this does not overflow: v <= u32::MAX, upper_bound_exclusive <= u32::MAX
    // therefore, prefix * num_buckets < u64::MAX,
    let bucket = v * (to_upper_bound_exclusive as u64) / from_upper_bound_exclusive;
    debug_assert!(bucket < to_upper_bound_exclusive as u64);
    bucket.try_into().unwrap()
}

/// Extract starting u32 value from first bytes of a slice.
/// If slice is less than four bytes long, assumes slice is padded with zeroes.
/// See test_starting_u32 for examples.
pub fn starting_u32(slice: &[u8]) -> u32 {
    let copy = cmp::min(slice.len(), 4);
    let mut p = [0u8; 4];
    p[..copy].copy_from_slice(&slice[..copy]);
    u32::from_be_bytes(p)
}

/// Extract ending u32 value from last bytes of a slice.
/// If slice is less than four bytes long, assumes slice is padded with zeroes at the start.
/// See test_ending_u32 for examples.
pub fn ending_u32(slice: &[u8]) -> u32 {
    let copy = cmp::min(slice.len(), 4);
    let mut p = [0u8; 4];
    // Copy the last 'copy' bytes to the end of the buffer
    p[4 - copy..].copy_from_slice(&slice[slice.len() - copy..]);
    u32::from_be_bytes(p)
}

pub fn starting_u64(slice: &[u8]) -> u64 {
    let copy = cmp::min(slice.len(), 8);
    let mut p = [0u8; 8];
    p[..copy].copy_from_slice(&slice[..copy]);
    u64::from_be_bytes(p)
}

/// Return next number in range 0..max_excluded, or None if at the end of the range
pub fn next_bounded(n: usize, max_excluded: usize, reverse: bool) -> Option<usize> {
    if reverse {
        n.checked_sub(1)
    } else if n >= max_excluded - 1 {
        None
    } else {
        Some(n + 1)
    }
}

#[test]
fn test_downscale_u32() {
    assert_eq!(0, downscale_u32(0, 1));
    assert_eq!(0, downscale_u32(1, 1));
    assert_eq!(0, downscale_u32(u32::MAX, 1));

    assert_eq!(0, downscale_u32(0, 16));
    assert_eq!(0, downscale_u32(1, 16));
    assert_eq!(7, downscale_u32(u32::MAX / 2 - 1, 16));
    assert_eq!(7, downscale_u32(u32::MAX / 2, 16));
    assert_eq!(8, downscale_u32(u32::MAX / 2 + 1, 16));
    assert_eq!(15, downscale_u32(u32::MAX - 1, 16));
    assert_eq!(15, downscale_u32(u32::MAX, 16));

    assert_eq!(0, downscale_u32(0, 15));
    assert_eq!(0, downscale_u32(1, 15));
    assert_eq!(7, downscale_u32(u32::MAX / 2 - 1, 15));
    assert_eq!(7, downscale_u32(u32::MAX / 2, 15));
    assert_eq!(7, downscale_u32(u32::MAX / 2 + 1, 15));
    assert_eq!(14, downscale_u32(u32::MAX - 1, 15));
    assert_eq!(14, downscale_u32(u32::MAX, 15));
}

#[test]
fn test_starting_u32() {
    assert_eq!(0, starting_u32(&[]));
    assert_eq!(0x15000000, starting_u32(&[0x15]));
    assert_eq!(0x1500, starting_u32(&[0, 0, 0x15]));
    assert_eq!(0x15, starting_u32(&[0, 0, 0, 0x15]));
    assert_eq!(0x01030507, starting_u32(&[0x01, 0x03, 0x05, 0x07]));
}

#[test]
fn test_ending_u32() {
    assert_eq!(0, ending_u32(&[]));
    assert_eq!(0x15, ending_u32(&[0x15]));
    assert_eq!(0x1500, ending_u32(&[0x15, 0]));
    assert_eq!(0x150000, ending_u32(&[0x15, 0, 0]));
    assert_eq!(0x15000000, ending_u32(&[0x15, 0, 0, 0]));
    assert_eq!(0x01030507, ending_u32(&[0x01, 0x03, 0x05, 0x07]));
    assert_eq!(0x03050709, ending_u32(&[0x01, 0x03, 0x05, 0x07, 0x09]));
}
