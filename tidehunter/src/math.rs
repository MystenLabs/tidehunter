use std::cmp;

/// Scale down u32 value from the range 0..=u32::MAX to the given range 0..upper_bound_exclusive
pub fn downscale_u32(v: u32, upper_bound_exclusive: u32) -> u32 {
    rescale_u32(v, u32::MAX as u64 + 1, upper_bound_exclusive)
}

/// Scale down u32 value from the range 0..=from_upper_bound_exclusive to the given range 0..upper_bound_exclusive
/// from_upper_bound_exclusive is specified as u64 to allow for value u32::MAX+1 as exclusive upper range
pub fn rescale_u32(v: u32, from_upper_bound_exclusive: u64, to_upper_bound_exclusive: u32) -> u32 {
    assert!((v as u64) < from_upper_bound_exclusive);
    assert!(to_upper_bound_exclusive > 0);
    let v = v as u64;
    // this does not overflow: v <= u32::MAX, upper_bound_exclusive <= u32::MAX
    // therefore, prefix * num_buckets < u64::MAX,
    let bucket = v * (to_upper_bound_exclusive as u64) / from_upper_bound_exclusive;
    debug_assert!(bucket < to_upper_bound_exclusive as u64);
    bucket.try_into().unwrap()
}

/// Extract ending u32 value from last bytes of a slice.
/// If slice is less than four bytes long, assumes slice is prefixed with zeroes.
/// See test_ending_u32 for examples.
pub fn ending_u32(slice: &[u8]) -> u32 {
    let copy = cmp::min(slice.len(), 4);
    let mut p = [0u8; 4];
    p[..copy].copy_from_slice(&slice[..copy]);
    u32::from_be_bytes(p)
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
fn test_ending_u32() {
    assert_eq!(0, ending_u32(&[]));
    // assert_eq!(0, ending_u32(&[0]));
    // assert_eq!(u32::MAX, ending_u32(&[u8::MAX]));
    // todo - need to re-assess if this is the desired behaviour for small keys
    assert_eq!(0x15000000, ending_u32(&[0x15]));
    assert_eq!(0x1500, ending_u32(&[0, 0, 0x15]));
    assert_eq!(0x15, ending_u32(&[0, 0, 0, 0x15]));
    assert_eq!(0x01030507, ending_u32(&[0x01, 0x03, 0x05, 0x07]));
}
