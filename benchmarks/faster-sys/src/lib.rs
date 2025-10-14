//! Low-level FFI bindings for FASTER and F2 key-value stores

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(improper_ctypes)]
#![allow(dead_code)]

// Include the generated bindings
// These are either real bindings (when feature enabled on Linux) or stubs
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

// Fixed-size API bindings (only when FASTER is enabled)
#[cfg(all(feature = "enable-faster", target_os = "linux"))]
pub mod fixed {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(improper_ctypes)]
    #![allow(dead_code)]

    include!(concat!(env!("OUT_DIR"), "/bindings_fixed.rs"));
}

// Export a flag to check if FASTER is available
#[cfg(all(feature = "enable-faster", target_os = "linux"))]
pub const FASTER_AVAILABLE: bool = true;

#[cfg(not(all(feature = "enable-faster", target_os = "linux")))]
pub const FASTER_AVAILABLE: bool = false;
