//! Low-level FFI bindings for FASTER and F2 key-value stores

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(improper_ctypes)]
#![allow(dead_code)]

// Include the generated bindings
// These include all type definitions and function declarations
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));