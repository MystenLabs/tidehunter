use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    // Only build if enable-faster feature is enabled
    if env::var("CARGO_FEATURE_ENABLE_FASTER").is_err() {
        println!("cargo:warning=FASTER feature not enabled, skipping build");
        generate_stub_bindings();
        return;
    }

    // Check if we're on a supported platform
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    if target_os != "linux" {
        println!("cargo:warning=FASTER only supported on Linux, generating stubs");
        generate_stub_bindings();
        return;
    }

    // Verify FASTER submodule is initialized
    let faster_dir = PathBuf::from("FASTER");
    if !faster_dir.join("cc").exists() {
        panic!(
            "FASTER submodule not initialized. Run:\n\
             cd benchmarks/faster-sys && git submodule update --init --recursive"
        );
    }

    // Build FASTER C++ library
    build_faster(&faster_dir);

    // Build our C wrapper
    build_c_wrapper(&faster_dir);

    // Generate Rust bindings
    generate_bindings();

    // Link libraries
    link_libraries();
}

fn build_faster(faster_dir: &Path) {
    let cc_dir = faster_dir.join("cc");
    let build_dir = cc_dir.join("build");

    // Create build directory
    std::fs::create_dir_all(&build_dir).unwrap();

    println!("cargo:warning=Building FASTER with cmake...");

    // Configure with cmake
    let cmake_status = Command::new("cmake")
        .args(["-DCMAKE_BUILD_TYPE=Release", "-DBUILD_TESTING=OFF", ".."])
        .current_dir(&build_dir)
        .status()
        .expect("Failed to run cmake");

    if !cmake_status.success() {
        panic!("Failed to configure FASTER with cmake");
    }

    // Build with make
    let make_status = Command::new("make")
        .args(["-j", "4", "faster"])
        .current_dir(&build_dir)
        .status()
        .expect("Failed to build FASTER");

    if !make_status.success() {
        panic!("Failed to build FASTER");
    }

    // Tell cargo where to find the built library
    println!("cargo:rustc-link-search=native={}", build_dir.display());
}

fn build_c_wrapper(faster_dir: &Path) {
    println!("cargo:warning=Building FASTER C++ wrapper...");

    let cc_dir = faster_dir.join("cc");

    // Build the original variable-length wrapper
    cc::Build::new()
        .cpp(true)
        .std("c++17")
        .file("cpp/faster-c.cc")
        .include(cc_dir.join("src"))
        .include(cc_dir.join("src/core"))
        .include(cc_dir.join("src/device"))
        // Compiler flags
        .flag("-O3")
        .flag("-DNDEBUG")
        .flag("-march=native")
        // Warning flags
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-unused-variable")
        .flag_if_supported("-Wno-missing-field-initializers")
        // Compile the wrapper
        .compile("faster_c_wrapper");

    // TODO: Fixed-size wrapper not yet complete, skip for now
    // Build the new fixed-size wrapper
    // println!("cargo:warning=Building FASTER fixed-size wrapper...");
    // cc::Build::new()
    //     .cpp(true)
    //     .std("c++17")
    //     .file("cpp/faster-fixed.cc")
    //     .include(cc_dir.join("src"))
    //     .include(cc_dir.join("src/core"))
    //     .include(cc_dir.join("src/device"))
    //     .include(cc_dir.join("src/environment"))
    //     .include("cpp") // For faster-fixed.h
    //     // Compiler flags
    //     .flag("-O3")
    //     .flag("-DNDEBUG")
    //     .flag("-march=native")
    //     // Warning flags
    //     .flag_if_supported("-Wno-unused-parameter")
    //     .flag_if_supported("-Wno-unused-variable")
    //     .flag_if_supported("-Wno-missing-field-initializers")
    //     // Compile the wrapper
    //     .compile("faster_fixed_wrapper");
}

fn generate_bindings() {
    println!("cargo:warning=Generating Rust bindings...");

    let faster_dir = PathBuf::from("FASTER");
    let cc_dir = faster_dir.join("cc");
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    // Generate bindings for variable-length API
    let bindings = bindgen::Builder::default()
        .header("cpp/faster-c.h")
        // Add include paths
        .clang_arg(format!("-I{}", cc_dir.join("src").display()))
        .clang_arg(format!("-I{}", cc_dir.join("src/core").display()))
        // C++ settings
        .clang_arg("-xc++")
        .clang_arg("-std=c++17")
        // Only expose our C interface
        .allowlist_function("faster_.*")
        .allowlist_function("f2_.*")
        .allowlist_type("faster_.*")
        .allowlist_type("f2_.*")
        // Rustify enums
        .rustified_enum("faster_status")
        // Generate bindings
        .generate()
        .expect("Unable to generate bindings");

    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings");

    // Generate bindings for fixed-size API
    println!("cargo:warning=Generating fixed-size bindings...");
    let fixed_bindings = bindgen::Builder::default()
        .header("cpp/faster-fixed-c.h")
        // Only expose fixed-size interface
        .allowlist_function("faster_fixed_.*")
        .allowlist_function("f2_fixed_.*")
        .allowlist_type("faster_status_t")
        // Generate bindings
        .generate()
        .expect("Unable to generate fixed-size bindings");

    fixed_bindings
        .write_to_file(out_path.join("bindings_fixed.rs"))
        .expect("Couldn't write fixed-size bindings");
}

fn link_libraries() {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();

    // Link the FASTER static library built by cmake
    let faster_dir = PathBuf::from("FASTER");
    let build_dir = faster_dir.join("cc/build");
    let absolute_build_dir =
        std::fs::canonicalize(&build_dir).unwrap_or_else(|_| build_dir.clone());
    println!(
        "cargo:rustc-link-search=native={}",
        absolute_build_dir.display()
    );
    println!("cargo:rustc-link-lib=static=faster");

    // Link C++ standard library
    if target_os == "linux" {
        println!("cargo:rustc-link-lib=stdc++");
    } else if target_os == "macos" {
        println!("cargo:rustc-link-lib=c++");
    }

    // Link system libraries
    println!("cargo:rustc-link-lib=pthread");

    // Linux-specific libraries
    if target_os == "linux" {
        println!("cargo:rustc-link-lib=aio");
        println!("cargo:rustc-link-lib=uuid");
        println!("cargo:rustc-link-lib=tbb");
    }

    // macOS-specific libraries
    if target_os == "macos" {
        // macOS uses different async I/O
        println!("cargo:rustc-link-lib=framework=Foundation");
    }
}

fn generate_stub_bindings() {
    // Generate stub bindings for non-Linux/macOS platforms
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let stub_content = r#"
        // Stub bindings for unsupported platforms

        #[repr(C)]
        pub struct faster_t;

        #[repr(C)]
        pub struct f2_t;

        #[repr(C)]
        #[derive(Debug, Copy, Clone, PartialEq, Eq)]
        pub enum faster_status {
            FASTER_SUCCESS = 0,
            FASTER_PENDING = 1,
            FASTER_NOT_FOUND = 2,
            FASTER_OUT_OF_MEMORY = 3,
            FASTER_IO_ERROR = 4,
            FASTER_CORRUPTED = 5,
            FASTER_ABORTED = 6,
            FASTER_ERROR = 7,
        }

        #[repr(C)]
        pub struct faster_config_t {
            pub storage_path: *const std::os::raw::c_char,
            pub initial_log_size: u64,
            pub max_log_size: u64,
            pub page_size: u64,
            pub segment_size: u64,
            pub hash_table_size: u64,
            pub enable_read_cache: bool,
            pub read_cache_size: u64,
            pub log_mutable_fraction: f64,
        }

        #[repr(C)]
        pub struct f2_config_t {
            pub storage_path: *const std::os::raw::c_char,
            pub hot_store_size: u64,
            pub cold_store_size: u64,
            pub index_size: u64,
            pub read_cache_size: u64,
            pub hot_threshold: f64,
            pub cold_threshold: f64,
            pub segment_size: u64,
        }
    "#;

    std::fs::write(out_dir.join("bindings.rs"), stub_content)
        .expect("Failed to write stub bindings");
}
