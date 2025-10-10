use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    // Check if we're on a supported platform
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    if target_os != "linux" {
        println!("cargo:warning=FASTER-sys only supports Linux platforms");
        // Create stub implementations for non-Linux platforms
        generate_stub_bindings();
        return;
    }

    // Clone or update FASTER repository
    let faster_dir = clone_or_update_faster();

    // Build FASTER C++ library
    build_faster(&faster_dir);

    // Generate Rust bindings
    generate_bindings(&faster_dir);

    // Link the built libraries
    println!("cargo:rustc-link-search=native={}/cc/build", faster_dir.display());
    println!("cargo:rustc-link-lib=static=faster");

    // Link system libraries
    println!("cargo:rustc-link-lib=stdc++");
    println!("cargo:rustc-link-lib=aio");
    println!("cargo:rustc-link-lib=uuid");
    println!("cargo:rustc-link-lib=tbb");
    println!("cargo:rustc-link-lib=pthread");
}

fn clone_or_update_faster() -> PathBuf {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let faster_dir = out_dir.join("FASTER");

    if !faster_dir.exists() {
        println!("cargo:warning=Cloning FASTER repository...");
        let status = Command::new("git")
            .args(&["clone", "--recursive", "https://github.com/microsoft/FASTER.git"])
            .current_dir(&out_dir)
            .status()
            .expect("Failed to clone FASTER repository");

        if !status.success() {
            panic!("Failed to clone FASTER repository");
        }
    } else {
        println!("cargo:warning=Updating FASTER repository...");
        let status = Command::new("git")
            .args(&["pull", "--recurse-submodules"])
            .current_dir(&faster_dir)
            .status()
            .expect("Failed to update FASTER repository");

        if !status.success() {
            println!("cargo:warning=Failed to update FASTER repository, using existing version");
        }
    }

    faster_dir
}

fn build_faster(faster_dir: &PathBuf) {
    let cc_dir = faster_dir.join("cc");
    let build_dir = cc_dir.join("build");

    // Create build directory
    std::fs::create_dir_all(&build_dir).unwrap();

    // Build our custom wrapper first
    println!("cargo:warning=Building FASTER C++ wrapper...");
    cc::Build::new()
        .cpp(true)
        .std("c++17")
        .file("cpp/wrapper.cpp")
        .include(&cc_dir.join("src"))
        .include(&cc_dir.join("src/core"))
        .flag("-O3")
        .flag("-DNDEBUG")
        .compile("faster_wrapper");

    // Run cmake to build FASTER
    println!("cargo:warning=Building FASTER with cmake...");
    let cmake_status = Command::new("cmake")
        .args(&["-DCMAKE_BUILD_TYPE=Release", ".."])
        .current_dir(&build_dir)
        .status()
        .expect("Failed to run cmake");

    if !cmake_status.success() {
        panic!("Failed to configure FASTER with cmake");
    }

    let make_status = Command::new("make")
        .arg("-j4")
        .current_dir(&build_dir)
        .status()
        .expect("Failed to build FASTER");

    if !make_status.success() {
        panic!("Failed to build FASTER");
    }
}

fn generate_bindings(faster_dir: &PathBuf) {
    let cc_dir = faster_dir.join("cc");

    println!("cargo:warning=Generating Rust bindings...");

    let bindings = bindgen::Builder::default()
        .header("cpp/wrapper.h")
        .clang_arg(format!("-I{}", cc_dir.join("src").display()))
        .clang_arg(format!("-I{}", cc_dir.join("src/core").display()))
        .clang_arg("-std=c++17")
        .clang_arg("-x")
        .clang_arg("c++")
        // Only expose our wrapper functions and types
        .allowlist_function("faster_.*")
        .allowlist_function("f2_.*")
        .allowlist_type("FasterKv")
        .allowlist_type("F2Kv")
        .allowlist_type("FasterStatus")
        .allowlist_type("FasterConfig")
        .allowlist_type("F2Config")
        // Ensure enums are properly generated
        .rustified_enum("FasterStatus")
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings");
}

fn generate_stub_bindings() {
    // Generate stub bindings for non-Linux platforms
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let stub_content = r#"
        // Stub bindings for non-Linux platforms
        pub struct FasterKv;
        pub struct F2Kv;
    "#;

    std::fs::write(out_dir.join("bindings.rs"), stub_content)
        .expect("Failed to write stub bindings");
}