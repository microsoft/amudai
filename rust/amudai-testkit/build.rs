use std::env;
use std::path::PathBuf;

fn main() {
    let src_dir = env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .expect("Failed to determine workspace root path during build.");

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=CARGO_MANIFEST_DIR");

    println!("cargo:rustc-env=TESTKIT_SRC_DIR={}", src_dir.display());
}
