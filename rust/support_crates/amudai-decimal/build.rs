extern crate cc;

fn main() {
    let litend = if cfg!(target_endian = "little") {
        "1"
    } else {
        "0"
    };
    cc::Build::new()
        .include("decNumber")
        .file("decNumber/decContext.c")
        .file("decNumber/decDouble.c")
        .file("decNumber/decNumber.c")
        .file("decNumber/decPacked.c")
        .file("decNumber/decQuad.c")
        .file("decNumber/decSingle.c")
        .file("decNumber/decimal128.c")
        .file("decNumber/decimal64.c") // for conversion tables
        .flag_if_supported("-Wno-unused-value")
        .define("DECLITEND", Some(litend))
        .compile("libdecNumber.a");
}
